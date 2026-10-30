// PodProvider runs the pods bound to this shard's Nodes through the plugin, holds their DaemonSet pods Pending, deletes
// pods that left a Node, and reports status from one batched /status call per interval; it is the single
// virtual-kubelet pod provider.

package main

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
)

// PodProvider implements the virtual kubelet's PodLifecycleHandler and PodNotifier for every served Node.
type PodProvider struct {
	plugin         *PodAPI
	kube           kubernetes.Interface
	nodes          *NodeManager
	statusInterval time.Duration

	mu     sync.Mutex
	pods   map[string]*corev1.Pod // namespace/name -> pod with the latest reported status
	notify func(*corev1.Pod)
	// bound is the pod informer's cache, which tells which pods are bound to a Node once boundSynced is true.
	bound       corev1listers.PodLister
	boundSynced func() bool
}

// NewPodProvider returns a provider that polls pod status every statusInterval.
func NewPodProvider(plugin *PodAPI, kube kubernetes.Interface, nodes *NodeManager, statusInterval time.Duration) *PodProvider {
	if statusInterval <= 0 {
		statusInterval = 5 * time.Second
	}
	return &PodProvider{plugin: plugin, kube: kube, nodes: nodes, statusInterval: statusInterval, pods: make(map[string]*corev1.Pod)}
}

func podKey(namespace, name string) string { return namespace + "/" + name }

// ReasonDaemonSetPod is the status reason of a DaemonSet pod on a served Node, which the agent never runs.
const ReasonDaemonSetPod = "CloudVMDaemonSetPod"

// isDaemonSetPod reports whether a DaemonSet owns the pod.
func isDaemonSetPod(pod *corev1.Pod) bool {
	for _, ref := range pod.OwnerReferences {
		if nodeclaim.IsDaemonSetOwner(ref.APIVersion, ref.Kind) {
			return true
		}
	}
	return false
}

// SetPodCache gives the provider the pod informer's cache and its sync check; the provider deletes no departed pod
// before the cache has synced.
func (p *PodProvider) SetPodCache(lister corev1listers.PodLister, synced func() bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bound, p.boundSynced = lister, synced
}

// CreatePod starts the pod on its Node's VM, or holds a DaemonSet pod Pending without sending it to the plugin. When
// the VM refuses the pod, the pods that left the Node are deleted from the VM and the create is tried once more, so
// a VM that still counts a pod deleted while the agent was down takes the pod the scheduler fitted in its place.
func (p *PodProvider) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	if !p.nodes.Serves(pod.Spec.NodeName) {
		return fmt.Errorf("node %s is not served by this agent", pod.Spec.NodeName)
	}
	if isDaemonSetPod(pod) {
		p.holdDaemonSetPod(pod)
		return nil
	}
	secrets := imagePullSecrets(ctx, p.kube, pod)
	err := p.plugin.Create(ctx, pod, secrets)
	if isRefusal(err) {
		if claim, getErr := p.nodes.cfg.Claims.Get(ctx, pod.Spec.NodeName); getErr == nil && p.DeleteDepartedPods(ctx, claim) > 0 {
			err = p.plugin.Create(ctx, pod, secrets)
		}
	}
	if err != nil {
		return err
	}
	tracked := pod.DeepCopy()
	now := metav1.Now()
	tracked.Status = corev1.PodStatus{
		Phase:     corev1.PodPending,
		HostIP:    p.nodes.cfg.NodeIP,
		StartTime: &now,
		Conditions: []corev1.PodCondition{
			{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: now},
		},
	}
	p.mu.Lock()
	p.pods[podKey(pod.Namespace, pod.Name)] = tracked
	p.mu.Unlock()
	return nil
}

// holdDaemonSetPod reports a DaemonSet pod Pending, once, with a reason that says it never runs. The DaemonSet
// controller deletes and recreates its pods only in phase Failed or Succeeded, so a Pending pod stays put.
func (p *PodProvider) holdDaemonSetPod(pod *corev1.Pod) {
	tracked := pod.DeepCopy()
	now := metav1.Now()
	message := "CloudVM nodes do not run DaemonSet pods; the VM runs only the workload it was launched for"
	tracked.Status = corev1.PodStatus{
		Phase:   corev1.PodPending,
		Reason:  ReasonDaemonSetPod,
		Message: message,
		Conditions: []corev1.PodCondition{
			{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: now},
			{Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: ReasonDaemonSetPod, Message: message, LastTransitionTime: now},
		},
	}
	p.mu.Lock()
	p.pods[podKey(pod.Namespace, pod.Name)] = tracked
	notify := p.notify
	p.mu.Unlock()
	if notify != nil {
		notify(tracked.DeepCopy())
	}
}

// UpdatePod records the new spec; the plugin has no update call, and a running container keeps its spec.
func (p *PodProvider) UpdatePod(_ context.Context, pod *corev1.Pod) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if old, ok := p.pods[podKey(pod.Namespace, pod.Name)]; ok {
		updated := pod.DeepCopy()
		updated.Status = old.Status
		p.pods[podKey(pod.Namespace, pod.Name)] = updated
	}
	return nil
}

// DeletePod stops the pod on its VM and forgets it; a DaemonSet pod never reached the VM.
func (p *PodProvider) DeletePod(ctx context.Context, pod *corev1.Pod) error {
	if !isDaemonSetPod(pod) {
		if err := p.plugin.Delete(ctx, pod); err != nil {
			return err
		}
	}
	p.mu.Lock()
	tracked, ok := p.pods[podKey(pod.Namespace, pod.Name)]
	delete(p.pods, podKey(pod.Namespace, pod.Name))
	notify := p.notify
	p.mu.Unlock()
	if !ok {
		return nil
	}
	terminated := tracked.DeepCopy()
	markTerminated(&terminated.Status, "Deleted", "the pod was deleted")
	if notify != nil {
		notify(terminated)
	}
	return nil
}

// DeleteDepartedPods calls the plugin's /delete for every pod the claim lists on its VM that is no longer bound to the
// claim's Node, such as one deleted while the agent was down, and returns how many it deleted. It deletes nothing
// before the pod informer's cache has synced.
func (p *PodProvider) DeleteDepartedPods(ctx context.Context, claim *nodeclaim.NodeClaim) int {
	p.mu.Lock()
	bound, synced := p.bound, p.boundSynced
	p.mu.Unlock()
	if bound == nil || !synced() {
		return 0
	}
	deleted := 0
	for _, ref := range claim.Pods {
		if pod, err := bound.Pods(ref.Namespace).Get(ref.Name); err == nil && string(pod.UID) == ref.UID && pod.Spec.NodeName == claim.Name {
			continue
		}
		departed := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, Name: ref.Name, UID: types.UID(ref.UID)}}
		if err := p.plugin.Delete(ctx, departed); err != nil {
			logf("deleting pod %s/%s (uid %s), which left node %s: %v", ref.Namespace, ref.Name, ref.UID, claim.Name, err)
			continue
		}
		p.mu.Lock()
		if tracked, ok := p.pods[podKey(ref.Namespace, ref.Name)]; ok && string(tracked.UID) == ref.UID {
			delete(p.pods, podKey(ref.Namespace, ref.Name))
		}
		p.mu.Unlock()
		logf("deleted pod %s/%s (uid %s) from the VM of node %s: it is no longer bound to the node", ref.Namespace, ref.Name, ref.UID, claim.Name)
		deleted++
	}
	return deleted
}

// GetPod returns the tracked pod.
func (p *PodProvider) GetPod(_ context.Context, namespace, name string) (*corev1.Pod, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pod, ok := p.pods[podKey(namespace, name)]
	if !ok {
		return nil, errdefs.NotFoundf("pod %s/%s is not known", namespace, name)
	}
	return pod.DeepCopy(), nil
}

// GetPodStatus returns the last status the plugin reported for the pod.
func (p *PodProvider) GetPodStatus(ctx context.Context, namespace, name string) (*corev1.PodStatus, error) {
	pod, err := p.GetPod(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	return &pod.Status, nil
}

// GetPods returns every tracked pod.
func (p *PodProvider) GetPods(context.Context) ([]*corev1.Pod, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*corev1.Pod, 0, len(p.pods))
	for _, pod := range p.pods {
		out = append(out, pod.DeepCopy())
	}
	return out, nil
}

// NotifyPods registers the status callback and starts the status loop.
func (p *PodProvider) NotifyPods(ctx context.Context, cb func(*corev1.Pod)) {
	p.mu.Lock()
	p.notify = cb
	p.mu.Unlock()
	go p.statusLoop(ctx)
}

func (p *PodProvider) statusLoop(ctx context.Context) {
	ticker := time.NewTicker(p.statusInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.PollStatus(ctx)
		}
	}
}

// PollStatus asks the plugin for the status of every tracked pod in one call and reports the pods that changed.
func (p *PodProvider) PollStatus(ctx context.Context) {
	p.mu.Lock()
	refs := make([]*corev1.Pod, 0, len(p.pods))
	for _, pod := range p.pods {
		if !isDaemonSetPod(pod) {
			refs = append(refs, statusRef(pod))
		}
	}
	p.mu.Unlock()
	statuses, err := p.plugin.Status(ctx, refs)
	if err != nil {
		logf("polling pod status: %v", err)
		return
	}
	var changed []*corev1.Pod
	p.mu.Lock()
	for i := range statuses {
		st := &statuses[i]
		pod, ok := p.pods[podKey(st.Namespace, st.Name)]
		if !ok || (st.UID != "" && st.UID != string(pod.UID)) || len(st.Containers) == 0 {
			continue
		}
		next := applyStatus(pod, st)
		if equality.Semantic.DeepEqual(next, pod.Status) {
			continue
		}
		pod.Status = next
		changed = append(changed, pod.DeepCopy())
	}
	notify := p.notify
	p.mu.Unlock()
	if notify == nil {
		return
	}
	for _, pod := range changed {
		notify(pod)
	}
}

// statusRef is the part of a pod the plugin's /status reads: identity, containers and service account.
func statusRef(pod *corev1.Pod) *corev1.Pod {
	ref := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace, UID: pod.UID}}
	ref.Spec.ServiceAccountName = pod.Spec.ServiceAccountName
	for _, c := range pod.Spec.Containers {
		ref.Spec.Containers = append(ref.Spec.Containers, corev1.Container{Name: c.Name, Image: c.Image, Env: c.Env})
	}
	for _, c := range pod.Spec.InitContainers {
		ref.Spec.InitContainers = append(ref.Spec.InitContainers, corev1.Container{Name: c.Name, Image: c.Image})
	}
	return ref
}

// applyStatus derives the pod status from the plugin's container states, as the kubelet would.
func applyStatus(pod *corev1.Pod, st *PodStatus) corev1.PodStatus {
	out := *pod.Status.DeepCopy()
	if st.PodIP != "" {
		out.PodIP = st.PodIP
		out.PodIPs = []corev1.PodIP{{IP: st.PodIP}}
	}
	out.ContainerStatuses = convertContainers(st.Containers, out.ContainerStatuses)
	out.InitContainerStatuses = convertContainers(st.InitContainers, out.InitContainerStatuses)

	running, succeeded, failed := 0, 0, 0
	allReady := len(out.ContainerStatuses) > 0
	for _, c := range out.ContainerStatuses {
		switch {
		case c.State.Running != nil:
			running++
		case c.State.Terminated != nil && c.State.Terminated.ExitCode == 0:
			succeeded++
		case c.State.Terminated != nil:
			failed++
		}
		allReady = allReady && c.Ready
	}
	total := len(out.ContainerStatuses)
	switch {
	case total > 0 && succeeded == total:
		out.Phase = corev1.PodSucceeded
	case total > 0 && failed > 0 && running == 0 && succeeded+failed == total:
		out.Phase = corev1.PodFailed
	case running > 0:
		out.Phase = corev1.PodRunning
	default:
		out.Phase = corev1.PodPending
	}
	ready := corev1.ConditionFalse
	if allReady && out.Phase == corev1.PodRunning {
		ready = corev1.ConditionTrue
	}
	setCondition(&out, corev1.PodInitialized, corev1.ConditionTrue)
	setCondition(&out, corev1.ContainersReady, ready)
	setCondition(&out, corev1.PodReady, ready)
	return out
}

func setCondition(st *corev1.PodStatus, t corev1.PodConditionType, s corev1.ConditionStatus) {
	for i := range st.Conditions {
		if st.Conditions[i].Type == t {
			if st.Conditions[i].Status != s {
				st.Conditions[i].Status = s
				st.Conditions[i].LastTransitionTime = metav1.Now()
			}
			return
		}
	}
	st.Conditions = append(st.Conditions, corev1.PodCondition{Type: t, Status: s, LastTransitionTime: metav1.Now()})
}

// convertContainers maps plugin container states to Kubernetes ones, keeping restart counts already reported.
func convertContainers(in []ContainerStatus, prev []corev1.ContainerStatus) []corev1.ContainerStatus {
	if len(in) == 0 {
		return prev
	}
	out := make([]corev1.ContainerStatus, 0, len(in))
	for _, c := range in {
		cs := corev1.ContainerStatus{Name: c.Name, Image: c.Image, Ready: c.Ready}
		switch s := c.State; {
		case s.Terminated != nil:
			cs.State.Terminated = &corev1.ContainerStateTerminated{
				ExitCode: s.Terminated.ExitCode, Reason: s.Terminated.Reason, Message: s.Terminated.Message,
				FinishedAt: metav1.NewTime(s.Terminated.FinishedAt), ContainerID: s.Terminated.ContainerID,
			}
			cs.ContainerID = s.Terminated.ContainerID
		case s.Running != nil:
			cs.State.Running = &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(s.Running.StartedAt)}
			started := true
			cs.Started = &started
		case s.Waiting != nil:
			cs.State.Waiting = &corev1.ContainerStateWaiting{Reason: s.Waiting.Reason, Message: s.Waiting.Message}
		default:
			cs.State.Waiting = &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}
		}
		for _, p := range prev {
			if p.Name == c.Name {
				cs.RestartCount = p.RestartCount
			}
		}
		out = append(out, cs)
	}
	return out
}

// markTerminated ends every container that has not ended yet.
func markTerminated(st *corev1.PodStatus, reason, message string) {
	now := metav1.Now()
	for i := range st.ContainerStatuses {
		c := &st.ContainerStatuses[i]
		if c.State.Terminated == nil {
			c.State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 137, Reason: reason, Message: message, FinishedAt: now,
			}}
			c.Ready = false
		}
	}
	setCondition(st, corev1.ContainersReady, corev1.ConditionFalse)
	setCondition(st, corev1.PodReady, corev1.ConditionFalse)
}

// ContainerLogs serves kubectl logs from the plugin's buffered container output.
func (p *PodProvider) ContainerLogs(ctx context.Context, namespace, name, container string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	pod, err := p.GetPod(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	return p.plugin.Logs(ctx, pod, container, logsOptions{
		Tail: opts.Tail, LimitBytes: opts.LimitBytes, Timestamps: opts.Timestamps, Follow: opts.Follow,
		Previous: opts.Previous, SinceSeconds: opts.SinceSeconds, SinceTime: opts.SinceTime,
	})
}
