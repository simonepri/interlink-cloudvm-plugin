// Tests that pod statuses carry the runtime's container IDs and init container states as a kubelet reports them, and
// that pod and host IP fieldRefs stay hidden from the pod controller and reach the plugin unresolved.

package main

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"

	corev1listers "k8s.io/client-go/listers/core/v1"
)

const testContainerID = "containerd://0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func condition(st corev1.PodStatus, t corev1.PodConditionType) corev1.ConditionStatus {
	for _, c := range st.Conditions {
		if c.Type == t {
			return c.Status
		}
	}
	return ""
}

func TestApplyStatusReportsContainerIDsAndInitContainers(t *testing.T) {
	pod := &corev1.Pod{}
	running := &PodStatus{
		Containers: []ContainerStatus{{Name: "main", Ready: true, ContainerID: testContainerID, State: ContainerState{Running: &RunningState{StartedAt: time.Unix(1, 0)}}}},
		InitContainers: []ContainerStatus{{Name: "setup", ContainerID: testContainerID, RestartCount: 2,
			State: ContainerState{Terminated: &TerminatedState{ExitCode: 0, Reason: "Completed", ContainerID: testContainerID}}}},
	}
	st := applyStatus(pod, running)
	if st.ContainerStatuses[0].ContainerID != testContainerID || st.InitContainerStatuses[0].ContainerID != testContainerID {
		t.Fatalf("container IDs = %q, %q", st.ContainerStatuses[0].ContainerID, st.InitContainerStatuses[0].ContainerID)
	}
	if st.InitContainerStatuses[0].RestartCount != 2 || condition(st, corev1.PodInitialized) != corev1.ConditionTrue || st.Phase != corev1.PodRunning {
		t.Fatalf("status = %+v", st)
	}

	initializing := &PodStatus{
		Containers:     []ContainerStatus{{Name: "main", State: ContainerState{Waiting: &WaitingState{Reason: "PodInitializing"}}}},
		InitContainers: []ContainerStatus{{Name: "setup", State: ContainerState{Running: &RunningState{StartedAt: time.Unix(1, 0)}}}},
	}
	st = applyStatus(&corev1.Pod{}, initializing)
	if condition(st, corev1.PodInitialized) != corev1.ConditionFalse || st.Phase != corev1.PodPending {
		t.Fatalf("a pod whose init container runs is Pending and not Initialized, got %+v", st)
	}
}

func TestPodControllerSeesNoPodIPFieldRefs(t *testing.T) {
	ipRef := &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.hostIP"}}
	plain := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "plain", Namespace: "team-a"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Env: []corev1.EnvVar{{Name: "A", Value: "b"}}}}}}
	withIP := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ip", Namespace: "team-a"},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: "init", Env: []corev1.EnvVar{{Name: "HOST_IP", ValueFrom: ipRef}}}},
			Containers:     []corev1.Container{{Name: "main", Env: []corev1.EnvVar{{Name: "A", Value: "b"}, {Name: "HOST_IP", ValueFrom: ipRef}}}},
		}}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, pod := range []*corev1.Pod{plain, withIP} {
		if err := indexer.Add(pod); err != nil {
			t.Fatal(err)
		}
	}
	lister := controllerPodLister{corev1listers.NewPodLister(indexer)}
	got, err := lister.Pods("team-a").Get("ip")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Spec.InitContainers[0].Env) != 0 || len(got.Spec.Containers[0].Env) != 1 || got.Spec.Containers[0].Env[0].Name != "A" {
		t.Fatalf("the pod controller sees %+v, want the host IP variables gone", got.Spec)
	}
	if len(withIP.Spec.Containers[0].Env) != 2 {
		t.Fatal("hiding the variables changed the informer's copy")
	}
	if same, _ := lister.Pods("team-a").Get("plain"); same != plain {
		t.Fatal("a pod without host IP variables is not copied")
	}
	if all, err := lister.List(labels.Everything()); err != nil || len(all) != 2 {
		t.Fatalf("list = %v, %v", all, err)
	}
}

func TestCreateKeepsPodIPFieldRefsForThePlugin(t *testing.T) {
	ipRef := &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}
	orig := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "team-a", UID: "u-1"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Env: []corev1.EnvVar{
			{Name: "POD_IP", ValueFrom: ipRef}, {Name: "NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		}}}},
	}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := indexer.Add(orig); err != nil {
		t.Fatal(err)
	}
	p := &PodProvider{}
	p.SetPodCache(corev1listers.NewPodLister(indexer), func() bool { return true })

	resolved := orig.DeepCopy()
	resolved.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "NAME", Value: "p"}}
	got := p.withHostIPFieldRefs(resolved).Spec.Containers[0].Env
	if len(got) != 2 || got[0].Value != "p" || got[0].ValueFrom != nil || got[1].ValueFrom == nil || got[1].ValueFrom.FieldRef.FieldPath != "status.podIP" {
		t.Fatalf("env = %+v, want NAME resolved and POD_IP left to the plugin", got)
	}
}
