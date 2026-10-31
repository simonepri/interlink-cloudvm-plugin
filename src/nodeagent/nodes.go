// NodeManager turns the plugin's NodeClaim VMs owned by this shard into Kubernetes Nodes: one virtual-kubelet
// NodeController per claim keeps the Node's lease and status fresh, and Nodes whose claim is gone are deleted.

package main

import (
	"context"
	"maps"
	"sync"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
	"github.com/virtual-kubelet/virtual-kubelet/node"
	"go.opentelemetry.io/otel/trace"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Node labels the agent sets besides the claim's own labels.
const (
	labelNodeType     = "type"
	nodeTypeValue     = "virtual-kubelet"
	labelInstanceType = "node.kubernetes.io/instance-type"
	labelRegion       = "topology.kubernetes.io/region"
	labelZone         = "topology.kubernetes.io/zone"
	labelCapacityType = "karpenter.sh/capacity-type"
	labelOS           = "kubernetes.io/os"
	labelArch         = "kubernetes.io/arch"
	labelHostname     = "kubernetes.io/hostname"
)

// reasonVMLaunching is the Ready=False reason of a Node whose VM has not connected its tunnel yet.
const reasonVMLaunching = "VMLaunching"

// reasonVMUnhealthy is the Ready=False reason of a Node whose VM cannot take pods safely.
const reasonVMUnhealthy = "VMUnhealthy"

// defaultPods is the pod capacity of a Node whose claim does not state one, the kubelet default.
const defaultPods = "110"

// NodeConfig configures a NodeManager.
type NodeConfig struct {
	Shard  Shard
	Claims nodeclaim.API
	Kube   kubernetes.Interface
	// NodeIP is the address every served Node advertises, the agent pod's IP.
	NodeIP string
	// KubeletPort is the port of the agent's kubelet API server.
	KubeletPort int32
	// KubeletVersion is reported in every Node's nodeInfo.
	KubeletVersion string
	// PollInterval is how often the claim list is read.
	PollInterval time.Duration
	// LeaseDurationSeconds and LeaseRenewInterval configure each Node's lease; the kubelet uses 40 s and 10 s.
	LeaseDurationSeconds int32
	LeaseRenewInterval   time.Duration
	// StatusInterval is how often each Node's status is written when nothing changed.
	StatusInterval time.Duration
	// OrphanSweepInterval is how often the agent lists its labeled Nodes to delete ones without a claim.
	OrphanSweepInterval time.Duration
	// MissingPolls is how many successful claim lists in a row must miss a claim before its Node is deleted.
	MissingPolls int
	// Tracer records the Node's stages in each claim's lifecycle trace; nil records nothing.
	Tracer trace.Tracer
}

// servedNode is one Node this process keeps alive.
type servedNode struct {
	claim nodeclaim.NodeClaim
	// reported is the claim state and capacity whose Node status the controller last received.
	reported         nodeclaim.State
	reportedCapacity map[string]string
	notify           func(*corev1.Node)
	notified         chan struct{}
	node             *corev1.Node
	cancel           context.CancelFunc
	done             chan struct{}
	// registeredAt is when this process started serving the Node.
	registeredAt time.Time
}

// NodeManager serves the Nodes of this shard's claims.
type NodeManager struct {
	cfg NodeConfig

	mu sync.Mutex
	// served holds the running controllers by node name.
	served map[string]*servedNode
	// retired holds names this process must not serve again: their claim is terminating or their Node was deleted.
	retired map[string]bool
	// missing counts successive successful lists that lacked a served claim.
	missing map[string]int
	// lastSweep is when orphan Nodes were last listed.
	lastSweep time.Time
	// synced is closed after the first successful claim list.
	synced     chan struct{}
	syncedOnce sync.Once
}

// NewNodeManager returns a manager for the configuration, filling unset intervals with kubelet-like defaults.
func NewNodeManager(cfg NodeConfig) *NodeManager {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 10 * time.Second
	}
	if cfg.LeaseDurationSeconds <= 0 {
		cfg.LeaseDurationSeconds = 40
	}
	if cfg.LeaseRenewInterval <= 0 {
		cfg.LeaseRenewInterval = 10 * time.Second
	}
	if cfg.StatusInterval <= 0 {
		cfg.StatusInterval = time.Minute
	}
	if cfg.OrphanSweepInterval <= 0 {
		cfg.OrphanSweepInterval = 5 * time.Minute
	}
	if cfg.MissingPolls <= 0 {
		cfg.MissingPolls = 2
	}
	if cfg.KubeletVersion == "" {
		cfg.KubeletVersion = "v1.36.0-node-agent"
	}
	return &NodeManager{
		cfg:     cfg,
		served:  make(map[string]*servedNode),
		retired: make(map[string]bool),
		missing: make(map[string]int),
		synced:  make(chan struct{}),
	}
}

// Synced is closed once the first claim list succeeded, so pod handling can tell served nodes from others.
func (m *NodeManager) Synced() <-chan struct{} { return m.synced }

// Serves reports whether this process runs pods for the node.
func (m *NodeManager) Serves(nodeName string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.served[nodeName]
	return ok
}

// ServedClaims returns the last listed claim of every Node this process keeps alive.
func (m *NodeManager) ServedClaims() []nodeclaim.NodeClaim {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]nodeclaim.NodeClaim, 0, len(m.served))
	for _, sn := range m.served {
		out = append(out, sn.claim)
	}
	return out
}

// ServedCount returns how many Nodes this process keeps alive.
func (m *NodeManager) ServedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.served)
}

// Run polls the claim list until ctx ends, then stops every controller.
func (m *NodeManager) Run(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.PollInterval)
	defer ticker.Stop()
	for {
		m.Sync(ctx)
		select {
		case <-ctx.Done():
			m.stopAll()
			return
		case <-ticker.C:
		}
	}
}

// Sync reads the claim list once and starts, updates or stops controllers to match it.
func (m *NodeManager) Sync(ctx context.Context) {
	claims, err := m.cfg.Claims.List(ctx)
	if err != nil {
		logf("listing node claims: %v; keeping every node as it is", err)
		return
	}
	listed := make(map[string]bool, len(claims))
	var toStart []nodeclaim.NodeClaim
	var toStop []*servedNode
	var toDelete []string

	m.mu.Lock()
	for i := range claims {
		c := claims[i]
		if !m.cfg.Shard.Owns(c.Name) {
			continue
		}
		listed[c.Name] = true
		delete(m.missing, c.Name)
		sn, serving := m.served[c.Name]
		switch {
		case c.State == nodeclaim.StateTerminating:
			m.retired[c.Name] = true
			if serving {
				delete(m.served, c.Name)
				toStop = append(toStop, sn)
			}
		case m.retired[c.Name]:
		case serving:
			sn.claim = c
			m.notifyLocked(sn)
		default:
			toStart = append(toStart, c)
		}
	}
	for name := range m.retired {
		if !listed[name] {
			if _, serving := m.served[name]; !serving {
				delete(m.retired, name)
			}
		}
	}
	for name, sn := range m.served {
		if listed[name] {
			continue
		}
		m.missing[name]++
		if m.missing[name] >= m.cfg.MissingPolls {
			delete(m.served, name)
			delete(m.missing, name)
			toStop = append(toStop, sn)
			toDelete = append(toDelete, name)
		}
	}
	sweep := time.Since(m.lastSweep) >= m.cfg.OrphanSweepInterval
	if sweep {
		m.lastSweep = time.Now()
	}
	m.mu.Unlock()
	m.syncedOnce.Do(func() { close(m.synced) })

	for _, sn := range toStop {
		sn.cancel()
	}
	for _, name := range toDelete {
		m.deleteNode(ctx, name)
	}
	for i := range toStart {
		m.start(ctx, toStart[i])
	}
	if sweep {
		m.sweepOrphans(ctx, listed)
	}
}

// notifyLocked hands the controller a new Node status when the claim state or capacity changed since the last one it
// received; before the controller registered its callback, the next poll retries.
func (m *NodeManager) notifyLocked(sn *servedNode) {
	if sn.claim.State == sn.reported && maps.Equal(sn.claim.Capacity, sn.reportedCapacity) {
		return
	}
	select {
	case <-sn.notified:
	default:
		return
	}
	if sn.reported != nodeclaim.StateRunning && sn.claim.State == nodeclaim.StateRunning {
		m.traceReady(&sn.claim, sn.registeredAt, time.Now())
	}
	sn.reported, sn.reportedCapacity = sn.claim.State, sn.claim.Capacity
	sn.node.Status = nodeStatus(&sn.claim, m.cfg)
	n := sn.node.DeepCopy()
	go sn.notify(n)
}

// start launches a controller for the claim unless its Node is being deleted.
func (m *NodeManager) start(ctx context.Context, c nodeclaim.NodeClaim) {
	existing, err := m.cfg.Kube.CoreV1().Nodes().Get(ctx, c.Name, metav1.GetOptions{})
	switch {
	case err == nil && existing.DeletionTimestamp != nil:
		logf("node %s is being deleted; not serving it", c.Name)
		m.mu.Lock()
		m.retired[c.Name] = true
		m.mu.Unlock()
		return
	case err != nil && !apierrors.IsNotFound(err):
		logf("reading node %s: %v; retrying on the next poll", c.Name, err)
		return
	}
	desired := buildNode(&c, m.cfg)
	sn := &servedNode{claim: c, reported: c.State, reportedCapacity: c.Capacity, node: desired, notified: make(chan struct{}), done: make(chan struct{}), registeredAt: time.Now()}
	provider := &nodeProvider{sn: sn}
	nodeCtx, cancel := context.WithCancel(ctx)
	sn.cancel = cancel
	ctrl, err := node.NewNodeController(provider, desired.DeepCopy(), m.cfg.Kube.CoreV1().Nodes(),
		node.WithNodeEnableLeaseV1WithRenewInterval(m.cfg.Kube.CoordinationV1().Leases(corev1.NamespaceNodeLease),
			m.cfg.LeaseDurationSeconds, m.cfg.LeaseRenewInterval),
		node.WithNodeStatusUpdateInterval(m.cfg.StatusInterval),
		node.WithNodePingInterval(m.cfg.LeaseRenewInterval),
		node.WithNodeStatusUpdateErrorHandler(func(_ context.Context, err error) error {
			if apierrors.IsNotFound(err) {
				logf("node %s was deleted; no longer serving it", c.Name)
				m.retire(c.Name, sn)
			}
			return err
		}),
	)
	if err != nil {
		cancel()
		logf("node %s: %v", c.Name, err)
		return
	}
	m.mu.Lock()
	if _, dup := m.served[c.Name]; dup || m.retired[c.Name] {
		m.mu.Unlock()
		cancel()
		return
	}
	m.served[c.Name] = sn
	m.mu.Unlock()
	m.traceRegistered(&c, sn.registeredAt)
	go func() {
		defer close(sn.done)
		if err := ctrl.Run(nodeCtx); err != nil && nodeCtx.Err() == nil {
			logf("node %s controller stopped: %v; restarting on the next poll", c.Name, err)
			m.mu.Lock()
			if m.served[c.Name] == sn {
				delete(m.served, c.Name)
			}
			m.mu.Unlock()
		}
		cancel()
	}()
}

// retire stops serving the node and keeps this process from serving it again while its claim is listed.
func (m *NodeManager) retire(name string, sn *servedNode) {
	m.mu.Lock()
	if m.served[name] == sn {
		delete(m.served, name)
	}
	m.retired[name] = true
	m.mu.Unlock()
	sn.cancel()
}

// deleteNode removes the Node of a claim the plugin no longer lists, as a cloud node lifecycle controller does.
func (m *NodeManager) deleteNode(ctx context.Context, name string) {
	n, err := m.cfg.Kube.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if err != nil || n.DeletionTimestamp != nil {
		return
	}
	precondition := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &n.UID}}
	if err := m.cfg.Kube.CoreV1().Nodes().Delete(ctx, name, precondition); err != nil && !apierrors.IsNotFound(err) {
		logf("deleting node %s whose claim is gone: %v", name, err)
		return
	}
	logf("deleted node %s: its claim is gone", name)
}

// sweepOrphans deletes labeled Nodes of this shard that no listed claim backs, such as ones whose claim vanished
// while the agent was down.
func (m *NodeManager) sweepOrphans(ctx context.Context, listed map[string]bool) {
	nodes, err := m.cfg.Kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: nodeclaim.LabelNodeAgent + "=true"})
	if err != nil {
		logf("listing agent nodes: %v", err)
		return
	}
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if !m.cfg.Shard.Owns(n.Name) || listed[n.Name] || n.DeletionTimestamp != nil {
			continue
		}
		m.mu.Lock()
		m.missing[n.Name]++
		due := m.missing[n.Name] >= m.cfg.MissingPolls
		if due {
			delete(m.missing, n.Name)
		}
		m.mu.Unlock()
		if due {
			m.deleteNode(ctx, n.Name)
		}
	}
}

func (m *NodeManager) stopAll() {
	m.mu.Lock()
	all := make([]*servedNode, 0, len(m.served))
	for _, sn := range m.served {
		all = append(all, sn)
	}
	m.served = make(map[string]*servedNode)
	m.mu.Unlock()
	for _, sn := range all {
		sn.cancel()
		<-sn.done
	}
}

// nodeProvider adapts a served node to the virtual kubelet's NodeProvider.
type nodeProvider struct{ sn *servedNode }

// Ping always succeeds: the claim list, not a per-node probe, tells whether the VM is up.
func (p *nodeProvider) Ping(ctx context.Context) error { return ctx.Err() }

func (p *nodeProvider) NotifyNodeStatus(_ context.Context, cb func(*corev1.Node)) {
	p.sn.notify = cb
	close(p.sn.notified)
}

// buildNode returns the Node the agent registers for the claim.
func buildNode(c *nodeclaim.NodeClaim, cfg NodeConfig) *corev1.Node {
	labels := maps.Clone(c.Labels)
	if labels == nil {
		labels = make(map[string]string)
	}
	setIfEmpty := func(k, v string) {
		if v != "" && labels[k] == "" {
			labels[k] = v
		}
	}
	labels[labelNodeType] = nodeTypeValue
	labels[nodeclaim.LabelNodeAgent] = "true"
	labels[labelHostname] = c.Name
	setIfEmpty(labelInstanceType, c.InstanceType)
	setIfEmpty(labelRegion, c.Region)
	setIfEmpty(labelZone, c.Zone)
	setIfEmpty(labelCapacityType, c.CapacityType)
	setIfEmpty(labelOS, "linux")
	setIfEmpty(labelArch, "amd64")

	taints := make([]corev1.Taint, 0, len(c.Taints)+1)
	unregistered := false
	for _, t := range c.Taints {
		taints = append(taints, corev1.Taint{Key: t.Key, Value: t.Value, Effect: corev1.TaintEffect(t.Effect)})
		unregistered = unregistered || t.Key == nodeclaim.TaintUnregistered
	}
	if !unregistered {
		taints = append(taints, corev1.Taint{Key: nodeclaim.TaintUnregistered, Effect: corev1.TaintEffectNoExecute})
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: c.Name, Labels: labels},
		Spec:       corev1.NodeSpec{ProviderID: c.ProviderID, Taints: taints},
		Status:     nodeStatus(c, cfg),
	}
}

// nodeStatus returns the status the agent reports for the claim's Node.
func nodeStatus(c *nodeclaim.NodeClaim, cfg NodeConfig) corev1.NodeStatus {
	capacity := corev1.ResourceList{}
	for name, q := range c.Capacity {
		parsed, err := resource.ParseQuantity(q)
		if err != nil {
			logf("node %s: ignoring capacity %s=%q: %v", c.Name, name, q, err)
			continue
		}
		capacity[corev1.ResourceName(name)] = parsed
	}
	if _, ok := capacity[corev1.ResourcePods]; !ok {
		capacity[corev1.ResourcePods] = resource.MustParse(defaultPods)
	}
	ready := corev1.NodeCondition{
		Type:    corev1.NodeReady,
		Status:  corev1.ConditionTrue,
		Reason:  "KubeletReady",
		Message: "the VM's tunnel is connected",
	}
	if c.State == nodeclaim.StateUnhealthy {
		ready.Status, ready.Reason, ready.Message = corev1.ConditionFalse, reasonVMUnhealthy, "the VM failed to clean up a pod's container"
	} else if c.State != nodeclaim.StateRunning {
		ready.Status, ready.Reason, ready.Message = corev1.ConditionFalse, reasonVMLaunching, "waiting for the VM's tunnel to connect"
	}
	now := metav1.Now()
	conditions := []corev1.NodeCondition{ready}
	for _, t := range []corev1.NodeConditionType{corev1.NodeMemoryPressure, corev1.NodeDiskPressure, corev1.NodePIDPressure, corev1.NodeNetworkUnavailable} {
		conditions = append(conditions, corev1.NodeCondition{Type: t, Status: corev1.ConditionFalse, Reason: "NodeAgent" + string(t) + "False"})
	}
	for i := range conditions {
		conditions[i].LastHeartbeatTime, conditions[i].LastTransitionTime = now, now
	}
	arch := c.Labels[labelArch]
	if arch == "" {
		arch = "amd64"
	}
	return corev1.NodeStatus{
		Capacity:    capacity,
		Allocatable: capacity.DeepCopy(),
		Conditions:  conditions,
		Addresses: []corev1.NodeAddress{
			{Type: corev1.NodeInternalIP, Address: cfg.NodeIP},
			{Type: corev1.NodeHostName, Address: c.Name},
		},
		DaemonEndpoints: corev1.NodeDaemonEndpoints{KubeletEndpoint: corev1.DaemonEndpoint{Port: cfg.KubeletPort}},
		NodeInfo: corev1.NodeSystemInfo{
			KubeletVersion:  cfg.KubeletVersion,
			OperatingSystem: "linux",
			Architecture:    arch,
		},
	}
}
