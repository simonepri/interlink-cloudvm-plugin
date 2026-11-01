// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// PodAllocation tracks the resources assigned to an active pod on a warm node.
type PodAllocation struct {
	PodUID       string         `json:"podUID"`
	PodNamespace string         `json:"namespace"`
	PodName      string         `json:"podName"`
	Allocated    ResourceVector `json:"allocated"`
	AssignedGPUs []int          `json:"assignedGPUs,omitempty"`
	AssignedTPUs []int          `json:"assignedTPUs,omitempty"`
	StartedAt    time.Time      `json:"startedAt"`
	Completed    bool           `json:"completed"`
	FinishedAt   *time.Time     `json:"finishedAt,omitempty"`
	ExitCode     int32          `json:"exitCode,omitempty"`
	Reason       string         `json:"reason,omitempty"`
	Message      string         `json:"message,omitempty"`
	// DaemonSet marks a pod a DaemonSet owns: it shares the VM with pods of any namespace, never binds the VM's
	// namespace, and does not keep the VM from being idle.
	DaemonSet bool `json:"daemonSet,omitempty"`
}

// CloudVMNode represents a live ephemeral cloud VM capable of hosting one or more pods.
type CloudVMNode struct {
	mu                      sync.RWMutex
	InstanceID              string         `json:"instanceId"`
	Provider                ProviderType   `json:"provider"`
	Region                  string         `json:"region"`
	InstanceType            string         `json:"instanceType,omitempty"`
	Namespace               string         `json:"namespace,omitempty"`
	Spot                    bool           `json:"spot"`
	Zone                    string         `json:"zone,omitempty"`
	MeshIP                  string         `json:"meshIp"`
	IP                      string         `json:"ip,omitempty"`
	Capacity                ResourceVector `json:"capacity"`
	Allocated               ResourceVector `json:"allocated"`
	AllocatedPods           int            `json:"allocatedPods,omitempty"`
	Pods                    []string       `json:"pods,omitempty"`
	CreatedAt               time.Time      `json:"createdAt"`
	IdleSince               *time.Time     `json:"idleSince,omitempty"`
	Cordoned                bool           `json:"cordoned,omitempty"`
	SpinDownReason          SpinDownReason `json:"spinDownReason,omitempty"`
	GPUType                 string         `json:"gpuType,omitempty"`
	UnknownCoTenants        bool           `json:"unknownCoTenants,omitempty"`
	UnknownCoTenantsRetries int            `json:"unknownCoTenantsRetries,omitempty"`
	UnknownSince            time.Time      `json:"unknownSince,omitempty"`
	RunnerAuthToken         string         `json:"runnerAuthToken,omitempty"`
	// ClaimName is the NodeClaim the VM was launched for. Such a VM takes only the pods bound to that claim's Node,
	// and only deleting the claim terminates it.
	ClaimName string `json:"claimName,omitempty"`

	// idleHoldLogged records that the idle collector logged why it keeps this expired idle node.
	idleHoldLogged bool

	// Discrete device maps: Slot Index -> PodUID
	GPUSlots map[int]string `json:"gpuSlots"`
	TPUSlots map[int]string `json:"tpuSlots"`

	// Active pods running on this node: PodUID -> *PodAllocation
	ActivePods map[string]*PodAllocation `json:"activePods"`
}

// NewCloudVMNode creates an initialized CloudVMNode.
// GetMeshIP thread-safely returns the mesh IP of the node.
func (node *CloudVMNode) GetMeshIP() string {
	if node == nil {
		return ""
	}
	node.mu.RLock()
	defer node.mu.RUnlock()
	if node.MeshIP != "" {
		return node.MeshIP
	}
	return node.IP
}

// GetInternalIP thread-safely returns the internal/mesh IP of the node.
func (node *CloudVMNode) GetInternalIP() string {
	if node == nil {
		return ""
	}
	node.mu.RLock()
	defer node.mu.RUnlock()
	if node.IP != "" {
		return node.IP
	}
	return node.MeshIP
}

// IsCordoned thread-safely returns whether the node is cordoned.
func (node *CloudVMNode) IsCordoned() bool {
	if node == nil {
		return false
	}
	node.mu.RLock()
	defer node.mu.RUnlock()
	return node.Cordoned
}

// SetMeshIP thread-safely updates the mesh IP of the node.
func (node *CloudVMNode) SetMeshIP(ip string) {
	if node == nil {
		return
	}
	node.mu.Lock()
	defer node.mu.Unlock()
	node.MeshIP = ip
}

func NewCloudVMNode(instanceID string, provider ProviderType, region, meshIP string, capacity ResourceVector) *CloudVMNode {
	gpuSlots := make(map[int]string, capacity.GPUs)
	for i := 0; i < capacity.GPUs; i++ {
		gpuSlots[i] = ""
	}

	tpuSlots := make(map[int]string, capacity.TPUs)
	for i := 0; i < capacity.TPUs; i++ {
		tpuSlots[i] = ""
	}

	return &CloudVMNode{
		InstanceID: instanceID,
		Provider:   provider,
		Region:     region,
		MeshIP:     meshIP,
		Capacity:   capacity,
		GPUSlots:   gpuSlots,
		TPUSlots:   tpuSlots,
		ActivePods: make(map[string]*PodAllocation),
		CreatedAt:  time.Now(),
	}
}

// FitsAndScores evaluates if a pod fits, assigns discrete devices, and scores the node.
// Higher score = tighter packing (leaves other nodes empty for zero-cost shutdown).
func (node *CloudVMNode) FitsAndScores(req ResourceVector) (bool, []int, []int, int64) {
	// 1. CPU & Memory capacity check
	newCPU := node.Allocated.CPU.DeepCopy()
	newCPU.Add(req.CPU)
	if newCPU.Cmp(node.Capacity.CPU) > 0 {
		return false, nil, nil, 0
	}

	newMem := node.Allocated.Memory.DeepCopy()
	newMem.Add(req.Memory)
	if newMem.Cmp(node.Capacity.Memory) > 0 {
		return false, nil, nil, 0
	}

	// 2. Discrete GPU slot allocation
	var assignedGPUs []int
	if req.GPUs > 0 {
		for slotID := 0; slotID < node.Capacity.GPUs; slotID++ {
			if node.GPUSlots[slotID] == "" {
				assignedGPUs = append(assignedGPUs, slotID)
				if len(assignedGPUs) == req.GPUs {
					break
				}
			}
		}
		if len(assignedGPUs) < req.GPUs {
			return false, nil, nil, 0
		}
	}

	// 3. Discrete TPU chip allocation
	var assignedTPUs []int
	if req.TPUs > 0 {
		for chipID := 0; chipID < node.Capacity.TPUs; chipID++ {
			if node.TPUSlots[chipID] == "" {
				assignedTPUs = append(assignedTPUs, chipID)
				if len(assignedTPUs) == req.TPUs {
					break
				}
			}
		}
		if len(assignedTPUs) < req.TPUs {
			return false, nil, nil, 0
		}
	}

	// 4. Kubernetes NodeResourcesMostAllocated Score (0-100)
	var cpuScore, memScore int64
	if node.Capacity.CPU.MilliValue() > 0 {
		cpuScore = (newCPU.MilliValue() * 100) / node.Capacity.CPU.MilliValue()
	}
	if node.Capacity.Memory.Value() > 0 {
		memScore = (newMem.Value() * 100) / node.Capacity.Memory.Value()
	}
	score := (cpuScore + memScore) / 2
	if node.Capacity.GPUs > 0 {
		gpuScore := int64((len(assignedGPUs) + node.Allocated.GPUs) * 100 / node.Capacity.GPUs)
		score = (score + gpuScore) / 2
	}

	return true, assignedGPUs, assignedTPUs, score
}

// ownerNamespace returns the namespace of the node and its workload pod allocations, empty when it has neither; false
// when they disagree. DaemonSet pods, which may come from any namespace, do not count.
func (node *CloudVMNode) ownerNamespace() (string, bool) {
	namespace := node.Namespace
	for _, alloc := range node.ActivePods {
		if alloc.DaemonSet {
			continue
		}
		if namespace == "" {
			namespace = alloc.PodNamespace
		}
		if alloc.PodNamespace != namespace {
			return "", false
		}
	}
	return namespace, true
}

// AssignPod commits resource and discrete device reservations for a pod.
func (node *CloudVMNode) AssignPod(alloc *PodAllocation) {
	node.ActivePods[alloc.PodUID] = alloc
	node.AllocatedPods = node.ActivePodCount()
	node.Allocated = node.Allocated.Add(alloc.Allocated)
	for _, gpu := range alloc.AssignedGPUs {
		node.GPUSlots[gpu] = alloc.PodUID
	}
	for _, tpu := range alloc.AssignedTPUs {
		node.TPUSlots[tpu] = alloc.PodUID
	}
	if !alloc.DaemonSet {
		node.IdleSince = nil
	}
}

// ActivePodCount returns the count of running (non-completed) pods on the node.
func (node *CloudVMNode) ActivePodCount() int {
	count := 0
	for _, p := range node.ActivePods {
		if !p.Completed {
			count++
		}
	}
	return count
}

// workloadPodCount returns the count of running (non-completed) pods on the node that no DaemonSet owns. A node whose
// only running pods are DaemonSet pods is idle, as Karpenter's emptiness ignores DaemonSet pods.
func (node *CloudVMNode) workloadPodCount() int {
	count := 0
	for _, p := range node.ActivePods {
		if !p.Completed && !p.DaemonSet {
			count++
		}
	}
	return count
}

// MarkPodCompleted frees hardware resources (GPUs, TPUs, CPU, Memory) for a finished container
// while retaining the allocation record so subsequent status queries and co-tenant deletions
// properly resolve the node and state without terminating the underlying VM prematurely.
func (node *CloudVMNode) MarkPodCompleted(podUID string, exitCode int32, reason, message string, finishedAt time.Time) bool {
	alloc, exists := node.ActivePods[podUID]
	if !exists {
		return false
	}
	if alloc.Completed {
		return false
	}

	alloc.Completed = true
	alloc.ExitCode = exitCode
	alloc.Reason = reason
	alloc.Message = message
	alloc.FinishedAt = &finishedAt

	node.Allocated = node.Allocated.Sub(alloc.Allocated)
	node.AllocatedPods = node.ActivePodCount()
	for _, gpu := range alloc.AssignedGPUs {
		if node.GPUSlots[gpu] == podUID {
			node.GPUSlots[gpu] = ""
		}
	}
	for _, tpu := range alloc.AssignedTPUs {
		if node.TPUSlots[tpu] == podUID {
			node.TPUSlots[tpu] = ""
		}
	}

	if !alloc.DaemonSet && node.workloadPodCount() == 0 {
		now := time.Now()
		node.IdleSince = &now
	}
	return true
}

// ReleasePod completely frees and removes a pod allocation (on explicit pod delete).
func (node *CloudVMNode) ReleasePod(podUID string) bool {
	alloc, exists := node.ActivePods[podUID]
	if !exists {
		return false
	}

	delete(node.ActivePods, podUID)
	node.AllocatedPods = node.ActivePodCount()
	if !alloc.Completed {
		node.Allocated = node.Allocated.Sub(alloc.Allocated)
		for _, gpu := range alloc.AssignedGPUs {
			if node.GPUSlots[gpu] == podUID {
				node.GPUSlots[gpu] = ""
			}
		}
		for _, tpu := range alloc.AssignedTPUs {
			if node.TPUSlots[tpu] == podUID {
				node.TPUSlots[tpu] = ""
			}
		}
	}

	if !alloc.DaemonSet && node.workloadPodCount() == 0 {
		now := time.Now()
		node.IdleSince = &now
	}
	return true
}

// NodePool manages a multi-cloud set of active VM nodes with Kubernetes bin-packing semantics.
type NodePool struct {
	mu              sync.RWMutex
	nodes           map[string]*CloudVMNode
	idleGracePeriod time.Duration
	maxPodsPerNode  int
	reuse           ReusePolicy
	// departed remembers the identity of the most recently released pods, so their workers' last log lines, which can
	// arrive after the release, still name the pod.
	departed      map[string]departedPod
	departedOrder []string
}

// maxDepartedPods bounds how many released pods NodePool.PodIdentity still recognizes.
const maxDepartedPods = 4096

type departedPod struct {
	instanceID string
	alloc      PodAllocation
}

// NewNodePool initializes a NodePool with a default idle grace period before node termination.
func NewNodePool(idleGracePeriod time.Duration) *NodePool {
	if idleGracePeriod < 0 {
		idleGracePeriod = 60 * time.Second
	}
	return &NodePool{
		nodes:           make(map[string]*CloudVMNode),
		idleGracePeriod: idleGracePeriod,
		maxPodsPerNode:  100,
	}
}

// IdleGracePeriod returns the configured idle cooldown period before an unused node is terminated.
func (np *NodePool) IdleGracePeriod() time.Duration {
	np.mu.RLock()
	defer np.mu.RUnlock()
	return np.idleGracePeriod
}

// MaxPodsPerNode returns the maximum number of active pods allowed on a single node.
func (np *NodePool) MaxPodsPerNode() int {
	np.mu.RLock()
	defer np.mu.RUnlock()
	return np.maxPodsPerNode
}

// SetMaxPodsPerNode updates the maximum number of active pods allowed on a single node.
func (np *NodePool) SetMaxPodsPerNode(n int) {
	np.mu.Lock()
	defer np.mu.Unlock()
	if n <= 0 {
		np.maxPodsPerNode = 100
		return
	}
	np.maxPodsPerNode = n
}

// SetReusePolicy sets the rules that decide whether an idle node may take a new pod.
func (np *NodePool) SetReusePolicy(p ReusePolicy) {
	np.mu.Lock()
	defer np.mu.Unlock()
	np.reuse = p
}

// SetIdleGracePeriod updates the idle grace period for the node pool.
func (np *NodePool) SetIdleGracePeriod(d time.Duration) {
	np.mu.Lock()
	defer np.mu.Unlock()
	np.idleGracePeriod = d
}

// RegisterNode adds a newly provisioned cloud node into the active bin-packing pool.
func (np *NodePool) RegisterNode(node *CloudVMNode) {
	np.mu.Lock()
	defer np.mu.Unlock()
	np.nodes[node.InstanceID] = node
}

// UnregisterNode removes a terminated cloud node from the pool.
func (np *NodePool) UnregisterNode(instanceID string) {
	np.mu.Lock()
	defer np.mu.Unlock()
	if node, ok := np.nodes[instanceID]; ok && node != nil {
		for _, alloc := range node.ActivePods {
			np.rememberDepartedLocked(instanceID, *alloc)
		}
	}
	delete(np.nodes, instanceID)
}

// rememberDepartedLocked records a released pod's identity, forgetting the oldest beyond maxDepartedPods.
func (np *NodePool) rememberDepartedLocked(instanceID string, alloc PodAllocation) {
	if np.departed == nil {
		np.departed = make(map[string]departedPod)
	}
	if _, ok := np.departed[alloc.PodUID]; !ok {
		if len(np.departedOrder) >= maxDepartedPods {
			delete(np.departed, np.departedOrder[0])
			np.departedOrder = np.departedOrder[1:]
		}
		np.departedOrder = append(np.departedOrder, alloc.PodUID)
	}
	np.departed[alloc.PodUID] = departedPod{instanceID: instanceID, alloc: alloc}
}

// PodIdentity returns the allocation of a pod that runs, or recently ran, on the node instanceID; log records use it
// to name the pod, including lines that arrive after the pod was deleted.
func (np *NodePool) PodIdentity(instanceID, podUID string) (PodAllocation, bool) {
	np.mu.RLock()
	defer np.mu.RUnlock()
	if node, ok := np.nodes[instanceID]; ok && node != nil {
		if alloc, active := node.ActivePods[podUID]; active {
			return *alloc, true
		}
	}
	if d, ok := np.departed[podUID]; ok && d.instanceID == instanceID {
		return d.alloc, true
	}
	return PodAllocation{}, false
}

// AllocationRequest describes a pod to bin-pack into a warm node.
type AllocationRequest struct {
	Resources ResourceVector
	Region    string
	PodUID    string
	Namespace string
	PodName   string
	// Spot restricts the allocation to spot (true) or on-demand (false) nodes when set.
	Spot *bool
	// Accepts reports whether a node meets the pod's instance constraints; nil accepts every node. AllocatePod calls it
	// under the pool lock, so it must not block or call back into the pool.
	Accepts func(*CloudVMNode) bool
	// ClaimName places the pod on the VM of that NodeClaim and nowhere else; empty places it on a VM of no claim.
	ClaimName string
	// DaemonSet places a DaemonSet pod, which shares a VM with pods of any namespace and never binds its namespace.
	DaemonSet bool
}

// Allocation is the node and devices a pod was bin-packed into.
type Allocation struct {
	Node *CloudVMNode
	GPUs []int
	TPUs []int
	// Reused is true when the node was idle, so the pod reuses a VM another pod left behind.
	Reused bool
}

// AllocatePod bin-packs a pod into the warm node with the highest MostAllocated score that fits it. Nodes marked for
// spin-down never take a pod, and an idle node takes one only while ReusePolicy allows it.
func (np *NodePool) AllocatePod(a AllocationRequest) (*Allocation, error) {
	np.mu.Lock()
	defer np.mu.Unlock()

	var bestNode *CloudVMNode
	var bestGPUs []int
	var bestTPUs []int
	var highestScore int64 = -1
	now := time.Now()

	req, region, namespace := a.Resources, a.Region, a.Namespace
	for _, node := range np.nodes {
		if node.Cordoned || node.ClaimName != a.ClaimName {
			continue
		}
		if a.Spot != nil && node.Spot != *a.Spot {
			continue
		}
		if region != "" && node.Region != region {
			continue
		}
		// A node never mixes namespaces, so the tunnel gateway can attribute its traffic to one and the node carries only
		// that namespace's firewall; a pod without a namespace never shares a node. A DaemonSet pod is the node's own
		// agent, so it joins a node of any namespace.
		if namespace == "" {
			continue
		}
		if owner, ok := node.ownerNamespace(); !a.DaemonSet && (!ok || (owner != "" && owner != namespace)) {
			continue
		}
		if np.maxPodsPerNode > 0 && node.ActivePodCount() >= np.maxPodsPerNode {
			continue
		}
		if a.Accepts != nil && !a.Accepts(node) {
			continue
		}
		// A claim VM lives until its claim is deleted, so it takes pods whether or not it is idle.
		if node.ClaimName == "" && node.isIdle() && !np.reusableLocked(node, now) {
			continue
		}

		fits, gpus, tpus, score := node.FitsAndScores(req)
		if fits && score > highestScore {
			highestScore = score
			bestNode = node
			bestGPUs = gpus
			bestTPUs = tpus
		}
	}

	if bestNode == nil {
		return nil, fmt.Errorf("no warm node capacity available")
	}

	reused := bestNode.isIdle()
	if !a.DaemonSet {
		bestNode.Namespace = namespace
	}
	bestNode.AssignPod(&PodAllocation{
		PodUID:       a.PodUID,
		PodNamespace: namespace,
		PodName:      a.PodName,
		Allocated:    req,
		AssignedGPUs: bestGPUs,
		AssignedTPUs: bestTPUs,
		StartedAt:    now,
		DaemonSet:    a.DaemonSet,
	})
	return &Allocation{Node: bestNode, GPUs: bestGPUs, TPUs: bestTPUs, Reused: reused}, nil
}

// MarkPodCompleted records a pod container as finished, frees its compute resources,
// and returns true if the pod was found and marked.
func (np *NodePool) MarkPodCompleted(podUID string, exitCode int32, reason, message string, finishedAt time.Time) {
	np.mu.Lock()
	defer np.mu.Unlock()

	for _, node := range np.nodes {
		if node.MarkPodCompleted(podUID, exitCode, reason, message, finishedAt) {
			np.checkIdleLocked(node, time.Now())
			return
		}
	}
}

// Release frees resources held by podUID across any active node in the pool and returns the remaining active pod count on that node.
func (np *NodePool) Release(podUID string) int {
	np.mu.Lock()
	defer np.mu.Unlock()

	for _, node := range np.nodes {
		if alloc, ok := node.ActivePods[podUID]; ok {
			np.rememberDepartedLocked(node.InstanceID, *alloc)
		}
		if node.ReleasePod(podUID) {
			np.checkIdleLocked(node, time.Now())
			return node.ActivePodCount()
		}
	}
	return 0
}

// HasActivePods returns true if the node exists in the pool and currently has non-completed pods assigned.
func (np *NodePool) HasActivePods(instanceID string) bool {
	np.mu.RLock()
	defer np.mu.RUnlock()
	node, found := np.nodes[instanceID]
	if !found {
		return false
	}
	return node.ActivePodCount() > 0
}

// SetUnknownCoTenants flags whether a node has unverified co-tenant workloads.
func (np *NodePool) SetUnknownCoTenants(instanceID string, unknown bool) {
	np.mu.Lock()
	defer np.mu.Unlock()
	if node, ok := np.nodes[instanceID]; ok {
		node.UnknownCoTenants = unknown
	}
}

// HasUnknownCoTenants reports whether the node has unverified co-tenant workloads.
func (np *NodePool) HasUnknownCoTenants(instanceID string) bool {
	np.mu.RLock()
	defer np.mu.RUnlock()
	if node, ok := np.nodes[instanceID]; ok {
		return node.UnknownCoTenants
	}
	return false
}

// NodesWithUnknownCoTenants returns all nodes that need co-tenant reconciliation.
func (np *NodePool) NodesWithUnknownCoTenants() []*CloudVMNode {
	np.mu.RLock()
	defer np.mu.RUnlock()
	var res []*CloudVMNode
	for _, n := range np.nodes {
		if n.UnknownCoTenants {
			res = append(res, n)
		}
	}
	return res
}

// IncrementUnknownCoTenantRetries increments retry counter and returns true if timeout duration has elapsed.
func (np *NodePool) IncrementUnknownCoTenantRetries(instanceID string, timeout time.Duration) (int, bool) {
	np.mu.Lock()
	defer np.mu.Unlock()
	if node, ok := np.nodes[instanceID]; ok {
		node.UnknownCoTenantsRetries++
		if !node.UnknownSince.IsZero() && time.Since(node.UnknownSince) >= timeout {
			node.UnknownCoTenants = false
			return node.UnknownCoTenantsRetries, true
		}
		return node.UnknownCoTenantsRetries, false
	}
	return 0, false
}

// HasNode returns true if the specified node exists in the pool.
func (np *NodePool) HasNode(instanceID string) bool {
	np.mu.RLock()
	defer np.mu.RUnlock()
	_, found := np.nodes[instanceID]
	return found
}

// Node returns the tracked node for instanceID.
func (np *NodePool) Node(instanceID string) (*CloudVMNode, bool) {
	np.mu.RLock()
	defer np.mu.RUnlock()
	node, found := np.nodes[instanceID]
	return node, found && node != nil
}

// FindPodNode returns the node and a thread-safe copy of the allocation for a given podUID.
func (np *NodePool) FindPodNode(podUID string) (*CloudVMNode, PodAllocation, bool) {
	np.mu.RLock()
	defer np.mu.RUnlock()

	for _, node := range np.nodes {
		if alloc, ok := node.ActivePods[podUID]; ok {
			return node, *alloc, true
		}
	}
	return nil, PodAllocation{}, false
}

// CollectExpiredIdleNodes removes and returns the nodes without active pods that are marked for spin-down or have been
// idle for > idleGracePeriod. Removing a node under the pool lock moves it to Terminating: no allocation can pick it
// after this returns. It first marks idle nodes for spin-down when ReusePolicy no longer allows reusing them.
func (np *NodePool) CollectExpiredIdleNodes() []*CloudVMNode {
	np.mu.Lock()
	defer np.mu.Unlock()

	var expired []*CloudVMNode
	now := time.Now()

	for id, node := range np.nodes {
		if node.workloadPodCount() != 0 || node.ClaimName != "" {
			node.idleHoldLogged = false
			continue
		}
		np.checkIdleLocked(node, now)
		// A node marked for spin-down can never be reused, so it does not wait out the grace period.
		graceOver := node.IdleSince != nil && now.Sub(*node.IdleSince) > np.idleGracePeriod
		if !node.Cordoned && !graceOver {
			node.idleHoldLogged = false
			continue
		}
		if node.UnknownCoTenants {
			if !node.idleHoldLogged {
				node.idleHoldLogged = true
				log.Printf("[cloudvm] Keeping idle node %s past its %v grace period until its co-tenants are verified", sanitizeLog(id), np.idleGracePeriod) //nolint:gosec // sanitized
			}
			continue
		}
		expired = append(expired, node)
		delete(np.nodes, id)
	}
	return expired
}

// RejectReuse marks a node for spin-down and releases the pod just placed on it, so the pod launches a new VM instead.
func (np *NodePool) RejectReuse(instanceID, podUID string, reason SpinDownReason) {
	np.mu.Lock()
	defer np.mu.Unlock()
	if node, ok := np.nodes[instanceID]; ok && node != nil {
		markSpinDown(node, reason)
		node.ReleasePod(podUID)
	}
}

// ClaimForTermination reports whether a path outside the idle collector may terminate a VM, and marks a pooled one
// for spin-down so no pod is allocated to it before it terminates. It refuses a pooled VM with active pods or
// unverified co-tenants, an idle one still in the grace period during which it may be reused, and a NodeClaim's VM.
func (np *NodePool) ClaimForTermination(instanceID string) bool {
	np.mu.Lock()
	defer np.mu.Unlock()
	node, ok := np.nodes[instanceID]
	if !ok || node == nil {
		return true
	}
	if node.workloadPodCount() > 0 || node.UnknownCoTenants || node.ClaimName != "" {
		return false
	}
	if !node.Cordoned && node.IdleSince != nil && np.idleGracePeriod > 0 && time.Since(*node.IdleSince) <= np.idleGracePeriod {
		return false
	}
	markSpinDown(node, SpinDownOrphaned)
	return true
}

// ReturnUnterminated puts back a node the idle collector removed but failed to terminate. The node stays marked for
// spin-down, so the collector retries its termination on its next pass and no allocation reuses it meanwhile.
func (np *NodePool) ReturnUnterminated(node *CloudVMNode) {
	np.mu.Lock()
	defer np.mu.Unlock()
	markSpinDown(node, SpinDownTerminateFailed)
	np.nodes[node.InstanceID] = node
}

// IdleNodeCount returns how many tracked nodes have no active pods other than DaemonSet pods.
func (np *NodePool) IdleNodeCount() int {
	np.mu.RLock()
	defer np.mu.RUnlock()
	count := 0
	for _, node := range np.nodes {
		if node.workloadPodCount() == 0 {
			count++
		}
	}
	return count
}

// InstanceByIP returns the instance ID of the registered node with the given private IP.
func (np *NodePool) InstanceByIP(ip string) (string, bool) {
	np.mu.RLock()
	defer np.mu.RUnlock()
	if ip == "" {
		return "", false
	}
	for _, node := range np.nodes {
		if node.GetInternalIP() == ip || node.GetMeshIP() == ip {
			return node.InstanceID, true
		}
	}
	return "", false
}

// InstanceNamespace returns the one namespace of the node and the workload pods allocated to it, ignoring DaemonSet
// pods; false when the node is unknown, has no namespace, or its pods disagree.
func (np *NodePool) InstanceNamespace(instanceID string) (string, bool) {
	np.mu.RLock()
	defer np.mu.RUnlock()
	node, found := np.nodes[instanceID]
	if !found || node == nil {
		return "", false
	}
	namespace, ok := node.ownerNamespace()
	return namespace, ok && namespace != ""
}

// PodNamespace returns the namespace of a pod allocated to the node, DaemonSet pods included; false when the node is
// unknown or holds no such pod.
func (np *NodePool) PodNamespace(instanceID, podUID string) (string, bool) {
	np.mu.RLock()
	defer np.mu.RUnlock()
	node, found := np.nodes[instanceID]
	if !found || node == nil {
		return "", false
	}
	alloc, ok := node.ActivePods[podUID]
	if !ok {
		return "", false
	}
	return alloc.PodNamespace, true
}

// PodNamespaceOnWorker returns the namespace of a pod a worker VM runs, DaemonSet pods included, so a stream a pod
// opens can be attributed to that pod's namespace; false when the worker holds no such pod.
func (s *Server) PodNamespaceOnWorker(instanceID, podUID string) (string, bool) {
	np := s.NodePool()
	if np == nil {
		return "", false
	}
	return np.PodNamespace(instanceID, podUID)
}

// ActiveNodes returns a slice of all active nodes currently tracked in the pool.
func (np *NodePool) ActiveNodes() []*CloudVMNode {
	if np == nil {
		return nil
	}
	np.mu.RLock()
	defer np.mu.RUnlock()
	res := make([]*CloudVMNode, 0, len(np.nodes))
	for _, node := range np.nodes {
		if node != nil {
			res = append(res, node)
		}
	}
	return res
}

// ActiveNodeCount returns total nodes currently tracked in the pool.
func (np *NodePool) ActiveNodeCount() int {
	np.mu.RLock()
	defer np.mu.RUnlock()
	return len(np.nodes)
}

// RegisterAndAllocate atomically registers a newly launched node and allocates the first pod onto it.
func (np *NodePool) RegisterAndAllocate(node *CloudVMNode, req ResourceVector, podUID, namespace, podName string) ([]int, []int) {
	np.mu.Lock()
	defer np.mu.Unlock()

	if node.Namespace == "" {
		node.Namespace = namespace
	}
	np.nodes[node.InstanceID] = node
	_, gpus, tpus, _ := node.FitsAndScores(req)
	alloc := &PodAllocation{
		PodUID:       podUID,
		PodNamespace: namespace,
		PodName:      podName,
		Allocated:    req,
		AssignedGPUs: gpus,
		AssignedTPUs: tpus,
		StartedAt:    time.Now(),
	}
	node.AssignPod(alloc)
	return gpus, tpus
}

// Cordon marks an instance for spin-down, excluding it from future bin-pack allocations; the idle collector
// terminates it on its first pass after its last active pod leaves.
func (np *NodePool) Cordon(instanceID string, reason SpinDownReason) {
	np.mu.Lock()
	defer np.mu.Unlock()
	if node, exists := np.nodes[instanceID]; exists && node != nil {
		markSpinDown(node, reason)
	}
}

// Uncordon clears the spin-down mark of an instance, allowing future bin-pack allocations.
func (np *NodePool) Uncordon(instanceID string) {
	np.mu.Lock()
	defer np.mu.Unlock()
	if node, exists := np.nodes[instanceID]; exists && node != nil {
		node.Cordoned = false
		node.SpinDownReason = ""
	}
}
