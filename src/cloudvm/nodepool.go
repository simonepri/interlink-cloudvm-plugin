// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"fmt"
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
}

// CloudVMNode represents a live ephemeral cloud VM capable of hosting one or more pods.
type CloudVMNode struct {
	InstanceID      string         `json:"instanceId"`
	Provider        ProviderType   `json:"provider"`
	Region          string         `json:"region"`
	Namespace       string         `json:"namespace,omitempty"`
	Spot            bool           `json:"spot"`
	Zone            string         `json:"zone,omitempty"`
	MeshIP          string         `json:"meshIp"`
	Capacity        ResourceVector `json:"capacity"`
	Allocated       ResourceVector `json:"allocated"`
	CreatedAt       time.Time      `json:"createdAt"`
	IdleSince       *time.Time     `json:"idleSince,omitempty"`
	Cordoned        bool           `json:"cordoned,omitempty"`
	RunnerAuthToken string         `json:"runnerAuthToken,omitempty"`

	// Discrete device maps: Slot Index -> PodUID
	GPUSlots map[int]string `json:"gpuSlots"`
	TPUSlots map[int]string `json:"tpuSlots"`

	// Active pods running on this node: PodUID -> *PodAllocation
	ActivePods map[string]*PodAllocation `json:"activePods"`
}

// NewCloudVMNode creates an initialized CloudVMNode.
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

// AssignPod commits resource and discrete device reservations for a pod.
func (node *CloudVMNode) AssignPod(alloc *PodAllocation) {
	node.ActivePods[alloc.PodUID] = alloc
	node.Allocated = node.Allocated.Add(alloc.Allocated)
	for _, gpu := range alloc.AssignedGPUs {
		node.GPUSlots[gpu] = alloc.PodUID
	}
	for _, tpu := range alloc.AssignedTPUs {
		node.TPUSlots[tpu] = alloc.PodUID
	}
	node.IdleSince = nil
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

	if node.ActivePodCount() == 0 {
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

	if node.ActivePodCount() == 0 {
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
}

// NewNodePool initializes a NodePool with a default idle grace period before node termination.
func NewNodePool(idleGracePeriod time.Duration) *NodePool {
	if idleGracePeriod <= 0 {
		idleGracePeriod = 60 * time.Second
	}
	return &NodePool{
		nodes:           make(map[string]*CloudVMNode),
		idleGracePeriod: idleGracePeriod,
	}
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
	delete(np.nodes, instanceID)
}

// Allocate attempts to bin-pack a pod into an existing warm node using MostAllocated scoring.
// Returns (node, assignedGPUs, assignedTPUs, nil) on a Bin-Pack Hit.
// Returns (nil, nil, nil, ErrNoWarmCapacity) on a Bin-Pack Miss.
func (np *NodePool) Allocate(req ResourceVector, region string, podUID, namespace, podName string) (*CloudVMNode, []int, []int, error) {
	np.mu.Lock()
	defer np.mu.Unlock()

	var bestNode *CloudVMNode
	var bestGPUs []int
	var bestTPUs []int
	var highestScore int64 = -1

	for _, node := range np.nodes {
		if node.Cordoned {
			continue
		}
		if region != "" && node.Region != region {
			continue
		}
		if node.Namespace != "" && namespace != "" && node.Namespace != namespace {
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
		return nil, nil, nil, fmt.Errorf("no warm node capacity available")
	}

	alloc := &PodAllocation{
		PodUID:       podUID,
		PodNamespace: namespace,
		PodName:      podName,
		Allocated:    req,
		AssignedGPUs: bestGPUs,
		AssignedTPUs: bestTPUs,
		StartedAt:    time.Now(),
	}
	bestNode.AssignPod(alloc)
	return bestNode, bestGPUs, bestTPUs, nil
}

// MarkPodCompleted records a pod container as finished, frees its compute resources,
// and returns true if the pod was found and marked.
func (np *NodePool) MarkPodCompleted(podUID string, exitCode int32, reason, message string, finishedAt time.Time) {
	np.mu.Lock()
	defer np.mu.Unlock()

	for _, node := range np.nodes {
		if node.MarkPodCompleted(podUID, exitCode, reason, message, finishedAt) {
			return
		}
	}
}

// Release frees resources held by podUID across any active node in the pool and returns the remaining active pod count on that node.
func (np *NodePool) Release(podUID string) int {
	np.mu.Lock()
	defer np.mu.Unlock()

	for _, node := range np.nodes {
		if node.ReleasePod(podUID) {
			return node.ActivePodCount()
		}
	}
	return 0
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

// CollectExpiredIdleNodes returns instance IDs of nodes that have had 0 active pods for > idleGracePeriod.
func (np *NodePool) CollectExpiredIdleNodes() []*CloudVMNode {
	np.mu.Lock()
	defer np.mu.Unlock()

	var expired []*CloudVMNode
	now := time.Now()

	for id, node := range np.nodes {
		if node.ActivePodCount() == 0 && node.IdleSince != nil {
			if now.Sub(*node.IdleSince) > np.idleGracePeriod {
				expired = append(expired, node)
				delete(np.nodes, id)
			}
		}
	}
	return expired
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
