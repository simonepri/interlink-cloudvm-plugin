// Reuse of idle VMs: the reasons a VM is marked for spin-down instead of taking new pods, the policy that decides
// whether an idle VM may be reused, and the instance constraints a pod places on the VM it lands on.
package main

import (
	"log"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// SpinDownReason is why a VM takes no new pods and is terminated by the idle collector's first pass after its last
// active pod leaves.
type SpinDownReason string

const (
	SpinDownCleanupFailed   SpinDownReason = "cleanup-failed"
	SpinDownInterruption    SpinDownReason = "interruption"
	SpinDownLost            SpinDownReason = "lost"
	SpinDownMaxAge          SpinDownReason = "max-age"
	SpinDownOrphaned        SpinDownReason = "orphaned"
	SpinDownPrice           SpinDownReason = "price"
	SpinDownRecovered       SpinDownReason = "recovered"
	SpinDownSingleTenant    SpinDownReason = "single-tenant"
	SpinDownStartFailed     SpinDownReason = "start-failed"
	SpinDownTerminateFailed SpinDownReason = "terminate-failed"
	SpinDownUnhealthy       SpinDownReason = "unhealthy"
)

// DefaultReuseMaxAge is the age past which an idle VM is spun down instead of reused.
const DefaultReuseMaxAge = 24 * time.Hour

// ReusePolicy decides whether an idle VM may take a new pod during its idle grace period. Its functions run under the
// pool lock, so they must not block or call back into the pool.
type ReusePolicy struct {
	// MaxAge spins down an idle VM older than this; zero disables the limit.
	MaxAge time.Duration
	// Healthy reports whether the VM's tunnel session and runner can serve a new pod; nil treats every VM as healthy.
	Healthy func(*CloudVMNode) bool
	// PriceTooHigh reports whether a spot VM costs materially more than the best current offer; nil disables the check.
	PriceTooHigh func(*CloudVMNode) bool
}

// DefaultReuseTunnelDownGrace is how long an idle VM's reverse tunnel may stay down before the VM is spun down.
const DefaultReuseTunnelDownGrace = 30 * time.Second

// ConfigureNodeReuse sets the node pool's reuse policy: spin down idle VMs older than maxAge (zero disables), whose
// reverse tunnel has been down for longer than tunnelDownGrace, or, with an embedded spot ticker and a positive
// pricePremium, whose spot price exceeds the cheapest zone of their region by more than that fraction. Call it after
// the tunnel and ticker are configured.
func (s *Server) ConfigureNodeReuse(maxAge, tunnelDownGrace time.Duration, pricePremium float64) {
	policy := ReusePolicy{MaxAge: maxAge}
	if t := s.tunnelConfig(); t != nil && !t.DirectFallback {
		policy.Healthy = newTunnelDownTracker(t.Registry.Has, tunnelDownGrace).healthy
	}
	s.mu.RLock()
	ticker := s.spotTicker
	s.mu.RUnlock()
	if ticker != nil && pricePremium > 0 {
		policy.PriceTooHigh = spotPriceAbove(ticker, pricePremium)
	}
	s.nodePool.SetReusePolicy(policy)
}

// spinDownReason returns why an idle node must not be reused, or "" when it may be.
func (p ReusePolicy) spinDownReason(node *CloudVMNode, now time.Time) SpinDownReason {
	switch {
	case p.MaxAge > 0 && now.Sub(node.CreatedAt) >= p.MaxAge:
		return SpinDownMaxAge
	case p.Healthy != nil && !p.Healthy(node):
		return SpinDownUnhealthy
	case p.PriceTooHigh != nil && node.Spot && p.PriceTooHigh(node):
		return SpinDownPrice
	}
	return ""
}

// markSpinDown cordons a node and records why, counting only its first reason. The caller holds the pool lock or owns
// a node not yet in the pool.
func markSpinDown(node *CloudVMNode, reason SpinDownReason) {
	if node.Cordoned {
		return
	}
	node.Cordoned = true
	node.SpinDownReason = reason
	idleMetrics.spinDowns.WithLabelValues(string(reason)).Inc()
	log.Printf("[cloudvm] Marked node %s for spin-down: %s", sanitizeLog(node.InstanceID), reason) //nolint:gosec // sanitized
}

// isIdle reports whether every pod the node ran has finished or left; a node that never ran a pod is not idle.
func (node *CloudVMNode) isIdle() bool {
	return node.IdleSince != nil && node.ActivePodCount() == 0
}

// reusableLocked reports whether an idle node may take a new pod: within its grace period, with the containers of
// earlier pods removed, and allowed by the reuse policy. A node the policy rejects is marked for spin-down.
func (np *NodePool) reusableLocked(node *CloudVMNode, now time.Time) bool {
	if np.idleGracePeriod <= 0 || now.Sub(*node.IdleSince) > np.idleGracePeriod || node.UnknownCoTenants {
		return false
	}
	// A finished pod that Kubernetes has not deleted yet still has its container on the VM.
	if len(node.ActivePods) > 0 {
		return false
	}
	if reason := np.reuse.spinDownReason(node, now); reason != "" {
		markSpinDown(node, reason)
		return false
	}
	return true
}

// checkIdleLocked marks an idle node for spin-down when the reuse policy no longer allows reusing it.
func (np *NodePool) checkIdleLocked(node *CloudVMNode, now time.Time) {
	if node.Cordoned || !node.isIdle() {
		return
	}
	if reason := np.reuse.spinDownReason(node, now); reason != "" {
		markSpinDown(node, reason)
	}
}

// nodeMeetsSpec reports whether a node satisfies the instance constraints of a pod: its provider, GPU type, explicit
// instance types, and Karpenter instance label requirements. A pod that runs natively on the VM never shares one.
func nodeMeetsSpec(node *CloudVMNode, spec *InstanceSpec) bool {
	if spec == nil {
		return true
	}
	if spec.AMIMode == "dlami" {
		return false
	}
	if spec.Provider != "" && !strings.EqualFold(spec.Provider, string(node.Provider)) {
		return false
	}
	if spec.GPUType != "" && !strings.EqualFold(spec.GPUType, node.GPUType) {
		return false
	}
	var types []string
	if spec.InstanceType != "" {
		types = append(types, spec.InstanceType)
	}
	types = append(types, spec.CandidateInstanceTypes...)
	if len(types) > 0 {
		return slices.Contains(types, node.InstanceType)
	}
	if len(spec.InstanceRequirements) == 0 {
		return true
	}
	facts, ok := globalInstanceFacts.cached(node.InstanceType)
	if !ok {
		return false
	}
	capacityType := karpv1.CapacityTypeOnDemand
	if node.Spot {
		capacityType = karpv1.CapacityTypeSpot
	}
	labels := facts.requirements()
	labels.Add(scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, capacityType))
	return labels.Compatible(scheduling.NewNodeSelectorRequirementsWithMinValues(spec.InstanceRequirements...), scheduling.AllowUndefinedWellKnownLabels) == nil
}

// spotPriceAbove returns a PriceTooHigh check that is true when a spot VM's current price in its zone exceeds the
// cheapest current spot price of its instance type in its region by more than premium, a fraction such as 0.2. It
// is false when the ticker has no price for either.
func spotPriceAbove(ticker *SpotPriceTicker, premium float64) func(*CloudVMNode) bool {
	return func(node *CloudVMNode) bool {
		if node.Zone == "" {
			return false
		}
		current, ok := ticker.GetPriceAZ(node.Region, node.Zone, node.InstanceType)
		if !ok {
			return false
		}
		best, ok := ticker.GetPrice(node.Region, node.InstanceType)
		return ok && current.SpotPriceUSD > best.SpotPriceUSD*(1+premium)
	}
}

// tunnelDownTracker reports a VM unhealthy once its reverse tunnel has been seen down for longer than grace, so a
// momentary reconnect does not spin down a warm VM. The runner check before each reuse still rejects a VM it cannot
// reach. Down time counts from the first check that saw the tunnel down; the idle collector checks idle VMs at least
// every 10 seconds.
type tunnelDownTracker struct {
	connected func(instanceID string) bool
	grace     time.Duration
	now       func() time.Time
	mu        sync.Mutex
	downSince map[string]time.Time
}

func newTunnelDownTracker(connected func(string) bool, grace time.Duration) *tunnelDownTracker {
	return &tunnelDownTracker{connected: connected, grace: grace, now: time.Now, downSince: make(map[string]time.Time)}
}

func (t *tunnelDownTracker) healthy(node *CloudVMNode) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.connected(node.InstanceID) {
		delete(t.downSince, node.InstanceID)
		return true
	}
	now := t.now()
	since, seen := t.downSince[node.InstanceID]
	if !seen {
		t.downSince[node.InstanceID] = now
		since = now
	}
	if now.Sub(since) <= t.grace {
		return true
	}
	// The VM is about to be spun down and is never checked again.
	delete(t.downSince, node.InstanceID)
	return false
}
