// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// idleCollectorMetrics counts the idle nodes and the terminations of the idle collector.
type idleCollectorMetrics struct {
	Registry     *prometheus.Registry
	idleNodes    prometheus.Gauge
	terminations prometheus.Counter
	errors       prometheus.Counter
}

func newIdleCollectorMetrics() *idleCollectorMetrics {
	m := &idleCollectorMetrics{
		Registry: prometheus.NewRegistry(),
		idleNodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cloudvm_idle_nodes",
			Help: "Tracked VMs that have no active pod",
		}),
		terminations: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cloudvm_idle_terminations_total",
			Help: "VMs the idle collector terminated after their idle grace period",
		}),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cloudvm_idle_termination_errors_total",
			Help: "Failed idle VM terminations; the collector retries each on its next pass",
		}),
	}
	m.Registry.MustRegister(m.idleNodes, m.terminations, m.errors)
	return m
}

// idleMetrics is the process-wide idle collector metrics registry.
var idleMetrics = newIdleCollectorMetrics()

// idleCollectorInterval returns how often the collector looks for nodes past their idle grace period.
func idleCollectorInterval(grace time.Duration) time.Duration {
	if grace <= 0 || grace >= 10*time.Second {
		return 10 * time.Second
	}
	return max(grace/2, time.Second)
}

// RunIdleCollector terminates nodes whose idle grace period expired until ctx is cancelled.
func (s *Server) RunIdleCollector(ctx context.Context) {
	ticker := time.NewTicker(idleCollectorInterval(s.nodePool.IdleGracePeriod()))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.CollectIdleNodes(ctx)
		}
	}
}

// CollectIdleNodes terminates every node whose idle grace period expired and updates the idle node gauge.
func (s *Server) CollectIdleNodes(ctx context.Context) {
	expired := s.nodePool.CollectExpiredIdleNodes()
	idleMetrics.idleNodes.Set(float64(s.nodePool.IdleNodeCount() + len(expired)))
	s.TerminateIdleNodes(ctx, expired)
}

// TerminateIdleNodes terminates idle VMs in batched or parallel provider calls and forgets each one it terminated. A
// node it fails to terminate goes back into the pool, so the next collector pass retries it.
func (s *Server) TerminateIdleNodes(ctx context.Context, nodes []*CloudVMNode) {
	byProvider := make(map[ProviderType][]*CloudVMNode)
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if s.isInstanceTombstoned(node.InstanceID) {
			log.Printf("[cloudvm] Skipping idle node %s: another path already terminated it", sanitizeLog(node.InstanceID)) //nolint:gosec // sanitized
			continue
		}
		byProvider[node.Provider] = append(byProvider[node.Provider], node)
	}
	for pt, group := range byProvider {
		prov := s.ProviderForType(pt)
		if prov == nil {
			for _, node := range group {
				log.Printf("[cloudvm] Keeping idle node %s: no %q provider is registered to terminate it", sanitizeLog(node.InstanceID), pt) //nolint:gosec // sanitized
				idleMetrics.errors.Inc()
				s.nodePool.RegisterNode(node)
			}
			continue
		}
		ids := make([]string, len(group))
		for i, node := range group {
			ids[i] = node.InstanceID
			log.Printf("[cloudvm] Terminating expired idle node %s", sanitizeLog(node.InstanceID)) //nolint:gosec // sanitized
		}
		errs := terminateInstances(ctx, prov, ids)
		for _, node := range group {
			if err := errs[node.InstanceID]; err != nil {
				log.Printf("[cloudvm] Error terminating idle instance %s, retrying on the next pass: %v", sanitizeLog(node.InstanceID), err) //nolint:gosec // sanitized
				idleMetrics.errors.Inc()
				if !s.isInstanceTombstoned(node.InstanceID) {
					s.nodePool.RegisterNode(node)
				}
				continue
			}
			idleMetrics.terminations.Inc()
			s.forgetTerminatedInstance(node.InstanceID)
		}
	}
}

// isInstanceTombstoned reports whether a path that terminates VMs already recorded instanceID as terminated.
func (s *Server) isInstanceTombstoned(instanceID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isTombstonedLocked(instanceID)
}

// forgetTerminatedInstance drops a terminated VM from the node pool, instance records, and token store, and
// tombstones it so no other path terminates or adopts it again.
func (s *Server) forgetTerminatedInstance(instanceID string) {
	s.nodePool.UnregisterNode(instanceID)
	if s.tokenStore != nil {
		_ = s.tokenStore.Delete(instanceID)
	}
	s.mu.Lock()
	for k, inst := range s.instances {
		if inst != nil && inst.ID == instanceID {
			delete(s.instances, k)
		}
	}
	s.tombstones[instanceID] = time.Now()
	s.mu.Unlock()
	// The persistent store write happens outside s.mu so a slow store does not stall every request handler.
	if s.tombstoneStore != nil {
		_ = s.tombstoneStore.Set(instanceID)
	}
}
