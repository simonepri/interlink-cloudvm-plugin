package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// counterValue reads the current value of a counter.
func counterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	_ = c.Write(&m)
	return m.GetCounter().GetValue()
}

// slowTerminateProvider is a mock provider whose Terminate takes delay and that counts calls per instance.
type slowTerminateProvider struct {
	*MockProvider
	delay    time.Duration
	inFlight atomic.Int32
	peak     atomic.Int32
	mu       sync.Mutex
	calls    map[string]int
}

func newSlowTerminateProvider(delay time.Duration) *slowTerminateProvider {
	return &slowTerminateProvider{MockProvider: NewMockProvider(), delay: delay, calls: make(map[string]int)}
}

func (p *slowTerminateProvider) Terminate(ctx context.Context, instanceID string) error {
	cur := p.inFlight.Add(1)
	defer p.inFlight.Add(-1)
	for peak := p.peak.Load(); cur > peak; peak = p.peak.Load() {
		if p.peak.CompareAndSwap(peak, cur) {
			break
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(p.delay):
	}
	p.mu.Lock()
	p.calls[instanceID]++
	p.mu.Unlock()
	_ = p.MockProvider.Terminate(ctx, instanceID)
	return nil
}

// terminated returns how many distinct instances were terminated.
func (p *slowTerminateProvider) terminated() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

// assertTerminatedOnce fails unless every instance was terminated exactly once.
func (p *slowTerminateProvider) assertTerminatedOnce(t *testing.T, ids []string) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.calls) != len(ids) {
		t.Fatalf("terminated %d instances, want %d", len(p.calls), len(ids))
	}
	for _, id := range ids {
		if p.calls[id] != 1 {
			t.Fatalf("instance %s terminated %d times, want 1", id, p.calls[id])
		}
	}
}

// newIdleTestServer returns a server whose only provider is prov and that persists nothing to disk.
func newIdleTestServer(prov Provider) *Server {
	s := NewServer(prov, "test-cluster")
	s.RegisterProvider(ProviderMock, prov)
	s.tokenStore, s.completedStore, s.tombstoneStore = nil, nil, nil
	return s
}

// registerBusyNodes launches n mock VMs and puts one pod on each, returning the instance IDs.
func registerBusyNodes(t *testing.T, s *Server, prov *slowTerminateProvider, n int) []string {
	t.Helper()
	capacity, _ := NewResourceVector("2", "4Gi", 0, 0)
	req, _ := NewResourceVector("1", "1Gi", 0, 0)
	ids := make([]string, n)
	for i := range n {
		inst, err := prov.Launch(context.Background(), &InstanceSpec{PodName: fmt.Sprintf("pod-%d", i), PodUID: fmt.Sprintf("uid-%d", i), ClusterTag: "test-cluster"})
		if err != nil {
			t.Fatalf("launch: %v", err)
		}
		node := NewCloudVMNode(inst.ID, ProviderMock, "", inst.IP, capacity)
		s.nodePool.RegisterAndAllocate(node, req, fmt.Sprintf("uid-%d", i), "default", fmt.Sprintf("pod-%d", i))
		ids[i] = inst.ID
	}
	return ids
}

// releaseAll releases every pod at once, as when a whole job is deleted.
func releaseAll(s *Server, n int) {
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.nodePool.Release(fmt.Sprintf("uid-%d", i))
		}()
	}
	wg.Wait()
}

func TestIdleCollector_TerminatesManyReleasedNodesWithinGraceAndOneInterval(t *testing.T) {
	const nodes = 500
	const delay = 50 * time.Millisecond
	grace := 200 * time.Millisecond

	prov := newSlowTerminateProvider(delay)
	s := newIdleTestServer(prov)
	s.SetIdleGracePeriod(grace)
	ids := registerBusyNodes(t, s, prov, nodes)
	terminatedBefore := counterValue(idleMetrics.terminations)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.RunIdleCollector(ctx)
	releasedAt := time.Now()
	releaseAll(s, nodes)

	// Serial termination would take nodes*delay = 25s; the window allows the grace period, one collector interval, and
	// one second of parallel provider calls.
	window := grace + idleCollectorInterval(grace) + time.Second
	deadline := releasedAt.Add(window)
	for prov.terminated() < nodes {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d idle nodes terminated %v after release", prov.terminated(), nodes, window)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	prov.assertTerminatedOnce(t, ids)
	if peak := prov.peak.Load(); peak > terminateConcurrency {
		t.Fatalf("peak concurrent Terminate calls %d exceed the bound %d", peak, terminateConcurrency)
	}
	if got := counterValue(idleMetrics.terminations) - terminatedBefore; got != nodes {
		t.Fatalf("idle terminations metric grew by %v, want %d", got, nodes)
	}
	for _, id := range ids {
		if !s.isInstanceTombstoned(id) {
			t.Fatalf("terminated instance %s is not tombstoned", id)
		}
	}
}

func TestIdleCollector_RetriesFailedTerminationOnNextPass(t *testing.T) {
	prov := newSlowTerminateProvider(0)
	s := newIdleTestServer(&failingTerminatorProvider{slowTerminateProvider: prov})
	s.SetIdleGracePeriod(time.Millisecond)
	ids := registerBusyNodes(t, s, prov, 1)
	releaseAll(s, 1)
	time.Sleep(5 * time.Millisecond)
	errorsBefore := counterValue(idleMetrics.errors)

	s.CollectIdleNodes(context.Background())
	if !s.nodePool.HasNode(ids[0]) {
		t.Fatalf("node whose termination failed left the pool; no pass would retry it")
	}
	if got := counterValue(idleMetrics.errors) - errorsBefore; got != 1 {
		t.Fatalf("idle termination errors metric grew by %v, want 1", got)
	}

	s.RegisterProvider(ProviderMock, prov)
	s.CollectIdleNodes(context.Background())
	if s.nodePool.HasNode(ids[0]) {
		t.Fatalf("node still tracked after a successful retry")
	}
	prov.assertTerminatedOnce(t, ids)
}

// failingTerminatorProvider fails every Terminate call.
type failingTerminatorProvider struct {
	*slowTerminateProvider
}

func (p *failingTerminatorProvider) Terminate(context.Context, string) error {
	return fmt.Errorf("throttled")
}

func TestReconciler_TerminatesManyZombiesInParallelAndCollectorSkipsThem(t *testing.T) {
	const nodes = 500
	prov := newSlowTerminateProvider(50 * time.Millisecond)
	s := newIdleTestServer(prov)
	s.SetIdleGracePeriod(time.Millisecond)
	ids := registerBusyNodes(t, s, prov, nodes)
	releaseAll(s, nodes)

	r := NewReconciler(prov, s, prov, "test-cluster", time.Millisecond)
	s.SetReconciler(r)
	time.Sleep(5 * time.Millisecond)

	start := time.Now()
	res, err := r.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("reconciling %d zombies took %v; terminations ran serially", nodes, elapsed)
	}
	if len(res.TerminatedZombies) != nodes {
		t.Fatalf("terminated %d zombies, want %d", len(res.TerminatedZombies), nodes)
	}
	if n := s.nodePool.ActiveNodeCount(); n != 0 {
		t.Fatalf("%d reaped VMs stay in the node pool for the idle collector to terminate again", n)
	}

	s.CollectIdleNodes(context.Background())
	prov.assertTerminatedOnce(t, ids)
}

func TestTerminateIdleNodes_SkipsNodeAnotherPathTerminated(t *testing.T) {
	prov := newSlowTerminateProvider(0)
	s := newIdleTestServer(prov)
	ids := registerBusyNodes(t, s, prov, 1)
	node, _ := s.nodePool.Node(ids[0])
	s.forgetTerminatedInstance(ids[0])

	s.TerminateIdleNodes(context.Background(), []*CloudVMNode{node})
	prov.assertTerminatedOnce(t, nil)
}
