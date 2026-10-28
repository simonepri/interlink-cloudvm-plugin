// Tests reuse of idle VMs: reuse within the grace period, namespace and instance constraints, spin-down reasons, and
// mutual exclusion between allocation and every path that terminates VMs.
package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func reuseTestRequest() ResourceVector {
	return ResourceVector{CPU: resource.MustParse("2"), Memory: resource.MustParse("4Gi")}
}

// idleNode registers a node that ran one pod of namespace team-a and released it.
func idleNode(t *testing.T, pool *NodePool, id string) *CloudVMNode {
	t.Helper()
	node := NewCloudVMNode(id, ProviderAWS, "us-east-1", "10.0.0.1", ResourceVector{CPU: resource.MustParse("8"), Memory: resource.MustParse("32Gi")})
	node.InstanceType = "m5.2xlarge"
	pool.RegisterAndAllocate(node, reuseTestRequest(), "first-"+id, "team-a", "first")
	pool.Release("first-" + id)
	if !node.isIdle() || node.Cordoned {
		t.Fatalf("expected %s idle and allocatable", id)
	}
	return node
}

func spinDownCount(reason SpinDownReason) float64 {
	return counterValue(idleMetrics.spinDowns.WithLabelValues(string(reason)))
}

func TestNodePool_ReusesIdleNodeWithinGracePeriod(t *testing.T) {
	pool := NewNodePool(time.Minute)
	node := idleNode(t, pool, "i-idle")

	alloc, err := pool.AllocatePod(AllocationRequest{Resources: reuseTestRequest(), PodUID: "second", Namespace: "team-a", PodName: "second"})
	if err != nil {
		t.Fatalf("expected the idle node to be reused: %v", err)
	}
	if alloc.Node != node || !alloc.Reused {
		t.Fatalf("expected reuse of %s, got %+v", node.InstanceID, alloc)
	}
	if node.IdleSince != nil {
		t.Fatalf("expected reuse to clear IdleSince")
	}

	// Packing a second pod onto the now busy node is not a reuse.
	alloc, err = pool.AllocatePod(AllocationRequest{Resources: reuseTestRequest(), PodUID: "third", Namespace: "team-a", PodName: "third"})
	if err != nil || alloc.Reused {
		t.Fatalf("expected a plain bin-pack hit, got %+v, %v", alloc, err)
	}
}

func TestNodePool_DoesNotReuseIdleNodeAfterGracePeriod(t *testing.T) {
	pool := NewNodePool(time.Minute)
	node := idleNode(t, pool, "i-expired")
	past := time.Now().Add(-2 * time.Minute)
	node.IdleSince = &past

	if _, err := pool.AllocatePod(AllocationRequest{Resources: reuseTestRequest(), PodUID: "late", Namespace: "team-a"}); err == nil {
		t.Fatalf("expected no reuse of a node past its idle grace period")
	}
}

func TestNodePool_DoesNotReuseIdleNodeWithoutGracePeriod(t *testing.T) {
	pool := NewNodePool(0)
	idleNode(t, pool, "i-no-grace")

	if _, err := pool.AllocatePod(AllocationRequest{Resources: reuseTestRequest(), PodUID: "p", Namespace: "team-a"}); err == nil {
		t.Fatalf("expected no reuse when the idle grace period is zero")
	}
}

func TestNodePool_DoesNotReuseIdleNodeAcrossNamespaces(t *testing.T) {
	pool := NewNodePool(time.Minute)
	idleNode(t, pool, "i-team-a")

	if _, err := pool.AllocatePod(AllocationRequest{Resources: reuseTestRequest(), PodUID: "b", Namespace: "team-b"}); err == nil {
		t.Fatalf("expected an idle node of team-a to refuse a team-b pod")
	}
	if _, err := pool.AllocatePod(AllocationRequest{Resources: reuseTestRequest(), PodUID: "a", Namespace: "team-a"}); err != nil {
		t.Fatalf("expected the idle node to take a pod of its own namespace: %v", err)
	}
}

func TestNodePool_DoesNotReuseIdleNodeWithSpinDownReason(t *testing.T) {
	cases := []struct {
		name   string
		policy ReusePolicy
		reason SpinDownReason
	}{
		{"max-age", ReusePolicy{MaxAge: time.Hour}, SpinDownMaxAge},
		{"unhealthy", ReusePolicy{Healthy: func(*CloudVMNode) bool { return false }}, SpinDownUnhealthy},
		{"price", ReusePolicy{PriceTooHigh: func(*CloudVMNode) bool { return true }}, SpinDownPrice},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := NewNodePool(time.Minute)
			node := idleNode(t, pool, "i-"+tc.name)
			node.Spot = true
			node.CreatedAt = time.Now().Add(-2 * time.Hour)
			pool.SetReusePolicy(tc.policy)
			before := spinDownCount(tc.reason)

			if _, err := pool.AllocatePod(AllocationRequest{Resources: reuseTestRequest(), PodUID: "p", Namespace: "team-a"}); err == nil {
				t.Fatalf("expected no reuse")
			}
			if !node.Cordoned || node.SpinDownReason != tc.reason {
				t.Fatalf("expected spin-down %q, got cordoned=%v reason=%q", tc.reason, node.Cordoned, node.SpinDownReason)
			}
			if got := spinDownCount(tc.reason) - before; got != 1 {
				t.Fatalf("spin-down metric for %q grew by %v, want 1", tc.reason, got)
			}
			// The collector still terminates it once the grace period ends.
			past := time.Now().Add(-2 * time.Minute)
			node.IdleSince = &past
			if expired := pool.CollectExpiredIdleNodes(); len(expired) != 1 {
				t.Fatalf("expected the spun-down node to be collected, got %d", len(expired))
			}
		})
	}
}

func TestNodePool_MarksIdleNodeForSpinDownOnRelease(t *testing.T) {
	pool := NewNodePool(time.Minute)
	pool.SetReusePolicy(ReusePolicy{MaxAge: time.Hour})
	node := NewCloudVMNode("i-old", ProviderAWS, "us-east-1", "10.0.0.1", ResourceVector{CPU: resource.MustParse("8"), Memory: resource.MustParse("32Gi")})
	node.CreatedAt = time.Now().Add(-2 * time.Hour)
	pool.RegisterAndAllocate(node, reuseTestRequest(), "p", "team-a", "p")

	pool.Release("p")
	if node.SpinDownReason != SpinDownMaxAge {
		t.Fatalf("expected the old node marked max-age when it went idle, got %q", node.SpinDownReason)
	}
}

func TestServer_DoesNotReuseVMAfterSpotInterruptionNotice(t *testing.T) {
	s := newIdleTestServer(NewMockProvider())
	s.SetIdleGracePeriod(time.Minute)
	node := NewCloudVMNode("i-interrupted", ProviderAWS, "us-east-1", "10.0.0.1", ResourceVector{CPU: resource.MustParse("8"), Memory: resource.MustParse("32Gi")})
	s.nodePool.RegisterAndAllocate(node, reuseTestRequest(), "p", "team-a", "p")

	s.handleSpotInterruption(context.Background(), node.InstanceID, SpotInterruptionNotice{Action: "terminate"})
	s.nodePool.Release("p")

	if _, err := s.nodePool.AllocatePod(AllocationRequest{Resources: reuseTestRequest(), PodUID: "q", Namespace: "team-a"}); err == nil {
		t.Fatalf("expected no reuse of a VM that received an interruption notice")
	}
	if node.SpinDownReason != SpinDownInterruption {
		t.Fatalf("expected spin-down reason %q, got %q", SpinDownInterruption, node.SpinDownReason)
	}
}

func TestServer_LaunchesNewVMWhenIdleVMFailsRunnerCheck(t *testing.T) {
	prov := NewMockProvider()
	s := newIdleTestServer(prov)
	s.SetIdleGracePeriod(time.Minute)
	runner := &failingContainerRunner{}
	s.SetContainerRunner(runner)
	ts := httptest.NewServer(s.Routes())
	defer ts.Close()

	post := func(path, body string) {
		t.Helper()
		resp, err := http.Post(ts.URL+path, "application/json", strings.NewReader(body)) //nolint:noctx // test request
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		_ = resp.Body.Close()
	}
	pod := func(name string) string {
		return fmt.Sprintf(`{"pod":{"metadata":{"name":%q,"namespace":"team-a","uid":"uid-%s"},"spec":{"containers":[{"name":"c","image":"busybox","resources":{"requests":{"cpu":"1","memory":"1Gi"}}}]}}}`, name, name)
	}

	post("/create", pod("one"))
	first, _, ok := s.nodePool.FindPodNode("uid-one")
	if !ok {
		t.Fatalf("pod one not placed")
	}
	post("/delete", `{"pod":{"metadata":{"name":"one","namespace":"team-a","uid":"uid-one"}}}`)
	if !first.isIdle() || first.Cordoned {
		t.Fatalf("expected the VM held warm and allocatable after delete")
	}

	reusesBefore := counterValue(idleMetrics.reuses)
	runner.mu.Lock()
	runner.shouldFail = true
	runner.mu.Unlock()
	post("/create", pod("two"))

	second, _, ok := s.nodePool.FindPodNode("uid-two")
	if !ok || second == first {
		t.Fatalf("expected pod two on a new VM, got %v", second)
	}
	if first.SpinDownReason != SpinDownUnhealthy || first.ActivePodCount() != 0 {
		t.Fatalf("expected the unreachable VM marked unhealthy without pods, got reason %q, %d pods", first.SpinDownReason, first.ActivePodCount())
	}
	if prov.LaunchCount() != 2 {
		t.Fatalf("expected 2 launches, got %d", prov.LaunchCount())
	}
	if got := counterValue(idleMetrics.reuses) - reusesBefore; got != 0 {
		t.Fatalf("reuse metric grew by %v for a rejected reuse", got)
	}

	// A healthy idle VM is reused and counted.
	runner.mu.Lock()
	runner.shouldFail = false
	runner.mu.Unlock()
	post("/delete", `{"pod":{"metadata":{"name":"two","namespace":"team-a","uid":"uid-two"}}}`)
	post("/create", pod("three"))
	if third, _, _ := s.nodePool.FindPodNode("uid-three"); third != second {
		t.Fatalf("expected pod three to reuse %s", second.InstanceID)
	}
	if got := counterValue(idleMetrics.reuses) - reusesBefore; got != 1 {
		t.Fatalf("reuse metric grew by %v, want 1", got)
	}
}

// TestNodePool_AllocateAndIdleCollectionAreExclusive runs allocations against the idle collector; run it with -race.
func TestNodePool_AllocateAndIdleCollectionAreExclusive(t *testing.T) {
	const nodes = 200
	pool := NewNodePool(20 * time.Millisecond)
	for i := range nodes {
		idleNode(t, pool, fmt.Sprintf("i-%03d", i))
	}

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		allocated = map[*CloudVMNode]bool{}
		collected = map[*CloudVMNode]bool{}
	)
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range nodes {
				alloc, err := pool.AllocatePod(AllocationRequest{Resources: reuseTestRequest(), PodUID: fmt.Sprintf("w%d-%d", w, i), Namespace: "team-a"})
				if err != nil {
					continue
				}
				mu.Lock()
				allocated[alloc.Node] = true
				mu.Unlock()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			for _, node := range pool.CollectExpiredIdleNodes() {
				mu.Lock()
				collected[node] = true
				mu.Unlock()
			}
		}
	}()
	wg.Wait()
	t.Logf("%d nodes reused, %d collected", len(allocated), len(collected))

	for node := range collected {
		if allocated[node] {
			t.Fatalf("node %s was both collected for termination and given a pod", node.InstanceID)
		}
	}
	if len(allocated)+len(collected) != nodes {
		t.Fatalf("%d allocated + %d collected nodes, want %d in total", len(allocated), len(collected), nodes)
	}
}

// TestNodePool_AllocateAndTerminationClaimsAreExclusive runs allocations against the reconciler's termination claims;
// run it with -race.
func TestNodePool_AllocateAndTerminationClaimsAreExclusive(t *testing.T) {
	const nodes = 200
	pool := NewNodePool(time.Minute)
	ids := make([]string, nodes)
	for i := range nodes {
		ids[i] = fmt.Sprintf("i-%03d", i)
		node := NewCloudVMNode(ids[i], ProviderAWS, "us-east-1", "10.0.0.1", ResourceVector{CPU: resource.MustParse("8"), Memory: resource.MustParse("32Gi")})
		node.Namespace = "team-a"
		pool.RegisterNode(node)
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		claimed []string
	)
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range nodes {
				_, _ = pool.AllocatePod(AllocationRequest{Resources: reuseTestRequest(), PodUID: fmt.Sprintf("w%d-%d", w, i), Namespace: "team-a"})
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, id := range ids {
			if pool.ClaimForTermination(id) {
				mu.Lock()
				claimed = append(claimed, id)
				mu.Unlock()
			}
		}
	}()
	wg.Wait()

	for _, id := range claimed {
		if pool.HasActivePods(id) {
			t.Fatalf("node %s was claimed for termination and given a pod", id)
		}
	}
}

func TestNodePool_ClaimForTerminationKeepsIdleNodeInGracePeriod(t *testing.T) {
	pool := NewNodePool(time.Minute)
	node := idleNode(t, pool, "i-warm")
	if pool.ClaimForTermination(node.InstanceID) {
		t.Fatalf("expected an idle node in its grace period to be kept for reuse")
	}
	past := time.Now().Add(-2 * time.Minute)
	node.IdleSince = &past
	if !pool.ClaimForTermination(node.InstanceID) || node.SpinDownReason != SpinDownOrphaned {
		t.Fatalf("expected an expired idle node to be claimed and marked orphaned, got reason %q", node.SpinDownReason)
	}
	if _, err := pool.AllocatePod(AllocationRequest{Resources: reuseTestRequest(), PodUID: "p", Namespace: "team-a"}); err == nil {
		t.Fatalf("expected no allocation on a node claimed for termination")
	}
}

func TestNodePool_ReturnUnterminatedNodeIsNotReused(t *testing.T) {
	pool := NewNodePool(time.Minute)
	node := idleNode(t, pool, "i-stuck")
	past := time.Now().Add(-2 * time.Minute)
	node.IdleSince = &past
	if expired := pool.CollectExpiredIdleNodes(); len(expired) != 1 {
		t.Fatalf("expected the node collected")
	}
	if pool.HasNode(node.InstanceID) {
		t.Fatalf("expected a collected node out of the pool")
	}
	pool.ReturnUnterminated(node)
	now := time.Now()
	node.IdleSince = &now
	if _, err := pool.AllocatePod(AllocationRequest{Resources: reuseTestRequest(), PodUID: "p", Namespace: "team-a"}); err == nil {
		t.Fatalf("expected no reuse of a node whose termination failed")
	}
	if node.SpinDownReason != SpinDownTerminateFailed {
		t.Fatalf("expected reason %q, got %q", SpinDownTerminateFailed, node.SpinDownReason)
	}
}

func TestNodeMeetsSpec(t *testing.T) {
	node := NewCloudVMNode("i-g5", ProviderAWS, "us-east-1", "", ResourceVector{})
	node.InstanceType = "g5.xlarge"
	node.Spot = true
	family := func(values ...string) []karpv1.NodeSelectorRequirementWithMinValues {
		return []karpv1.NodeSelectorRequirementWithMinValues{{Key: labelInstanceFamily, Operator: corev1.NodeSelectorOpIn, Values: values}}
	}
	cases := []struct {
		name string
		spec *InstanceSpec
		want bool
	}{
		{"no constraints", &InstanceSpec{}, true},
		{"same type", &InstanceSpec{InstanceType: "g5.xlarge"}, true},
		{"other type", &InstanceSpec{InstanceType: "g5.2xlarge"}, false},
		{"candidate type", &InstanceSpec{InstanceType: "g6.xlarge", CandidateInstanceTypes: []string{"g5.xlarge"}}, true},
		{"matching family", &InstanceSpec{InstanceRequirements: family("g5")}, true},
		{"other family", &InstanceSpec{InstanceRequirements: family("g6")}, false},
		{"on-demand only", &InstanceSpec{InstanceRequirements: []karpv1.NodeSelectorRequirementWithMinValues{{Key: karpv1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{karpv1.CapacityTypeOnDemand}}}}, false},
		{"other provider", &InstanceSpec{Provider: "gcp"}, false},
		{"other GPU type", &InstanceSpec{GPUType: "nvidia-l4"}, false},
		{"native AMI", &InstanceSpec{AMIMode: "dlami"}, false},
	}
	for _, tc := range cases {
		if got := nodeMeetsSpec(node, tc.spec); got != tc.want {
			t.Errorf("%s: nodeMeetsSpec = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestSpotPriceAbove(t *testing.T) {
	ticker := NewSpotPriceTicker([]string{"us-east-1"}, []string{"g5.xlarge"}, time.Minute)
	ticker.mu.Lock()
	ticker.prices[tickerKey("us-east-1", "us-east-1a", "g5.xlarge")] = &SpotPriceEntry{Region: "us-east-1", AvailabilityZone: "us-east-1a", InstanceType: "g5.xlarge", SpotPriceUSD: 0.50}
	ticker.prices[tickerKey("us-east-1", "us-east-1b", "g5.xlarge")] = &SpotPriceEntry{Region: "us-east-1", AvailabilityZone: "us-east-1b", InstanceType: "g5.xlarge", SpotPriceUSD: 0.40}
	ticker.mu.Unlock()
	node := &CloudVMNode{Region: "us-east-1", Zone: "us-east-1a", InstanceType: "g5.xlarge", Spot: true}

	if !spotPriceAbove(ticker, 0.2)(node) {
		t.Fatalf("expected 0.50 to exceed 0.40 by more than 20%%")
	}
	if spotPriceAbove(ticker, 0.3)(node) {
		t.Fatalf("expected 0.50 within 30%% of 0.40")
	}
	node.Zone = ""
	if spotPriceAbove(ticker, 0.2)(node) {
		t.Fatalf("expected no verdict without the node's zone")
	}
}
