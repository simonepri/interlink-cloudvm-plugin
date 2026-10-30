// Scale test: one shard serving 2000 fake NodeClaim VMs against a fake clientset, measuring agent heap and goroutines
// per node and API calls per node per minute with lease and status intervals compressed by a fixed factor.

package main

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const scaleNodes = 2000

func heapAfterGC() uint64 {
	var ms runtime.MemStats
	for range 3 {
		runtime.GC()
	}
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

func TestScaleTwoThousandNodes(t *testing.T) {
	// Real intervals are a 10 s lease renewal and a 60 s status update; the test runs them speedup times faster.
	speedup := 20
	window := 9 * time.Second
	if raceEnabled {
		window = 6 * time.Second
	}
	renew := 10 * time.Second / time.Duration(speedup)
	status := time.Minute / time.Duration(speedup)

	claims := make([]nodeclaim.NodeClaim, 0, scaleNodes)
	for i := range scaleNodes {
		claims = append(claims, gpuClaim(fmt.Sprintf("vm-%04d", i), nodeclaim.StateRunning))
	}
	// The simple clientset skips server-side field management, which would dominate the fake apiserver's cost.
	kube := fake.NewSimpleClientset() //nolint:staticcheck // see above
	calls := countCalls(kube)
	fc := newFakeClaims(claims...)
	m := NewNodeManager(NodeConfig{
		Shard: Shard{Count: 1}, Claims: fc, Kube: kube, NodeIP: "10.0.0.7", KubeletPort: 10250,
		PollInterval: 10 * time.Second / time.Duration(speedup), LeaseDurationSeconds: 40, LeaseRenewInterval: renew,
		StatusInterval: status, OrphanSweepInterval: time.Hour,
	})

	baseHeap := heapAfterGC()
	baseGoroutines := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { m.Run(ctx); close(runDone) }()

	start := time.Now()
	for calls.snapshot()["create leases"] < scaleNodes {
		if time.Since(start) > 2*time.Minute {
			t.Fatalf("only %d of %d leases after 2 minutes", calls.snapshot()["create leases"], scaleNodes)
		}
		time.Sleep(50 * time.Millisecond)
	}
	registration := time.Since(start)
	// Let every node complete one status cycle so the window measures the steady state.
	time.Sleep(status)

	calls.reset()
	listsBefore := fc.lists.Load()
	time.Sleep(window)
	steady := calls.snapshot()
	lists := fc.lists.Load() - listsBefore

	servingHeap := heapAfterGC()
	goroutines := runtime.NumGoroutine() - baseGoroutines
	cancel()
	<-runDone
	stoppedHeap := heapAfterGC()

	nodes, err := kube.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil || len(nodes.Items) != scaleNodes {
		t.Fatalf("listed %d nodes, err %v; want %d", len(nodes.Items), err, scaleNodes)
	}
	notReady := 0
	for i := range nodes.Items {
		if readyStatus(&nodes.Items[i]) != corev1.ConditionTrue {
			notReady++
		}
	}
	if notReady > 0 {
		t.Errorf("%d of %d nodes are not Ready", notReady, scaleNodes)
	}

	realMinutes := window.Minutes() * float64(speedup)
	perNodeMinute := func(n int) float64 { return float64(n) / scaleNodes / realMinutes }
	total := 0
	keys := make([]string, 0, len(steady))
	for k, v := range steady {
		keys = append(keys, k)
		total += v
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Logf("steady state %-28s %8.2f calls per node per minute", k, perNodeMinute(steady[k]))
	}
	agentHeap := float64(servingHeap) - float64(stoppedHeap)
	t.Logf("nodes=%d registration=%v race=%v", scaleNodes, registration.Round(time.Millisecond), raceEnabled)
	t.Logf("agent heap per node: %.0f bytes (serving %d MiB, after stop %d MiB, before start %d MiB)",
		agentHeap/scaleNodes, servingHeap>>20, stoppedHeap>>20, baseHeap>>20)
	t.Logf("goroutines per node: %.2f", float64(goroutines)/scaleNodes)
	t.Logf("API calls per node per minute: %.2f; claim lists per shard per minute: %.2f",
		perNodeMinute(total), float64(lists)/realMinutes)

	// The kubelet makes 6 lease writes and 1 status write per minute; allow the GET before each status patch and
	// scheduling slack, but fail if anything scales worse than the kubelet.
	if got := perNodeMinute(total); got > 10 {
		t.Errorf("%.2f API calls per node per minute, want at most 10", got)
	}
	for _, k := range keys {
		if k == "list nodes" || k == "list pods" || k == "list secrets" || k == "list configmaps" {
			t.Errorf("steady state made %d %s calls", steady[k], k)
		}
	}
}
