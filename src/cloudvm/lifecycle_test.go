// Verifies the pod and VM lifecycle recorder: the spans, stage metrics and pod events each runner status and worker
// handshake produce, and what a plugin restarted mid-start records.
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim/lifecycletrace"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// lifecycleEvents collects the reasons of the pod events a Lifecycle posts.
type lifecycleEvents struct {
	mu      sync.Mutex
	reasons []string
	posted  chan struct{}
}

func (e *lifecycleEvents) RecordPodEvent(_ context.Context, _, _, _, _, reason, _ string) error {
	e.mu.Lock()
	e.reasons = append(e.reasons, reason)
	e.mu.Unlock()
	e.posted <- struct{}{}
	return nil
}

// wait returns the reasons once n events were posted.
func (e *lifecycleEvents) wait(t *testing.T, n int) []string {
	t.Helper()
	for range n {
		select {
		case <-e.posted:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %d pod events", n)
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.reasons...)
}

type lifecycleHarness struct {
	l      *Lifecycle
	spans  *tracetest.InMemoryExporter
	events *lifecycleEvents
	now    time.Time
}

func newLifecycleHarness() *lifecycleHarness {
	h := &lifecycleHarness{
		spans:  tracetest.NewInMemoryExporter(),
		events: &lifecycleEvents{posted: make(chan struct{}, 64)},
		now:    time.Unix(10_000, 0),
	}
	h.l = NewLifecycle(NewLifecycleMetrics(), func() PodEventRecorder { return h.events })
	h.l.now = func() time.Time { return h.now }
	h.l.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(h.spans), sdktrace.WithIDGenerator(lifecycletrace.IDGenerator{})))
	return h
}

func (h *lifecycleHarness) spanNames() []string {
	var names []string
	for _, s := range h.spans.GetSpans() {
		names = append(names, s.Name)
	}
	return names
}

func (h *lifecycleHarness) span(t *testing.T, name string) tracetest.SpanStub {
	t.Helper()
	for _, s := range h.spans.GetSpans() {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no span %s among %v", name, h.spanNames())
	return tracetest.SpanStub{}
}

func lifecyclePod(uid string, created time.Time) (*Pod, *InstanceSpec) {
	pod := &Pod{Metadata: PodMeta{Name: "train", Namespace: "team-a", UID: uid, CreationTimestamp: created}}
	pod.Spec.Containers = []ContainerSpec{{Name: "main", Image: "repo/train:1"}}
	spec := &InstanceSpec{PodUID: uid, PodName: "train", PodNamespace: "team-a", TargetContainerImage: "repo/train:1"}
	return pod, spec
}

var lifecycleNode = &CloudVMNode{InstanceID: "i-1", Provider: ProviderAWS, Region: "us-east-2", InstanceType: "g6.xlarge", Spot: true, ClaimName: "gpu-abc"}

func TestLifecycle_PodStartRecordsEveryStageOnce(t *testing.T) {
	h := newLifecycleHarness()
	t0 := h.now
	pod, spec := lifecyclePod("uid-1", t0.Add(-30*time.Second))
	h.l.PodAccepted(pod, spec, "gpu-abc", lifecycleNode)

	timings := &RunnerTimings{AcceptedAt: t0.Add(20 * time.Second), PullStartedAt: t0.Add(21 * time.Second)}
	h.now = t0.Add(25 * time.Second)
	h.l.RunnerStatus(lifecycleNode, "uid-1", &ContainerStatusResult{Status: "created", Reason: "ContainerCreating", Timings: timings})
	pulled := *timings
	pulled.PullFinishedAt, pulled.Image, pulled.PullAttempts = t0.Add(81*time.Second), "mirror/train:1", 1
	running := &ContainerStatusResult{Running: true, Status: "running", StartedAt: t0.Add(83 * time.Second), Timings: &pulled}
	h.now = t0.Add(85 * time.Second)
	h.l.RunnerStatus(lifecycleNode, "uid-1", running)
	h.l.RunnerStatus(lifecycleNode, "uid-1", running)

	want := []string{"cloudvm.pod.pending", "cloudvm.pod.runner_wait", "cloudvm.pod.image_pull", "cloudvm.pod.container_start", "cloudvm.pod.status_report", "cloudvm.pod"}
	if got := h.spanNames(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("spans = %v, want %v", got, want)
	}
	pull := h.span(t, "cloudvm.pod.image_pull")
	if pull.EndTime.Sub(pull.StartTime) != time.Minute {
		t.Errorf("image pull span lasts %s, want the runner's 1m", pull.EndTime.Sub(pull.StartTime))
	}
	root := h.span(t, "cloudvm.pod")
	tr := lifecycletrace.ForPod("uid-1")
	if root.SpanContext.TraceID() != tr.ID || root.SpanContext.SpanID() != tr.Root {
		t.Errorf("root span must carry the pod's derived IDs")
	}
	if !root.StartTime.Equal(t0.Add(-30*time.Second)) || !root.EndTime.Equal(h.now) {
		t.Errorf("root must span creation to the observed start, got %s..%s", root.StartTime, root.EndTime)
	}
	if len(root.Links) != 1 || root.Links[0].SpanContext.TraceID() != lifecycletrace.ForNodeClaim("gpu-abc").ID {
		t.Errorf("root must link the claim's trace, got %+v", root.Links)
	}
	for _, s := range h.spans.GetSpans() {
		if s.Name != "cloudvm.pod" && s.Parent.SpanID() != tr.Root {
			t.Errorf("stage %s must be a child of the root", s.Name)
		}
	}
	if got := h.events.wait(t, 4); strings.Join(slices.Sorted(slices.Values(got)), ",") != "Created,Pulled,Pulling,Started" {
		t.Errorf("events = %v", got)
	}
	if n := testutil.CollectAndCount(h.l.metrics.podStages); n != 6 {
		t.Errorf("want 6 pod stage series (5 stages and ready), got %d", n)
	}
}

func TestLifecycle_PodAdoptedAfterRestartRecordsNothingAlreadyDone(t *testing.T) {
	h := newLifecycleHarness()
	timings := &RunnerTimings{AcceptedAt: h.now, PullStartedAt: h.now, PullFinishedAt: h.now}
	h.l.RunnerStatus(lifecycleNode, "uid-old", &ContainerStatusResult{Running: true, StartedAt: h.now, Timings: timings})
	h.l.RunnerStatus(lifecycleNode, "uid-old", &ContainerStatusResult{Running: true, StartedAt: h.now, Timings: timings})
	if got := h.spanNames(); len(got) != 0 {
		t.Errorf("a pod running before the restart must record no spans, got %v", got)
	}
	select {
	case <-h.events.posted:
		t.Error("a pod running before the restart must get no events")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestLifecycle_StartErrorAndDeleteEndTheTraceWithAnError(t *testing.T) {
	h := newLifecycleHarness()
	pod, spec := lifecyclePod("uid-fail", time.Time{})
	h.l.PodAccepted(pod, spec, "", nil)
	h.l.RunnerStatus(lifecycleNode, "uid-fail", &ContainerStatusResult{Status: "dead", Reason: "StartError", Message: "no GPU"})
	if root := h.span(t, "cloudvm.pod"); root.Status.Code != codes.Error {
		t.Errorf("a start error must end the trace with an error, got %+v", root.Status)
	}

	pod, spec = lifecyclePod("uid-gone", time.Time{})
	h.l.PodAccepted(pod, spec, "", nil)
	h.l.PodDeleted("uid-gone")
	h.l.PodDeleted("uid-gone")
	roots := 0
	for _, s := range h.spans.GetSpans() {
		if s.Name == "cloudvm.pod" && s.SpanContext.TraceID() == lifecycletrace.ForPod("uid-gone").ID {
			roots++
		}
	}
	if roots != 1 {
		t.Errorf("deleting a pod that never ran must end its trace once, got %d roots", roots)
	}
}

func TestLifecycle_ClaimVMRecordsLaunchBootPhasesAndRoot(t *testing.T) {
	h := newLifecycleHarness()
	t0 := h.now
	tr := lifecycletrace.ForNodeClaim("gpu-abc")
	spec := &InstanceSpec{Region: "us-east-2", InstanceType: "g6.xlarge", MarketType: "spot"}
	h.now = t0.Add(3 * time.Second)
	h.l.VMLaunched(tr, spec, "gpu-abc", t0, &Instance{ID: "i-1", Provider: ProviderAWS, Region: "us-east-2", Metadata: map[string]string{}}, nil)

	boot, err := json.Marshal([]BootMark{
		{Phase: "os_boot", At: t0.Add(40 * time.Second)},
		{Phase: "instance_start", At: t0.Add(20 * time.Second)},
		{Phase: "Bad Phase!", At: t0.Add(41 * time.Second)},
		{Phase: "runner", At: t0.Add(90 * time.Second)},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.now = t0.Add(95 * time.Second)
	h.l.WorkerConnected(&TunnelHandshakeRequest{InstanceID: "i-1", Boot: boot})
	h.l.WorkerConnected(&TunnelHandshakeRequest{InstanceID: "i-1", Boot: boot})

	want := "cloudvm.vm.launch,cloudvm.vm.instance_start,cloudvm.vm.os_boot,cloudvm.vm.runner,cloudvm.vm.tunnel_connect,cloudvm.nodeclaim"
	if got := strings.Join(h.spanNames(), ","); got != want {
		t.Fatalf("spans = %s, want %s", got, want)
	}
	if s := h.span(t, "cloudvm.vm.instance_start"); !s.StartTime.Equal(t0.Add(3*time.Second)) || !s.EndTime.Equal(t0.Add(20*time.Second)) {
		t.Errorf("instance_start must run from the launch call's return to kernel boot, got %s..%s", s.StartTime, s.EndTime)
	}
	root := h.span(t, "cloudvm.nodeclaim")
	if root.SpanContext.SpanID() != tr.Root || !root.StartTime.Equal(t0) || !root.EndTime.Equal(h.now) {
		t.Errorf("claim root must carry the derived ID and span launch request to connect, got %s..%s", root.StartTime, root.EndTime)
	}
	if got := h.l.LogTraceID("i-1", ""); got != tr.ID.String() {
		t.Errorf("host logs of a claim VM must join the claim's trace, got %q", got)
	}
}

func TestLifecycle_FailedClaimLaunchEndsTheClaimTrace(t *testing.T) {
	h := newLifecycleHarness()
	h.l.VMLaunched(lifecycletrace.ForNodeClaim("gpu-ice"), &InstanceSpec{Region: "us-east-2"}, "gpu-ice", h.now, nil, errRegionFallbackTest)
	if root := h.span(t, "cloudvm.nodeclaim"); root.Status.Code != codes.Error {
		t.Errorf("a failed claim launch must end its trace with an error")
	}
	if launch := h.span(t, "cloudvm.vm.launch"); launch.Status.Code != codes.Error {
		t.Errorf("a failed launch span must carry the error")
	}
}

var errRegionFallbackTest = &claimAPIError{status: http.StatusServiceUnavailable, reason: "InsufficientCapacity", msg: "InsufficientInstanceCapacity"}

func TestLifecycle_CountsCreateResponsesByStatus(t *testing.T) {
	m := NewLifecycleMetrics()
	handler := m.countCreateResponses("pod", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "claim VM refuses pod", http.StatusConflict)
	})
	handler(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/create", nil))
	handler(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/create", nil))
	if got := testutil.ToFloat64(m.createResponses.WithLabelValues("pod", "409")); got != 1 {
		t.Errorf("want one counted 409, got %v", got)
	}
}

func TestTunnelHandshake_MalformedBootMarksNeverFailTheHandshake(t *testing.T) {
	var req TunnelHandshakeRequest
	if err := json.Unmarshal([]byte(`{"instanceId":"i-1","boot":[{"phase":"os_boot","at":"yesterday"}]}`), &req); err != nil {
		t.Fatalf("a malformed boot list must still decode the handshake: %v", err)
	}
	if req.InstanceID != "i-1" || req.BootMarks() != nil {
		t.Errorf("want the instance and no boot marks, got %q %v", req.InstanceID, req.BootMarks())
	}
}
