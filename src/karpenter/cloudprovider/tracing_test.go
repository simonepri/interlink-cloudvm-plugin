// Verifies that Create adds Karpenter's wait and its launch call to the NodeClaim's lifecycle trace.
package cloudprovider

import (
	"context"
	"testing"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim/lifecycletrace"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCreateRecordsItsStagesInTheClaimTrace(t *testing.T) {
	h := newHarness(t, nil)
	spans := tracetest.NewInMemoryExporter()
	h.cp.SetTracer(sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans)).Tracer(TracerName))
	h.api.capacityErrors["g6.2xlarge/eu-west-1a/spot"] = true

	nc := testNodeClaim(req(corev1.LabelInstanceTypeStable, "g6.2xlarge"))
	nc.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Minute).Truncate(time.Second))
	if _, err := h.cp.Create(context.Background(), nc); err == nil {
		t.Fatal("want the capacity error")
	}
	if _, err := h.cp.Create(context.Background(), nc); err != nil {
		t.Fatal(err)
	}
	got := spans.GetSpans()
	if len(got) != 4 {
		t.Fatalf("want pending and create spans for both calls, got %d", len(got))
	}
	tr := lifecycletrace.ForNodeClaim(nc.Name)
	for _, s := range got {
		if s.SpanContext.TraceID() != tr.ID || s.Parent.SpanID() != tr.Root {
			t.Errorf("%s must be a child of the claim trace's root", s.Name)
		}
	}
	if got[0].Name != "cloudvm.karpenter.pending" || !got[0].StartTime.Equal(nc.CreationTimestamp.Time) {
		t.Errorf("first span = %s from %s, want the wait from the NodeClaim's creation", got[0].Name, got[0].StartTime)
	}
	if got[1].Name != "cloudvm.karpenter.create" || got[1].Status.Code != codes.Error {
		t.Errorf("the refused create must carry its error, got %s %+v", got[1].Name, got[1].Status)
	}
	if got[3].Status.Code == codes.Error {
		t.Errorf("the successful create must not carry an error")
	}
}
