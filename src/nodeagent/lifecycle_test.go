// Verifies that the node agent adds its registration and Ready stages to the NodeClaim's lifecycle trace.
package main

import (
	"context"
	"testing"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim/lifecycletrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"k8s.io/client-go/kubernetes/fake"
)

func TestNodeStagesJoinTheClaimTrace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	spans := tracetest.NewInMemoryExporter()
	claim := gpuClaim("vm-t", nodeclaim.StateLaunching)
	claim.CreatedAt = time.Now().Add(-time.Minute)
	claims := newFakeClaims(claim)
	m := testManager(claims, fake.NewClientset())
	m.cfg.Tracer = sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans)).Tracer(lifecycleTracerName)
	m.Sync(ctx)
	defer m.stopAll()

	claim.State = nodeclaim.StateRunning
	claims.set(claim)
	waitFor(t, "the ready span", func() bool {
		m.Sync(ctx)
		return len(spans.GetSpans()) == 2
	})
	tr := lifecycletrace.ForNodeClaim("vm-t")
	got := spans.GetSpans()
	if got[0].Name != "cloudvm.node.register" || got[1].Name != "cloudvm.node.ready" {
		t.Fatalf("spans = %s, %s", got[0].Name, got[1].Name)
	}
	for _, s := range got {
		if s.SpanContext.TraceID() != tr.ID || s.Parent.SpanID() != tr.Root {
			t.Errorf("%s must be a child of the claim trace's root", s.Name)
		}
	}
	if !got[0].StartTime.Equal(claim.CreatedAt) {
		t.Errorf("registration must start at the claim's launch, got %s", got[0].StartTime)
	}
}
