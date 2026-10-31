// Verifies that lifecycle traces derive stable IDs and that recorded root and stage spans join one trace.
package lifecycletrace

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestDerivedTracesAreStableAndDistinct(t *testing.T) {
	if uid := "uid-1"; ForPod(uid) != derive("pod/"+uid) {
		t.Fatal("the same pod UID must derive the same trace")
	}
	if ForPod("x") == ForNodeClaim("x") {
		t.Fatal("a pod and a NodeClaim with the same key must derive different traces")
	}
	if !ForNodeClaim("gpu-abc").Valid() {
		t.Fatal("derived traces must be valid")
	}
}

func TestRecordRootAndStageShareTheDerivedTrace(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp), sdktrace.WithIDGenerator(IDGenerator{}))
	tracer := tp.Tracer("test")
	tr := ForNodeClaim("gpu-abc")
	start := time.Unix(1000, 0)

	RecordStage(context.Background(), tracer, tr, "stage", start, start.Add(time.Second), nil, nil)
	RecordRoot(context.Background(), tracer, tr, "root", start, start.Add(2*time.Second), errors.New("boom"), nil, ForPod("p").Link())

	spans := exp.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("want 2 spans, got %d", len(spans))
	}
	stage, root := spans[0], spans[1]
	if stage.SpanContext.TraceID() != tr.ID || stage.Parent.SpanID() != tr.Root {
		t.Errorf("stage must be a child of the derived root, got trace %s parent %s", stage.SpanContext.TraceID(), stage.Parent.SpanID())
	}
	if root.SpanContext.TraceID() != tr.ID || root.SpanContext.SpanID() != tr.Root || root.Parent.IsValid() {
		t.Errorf("root must carry the derived IDs and no parent, got %s/%s parent %v", root.SpanContext.TraceID(), root.SpanContext.SpanID(), root.Parent.IsValid())
	}
	if root.Status.Code != codes.Error || len(root.Links) != 1 || root.Links[0].SpanContext.TraceID() != ForPod("p").ID {
		t.Errorf("root must carry its error status and link, got %+v %+v", root.Status, root.Links)
	}
	if !stage.StartTime.Equal(start) || !stage.EndTime.Equal(start.Add(time.Second)) {
		t.Errorf("stage must keep the observed times, got %s..%s", stage.StartTime, stage.EndTime)
	}
}

func TestRecordClampsAnEndBeforeTheStart(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp)).Tracer("test")
	start := time.Unix(1000, 0)
	RecordStage(context.Background(), tracer, ForPod("p"), "stage", start, start.Add(-time.Second), nil, nil)
	if s := exp.GetSpans()[0]; !s.EndTime.Equal(start) {
		t.Errorf("an end before the start must clamp to the start, got %s", s.EndTime)
	}
}

func TestNewProviderRejectsNonHTTPEndpoints(t *testing.T) {
	for _, endpoint := range []string{"", "collector:4318", "grpc://collector:4317"} {
		if _, err := NewProvider(context.Background(), endpoint, "svc"); err == nil {
			t.Errorf("endpoint %q must be rejected", endpoint)
		}
	}
	tp, err := NewProvider(context.Background(), "http://collector.example:4318", "svc")
	if err != nil {
		t.Fatalf("http endpoint: %v", err)
	}
	_ = tp.Shutdown(context.Background())
}
