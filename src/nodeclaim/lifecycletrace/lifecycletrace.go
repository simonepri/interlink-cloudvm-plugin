// Package lifecycletrace derives the trace of a NodeClaim's or pod's lifecycle from its name or UID, so the Karpenter
// provider, the plugin and the node agent add the stages they observe to one trace without passing trace context.
package lifecycletrace

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Trace names one lifecycle trace and the span ID of its root span.
type Trace struct {
	ID   trace.TraceID
	Root trace.SpanID
}

// ForNodeClaim returns the trace that follows the NodeClaim, and the VM under it, from its creation until it is ready.
func ForNodeClaim(name string) Trace { return derive("nodeclaim/" + name) }

// ForPod returns the trace that follows the pod with the UID from its creation until its container runs.
func ForPod(uid string) Trace { return derive("pod/" + uid) }

func derive(key string) Trace {
	sum := sha256.Sum256([]byte(key))
	var t Trace
	copy(t.ID[:], sum[:16])
	copy(t.Root[:], sum[16:24])
	return t
}

// Valid reports whether the trace has a usable ID; the zero Trace has none.
func (t Trace) Valid() bool { return t.ID.IsValid() && t.Root.IsValid() }

// Parent returns ctx with the trace's root span as the remote parent, so spans started from it are children of the
// root whichever process records the root.
func (t Trace) Parent(ctx context.Context) context.Context {
	return trace.ContextWithRemoteSpanContext(ctx, t.spanContext())
}

// Link returns a span link to the trace's root span.
func (t Trace) Link(attrs ...attribute.KeyValue) trace.Link {
	return trace.Link{SpanContext: t.spanContext(), Attributes: attrs}
}

func (t Trace) spanContext() trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: t.ID, SpanID: t.Root, TraceFlags: trace.FlagsSampled, Remote: true})
}

type rootKey struct{}

// rootContext returns ctx without a parent span, marked so the root span started from it gets the trace's IDs.
func (t Trace) rootContext(ctx context.Context) context.Context {
	return context.WithValue(trace.ContextWithSpanContext(ctx, trace.SpanContext{}), rootKey{}, t)
}

// RecordRoot records the trace's root span, which covers the whole lifecycle from start to end.
func RecordRoot(ctx context.Context, tracer trace.Tracer, t Trace, name string, start, end time.Time, err error, attrs []attribute.KeyValue, links ...trace.Link) {
	record(t.rootContext(ctx), tracer, name, start, end, err, attrs, links)
}

// RecordStage records a finished stage of the lifecycle as a child of the trace's root span.
func RecordStage(ctx context.Context, tracer trace.Tracer, t Trace, name string, start, end time.Time, err error, attrs []attribute.KeyValue) {
	record(t.Parent(ctx), tracer, name, start, end, err, attrs, nil)
}

func record(ctx context.Context, tracer trace.Tracer, name string, start, end time.Time, err error, attrs []attribute.KeyValue, links []trace.Link) {
	if end.Before(start) {
		// Stage ends observed on another machine's clock can precede a start observed here.
		end = start
	}
	_, span := tracer.Start(ctx, name, trace.WithTimestamp(start), trace.WithAttributes(attrs...), trace.WithLinks(links...))
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
	}
	span.End(trace.WithTimestamp(end))
}

// IDGenerator gives a root span recorded by RecordRoot its trace's derived IDs and every other span random IDs.
type IDGenerator struct{}

var _ sdktrace.IDGenerator = IDGenerator{}

// NewIDs returns the derived IDs of a lifecycle root span, or random ones.
func (IDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	if t, ok := ctx.Value(rootKey{}).(Trace); ok && t.Valid() {
		return t.ID, t.Root
	}
	var tid trace.TraceID
	for !tid.IsValid() {
		_, _ = rand.Read(tid[:])
	}
	return tid, randomSpanID()
}

// NewSpanID returns a random span ID for a child span.
func (IDGenerator) NewSpanID(context.Context, trace.TraceID) trace.SpanID { return randomSpanID() }

func randomSpanID() trace.SpanID {
	var sid trace.SpanID
	for !sid.IsValid() {
		_, _ = rand.Read(sid[:])
	}
	return sid
}
