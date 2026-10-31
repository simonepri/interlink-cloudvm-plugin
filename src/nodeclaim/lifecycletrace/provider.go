// Builds the tracer provider that batches lifecycle spans to an OTLP/HTTP collector under the component's service name,
// giving lifecycle root spans their derived IDs.

package lifecycletrace

import (
	"context"
	"fmt"
	"net/url"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// NewProvider returns a tracer provider exporting to endpoint, an http(s) URL of an OTLP/HTTP collector whose empty
// path becomes /v1/traces. OTEL_RESOURCE_ATTRIBUTES adds resource attributes; service names the component.
func NewProvider(ctx context.Context, endpoint, service string) (*sdktrace.TracerProvider, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid OTLP traces endpoint %q: want an http(s) URL", endpoint)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/v1/traces"
	}
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(u.String()))
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(attribute.String("service.name", service)))
	if err != nil {
		return nil, fmt.Errorf("build trace resource: %w", err)
	}
	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithIDGenerator(IDGenerator{}),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	), nil
}
