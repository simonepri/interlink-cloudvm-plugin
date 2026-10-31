// Adds the node agent's stages to each NodeClaim's lifecycle trace: when the agent registered the claim's Node, and
// when it reported the Node Ready because the claim's worker connected.

package main

import (
	"context"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim/lifecycletrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// lifecycleTracerName is the instrumentation scope of the node agent's lifecycle spans.
const lifecycleTracerName = "github.com/simonepri/interlink-cloudvm-plugin/nodeagent"

func claimAttrs(c *nodeclaim.NodeClaim) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("cloudvm.nodeclaim", c.Name),
		attribute.String("cloud.region", c.Region),
		attribute.String("cloudvm.instance_type", c.InstanceType),
		attribute.String("cloudvm.capacity_type", c.CapacityType),
	}
}

func (m *NodeManager) tracer() trace.Tracer {
	if m.cfg.Tracer == nil {
		return noop.NewTracerProvider().Tracer(lifecycleTracerName)
	}
	return m.cfg.Tracer
}

// traceRegistered records the wait from the claim's launch until this agent registered its Node.
func (m *NodeManager) traceRegistered(c *nodeclaim.NodeClaim, at time.Time) {
	if c.CreatedAt.IsZero() {
		return
	}
	lifecycletrace.RecordStage(context.Background(), m.tracer(), lifecycletrace.ForNodeClaim(c.Name), "cloudvm.node.register",
		c.CreatedAt, at, nil, append(claimAttrs(c), attribute.String("cloudvm.nodeclaim.state", string(c.State))))
}

// traceReady records the wait from the Node's registration until the agent reported it Ready.
func (m *NodeManager) traceReady(c *nodeclaim.NodeClaim, registeredAt, at time.Time) {
	lifecycletrace.RecordStage(context.Background(), m.tracer(), lifecycletrace.ForNodeClaim(c.Name), "cloudvm.node.ready",
		registeredAt, at, nil, claimAttrs(c))
}
