// Adds Karpenter's stages to each NodeClaim's lifecycle trace: how long the NodeClaim waited for Create, and the
// Create call that chose its offering and launched its VM through the plugin.

package cloudprovider

import (
	"context"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim/lifecycletrace"
	"go.opentelemetry.io/otel/attribute"
	corev1 "k8s.io/api/core/v1"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// traceCreate records the NodeClaim's wait from creation until Create was called, and the Create call itself.
func (c *CloudProvider) traceCreate(nc *karpv1.NodeClaim, called time.Time, out *karpv1.NodeClaim, err error) {
	tr := lifecycletrace.ForNodeClaim(nc.Name)
	attrs := []attribute.KeyValue{attribute.String("cloudvm.nodeclaim", nc.Name)}
	if pool := nc.Labels[karpv1.NodePoolLabelKey]; pool != "" {
		attrs = append(attrs, attribute.String("karpenter.nodepool", pool))
	}
	if !nc.CreationTimestamp.IsZero() {
		lifecycletrace.RecordStage(context.Background(), c.tracer, tr, "cloudvm.karpenter.pending", nc.CreationTimestamp.Time, called, nil, attrs)
	}
	if out != nil {
		for label, key := range map[string]string{
			corev1.LabelTopologyRegion:     "cloud.region",
			corev1.LabelInstanceTypeStable: "cloudvm.instance_type",
			karpv1.CapacityTypeLabelKey:    "cloudvm.capacity_type",
		} {
			if v := out.Labels[label]; v != "" {
				attrs = append(attrs, attribute.String(key, v))
			}
		}
	}
	lifecycletrace.RecordStage(context.Background(), c.tracer, tr, "cloudvm.karpenter.create", called, time.Now(), err, attrs)
}
