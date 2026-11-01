// Prometheus metrics for the CloudVM Karpenter provider.

package cloudprovider

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// OfferingsDropped counts offerings dropped before Karpenter sees them (e.g. by price cap), partitioned by NodePool.
	OfferingsDropped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "cloudvm",
			Subsystem: "karpenter",
			Name:      "offerings_dropped_total",
			Help:      "Offerings dropped before Karpenter sees them, partitioned by NodePool.",
		},
		[]string{"nodepool"},
	)
)

func registerMetrics(reg prometheus.Registerer) {
	if reg == nil {
		reg = crmetrics.Registry
	}
	if err := reg.Register(OfferingsDropped); err != nil {
		var are prometheus.AlreadyRegisteredError
		if !errors.As(err, &are) {
			panic(err)
		}
	}
}

func init() {
	registerMetrics(crmetrics.Registry)
}
