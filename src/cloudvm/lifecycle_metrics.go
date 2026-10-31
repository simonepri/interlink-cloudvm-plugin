// Prometheus metrics of the pod and VM lifecycle: how long each stage takes, from pod creation to a running container
// and from launch request to a connected worker, and how the plugin answered pod and NodeClaim create requests.

package main

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// lifecycleStageBuckets spans sub-second stages, such as a cached image pull, up to an hour-long wait for capacity.
var lifecycleStageBuckets = []float64{0.5, 1, 2, 5, 10, 20, 30, 45, 60, 90, 120, 180, 240, 300, 450, 600, 900, 1200, 1800, 3600}

// lifecycleLabels are the labels of both stage histograms; the pod and the namespace are span attributes only, which
// keeps the series count bounded by the fleet's offerings.
var lifecycleLabels = []string{"stage", "provider", "region", "instance_type", "capacity_type"}

// LifecycleMetrics holds the lifecycle collectors in a registry of their own.
type LifecycleMetrics struct {
	Registry        *prometheus.Registry
	podStages       *prometheus.HistogramVec
	vmStages        *prometheus.HistogramVec
	createResponses *prometheus.CounterVec
}

// NewLifecycleMetrics registers the lifecycle collectors.
func NewLifecycleMetrics() *LifecycleMetrics {
	m := &LifecycleMetrics{
		Registry: prometheus.NewRegistry(),
		podStages: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cloudvm_pod_stage_duration_seconds",
			Help:    "Duration of one stage of a pod's start, from its creation to its running container",
			Buckets: lifecycleStageBuckets,
		}, lifecycleLabels),
		vmStages: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cloudvm_vm_stage_duration_seconds",
			Help:    "Duration of one stage of a VM's start, from its launch request to its connected worker",
			Buckets: lifecycleStageBuckets,
		}, lifecycleLabels),
		createResponses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cloudvm_create_responses_total",
			Help: "Pod and NodeClaim create requests by the HTTP status the plugin answered, such as 409 for a pod its node's VM refuses",
		}, []string{"api", "code"}),
	}
	m.Registry.MustRegister(m.podStages, m.vmStages, m.createResponses)
	return m
}

// lifecycleOffering is the offering a pod or VM runs on, as the stage histograms label it.
type lifecycleOffering struct {
	provider, region, instanceType, capacityType string
}

func (m *LifecycleMetrics) observePodStage(stage string, o lifecycleOffering, seconds float64) {
	m.podStages.WithLabelValues(stage, o.provider, o.region, o.instanceType, o.capacityType).Observe(max(seconds, 0))
}

func (m *LifecycleMetrics) observeVMStage(stage string, o lifecycleOffering, seconds float64) {
	m.vmStages.WithLabelValues(stage, o.provider, o.region, o.instanceType, o.capacityType).Observe(max(seconds, 0))
}

// countCreateResponses wraps a handler so the status of every POST it answers counts under api.
func (m *LifecycleMetrics) countCreateResponses(api string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			next(w, r)
			return
		}
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next(rec, r)
		m.createResponses.WithLabelValues(api, strconv.Itoa(rec.code)).Inc()
	}
}

// statusRecorder remembers the status code a handler wrote.
type statusRecorder struct {
	http.ResponseWriter
	code        int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.code, r.wroteHeader = code, true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	r.wroteHeader = true
	return r.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the wrapped writer.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
