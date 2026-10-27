// Package main implements the interlink-cloudvm-plugin daemon.
// This file exports Prometheus metrics for reverse tunnel sessions: connected workers per region, refused handshakes, and
// bootstrap script requests.
package main

import (
	"github.com/prometheus/client_golang/prometheus"
)

// TunnelMetrics owns the Prometheus registry for reverse tunnel gateway telemetry.
type TunnelMetrics struct {
	Registry   *prometheus.Registry
	rejections *prometheus.CounterVec
	bootstraps *prometheus.CounterVec
}

// NewTunnelMetrics registers a per-region session gauge, computed at scrape time from the session registry, and
// rejection and bootstrap request counters.
func NewTunnelMetrics(sessions *SessionRegistry, regionOf func(instanceID string) string) *TunnelMetrics {
	reg := prometheus.NewRegistry()
	m := &TunnelMetrics{
		Registry: reg,
		rejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cloudvm_tunnel_handshake_rejections_total",
			Help: "Reverse tunnel handshakes refused by the gateway, by reason.",
		}, []string{"reason"}),
		bootstraps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cloudvm_tunnel_bootstrap_requests_total",
			Help: "Bootstrap script requests the gateway answered, by result.",
		}, []string{"result"}),
	}
	reg.MustRegister(m.rejections, m.bootstraps, &tunnelSessionCollector{sessions: sessions, regionOf: regionOf})
	return m
}

// RecordRejection increments the rejection counter for reason.
func (m *TunnelMetrics) RecordRejection(reason string) {
	m.rejections.WithLabelValues(reason).Inc()
}

// RecordBootstrapRequest increments the bootstrap request counter for result.
func (m *TunnelMetrics) RecordBootstrapRequest(result string) {
	m.bootstraps.WithLabelValues(result).Inc()
}

var tunnelSessionsDesc = prometheus.NewDesc(
	"cloudvm_tunnel_sessions",
	"Workers with a connected reverse tunnel session, by cloud region.",
	[]string{"region"}, nil,
)

type tunnelSessionCollector struct {
	sessions *SessionRegistry
	regionOf func(instanceID string) string
}

func (c *tunnelSessionCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- tunnelSessionsDesc
}

func (c *tunnelSessionCollector) Collect(ch chan<- prometheus.Metric) {
	counts := make(map[string]int)
	for _, id := range c.sessions.List() {
		region := ""
		if c.regionOf != nil {
			region = c.regionOf(id)
		}
		if region == "" {
			region = "unknown"
		}
		counts[region]++
	}
	for region, n := range counts {
		ch <- prometheus.MustNewConstMetric(tunnelSessionsDesc, prometheus.GaugeValue, float64(n), region)
	}
}
