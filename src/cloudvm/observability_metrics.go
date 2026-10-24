// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ObservabilityMetrics manages Prometheus metric collectors and exposition for GPU market and fleet observability.
type ObservabilityMetrics struct {
	Registry *prometheus.Registry

	// Market & Pricing metrics
	gpuPrice      *prometheus.GaugeVec
	vcpuPrice     *prometheus.GaugeVec
	instancePrice *prometheus.GaugeVec

	// Availability & Signals
	availScore     *prometheus.GaugeVec
	expectedUptime *prometheus.GaugeVec
	signalAge      *prometheus.GaugeVec

	// Quotas
	quotaLimit *prometheus.GaugeVec
	quotaUsed  *prometheus.GaugeVec

	// Ladder Fill Quotes
	fillPrice   *prometheus.GaugeVec
	fillGPUs    *prometheus.GaugeVec
	fillLimited *prometheus.GaugeVec

	// Fleet Metrics
	instancesRunning *prometheus.GaugeVec
	gpusRunning      *prometheus.GaugeVec
	spendPerHour     *prometheus.GaugeVec
	launchAttempts   *prometheus.CounterVec
	launchDuration   *prometheus.HistogramVec
	interruptions    *prometheus.CounterVec
	instanceSeconds  *prometheus.CounterVec

	// Legacy compatibility metrics (kept for 1 release)
	legacySpotPrice      *prometheus.GaugeVec
	legacyOnDemandPrice  *prometheus.GaugeVec
	legacySpotSavings    *prometheus.GaugeVec
	legacyPlacementScore *prometheus.GaugeVec
}

// GlobalMetrics is the singleton metrics registry.
var GlobalMetrics = NewObservabilityMetrics()

// NewObservabilityMetrics creates and registers all market and fleet observability metrics.
func NewObservabilityMetrics() *ObservabilityMetrics {
	reg := prometheus.NewRegistry()

	marketLabels := []string{"provider", "region", "zone", "gpu_model", "gpu_memory_gb", "gpu_count", "capacity_type", "instance_type"}
	fleetLabels := []string{"provider", "region", "gpu_model", "capacity_type"}

	m := &ObservabilityMetrics{
		Registry: reg,

		gpuPrice: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_gpu_price_usd_per_hour",
			Help: "Current price in USD per GPU-hour (all-in instance price divided by GPU count)",
		}, marketLabels),

		vcpuPrice: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_vcpu_price_usd_per_hour",
			Help: "Current price in USD per vCPU-hour for CPU-only shapes",
		}, []string{"provider", "region", "zone", "capacity_type", "instance_type"}),

		instancePrice: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_instance_price_usd_per_hour",
			Help: "Current price in USD per instance-hour",
		}, marketLabels),

		availScore: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_availability_score",
			Help: "Current availability score between 0.0 and 1.0 indicating capacity depth",
		}, []string{"provider", "region", "zone", "gpu_model", "instance_type", "source", "target_gpus"}),

		expectedUptime: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_expected_uptime_seconds",
			Help: "Expected uptime in seconds before preemption or spot interruption",
		}, []string{"provider", "region", "instance_type", "gpu_model", "source"}),

		signalAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_signal_age_seconds",
			Help: "Age in seconds of the provider market signal",
		}, []string{"provider", "region", "source"}),

		quotaLimit: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_quota_gpus",
			Help: "Configured GPU quota limit",
		}, []string{"provider", "region", "quota"}),

		quotaUsed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_quota_used_gpus",
			Help: "Currently utilized GPU quota",
		}, []string{"provider", "region", "quota"}),

		fillPrice: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_fill_price_usd_per_gpu_hour",
			Help: "Blended fill quote price in USD per GPU-hour for N GPUs at a given memory floor",
		}, []string{"min_gpu_memory_gb", "gpus", "max_regions", "capacity_type"}),

		fillGPUs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_fill_gpus",
			Help: "Number of GPUs allocated from a provider, region, and GPU model to satisfy a fill quote",
		}, []string{"min_gpu_memory_gb", "gpus", "max_regions", "capacity_type", "provider", "region", "gpu_model"}),

		fillLimited: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_fill_limited",
			Help: "Flag indicating whether a fill quote was constrained short of the target capacity (1 if limited, 0 otherwise)",
		}, []string{"min_gpu_memory_gb", "gpus", "max_regions", "capacity_type", "reason"}),

		instancesRunning: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_instances_running",
			Help: "Current number of running cloud VM instances",
		}, fleetLabels),

		gpusRunning: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_gpus_running",
			Help: "Current number of running GPUs across all cloud VM instances",
		}, fleetLabels),

		spendPerHour: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_spend_usd_per_hour",
			Help: "Current run-rate spend in USD per hour computed from fulfilled instance prices",
		}, fleetLabels),

		launchAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cloudvm_launch_attempts_total",
			Help: "Total number of instance launch attempts classified by outcome result",
		}, []string{"provider", "region", "gpu_model", "capacity_type", "result"}),

		launchDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cloudvm_launch_duration_seconds",
			Help:    "Duration in seconds from instance launch request until worker is ready",
			Buckets: []float64{5, 15, 30, 60, 120, 180, 300, 600},
		}, fleetLabels),

		interruptions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cloudvm_interruptions_total",
			Help: "Total number of unexpected instance interruptions classified by provider signal",
		}, fleetLabels),

		instanceSeconds: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cloudvm_instance_seconds_total",
			Help: "Total cumulative instance runtime in seconds",
		}, fleetLabels),

		// Legacy metrics
		legacySpotPrice: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_spot_price_usd_per_hour",
			Help: "Legacy gauge: Current spot price in USD per hour",
		}, []string{"region", "availability_zone", "instance_type"}),

		legacyOnDemandPrice: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_ondemand_price_usd_per_hour",
			Help: "Legacy gauge: Baseline on-demand price in USD per hour",
		}, []string{"region", "instance_type"}),

		legacySpotSavings: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_spot_savings_percentage",
			Help: "Legacy gauge: Spot savings percentage relative to on-demand",
		}, []string{"region", "availability_zone", "instance_type"}),

		legacyPlacementScore: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_spot_placement_score",
			Help: "Legacy gauge: Spot placement score (1-10)",
		}, []string{"region", "availability_zone", "instance_type"}),
	}

	reg.MustRegister(
		m.gpuPrice,
		m.vcpuPrice,
		m.instancePrice,
		m.availScore,
		m.expectedUptime,
		m.signalAge,
		m.quotaLimit,
		m.quotaUsed,
		m.fillPrice,
		m.fillGPUs,
		m.fillLimited,
		m.instancesRunning,
		m.gpusRunning,
		m.spendPerHour,
		m.launchAttempts,
		m.launchDuration,
		m.interruptions,
		m.instanceSeconds,
		m.legacySpotPrice,
		m.legacyOnDemandPrice,
		m.legacySpotSavings,
		m.legacyPlacementScore,
	)

	return m
}

// Handler returns the HTTP handler exposing Prometheus metrics for scraping.
func (m *ObservabilityMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: false,
	})
}

// SetGPUPrice exports all-in price per GPU-hour and full instance price.
func (m *ObservabilityMetrics) SetGPUPrice(provider, region, zone, gpuModel string, gpuMemoryGB, gpuCount int, capacityType, instanceType string, instancePriceUSD float64) {
	if gpuCount <= 0 {
		return
	}
	pricePerGPU := instancePriceUSD / float64(gpuCount)
	memGBStr := fmt.Sprintf("%d", gpuMemoryGB)
	countStr := fmt.Sprintf("%d", gpuCount)

	m.gpuPrice.WithLabelValues(provider, region, zone, gpuModel, memGBStr, countStr, capacityType, instanceType).Set(pricePerGPU)
	m.instancePrice.WithLabelValues(provider, region, zone, gpuModel, memGBStr, countStr, capacityType, instanceType).Set(instancePriceUSD)
}

// SetVCPUPrice exports price per vCPU-hour for CPU-only shapes.
func (m *ObservabilityMetrics) SetVCPUPrice(provider, region, zone, capacityType, instanceType string, vcpuPriceUSD float64) {
	m.vcpuPrice.WithLabelValues(provider, region, zone, capacityType, instanceType).Set(vcpuPriceUSD)
}

// SetAvailabilityScore exports availability score (0.0 to 1.0).
func (m *ObservabilityMetrics) SetAvailabilityScore(provider, region, zone, gpuModel, instanceType, source string, targetGPUs int, score float64) {
	targetStr := fmt.Sprintf("%d", targetGPUs)
	m.availScore.WithLabelValues(provider, region, zone, gpuModel, instanceType, source, targetStr).Set(score)
}

// SetExpectedUptime exports expected uptime in seconds before interruption.
func (m *ObservabilityMetrics) SetExpectedUptime(provider, region, instanceType, gpuModel, source string, uptimeSeconds float64) {
	m.expectedUptime.WithLabelValues(provider, region, instanceType, gpuModel, source).Set(uptimeSeconds)
}

// SetSignalAge exports signal age in seconds.
func (m *ObservabilityMetrics) SetSignalAge(provider, region, source string, ageSeconds float64) {
	m.signalAge.WithLabelValues(provider, region, source).Set(ageSeconds)
}

// SetQuota exports GPU quota limits and utilized GPUs.
func (m *ObservabilityMetrics) SetQuota(provider, region, quota string, limitGPUs, usedGPUs float64) {
	m.quotaLimit.WithLabelValues(provider, region, quota).Set(limitGPUs)
	m.quotaUsed.WithLabelValues(provider, region, quota).Set(usedGPUs)
}

// SetFillQuote exports solver fill quotes for a specific request.
func (m *ObservabilityMetrics) SetFillQuote(minGPUMemoryGB, gpus int, maxRegions, capacityType string, pricePerGPUHour float64, _ map[string]float64, limitedReason string) {
	memStr := fmt.Sprintf("%d", minGPUMemoryGB)
	gpusStr := fmt.Sprintf("%d", gpus)

	m.fillPrice.WithLabelValues(memStr, gpusStr, maxRegions, capacityType).Set(pricePerGPUHour)

	if limitedReason != "" {
		m.fillLimited.WithLabelValues(memStr, gpusStr, maxRegions, capacityType, limitedReason).Set(1)
	} else {
		m.fillLimited.WithLabelValues(memStr, gpusStr, maxRegions, capacityType, "none").Set(0)
	}
}

// SetFillGPUAllocation exports individual allocation contributions to a fill quote.
func (m *ObservabilityMetrics) SetFillGPUAllocation(minGPUMemoryGB, gpus int, maxRegions, capacityType, provider, region, gpuModel string, count float64) {
	memStr := fmt.Sprintf("%d", minGPUMemoryGB)
	gpusStr := fmt.Sprintf("%d", gpus)
	m.fillGPUs.WithLabelValues(memStr, gpusStr, maxRegions, capacityType, provider, region, gpuModel).Set(count)
}

// ResetFleetGauges resets running instance, GPU, and spend gauges before a fresh reconciliation pass.
func (m *ObservabilityMetrics) ResetFleetGauges() {
	m.instancesRunning.Reset()
	m.gpusRunning.Reset()
	m.spendPerHour.Reset()
}

// SetFleetGauges sets live instance, GPU count, and run-rate spend.
func (m *ObservabilityMetrics) SetFleetGauges(provider, region, gpuModel, capacityType string, instances, gpus, spendPerHour float64) {
	m.instancesRunning.WithLabelValues(provider, region, gpuModel, capacityType).Set(instances)
	m.gpusRunning.WithLabelValues(provider, region, gpuModel, capacityType).Set(gpus)
	m.spendPerHour.WithLabelValues(provider, region, gpuModel, capacityType).Set(spendPerHour)
}

// RecordLaunchAttempt increments launch attempt counters classified by outcome result.
func (m *ObservabilityMetrics) RecordLaunchAttempt(provider, region, gpuModel, capacityType, result string) {
	m.launchAttempts.WithLabelValues(provider, region, gpuModel, capacityType, result).Inc()
}

// RecordLaunchDuration records the launch duration histogram observation.
func (m *ObservabilityMetrics) RecordLaunchDuration(provider, region, gpuModel, capacityType string, seconds float64) {
	m.launchDuration.WithLabelValues(provider, region, gpuModel, capacityType).Observe(seconds)
}

// RecordInterruption increments the unexpected interruption counter.
func (m *ObservabilityMetrics) RecordInterruption(provider, region, gpuModel, capacityType string) {
	m.interruptions.WithLabelValues(provider, region, gpuModel, capacityType).Inc()
}

// AddInstanceSeconds increments cumulative instance seconds.
func (m *ObservabilityMetrics) AddInstanceSeconds(provider, region, gpuModel, capacityType string, seconds float64) {
	m.instanceSeconds.WithLabelValues(provider, region, gpuModel, capacityType).Add(seconds)
}

// SetLegacySpotMetrics updates legacy gauges for backward compatibility.
func (m *ObservabilityMetrics) SetLegacySpotMetrics(region, az, itype string, spotPrice, onDemandPrice, savingsPct float64) {
	zone := az
	if zone == "" {
		zone = "all"
	}
	m.legacySpotPrice.WithLabelValues(region, zone, itype).Set(spotPrice)
	m.legacyOnDemandPrice.WithLabelValues(region, itype).Set(onDemandPrice)
	m.legacySpotSavings.WithLabelValues(region, zone, itype).Set(savingsPct)
}

// SetLegacyPlacementScore updates legacy placement score gauge.
func (m *ObservabilityMetrics) SetLegacyPlacementScore(region, az, itype string, score float64) {
	zone := az
	if zone == "" {
		zone = "all"
	}
	m.legacyPlacementScore.WithLabelValues(region, zone, itype).Set(score)
}
