// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

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
	instancesRunning     *prometheus.GaugeVec
	vcpusRunning         *prometheus.GaugeVec
	memoryGiBRunning     *prometheus.GaugeVec
	gpusRunning          *prometheus.GaugeVec
	vramGBRunning        *prometheus.GaugeVec
	spendPerHour         *prometheus.GaugeVec
	podsRunning          *prometheus.GaugeVec
	spendBaselinePerHour *prometheus.GaugeVec
	launchAttempts       *prometheus.CounterVec
	launchDuration       *prometheus.HistogramVec
	interruptions        *prometheus.CounterVec
	instanceSeconds      *prometheus.CounterVec
	networkRxBytes       *prometheus.CounterVec
	networkTxBytes       *prometheus.CounterVec

	// Legacy compatibility metrics (kept for 1 release)
	legacySpotPrice      *prometheus.GaugeVec
	legacyOnDemandPrice  *prometheus.GaugeVec
	legacySpotSavings    *prometheus.GaugeVec
	legacyPlacementScore *prometheus.GaugeVec

	market marketSeries
}

// GlobalMetrics is the singleton metrics registry.
var GlobalMetrics = NewObservabilityMetrics()

// NewObservabilityMetrics creates and registers all market and fleet observability metrics.
func NewObservabilityMetrics() *ObservabilityMetrics {
	reg := prometheus.NewRegistry()

	marketLabels := []string{"provider", "region", "zone", "accelerator", "gpu_model", "gpu_memory_gb", "gpu_count", "capacity_type", "instance_type"}
	fleetLabels := []string{"provider", "region", "accelerator", "instance_type", "gpu_model", "capacity_type"}
	networkLabels := []string{"provider", "region", "accelerator", "instance_type"}

	m := &ObservabilityMetrics{
		Registry: reg,

		gpuPrice: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_gpu_price_usd_per_hour",
			Help: "Current price in USD per GPU-hour (all-in instance price divided by GPU count)",
		}, marketLabels),

		vcpuPrice: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_vcpu_price_usd_per_hour",
			Help: "Current price in USD per vCPU-hour for CPU-only shapes",
		}, []string{"provider", "region", "zone", "accelerator", "capacity_type", "instance_type"}),

		instancePrice: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_instance_price_usd_per_hour",
			Help: "Current price in USD per instance-hour",
		}, marketLabels),

		availScore: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_availability_score",
			Help: "Current availability score between 0.0 and 1.0 indicating capacity depth",
		}, []string{"provider", "region", "zone", "accelerator", "gpu_model", "instance_type", "source", "target_gpus"}),

		expectedUptime: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_expected_uptime_seconds",
			Help: "Expected uptime in seconds before preemption or spot interruption",
		}, []string{"provider", "region", "accelerator", "instance_type", "gpu_model", "source"}),

		signalAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_signal_age_seconds",
			Help: "Age in seconds of the provider market signal",
		}, []string{"provider", "region", "source"}),

		quotaLimit: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_quota_gpus",
			Help: "Configured GPU quota limit",
		}, []string{"provider", "region", "accelerator", "quota"}),

		quotaUsed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_quota_used_gpus",
			Help: "Currently utilized GPU quota",
		}, []string{"provider", "region", "accelerator", "quota"}),

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

		vcpusRunning: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_vcpus_running",
			Help: "Current number of running vCPUs across all cloud VM instances",
		}, fleetLabels),

		memoryGiBRunning: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_memory_gib_running",
			Help: "Current number of running system memory in GiB across all cloud VM instances",
		}, fleetLabels),

		gpusRunning: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_gpus_running",
			Help: "Current number of running GPUs across all cloud VM instances",
		}, fleetLabels),

		vramGBRunning: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_vram_gb_running",
			Help: "Current number of running GPU VRAM in GB across all cloud VM instances",
		}, fleetLabels),

		spendPerHour: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_spend_usd_per_hour",
			Help: "Current run-rate spend in USD per hour computed from fulfilled instance prices",
		}, fleetLabels),

		podsRunning: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_pods_running",
			Help: "Current number of running pods across cloud VM instances",
		}, fleetLabels),

		spendBaselinePerHour: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_spend_ondemand_baseline_usd_per_hour",
			Help: "Current run-rate on-demand baseline spend in USD per hour for running instances",
		}, fleetLabels),

		launchAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cloudvm_launch_attempts_total",
			Help: "Total number of instance launch attempts classified by outcome result",
		}, []string{"provider", "region", "accelerator", "instance_type", "gpu_model", "capacity_type", "result"}),

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

		networkRxBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cloudvm_network_receive_bytes_total",
			Help: "Total cumulative network bytes received across cloud VM instances",
		}, networkLabels),

		networkTxBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cloudvm_network_transmit_bytes_total",
			Help: "Total cumulative network bytes transmitted across cloud VM instances",
		}, networkLabels),

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
		m.vcpusRunning,
		m.memoryGiBRunning,
		m.gpusRunning,
		m.vramGBRunning,
		m.spendPerHour,
		m.podsRunning,
		m.spendBaselinePerHour,
		m.launchAttempts,
		m.launchDuration,
		m.interruptions,
		m.instanceSeconds,
		m.networkRxBytes,
		m.networkTxBytes,
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
func (m *ObservabilityMetrics) SetGPUPrice(provider, region, zone, accelerator, gpuModel string, gpuMemoryGB, gpuCount int, capacityType, instanceType string, instancePriceUSD float64) {
	if gpuCount <= 0 {
		return
	}
	if accelerator == "" {
		accelerator = "gpu"
	}
	pricePerGPU := instancePriceUSD / float64(gpuCount)
	memGBStr := fmt.Sprintf("%d", gpuMemoryGB)
	countStr := fmt.Sprintf("%d", gpuCount)

	m.market.set(m.gpuPrice, pricePerGPU, provider, region, zone, accelerator, gpuModel, memGBStr, countStr, capacityType, instanceType)
	m.market.set(m.instancePrice, instancePriceUSD, provider, region, zone, accelerator, gpuModel, memGBStr, countStr, capacityType, instanceType)
}

// SetVCPUPrice exports price per vCPU-hour for CPU-only shapes.
func (m *ObservabilityMetrics) SetVCPUPrice(provider, region, zone, capacityType, instanceType string, vcpuPriceUSD float64) {
	m.market.set(m.vcpuPrice, vcpuPriceUSD, provider, region, zone, "none", capacityType, instanceType)
}

// SetAvailabilityScore exports availability score (0.0 to 1.0).
func (m *ObservabilityMetrics) SetAvailabilityScore(provider, region, zone, accelerator, gpuModel, instanceType, source string, targetGPUs int, score float64) {
	if accelerator == "" {
		accelerator = "gpu"
	}
	targetStr := fmt.Sprintf("%d", targetGPUs)
	m.market.set(m.availScore, score, provider, region, zone, accelerator, gpuModel, instanceType, source, targetStr)
}

// SetExpectedUptime exports expected uptime in seconds before interruption.
func (m *ObservabilityMetrics) SetExpectedUptime(provider, region, accelerator, instanceType, gpuModel, source string, uptimeSeconds float64) {
	if accelerator == "" {
		accelerator = "gpu"
	}
	m.market.set(m.expectedUptime, uptimeSeconds, provider, region, accelerator, instanceType, gpuModel, source)
}

// SetSignalAge exports signal age in seconds.
func (m *ObservabilityMetrics) SetSignalAge(provider, region, source string, ageSeconds float64) {
	m.market.set(m.signalAge, ageSeconds, provider, region, source)
}

// SetQuota exports GPU quota limits and utilized GPUs.
func (m *ObservabilityMetrics) SetQuota(provider, region, accelerator, quota string, limitGPUs, usedGPUs float64) {
	if accelerator == "" {
		accelerator = "gpu"
	}
	m.market.set(m.quotaLimit, limitGPUs, provider, region, accelerator, quota)
	m.market.set(m.quotaUsed, usedGPUs, provider, region, accelerator, quota)
}

// SetFillQuote exports solver fill quotes for a specific request.
func (m *ObservabilityMetrics) SetFillQuote(minGPUMemoryGB, gpus int, maxRegions, capacityType string, pricePerGPUHour float64, _ map[string]float64, limitedReason string) {
	memStr := fmt.Sprintf("%d", minGPUMemoryGB)
	gpusStr := fmt.Sprintf("%d", gpus)

	m.market.set(m.fillPrice, pricePerGPUHour, memStr, gpusStr, maxRegions, capacityType)

	if limitedReason != "" {
		m.market.set(m.fillLimited, 1, memStr, gpusStr, maxRegions, capacityType, limitedReason)
	} else {
		m.market.set(m.fillLimited, 0, memStr, gpusStr, maxRegions, capacityType, "none")
	}
}

// SetFillGPUAllocation exports individual allocation contributions to a fill quote.
func (m *ObservabilityMetrics) SetFillGPUAllocation(minGPUMemoryGB, gpus int, maxRegions, capacityType, provider, region, gpuModel string, count float64) {
	memStr := fmt.Sprintf("%d", minGPUMemoryGB)
	gpusStr := fmt.Sprintf("%d", gpus)
	m.market.set(m.fillGPUs, count, memStr, gpusStr, maxRegions, capacityType, provider, region, gpuModel)
}

// marketSeries remembers which label sets each market refresh pass set, so the next sweep drops series no pass reports anymore.
type marketSeries struct {
	mu       sync.Mutex
	current  map[*prometheus.GaugeVec]map[string][]string
	previous map[*prometheus.GaugeVec]map[string][]string
}

func (s *marketSeries) set(vec *prometheus.GaugeVec, value float64, labels ...string) {
	vec.WithLabelValues(labels...).Set(value)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		s.current = make(map[*prometheus.GaugeVec]map[string][]string)
	}
	if s.current[vec] == nil {
		s.current[vec] = make(map[string][]string)
	}
	s.current[vec][strings.Join(labels, "\x00")] = labels
}

func (s *marketSeries) sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for vec, series := range s.previous {
		for key, labels := range series {
			if _, ok := s.current[vec][key]; !ok {
				vec.DeleteLabelValues(labels...)
			}
		}
	}
	s.previous = s.current
	s.current = nil
}

// SweepMarketGauges ends a market refresh pass: it deletes market series the previous pass set and this pass did not.
func (m *ObservabilityMetrics) SweepMarketGauges() {
	m.market.sweep()
}

// ResetFleetGauges resets running instance, vCPU, memory, GPU, VRAM, and spend gauges before a fresh reconciliation pass.
func (m *ObservabilityMetrics) ResetFleetGauges() {
	m.instancesRunning.Reset()
	m.vcpusRunning.Reset()
	m.memoryGiBRunning.Reset()
	m.gpusRunning.Reset()
	m.vramGBRunning.Reset()
	m.spendPerHour.Reset()
	m.podsRunning.Reset()
	m.spendBaselinePerHour.Reset()
}

// SetFleetGauges sets live instance, vCPU, memory, GPU, VRAM count, run-rate spend, pods, and baseline spend.
func (m *ObservabilityMetrics) SetFleetGauges(provider, region, accelerator, instanceType, gpuModel, capacityType string, instances, vcpus, memGiB, gpus, vramGB, spendPerHour float64, extras ...float64) {
	if accelerator == "" {
		accelerator = "none"
	}
	if instanceType == "" {
		instanceType = "none"
	}
	if gpuModel == "" {
		gpuModel = "none"
	}
	m.instancesRunning.WithLabelValues(provider, region, accelerator, instanceType, gpuModel, capacityType).Set(instances)
	m.vcpusRunning.WithLabelValues(provider, region, accelerator, instanceType, gpuModel, capacityType).Set(vcpus)
	m.memoryGiBRunning.WithLabelValues(provider, region, accelerator, instanceType, gpuModel, capacityType).Set(memGiB)
	m.gpusRunning.WithLabelValues(provider, region, accelerator, instanceType, gpuModel, capacityType).Set(gpus)
	m.vramGBRunning.WithLabelValues(provider, region, accelerator, instanceType, gpuModel, capacityType).Set(vramGB)
	m.spendPerHour.WithLabelValues(provider, region, accelerator, instanceType, gpuModel, capacityType).Set(spendPerHour)
	if len(extras) > 0 {
		m.podsRunning.WithLabelValues(provider, region, accelerator, instanceType, gpuModel, capacityType).Set(extras[0])
	}
	if len(extras) > 1 {
		m.spendBaselinePerHour.WithLabelValues(provider, region, accelerator, instanceType, gpuModel, capacityType).Set(extras[1])
	}
}

// RecordLaunchAttempt increments launch attempt counters classified by outcome result.
func (m *ObservabilityMetrics) RecordLaunchAttempt(provider, region, accelerator, instanceType, gpuModel, capacityType, result string) {
	if accelerator == "" {
		accelerator = "none"
	}
	if instanceType == "" {
		instanceType = "none"
	}
	if gpuModel == "" {
		gpuModel = "none"
	}
	m.launchAttempts.WithLabelValues(provider, region, accelerator, instanceType, gpuModel, capacityType, result).Inc()
}

// RecordLaunchDuration records the launch duration histogram observation.
func (m *ObservabilityMetrics) RecordLaunchDuration(provider, region, accelerator, instanceType, gpuModel, capacityType string, seconds float64) {
	if accelerator == "" {
		accelerator = "none"
	}
	if instanceType == "" {
		instanceType = "none"
	}
	if gpuModel == "" {
		gpuModel = "none"
	}
	m.launchDuration.WithLabelValues(provider, region, accelerator, instanceType, gpuModel, capacityType).Observe(seconds)
}

// RecordInterruption increments the unexpected interruption counter.
func (m *ObservabilityMetrics) RecordInterruption(provider, region, accelerator, instanceType, gpuModel, capacityType string) {
	if accelerator == "" {
		accelerator = "none"
	}
	if instanceType == "" {
		instanceType = "none"
	}
	if gpuModel == "" {
		gpuModel = "none"
	}
	m.interruptions.WithLabelValues(provider, region, accelerator, instanceType, gpuModel, capacityType).Inc()
}

// AddInstanceSeconds increments cumulative instance seconds.
func (m *ObservabilityMetrics) AddInstanceSeconds(provider, region, accelerator, instanceType, gpuModel, capacityType string, seconds float64) {
	if accelerator == "" {
		accelerator = "none"
	}
	if instanceType == "" {
		instanceType = "none"
	}
	if gpuModel == "" {
		gpuModel = "none"
	}
	m.instanceSeconds.WithLabelValues(provider, region, accelerator, instanceType, gpuModel, capacityType).Add(seconds)
}

// SetLegacySpotMetrics updates legacy gauges for backward compatibility.
func (m *ObservabilityMetrics) SetLegacySpotMetrics(region, az, itype string, spotPrice, onDemandPrice, savingsPct float64) {
	zone := az
	if zone == "" {
		zone = "all"
	}
	m.market.set(m.legacySpotPrice, spotPrice, region, zone, itype)
	m.market.set(m.legacyOnDemandPrice, onDemandPrice, region, itype)
	m.market.set(m.legacySpotSavings, savingsPct, region, zone, itype)
}

// SetLegacyPlacementScore updates legacy placement score gauge.
func (m *ObservabilityMetrics) SetLegacyPlacementScore(region, az, itype string, score float64) {
	zone := az
	if zone == "" {
		zone = "all"
	}
	m.market.set(m.legacyPlacementScore, score, region, zone, itype)
}

// AddNetworkReceiveBytes increments cumulative network receive byte counter.
func (m *ObservabilityMetrics) AddNetworkReceiveBytes(provider, region, accelerator, instanceType string, bytes float64) {
	if accelerator == "" {
		accelerator = "none"
	}
	if instanceType == "" {
		instanceType = "none"
	}
	if bytes > 0 {
		m.networkRxBytes.WithLabelValues(provider, region, accelerator, instanceType).Add(bytes)
	}
}

// AddNetworkTransmitBytes increments cumulative network transmit byte counter.
func (m *ObservabilityMetrics) AddNetworkTransmitBytes(provider, region, accelerator, instanceType string, bytes float64) {
	if accelerator == "" {
		accelerator = "none"
	}
	if instanceType == "" {
		instanceType = "none"
	}
	if bytes > 0 {
		m.networkTxBytes.WithLabelValues(provider, region, accelerator, instanceType).Add(bytes)
	}
}

// AddNetworkBytes increments cumulative network receive and transmit byte counters.
func (m *ObservabilityMetrics) AddNetworkBytes(provider, region, accelerator, instanceType string, rxBytes, txBytes float64) {
	m.AddNetworkReceiveBytes(provider, region, accelerator, instanceType, rxBytes)
	m.AddNetworkTransmitBytes(provider, region, accelerator, instanceType, txBytes)
}
