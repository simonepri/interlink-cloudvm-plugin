// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

// ObservabilityMetrics manages Prometheus metric collectors and exposition for GPU market and fleet observability.
type ObservabilityMetrics struct {
	Registry *prometheus.Registry
	// marketRegistry holds the market gauges, served only while marketShown is true.
	marketRegistry *prometheus.Registry
	marketShown    atomic.Bool

	// Market & Pricing metrics
	gpuPrice      *prometheus.GaugeVec
	vcpuPrice     *prometheus.GaugeVec
	instancePrice *prometheus.GaugeVec
	transferPrice *prometheus.GaugeVec

	// Availability & Signals
	availScore     *prometheus.GaugeVec
	expectedUptime *prometheus.GaugeVec
	signalAge      *prometheus.GaugeVec

	// Quotas
	quotaLimit *prometheus.GaugeVec
	quotaUsed  *prometheus.GaugeVec

	// Ladder fill plans and the spot placement scores they rest on
	fillPrice        *prometheus.GaugeVec
	fillPlannedGPUs  *prometheus.GaugeVec
	fillExpectedGPUs *prometheus.GaugeVec
	fillGPUs         *prometheus.GaugeVec
	fillLimited      *prometheus.GaugeVec
	spotCapacity     *prometheus.GaugeVec

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

	// Data transfer by peer, and its estimated cost
	transferBytes    *prometheus.CounterVec
	transferCost     *prometheus.CounterVec
	imagePullBytes   *prometheus.CounterVec
	podTransferBytes *prometheus.CounterVec
	podTransferCost  *prometheus.CounterVec

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

		transferPrice: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_transfer_price_usd_per_gb",
			Help: "List price in USD per GB of data leaving source_region: cross_region to destination_region, cross_zone within source_region in each direction, or internet at the first paid volume tier after any free allowance",
		}, []string{"provider", "source_region", "destination_region", "transfer_kind"}),

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
			Help: "Blended price in USD per GPU-hour of the cheapest spot and on-demand plan for N GPUs at a memory floor",
		}, []string{"min_gpu_memory_gb", "gpus"}),

		fillPlannedGPUs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_fill_planned_gpus",
			Help: "GPUs the fill plan for N GPUs found within quotas and spot placement scores; whole instances can exceed N",
		}, []string{"min_gpu_memory_gb", "gpus"}),

		fillExpectedGPUs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_fill_expected_gpus",
			Help: "GPUs the fill plan is expected to obtain, counting spot GPUs at their placement score divided by 10 and on-demand GPUs in full",
		}, []string{"min_gpu_memory_gb", "gpus"}),

		fillGPUs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_fill_gpus",
			Help: "GPUs a fill plan takes from one provider, region, GPU model, and capacity type",
		}, []string{"min_gpu_memory_gb", "gpus", "provider", "region", "gpu_model", "capacity_type"}),

		fillLimited: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_fill_limited",
			Help: "1 when the fill plan for N GPUs falls short, labeled by what stopped it: quota, no_quota_data, or capacity",
		}, []string{"min_gpu_memory_gb", "gpus", "reason"}),

		spotCapacity: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cloudvm_spot_capacity_score",
			Help: "AWS spot placement score (1-10) for getting N GPUs in instances with gpus_per_instance GPUs at a memory floor in a region",
		}, []string{"provider", "region", "min_gpu_memory_gb", "gpus_per_instance", "gpus"}),

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
			Help: "Current run-rate spend in USD per hour: component compute from fulfilled instance prices, and component transfer from the estimated data transfer cost of the last 15 minutes",
		}, append(slices.Clone(fleetLabels), "component")),

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

		transferBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cloudvm_network_transfer_bytes_total",
			Help: "Bytes cloud VMs sent (transmit) or received (receive) on their NICs, by peer class relative to the VM (same_region, home_region, home_storage for the home region's object storage, cross_region, internet, unclassified) and path (tunnel or direct)",
		}, []string{"provider", "region", "instance_type", "direction", "peer", "path"}),

		transferCost: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cloudvm_transfer_cost_usd_total",
			Help: "Estimated USD cloud VM data transfer costs at list rates, billed to the sending side named by charged_provider and charged_region; bytes no rate covers are left out",
		}, []string{"provider", "region", "direction", "peer", "path", "charged_provider", "charged_region"}),

		imagePullBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cloudvm_image_pull_receive_bytes_total",
			Help: "Bytes cloud VMs received for image pulls (containerd and the stargz snapshotter), by peer class",
		}, []string{"provider", "region", "instance_type", "peer"}),

		podTransferBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cloudvm_pod_network_transfer_bytes_total",
			Help: "Bytes a pod's own sockets sent or received on its cloud VM's NICs by peer class, and through the tunnel agent's proxies under peer tunnel",
		}, []string{"namespace", "pod", "pod_uid", "provider", "region", "direction", "peer"}),

		podTransferCost: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cloudvm_pod_transfer_cost_usd_total",
			Help: "Estimated USD a pod's direct data transfer costs at list rates; bytes no rate covers are left out",
		}, []string{"namespace", "pod", "pod_uid", "provider", "region", "direction", "peer"}),

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

	m.marketRegistry = prometheus.NewRegistry()
	m.marketRegistry.MustRegister(
		m.gpuPrice,
		m.vcpuPrice,
		m.instancePrice,
		m.transferPrice,
		m.availScore,
		m.expectedUptime,
		m.signalAge,
		m.quotaLimit,
		m.quotaUsed,
		m.fillPrice,
		m.fillPlannedGPUs,
		m.fillExpectedGPUs,
		m.fillGPUs,
		m.fillLimited,
		m.spotCapacity,
		m.legacySpotPrice,
		m.legacyOnDemandPrice,
		m.legacySpotSavings,
		m.legacyPlacementScore,
	)
	m.marketShown.Store(true)
	reg.MustRegister(
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
		m.transferBytes,
		m.transferCost,
		m.imagePullBytes,
		m.podTransferBytes,
		m.podTransferCost,
	)

	return m
}

// Handler returns the HTTP handler exposing Prometheus metrics for scraping.
func (m *ObservabilityMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Gatherer(), promhttp.HandlerOpts{
		EnableOpenMetrics: false,
	})
}

// Gatherer returns every metric this instance serves: the fleet metrics, and the market gauges while they are shown.
func (m *ObservabilityMetrics) Gatherer() prometheus.Gatherer {
	return prometheus.Gatherers{m.Registry, marketGatherer{m}}
}

// marketGatherer serves the market gauges once they are shown, and nothing before.
type marketGatherer struct{ m *ObservabilityMetrics }

func (g marketGatherer) Gather() ([]*dto.MetricFamily, error) {
	if !g.m.marketShown.Load() {
		return nil, nil
	}
	return g.m.marketRegistry.Gather()
}

// HideMarketUntilRefreshed withholds the market gauges until ShowMarket. A refresh sets gauges one by one, slowly
// on a cold start while on-demand prices load, so a scrape during the first pass would see part of the market, and
// per-model minimums would jump to whichever offers happened to be set.
func (m *ObservabilityMetrics) HideMarketUntilRefreshed() {
	m.marketShown.Store(false)
}

// MarketShown reports whether the market gauges are served.
func (m *ObservabilityMetrics) MarketShown() bool {
	return m.marketShown.Load()
}

// ShowMarket serves the market gauges, after a refresh pass has set all of them.
func (m *ObservabilityMetrics) ShowMarket() {
	m.marketShown.Store(true)
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

// SetTransferPrice exports the USD per GB price of moving data out of sourceRegion to destinationRegion by transferKind.
func (m *ObservabilityMetrics) SetTransferPrice(provider, sourceRegion, destinationRegion, transferKind string, usdPerGB float64) {
	m.market.set(m.transferPrice, usdPerGB, provider, sourceRegion, destinationRegion, transferKind)
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

// SetQuota exports a quota's limit in approximate GPUs.
func (m *ObservabilityMetrics) SetQuota(provider, region, accelerator, quota string, limitGPUs float64) {
	if accelerator == "" {
		accelerator = "gpu"
	}
	m.market.set(m.quotaLimit, limitGPUs, provider, region, accelerator, quota)
}

// SetQuotaUsed exports how much of a quota the account uses, in approximate GPUs.
func (m *ObservabilityMetrics) SetQuotaUsed(provider, region, accelerator, quota string, usedGPUs float64) {
	if accelerator == "" {
		accelerator = "gpu"
	}
	m.market.set(m.quotaUsed, usedGPUs, provider, region, accelerator, quota)
}

// SetFillPlan exports one ladder point's fill plan: price, planned and expected GPUs, allocations, and why it fell short.
func (m *ObservabilityMetrics) SetFillPlan(plan FillPlan) {
	memStr := strconv.Itoa(plan.MinGPUMemoryGB)
	gpusStr := strconv.Itoa(plan.TargetGPUs)
	if plan.PlannedGPUs > 0 {
		m.market.set(m.fillPrice, plan.PricePerGPUHour, memStr, gpusStr)
	}
	m.market.set(m.fillPlannedGPUs, float64(plan.PlannedGPUs), memStr, gpusStr)
	m.market.set(m.fillExpectedGPUs, plan.ExpectedGPUs, memStr, gpusStr)
	for _, a := range plan.Allocations {
		m.market.set(m.fillGPUs, float64(a.GPUs), memStr, gpusStr, a.Provider, a.Region, a.GPUModel, a.CapacityType)
	}
	if plan.LimitedReason != "" {
		m.market.set(m.fillLimited, 1, memStr, gpusStr, plan.LimitedReason)
	}
}

// SetSpotCapacityScore exports the spot placement score for getting gpus GPUs in instances of gpusPerInstance GPUs at a memory floor in a region.
func (m *ObservabilityMetrics) SetSpotCapacityScore(provider, region string, minGPUMemoryGB, gpusPerInstance, gpus int, score float64) {
	m.market.set(m.spotCapacity, score, provider, region, strconv.Itoa(minGPUMemoryGB), strconv.Itoa(gpusPerInstance), strconv.Itoa(gpus))
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
	m.spendPerHour.WithLabelValues(provider, region, accelerator, instanceType, gpuModel, capacityType, spendComponentCompute).Set(spendPerHour)
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

// SetLegacySpotMetrics updates legacy gauges for backward compatibility; an onDemandPrice of 0 means unknown and exports no on-demand or savings series.
func (m *ObservabilityMetrics) SetLegacySpotMetrics(region, az, itype string, spotPrice, onDemandPrice, savingsPct float64) {
	zone := az
	if zone == "" {
		zone = "all"
	}
	m.market.set(m.legacySpotPrice, spotPrice, region, zone, itype)
	if onDemandPrice > 0 {
		m.market.set(m.legacyOnDemandPrice, onDemandPrice, region, itype)
		m.market.set(m.legacySpotSavings, savingsPct, region, zone, itype)
	}
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

// AddTransferBytes counts bytes a VM moved in direction ("receive" or "transmit") with a peer class over path.
func (m *ObservabilityMetrics) AddTransferBytes(provider, region, instanceType, direction, peer, path string, bytes float64) {
	if bytes > 0 {
		m.transferBytes.WithLabelValues(provider, region, instanceType, direction, peer, path).Add(bytes)
	}
}

// AddTransferCost adds the estimated USD of a VM's transfer, billed to the charged provider and region.
func (m *ObservabilityMetrics) AddTransferCost(provider, region, direction, peer, path, chargedProvider, chargedRegion string, usd float64) {
	if usd > 0 {
		m.transferCost.WithLabelValues(provider, region, direction, peer, path, chargedProvider, chargedRegion).Add(usd)
	}
}

// AddImagePullBytes counts bytes a VM received for image pulls from a peer class.
func (m *ObservabilityMetrics) AddImagePullBytes(provider, region, instanceType, peer string, bytes float64) {
	if bytes > 0 {
		m.imagePullBytes.WithLabelValues(provider, region, instanceType, peer).Add(bytes)
	}
}

// AddPodTransfer counts bytes a pod moved in direction with a peer class and their estimated USD cost, when priced.
func (m *ObservabilityMetrics) AddPodTransfer(namespace, pod, podUID, provider, region, direction, peer string, bytes, usd float64) {
	if bytes > 0 {
		m.podTransferBytes.WithLabelValues(namespace, pod, podUID, provider, region, direction, peer).Add(bytes)
	}
	if usd > 0 {
		m.podTransferCost.WithLabelValues(namespace, pod, podUID, provider, region, direction, peer).Add(usd)
	}
}

// DeletePodTransfer drops every transfer series of a pod that is gone.
func (m *ObservabilityMetrics) DeletePodTransfer(podUID string) {
	m.podTransferBytes.DeletePartialMatch(prometheus.Labels{"pod_uid": podUID})
	m.podTransferCost.DeletePartialMatch(prometheus.Labels{"pod_uid": podUID})
}

// Components of the spend gauge.
const (
	spendComponentCompute  = "compute"
	spendComponentTransfer = "transfer"
)

// SetTransferSpend sets the run-rate data transfer spend of a fleet group, next to its compute spend.
func (m *ObservabilityMetrics) SetTransferSpend(provider, region, accelerator, instanceType, gpuModel, capacityType string, usdPerHour float64) {
	if accelerator == "" {
		accelerator = "none"
	}
	if instanceType == "" {
		instanceType = "none"
	}
	if gpuModel == "" {
		gpuModel = "none"
	}
	m.spendPerHour.WithLabelValues(provider, region, accelerator, instanceType, gpuModel, capacityType, spendComponentTransfer).Set(usdPerHour)
}
