// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FleetTracker maintains runtime fleet metrics and instance lifetimes.
type FleetTracker struct {
	mu           sync.RWMutex
	metrics      *ObservabilityMetrics
	catalog      *HardwareCatalog
	instanceAges map[string]time.Time // instanceID -> lastSampleTime
}

// GlobalFleetTracker is the singleton instance of the fleet tracker.
var GlobalFleetTracker = NewFleetTracker(GlobalMetrics, GlobalCatalog)

// NewFleetTracker creates a fleet tracker.
func NewFleetTracker(metrics *ObservabilityMetrics, catalog *HardwareCatalog) *FleetTracker {
	if metrics == nil {
		metrics = GlobalMetrics
	}
	if catalog == nil {
		catalog = GlobalCatalog
	}
	return &FleetTracker{
		metrics:      metrics,
		catalog:      catalog,
		instanceAges: make(map[string]time.Time),
	}
}

// ClassifyAWSError maps AWS EC2 Fleet error strings or error objects to result categories:
// "no_capacity", "quota", "error".
func ClassifyAWSError(err error) string {
	if err == nil {
		return "ok"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "InsufficientInstanceCapacity") || strings.Contains(msg, "Unsupported") || strings.Contains(msg, "no instances launched"):
		return "no_capacity"
	case strings.Contains(msg, "MaxSpotInstanceCountExceeded") || strings.Contains(msg, "VcpuLimitExceeded") || strings.Contains(msg, "ResourceLimitExceeded"):
		return "quota"
	default:
		return "error"
	}
}

// ClassifyGCPError maps GCE errors to result categories: "no_capacity", "quota", "error".
func ClassifyGCPError(err error) string {
	if err == nil {
		return "ok"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "ZONE_RESOURCE_POOL_EXHAUSTED") || strings.Contains(msg, "RESOURCE_POOL_EXHAUSTED"):
		return "no_capacity"
	case strings.Contains(msg, "QUOTA_EXCEEDED") || strings.Contains(msg, "QuotaExceeded"):
		return "quota"
	default:
		return "error"
	}
}

// RecordLaunchAttempt records launch results for AWS or GCP.
func (f *FleetTracker) RecordLaunchAttempt(provider, region, itype, capacityType, result string) {
	if f.metrics == nil {
		return
	}
	gpuModel := itype
	if f.catalog != nil {
		if resCap, ok := f.catalog.Get(itype); ok && resCap.GPUModel != "" {
			gpuModel = resCap.GPUModel
		}
	}
	f.metrics.RecordLaunchAttempt(provider, region, gpuModel, capacityType, result)
}

// RecordLaunchDuration records the latency from request to instance readiness.
func (f *FleetTracker) RecordLaunchDuration(provider, region, itype, capacityType string, duration time.Duration) {
	if f.metrics == nil {
		return
	}
	gpuModel := itype
	if f.catalog != nil {
		if resCap, ok := f.catalog.Get(itype); ok && resCap.GPUModel != "" {
			gpuModel = resCap.GPUModel
		}
	}
	f.metrics.RecordLaunchDuration(provider, region, gpuModel, capacityType, duration.Seconds())
}

// RecordInterruption records an unexpected spot interruption or node lost.
func (f *FleetTracker) RecordInterruption(provider, region, itype, capacityType string) {
	if f.metrics == nil {
		return
	}
	gpuModel := itype
	if f.catalog != nil {
		if resCap, ok := f.catalog.Get(itype); ok && resCap.GPUModel != "" {
			gpuModel = resCap.GPUModel
		}
	}
	f.metrics.RecordInterruption(provider, region, gpuModel, capacityType)
}

// UpdateLiveFleet is invoked by the reconciler on each pass to update running instances, GPUs, and spend $/h.
func (f *FleetTracker) UpdateLiveFleet(instances []*Instance) {
	if f.metrics == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	type fleetKey struct {
		provider     string
		region       string
		gpuModel     string
		capacityType string
	}

	instanceCounts := make(map[fleetKey]int)
	vcpuCounts := make(map[fleetKey]int)
	memGiBCounts := make(map[fleetKey]int)
	gpuCounts := make(map[fleetKey]int)
	vramGBCounts := make(map[fleetKey]int)
	spendRates := make(map[fleetKey]float64)

	now := time.Now()
	for _, inst := range instances {
		if inst == nil || inst.State == StateTerminated || inst.State == StateFailed {
			continue
		}

		provider := string(inst.Provider)
		region := inst.Region
		itype := inst.InstanceType
		capacityType := "on-demand"
		if inst.Metadata != nil && (inst.Metadata["marketType"] == "spot" || inst.Metadata["Spot"] == "true") {
			capacityType = "spot"
		}

		resCap := CapacityForInstanceType(itype, ResourceVector{})
		if f.catalog != nil {
			resolved := f.catalog.ResolveCapacity(context.Background(), nil, itype, resCap)
			if resolved.GPUModel != "" {
				resCap.GPUModel = resolved.GPUModel
			}
			if resolved.GPUs > 0 {
				resCap.GPUs = resolved.GPUs
			}
			if resolved.GPUMemoryGB > 0 {
				resCap.GPUMemoryGB = resolved.GPUMemoryGB
			}
			if !resolved.CPU.IsZero() {
				resCap.CPU = resolved.CPU
			}
			if !resolved.Memory.IsZero() {
				resCap.Memory = resolved.Memory
			}
		}

		gpuModel := itype
		if resCap.GPUModel != "" {
			gpuModel = resCap.GPUModel
		}
		gpuCount := resCap.GPUs
		vramGB := resCap.GPUMemoryGB * resCap.GPUs
		cpuCount := 0
		if c, err := strconv.Atoi(resCap.CPU.String()); err == nil && c > 0 {
			cpuCount = c
		}
		memGiB := 0
		if memBytes, _ := resCap.Memory.AsInt64(); memBytes > 0 {
			memGiB = int(memBytes / (1024 * 1024 * 1024))
		}

		// Calculate fulfilled price
		hourlyPrice := 0.0
		if inst.Metadata != nil && inst.Metadata["fulfilledPrice"] != "" {
			if p, err := strconv.ParseFloat(inst.Metadata["fulfilledPrice"], 64); err == nil && p > 0 {
				hourlyPrice = p
			}
		}
		if hourlyPrice == 0 {
			switch provider {
			case "aws":
				hourlyPrice = OnDemandBaselineUSD(itype)
			case "gcp":
				onDemand, _ := GetGCPPrice(itype)
				hourlyPrice = onDemand
			}
		}

		key := fleetKey{
			provider:     provider,
			region:       region,
			gpuModel:     gpuModel,
			capacityType: capacityType,
		}

		instanceCounts[key]++
		vcpuCounts[key] += cpuCount
		memGiBCounts[key] += memGiB
		gpuCounts[key] += gpuCount
		vramGBCounts[key] += vramGB
		spendRates[key] += hourlyPrice

		// Accumulate instance seconds
		if start, ok := f.instanceAges[inst.ID]; ok {
			elapsed := now.Sub(start).Seconds()
			if elapsed > 0 {
				f.metrics.AddInstanceSeconds(provider, region, gpuModel, capacityType, elapsed)
			}
		}
		f.instanceAges[inst.ID] = now
	}

	// Clean up vanished instances from instanceAges
	activeSet := make(map[string]bool, len(instances))
	for _, inst := range instances {
		if inst != nil {
			activeSet[inst.ID] = true
		}
	}
	for id := range f.instanceAges {
		if !activeSet[id] {
			delete(f.instanceAges, id)
		}
	}

	f.metrics.ResetFleetGauges()
	for k, count := range instanceCounts {
		f.metrics.SetFleetGauges(k.provider, k.region, k.gpuModel, k.capacityType, float64(count), float64(vcpuCounts[k]), float64(memGiBCounts[k]), float64(gpuCounts[k]), float64(vramGBCounts[k]), spendRates[k])
	}
}
