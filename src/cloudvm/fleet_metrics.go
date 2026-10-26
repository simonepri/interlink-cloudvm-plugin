// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FleetTracker maintains runtime fleet metrics and instance lifetimes.
type FleetTracker struct {
	mu              sync.RWMutex
	metrics         *ObservabilityMetrics
	catalog         *HardwareCatalog
	instanceAges    map[string]time.Time // instanceID -> lastSampleTime
	lastNetworkRx   map[string]int64     // instanceID -> lastCumulativeRxBytes
	lastNetworkTx   map[string]int64     // instanceID -> lastCumulativeTxBytes
	defaultProvider string
	defaultRegion   string
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
	defReg := os.Getenv("AWS_REGION")
	if defReg == "" {
		defReg = "us-west-2"
	}
	defProv := os.Getenv("CLOUD_PROVIDER")
	if defProv == "" {
		defProv = "aws"
	}
	return &FleetTracker{
		metrics:         metrics,
		catalog:         catalog,
		instanceAges:    make(map[string]time.Time),
		lastNetworkRx:   make(map[string]int64),
		lastNetworkTx:   make(map[string]int64),
		defaultProvider: defProv,
		defaultRegion:   defReg,
	}
}

func (f *FleetTracker) resolveAccelerator(itype string) (string, string) {
	accelerator := "none"
	gpuModel := "none"
	var resCap ResourceVector
	var found bool
	if f.catalog != nil {
		resCap, found = f.catalog.Get(itype)
	}
	if !found {
		resCap = CapacityForInstanceType(itype, ResourceVector{})
		if resCap.GPUs > 0 || resCap.TPUs > 0 {
			found = true
		}
	}
	if found {
		if resCap.TPUs > 0 {
			accelerator = "tpu"
			if resCap.GPUModel != "" {
				gpuModel = resCap.GPUModel
			} else {
				gpuModel = itype
			}
		} else if resCap.GPUs > 0 {
			accelerator = "gpu"
			if resCap.GPUModel != "" {
				gpuModel = resCap.GPUModel
			} else {
				gpuModel = itype
			}
		}
	}
	return accelerator, gpuModel
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
	accelerator, gpuModel := f.resolveAccelerator(itype)
	f.metrics.RecordLaunchAttempt(provider, region, accelerator, itype, gpuModel, capacityType, result)
}

// RecordLaunchDuration records the latency from request to instance readiness.
func (f *FleetTracker) RecordLaunchDuration(provider, region, itype, capacityType string, duration time.Duration) {
	if f.metrics == nil {
		return
	}
	accelerator, gpuModel := f.resolveAccelerator(itype)
	f.metrics.RecordLaunchDuration(provider, region, accelerator, itype, gpuModel, capacityType, duration.Seconds())
}

// RecordInterruption records an unexpected spot interruption or node lost.
func (f *FleetTracker) RecordInterruption(provider, region, itype, capacityType string) {
	if f.metrics == nil {
		return
	}
	accelerator, gpuModel := f.resolveAccelerator(itype)
	f.metrics.RecordInterruption(provider, region, accelerator, itype, gpuModel, capacityType)
}

// UpdateLiveFleet is invoked by the reconciler on each pass to update running instances, GPUs, and spend $/h.
func (f *FleetTracker) UpdateLiveFleet(instances []*Instance, activePods ...map[string]string) {
	if f.metrics == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	type fleetKey struct {
		provider     string
		region       string
		accelerator  string
		instanceType string
		gpuModel     string
		capacityType string
	}

	instanceCounts := make(map[fleetKey]int)
	vcpuCounts := make(map[fleetKey]int)
	memGiBCounts := make(map[fleetKey]int)
	gpuCounts := make(map[fleetKey]int)
	vramGBCounts := make(map[fleetKey]int)
	spendRates := make(map[fleetKey]float64)
	podCounts := make(map[fleetKey]int)
	spendBaselineRates := make(map[fleetKey]float64)

	podCountsByInstance := make(map[string]int)
	if len(activePods) > 0 && activePods[0] != nil {
		for _, instID := range activePods[0] {
			if instID != "" {
				podCountsByInstance[instID]++
			}
		}
	}

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

		accelerator := "none"
		gpuModel := "none"
		if resCap.TPUs > 0 {
			accelerator = "tpu"
			if resCap.GPUModel != "" {
				gpuModel = resCap.GPUModel
			} else {
				gpuModel = itype
			}
		} else if resCap.GPUs > 0 {
			accelerator = "gpu"
			if resCap.GPUModel != "" {
				gpuModel = resCap.GPUModel
			} else {
				gpuModel = itype
			}
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
				hourlyPrice = GlobalOnDemandPrices.Cached(region, itype)
			case "gcp":
				onDemand, _ := GetGCPPrice(itype)
				hourlyPrice = onDemand
			}
		}

		if provider != "" {
			f.defaultProvider = provider
		}
		if region != "" {
			f.defaultRegion = region
		}

		key := fleetKey{
			provider:     provider,
			region:       region,
			accelerator:  accelerator,
			instanceType: itype,
			gpuModel:     gpuModel,
			capacityType: capacityType,
		}

		ondemandBaseline := 0.0
		switch provider {
		case "aws":
			ondemandBaseline = GlobalOnDemandPrices.Cached(region, itype)
		case "gcp":
			onDemand, _ := GetGCPPrice(itype)
			ondemandBaseline = onDemand
		}
		if ondemandBaseline == 0 {
			ondemandBaseline = hourlyPrice
		}

		instanceCounts[key]++
		vcpuCounts[key] += cpuCount
		memGiBCounts[key] += memGiB
		gpuCounts[key] += gpuCount
		vramGBCounts[key] += vramGB
		spendRates[key] += hourlyPrice
		podCounts[key] += podCountsByInstance[inst.ID]
		spendBaselineRates[key] += ondemandBaseline

		// Accumulate instance seconds
		if start, ok := f.instanceAges[inst.ID]; ok {
			elapsed := now.Sub(start).Seconds()
			if elapsed > 0 {
				f.metrics.AddInstanceSeconds(provider, region, accelerator, itype, gpuModel, capacityType, elapsed)
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
			delete(f.lastNetworkRx, id)
			delete(f.lastNetworkTx, id)
		}
	}

	f.metrics.ResetFleetGauges()
	if len(instanceCounts) == 0 {
		defProv := f.defaultProvider
		if defProv == "" {
			defProv = "aws"
		}
		defReg := f.defaultRegion
		if defReg == "" {
			defReg = "us-west-2"
		}
		f.metrics.SetFleetGauges(defProv, defReg, "none", "none", "none", "spot", 0, 0, 0, 0, 0, 0, 0, 0)
		f.metrics.SetFleetGauges(defProv, defReg, "none", "none", "none", "on-demand", 0, 0, 0, 0, 0, 0, 0, 0)
	} else {
		for k, count := range instanceCounts {
			f.metrics.SetFleetGauges(k.provider, k.region, k.accelerator, k.instanceType, k.gpuModel, k.capacityType, float64(count), float64(vcpuCounts[k]), float64(memGiBCounts[k]), float64(gpuCounts[k]), float64(vramGBCounts[k]), spendRates[k], float64(podCounts[k]), spendBaselineRates[k])
		}
	}
}

// RecordNetworkBytes directly increments network receive and transmit counters by delta bytes.
func (f *FleetTracker) RecordNetworkBytes(provider, region, itype string, rxDelta, txDelta float64) {
	if f.metrics == nil {
		return
	}
	accelerator, _ := f.resolveAccelerator(itype)
	f.metrics.AddNetworkBytes(provider, region, accelerator, itype, rxDelta, txDelta)
}

// RecordNetworkTraffic records delta network bytes from cumulative readings for an instance.
func (f *FleetTracker) RecordNetworkTraffic(provider, region, itype, instanceID string, cumulativeRx, cumulativeTx int64) {
	if f.metrics == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	accelerator, _ := f.resolveAccelerator(itype)

	var deltaRx, deltaTx int64
	if lastRx, ok := f.lastNetworkRx[instanceID]; ok {
		if cumulativeRx >= lastRx {
			deltaRx = cumulativeRx - lastRx
		} else {
			deltaRx = cumulativeRx
		}
	} else if cumulativeRx > 0 {
		deltaRx = cumulativeRx
	}

	if lastTx, ok := f.lastNetworkTx[instanceID]; ok {
		if cumulativeTx >= lastTx {
			deltaTx = cumulativeTx - lastTx
		} else {
			deltaTx = cumulativeTx
		}
	} else if cumulativeTx > 0 {
		deltaTx = cumulativeTx
	}

	if instanceID != "" {
		if cumulativeRx > 0 {
			f.lastNetworkRx[instanceID] = cumulativeRx
		}
		if cumulativeTx > 0 {
			f.lastNetworkTx[instanceID] = cumulativeTx
		}
	}

	if deltaRx > 0 || deltaTx > 0 {
		f.metrics.AddNetworkBytes(provider, region, accelerator, itype, float64(deltaRx), float64(deltaTx))
	}
}

// CollectFleetTelemetry records telemetry such as network bytes for a given instance.
func (f *FleetTracker) CollectFleetTelemetry(provider, region, itype, instanceID string, rxBytes, txBytes int64) {
	f.RecordNetworkTraffic(provider, region, itype, instanceID, rxBytes, txBytes)
}
