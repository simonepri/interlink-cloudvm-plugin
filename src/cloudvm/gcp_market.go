// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"strings"
	"time"
)

// GCPPriceBaseline maps GCE machine types to standard on-demand and spot hourly USD rates.
type GCPPriceBaseline struct {
	OnDemandUSD float64
	SpotUSD     float64
}

// KnownGCPPrices provides baseline catalog pricing for standard GCE accelerator machine types.
var KnownGCPPrices = map[string]GCPPriceBaseline{
	// NVIDIA L4 instances (us-central1, us-east1, etc.)
	"g2-standard-4":  {OnDemandUSD: 0.7024, SpotUSD: 0.2308},
	"g2-standard-8":  {OnDemandUSD: 0.8066, SpotUSD: 0.2642},
	"g2-standard-12": {OnDemandUSD: 0.9109, SpotUSD: 0.2976},
	"g2-standard-16": {OnDemandUSD: 1.0152, SpotUSD: 0.3311},
	"g2-standard-24": {OnDemandUSD: 1.7217, SpotUSD: 0.5619},
	"g2-standard-32": {OnDemandUSD: 2.0304, SpotUSD: 0.6622},
	"g2-standard-48": {OnDemandUSD: 3.4435, SpotUSD: 1.1238},
	"g2-standard-96": {OnDemandUSD: 6.8869, SpotUSD: 2.2475},

	// NVIDIA A100 40GB instances
	"a2-highgpu-1g":  {OnDemandUSD: 3.6732, SpotUSD: 1.1020},
	"a2-highgpu-2g":  {OnDemandUSD: 7.3464, SpotUSD: 2.2040},
	"a2-highgpu-4g":  {OnDemandUSD: 14.6928, SpotUSD: 4.4080},
	"a2-highgpu-8g":  {OnDemandUSD: 29.3856, SpotUSD: 8.8160},
	"a2-megagpu-16g": {OnDemandUSD: 58.7712, SpotUSD: 17.6320},

	// NVIDIA A100 80GB instances
	"a2-ultragpu-1g": {OnDemandUSD: 5.0361, SpotUSD: 1.5108},
	"a2-ultragpu-2g": {OnDemandUSD: 10.0722, SpotUSD: 3.0216},
	"a2-ultragpu-4g": {OnDemandUSD: 20.1444, SpotUSD: 6.0432},
	"a2-ultragpu-8g": {OnDemandUSD: 40.2888, SpotUSD: 12.0864},

	// NVIDIA H100 80GB instances
	"a3-highgpu-8g": {OnDemandUSD: 31.2184, SpotUSD: 9.3655},
	"a3-megagpu-8g": {OnDemandUSD: 31.2184, SpotUSD: 9.3655},
	"a3-edgegpu-8g": {OnDemandUSD: 31.2184, SpotUSD: 9.3655},

	// NVIDIA T4 instances
	"n1-standard-4-1t4":  {OnDemandUSD: 0.5400, SpotUSD: 0.1620},
	"n1-standard-8-1t4":  {OnDemandUSD: 0.7300, SpotUSD: 0.2190},
	"n1-standard-16-2t4": {OnDemandUSD: 1.4600, SpotUSD: 0.4380},
	"n1-standard-32-4t4": {OnDemandUSD: 2.9200, SpotUSD: 0.8760},
}

// GetGCPPrice returns the hourly on-demand and spot rates for a GCE machine type.
func GetGCPPrice(mtype string) (onDemand float64, spot float64) {
	clean := strings.ToLower(strings.TrimSpace(mtype))
	if p, ok := KnownGCPPrices[clean]; ok {
		return p.OnDemandUSD, p.SpotUSD
	}
	return 1.0, 0.3
}

// GCPAdviceCapacityResponse captures obtainability and estimated uptime from GCP advice.capacity.
type GCPAdviceCapacityResponse struct {
	ObtainabilityScore float64 // 0.0 to 1.0 (0.7-1.0 high, 0.4-0.6 medium, 0.0-0.3 low)
	EstimatedUptimeSec float64 // Estimated seconds before preemption
}

// GCPAdviceCapacityClient abstracts calling GCP advice.capacity.
type GCPAdviceCapacityClient interface {
	QueryAdviceCapacity(ctx context.Context, region, machineType string, vmCount int) (*GCPAdviceCapacityResponse, error)
}

// DefaultGCPAdviceClient provides a default/fallback implementation estimating obtainability and uptime.
type DefaultGCPAdviceClient struct{}

// QueryAdviceCapacity implements GCPAdviceCapacityClient with conservative estimates.
func (c *DefaultGCPAdviceClient) QueryAdviceCapacity(_ context.Context, _, machineType string, vmCount int) (*GCPAdviceCapacityResponse, error) {
	clean := strings.ToLower(strings.TrimSpace(machineType))

	score := 0.85
	if vmCount > 64 {
		score = 0.55
	}
	if vmCount > 256 {
		score = 0.25
	}

	uptime := 28800.0 // 8 hours baseline
	if strings.HasPrefix(clean, "a3-") {
		uptime = 18000.0 // 5 hours for scarce H100
		score -= 0.15
	} else if strings.HasPrefix(clean, "g2-") {
		uptime = 50400.0 // 14 hours for abundant L4
	}

	if score < 0.1 {
		score = 0.1
	}

	return &GCPAdviceCapacityResponse{
		ObtainabilityScore: score,
		EstimatedUptimeSec: uptime,
	}, nil
}

// GCPMarketCollector collects and exports GCP GPU market prices, availability signals, and quotas.
type GCPMarketCollector struct {
	regions      []string
	adviceClient GCPAdviceCapacityClient
}

// NewGCPMarketCollector creates a new collector for GCP market observability.
func NewGCPMarketCollector(regions []string, adviceClient GCPAdviceCapacityClient) *GCPMarketCollector {
	if len(regions) == 0 {
		regions = []string{"us-central1"}
	}
	if adviceClient == nil {
		adviceClient = &DefaultGCPAdviceClient{}
	}
	return &GCPMarketCollector{
		regions:      regions,
		adviceClient: adviceClient,
	}
}

// KnownGCPGPUQuotas maps standard GCP GPU quota names.
var KnownGCPGPUQuotas = map[string]float64{
	"NVIDIA_L4_GPUS":               64.0,
	"PREEMPTIBLE_NVIDIA_L4_GPUS":   128.0,
	"NVIDIA_A100_GPUS":             16.0,
	"PREEMPTIBLE_NVIDIA_A100_GPUS": 32.0,
	"NVIDIA_H100_GPUS":             8.0,
	"PREEMPTIBLE_NVIDIA_H100_GPUS": 16.0,
}

// Collect exports prices, advice signals, and quotas for all configured GCP regions.
func (c *GCPMarketCollector) Collect(ctx context.Context, metrics *ObservabilityMetrics) {
	if metrics == nil {
		return
	}
	now := time.Now()

	for _, region := range c.regions {
		// 1. Quotas
		for qName, qLimit := range KnownGCPGPUQuotas {
			metrics.SetQuota("gcp", region, qName, qLimit, 0)
		}

		// 2. Machine types & prices
		for mtype, profile := range KnownGCPHardwareProfiles {
			if profile.GPUs <= 0 {
				continue
			}
			onDemand, spot := GetGCPPrice(mtype)

			// Spot prices in GCP are regional; each zone in region gets the same spot value
			metrics.SetGPUPrice("gcp", region, "", profile.GPUModel, profile.GPUMemoryGB, profile.GPUs, "spot", mtype, spot)
			metrics.SetGPUPrice("gcp", region, "", profile.GPUModel, profile.GPUMemoryGB, profile.GPUs, "on-demand", mtype, onDemand)

			// 3. Advice signals
			if resp, err := c.adviceClient.QueryAdviceCapacity(ctx, region, mtype, profile.GPUs); err == nil && resp != nil {
				metrics.SetAvailabilityScore("gcp", region, "", profile.GPUModel, mtype, "gcp_advice_capacity", profile.GPUs, resp.ObtainabilityScore)
				metrics.SetExpectedUptime("gcp", region, mtype, profile.GPUModel, "gcp_advice_capacity", resp.EstimatedUptimeSec)
				metrics.SetSignalAge("gcp", region, "gcp_advice_capacity", time.Since(now).Seconds())
			}
		}
	}
}
