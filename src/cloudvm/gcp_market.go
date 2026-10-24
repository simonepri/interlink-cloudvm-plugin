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
// Loaded dynamically from GlobalPricingCatalog, overridable via PRICING_CATALOG_FILE.
var KnownGCPPrices = func() map[string]GCPPriceBaseline {
	res := make(map[string]GCPPriceBaseline)
	if GlobalPricingCatalog != nil {
		GlobalPricingCatalog.mu.RLock()
		defer GlobalPricingCatalog.mu.RUnlock()
		for k, v := range GlobalPricingCatalog.gcpRates {
			res[k] = v
		}
	}
	return res
}()

// GetGCPPrice returns the hourly on-demand and spot rates for a GCE machine type from the pricing catalog.
func GetGCPPrice(mtype string) (onDemand float64, spot float64) {
	if GlobalPricingCatalog != nil {
		if od, sp, ok := GlobalPricingCatalog.GetGCPRate(mtype); ok {
			return od, sp
		}
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

// SetRegions updates the target GCP regions.
func (c *GCPMarketCollector) SetRegions(regions []string) {
	if len(regions) > 0 {
		c.regions = regions
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
