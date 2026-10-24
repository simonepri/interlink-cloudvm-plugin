// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// MarketOffer represents a standardized capacity offering from any cloud provider.
type MarketOffer struct {
	Provider        string  `json:"provider"` // "aws" or "gcp"
	Region          string  `json:"region"`
	Zone            string  `json:"zone"`
	InstanceType    string  `json:"instanceType"`
	GPUModel        string  `json:"gpuModel"`    // e.g. "A10G 24GB", "L4 24GB", "A100 80GB"
	GPUMemoryGB     int     `json:"gpuMemoryGb"` // e.g. 24, 40, 80
	GPUCount        int     `json:"gpuCount"`
	VCPUCount       int     `json:"vcpuCount"`
	CapacityType    string  `json:"capacityType"`    // "spot" or "on-demand"
	PricePerHour    float64 `json:"pricePerHour"`    // Full instance price
	PricePerGPUHour float64 `json:"pricePerGpuHour"` // All-in normalized price
	Availability    float64 `json:"availability"`    // Score 0.0 to 1.0
	ExpectedUptime  float64 `json:"expectedUptime"`  // Expected seconds before interruption
	QuotaRemaining  int     `json:"quotaRemaining"`  // Remaining GPUs in quota limit
	ProbeLimit      bool    `json:"probeLimit"`      // True if provider refused probe
}

// Fixed ladder probing tiers as specified in the design document.
var (
	LadderSizes       = []int{8, 64, 256, 1000}
	LadderMemoryTiers = []int{24, 40, 80}
	LadderRegionCaps  = []string{"1", "2", "unlimited"}
)

// FillResult describes the outcome of solving a fill request.
type FillResult struct {
	MinGPUMemoryGB  int            `json:"minGpuMemoryGb"`
	TargetGPUs      int            `json:"targetGpus"`
	MaxRegions      string         `json:"maxRegions"`
	CapacityType    string         `json:"capacityType"`
	BlendedPriceUSD float64        `json:"blendedPriceUsd"`
	FulfilledGPUs   int            `json:"fulfilledGpus"`
	Allocations     map[string]int `json:"allocations"` // key: "provider/region/gpu_model" -> count
	LimitedReason   string         `json:"limitedReason,omitempty"`
}

// FillSolver evaluates market offers against target capacity ladders.
type FillSolver struct {
	mu     sync.RWMutex
	offers []*MarketOffer
}

// NewFillSolver initializes a fill solver.
func NewFillSolver() *FillSolver {
	return &FillSolver{}
}

// SetOffers updates the current pool of market offers.
func (s *FillSolver) SetOffers(offers []*MarketOffer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offers = offers
}

// AllOffers returns a copy of current offers.
func (s *FillSolver) AllOffers() []*MarketOffer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*MarketOffer, len(s.offers))
	copy(out, s.offers)
	return out
}

// SolveAll solves all fixed ladder combinations and exports metrics.
func (s *FillSolver) SolveAll(metrics *ObservabilityMetrics) []FillResult {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []FillResult
	for _, minMem := range LadderMemoryTiers {
		for _, size := range LadderSizes {
			for _, capType := range []string{"spot", "on-demand"} {
				for _, maxReg := range LadderRegionCaps {
					res := s.solveOne(minMem, size, maxReg, capType)
					results = append(results, res)
					if metrics != nil {
						metrics.SetFillQuote(minMem, size, maxReg, capType, res.BlendedPriceUSD, nil, res.LimitedReason)
						for k, count := range res.Allocations {
							parts := strings.Split(k, "/")
							if len(parts) == 3 {
								metrics.SetFillGPUAllocation(minMem, size, maxReg, capType, parts[0], parts[1], parts[2], float64(count))
							}
						}
					}
				}
			}
		}
	}
	return results
}

// solveOne finds the cheapest combination of offers satisfying a single ladder point.
func (s *FillSolver) solveOne(minMem, targetSize int, maxRegions, capacityType string) FillResult {
	result := FillResult{
		MinGPUMemoryGB: minMem,
		TargetGPUs:     targetSize,
		MaxRegions:     maxRegions,
		CapacityType:   capacityType,
		Allocations:    make(map[string]int),
	}

	var candidates []*MarketOffer
	hadProbeLimit := false
	hadQuotaLimit := false

	for _, o := range s.offers {
		if o.GPUMemoryGB >= minMem && o.CapacityType == capacityType && o.PricePerGPUHour > 0 {
			if o.ProbeLimit {
				hadProbeLimit = true
			}
			candidates = append(candidates, o)
		}
	}

	// Sort candidates by spot/on-demand price per GPU hour ascending
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].PricePerGPUHour < candidates[j].PricePerGPUHour
	})

	maxRegCount := 0
	switch maxRegions {
	case "1":
		maxRegCount = 1
	case "2":
		maxRegCount = 2
	}

	regionsUsed := make(map[string]bool)
	remainingNeeded := targetSize
	totalCost := 0.0

	for _, o := range candidates {
		if maxRegCount > 0 && len(regionsUsed) >= maxRegCount && !regionsUsed[o.Region] {
			continue
		}

		// Cap by quota if set
		maxFromOffer := targetSize
		if o.QuotaRemaining > 0 {
			if o.QuotaRemaining < maxFromOffer {
				maxFromOffer = o.QuotaRemaining
				hadQuotaLimit = true
			}
		}

		// Don't accumulate more than available
		take := remainingNeeded
		if take > maxFromOffer {
			take = maxFromOffer
		}
		if take <= 0 {
			continue
		}

		regionsUsed[o.Region] = true
		remainingNeeded -= take
		totalCost += float64(take) * o.PricePerGPUHour

		key := fmt.Sprintf("%s/%s/%s", o.Provider, o.Region, o.GPUModel)
		result.Allocations[key] += take

		if remainingNeeded == 0 {
			break
		}
	}

	result.FulfilledGPUs = targetSize - remainingNeeded
	if result.FulfilledGPUs > 0 {
		result.BlendedPriceUSD = totalCost / float64(result.FulfilledGPUs)
	}

	if remainingNeeded > 0 {
		switch {
		case hadQuotaLimit:
			result.LimitedReason = "quota"
		case hadProbeLimit:
			result.LimitedReason = "probe_limit"
		default:
			result.LimitedReason = "capacity"
		}
	}

	return result
}
