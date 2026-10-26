// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// MarketOffer is one instance type at one price in one zone or region, from any cloud provider.
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
}

// Default ladder tiers: GPU counts to plan for and the GPU memory floors to plan them at.
var (
	DefaultLadderSizes       = []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192}
	DefaultLadderMemoryTiers = []int{24, 40, 80}

	LadderSizes       = DefaultLadderSizes
	LadderMemoryTiers = DefaultLadderMemoryTiers
)

// MinSpotPlacementScore is the lowest AWS spot placement score (1-10) at which a plan still counts on spot capacity.
const MinSpotPlacementScore = 3

func parseIntList(val string) []int {
	var out []int
	for _, item := range strings.Split(val, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			if n, err := strconv.Atoi(item); err == nil && n > 0 {
				out = append(out, n)
			}
		}
	}
	return out
}

// LoadLadderConfigFromEnv initializes ladder parameters from environment variables if present.
func LoadLadderConfigFromEnv() {
	if raw := os.Getenv("LADDER_SIZES"); raw != "" {
		if sizes := parseIntList(raw); len(sizes) > 0 {
			LadderSizes = sizes
		}
	}
	if raw := os.Getenv("LADDER_MEMORY_TIERS"); raw != "" {
		if tiers := parseIntList(raw); len(tiers) > 0 {
			LadderMemoryTiers = tiers
		}
	}
}

func init() {
	LoadLadderConfigFromEnv()
}

// FillLimits holds the measured limits a fill plan must respect.
type FillLimits struct {
	// QuotaVCPUs maps quotaKey(provider, region, quota name) to the vCPU limit; a missing key leaves that quota uncapped.
	QuotaVCPUs map[string]float64
	// SpotScores maps spotScoreKey(provider, region, memory tier) to the placement score (1-10) per probed GPU count.
	SpotScores map[string]map[int]float64
}

func quotaKey(provider, region, quota string) string { return provider + "/" + region + "/" + quota }

func spotScoreKey(provider, region string, minGPUMemoryGB int) string {
	return provider + "/" + region + "/" + strconv.Itoa(minGPUMemoryGB)
}

// awsQuotaName returns the EC2 quota an instance type counts against, matching KnownAWSGPUQuotaCodes names.
func awsQuotaName(instanceType, capacityType string) string {
	family := "standard"
	switch {
	case strings.HasPrefix(instanceType, "p"):
		family = "p"
	case strings.HasPrefix(instanceType, "g"), strings.HasPrefix(instanceType, "vt"):
		family = "g_vt"
	}
	if capacityType == "spot" {
		return family + "_spot"
	}
	return family + "_ondemand"
}

// FillAllocation is the part of a plan taken from one provider, region, GPU model, and capacity type.
type FillAllocation struct {
	Provider     string  `json:"provider"`
	Region       string  `json:"region"`
	GPUModel     string  `json:"gpuModel"`
	CapacityType string  `json:"capacityType"`
	GPUs         int     `json:"gpus"`
	ExpectedGPUs float64 `json:"expectedGpus"`
}

// FillPlan is the cheapest mix of spot and on-demand GPUs for a target count at a GPU memory floor.
type FillPlan struct {
	MinGPUMemoryGB  int              `json:"minGpuMemoryGb"`
	TargetGPUs      int              `json:"targetGpus"`
	PlannedGPUs     int              `json:"plannedGpus"`
	ExpectedGPUs    float64          `json:"expectedGpus"`
	PricePerGPUHour float64          `json:"pricePerGpuHour"`
	Allocations     []FillAllocation `json:"allocations"`
	LimitedReason   string           `json:"limitedReason,omitempty"`
}

// FillSolver plans GPU fills from market offers within measured quotas and spot placement scores.
type FillSolver struct {
	mu          sync.RWMutex
	offers      []*MarketOffer
	limits      FillLimits
	sizes       []int
	memoryTiers []int
}

// NewFillSolver initializes a fill solver.
func NewFillSolver() *FillSolver {
	return &FillSolver{}
}

// SetLadderSizes overrides target ladder sizes for this solver.
func (s *FillSolver) SetLadderSizes(sizes []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sizes = sizes
}

// SetMemoryTiers overrides target memory tiers for this solver.
func (s *FillSolver) SetMemoryTiers(tiers []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.memoryTiers = tiers
}

// SetOffers updates the current pool of market offers.
func (s *FillSolver) SetOffers(offers []*MarketOffer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offers = offers
}

// SetLimits updates the quotas and spot placement scores plans must respect.
func (s *FillSolver) SetLimits(limits FillLimits) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limits = limits
}

// AllOffers returns a copy of current offers.
func (s *FillSolver) AllOffers() []*MarketOffer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*MarketOffer, len(s.offers))
	copy(out, s.offers)
	return out
}

// SolveAll plans every ladder point and exports the plans as metrics.
func (s *FillSolver) SolveAll(metrics *ObservabilityMetrics) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	memTiers := s.memoryTiers
	if len(memTiers) == 0 {
		memTiers = LadderMemoryTiers
	}
	sizes := s.sizes
	if len(sizes) == 0 {
		sizes = LadderSizes
	}

	for _, minMem := range memTiers {
		for _, size := range sizes {
			if metrics != nil {
				metrics.SetFillPlan(s.plan(minMem, size))
			}
		}
	}
}

// plan fills targetGPUs from the cheapest spot offers the placement scores support, then from the cheapest on-demand offers, all within quotas.
func (s *FillSolver) plan(minMem, targetGPUs int) FillPlan {
	plan := FillPlan{MinGPUMemoryGB: minMem, TargetGPUs: targetGPUs}

	var spot, onDemand []*MarketOffer
	for _, o := range s.offers {
		if o.GPUCount <= 0 || o.GPUMemoryGB < minMem || o.PricePerGPUHour <= 0 {
			continue
		}
		switch o.CapacityType {
		case "spot":
			spot = append(spot, o)
		case "on-demand":
			onDemand = append(onDemand, o)
		}
	}
	byPrice := func(offers []*MarketOffer) {
		sort.SliceStable(offers, func(i, j int) bool { return offers[i].PricePerGPUHour < offers[j].PricePerGPUHour })
	}
	byPrice(spot)
	byPrice(onDemand)

	quotaUsed := make(map[string]float64)
	spotUsed := make(map[string]int)
	allocations := make(map[FillAllocation]*FillAllocation)
	remaining := targetGPUs
	cost := 0.0
	quotaLimited := false

	take := func(o *MarketOffer, maxGPUs int, scores map[int]float64) {
		instances := ceilDiv(min(remaining, maxGPUs), o.GPUCount)
		if instances*o.GPUCount > maxGPUs {
			instances = maxGPUs / o.GPUCount
		}
		qKey := quotaKey(o.Provider, o.Region, awsQuotaName(o.InstanceType, o.CapacityType))
		if limit, ok := s.limits.QuotaVCPUs[qKey]; ok && o.VCPUCount > 0 {
			allowed := int((limit - quotaUsed[qKey]) / float64(o.VCPUCount))
			if allowed < instances {
				instances = max(allowed, 0)
				quotaLimited = true
			}
		}
		if instances <= 0 {
			return
		}
		gpus := instances * o.GPUCount
		quotaUsed[qKey] += float64(instances * o.VCPUCount)
		expected := float64(gpus)
		if scores != nil {
			poolKey := spotScoreKey(o.Provider, o.Region, minMem)
			expected = float64(gpus) * scoreAt(scores, spotUsed[poolKey]+gpus) / 10
			spotUsed[poolKey] += gpus
		}
		remaining -= min(gpus, remaining)
		cost += float64(gpus) * o.PricePerGPUHour
		key := FillAllocation{Provider: o.Provider, Region: o.Region, GPUModel: o.GPUModel, CapacityType: o.CapacityType}
		if allocations[key] == nil {
			a := key
			allocations[key] = &a
		}
		allocations[key].GPUs += gpus
		allocations[key].ExpectedGPUs += expected
	}

	for _, o := range spot {
		if remaining == 0 {
			break
		}
		poolKey := spotScoreKey(o.Provider, o.Region, minMem)
		scores := s.limits.SpotScores[poolKey]
		if capacity := spotCapacity(scores) - spotUsed[poolKey]; capacity > 0 {
			take(o, capacity, scores)
		}
	}
	for _, o := range onDemand {
		if remaining == 0 {
			break
		}
		take(o, math.MaxInt32, nil)
	}

	for _, a := range allocations {
		plan.Allocations = append(plan.Allocations, *a)
		plan.PlannedGPUs += a.GPUs
		plan.ExpectedGPUs += a.ExpectedGPUs
	}
	sort.Slice(plan.Allocations, func(i, j int) bool { return plan.Allocations[i].GPUs > plan.Allocations[j].GPUs })
	if plan.PlannedGPUs > 0 {
		plan.PricePerGPUHour = cost / float64(plan.PlannedGPUs)
	}
	if remaining > 0 {
		plan.LimitedReason = "capacity"
		if quotaLimited {
			plan.LimitedReason = "quota"
		}
	}
	return plan
}

// spotCapacity returns the largest probed GPU count whose placement score still meets MinSpotPlacementScore, or 0.
func spotCapacity(scores map[int]float64) int {
	capacity := 0
	for gpus, score := range scores {
		if score >= MinSpotPlacementScore && gpus > capacity {
			capacity = gpus
		}
	}
	return capacity
}

// scoreAt returns the placement score of the smallest probed GPU count that covers gpus.
func scoreAt(scores map[int]float64, gpus int) float64 {
	best, score := math.MaxInt, 0.0
	for probed, s := range scores {
		if probed >= gpus && probed < best {
			best, score = probed, s
		}
	}
	return score
}

func ceilDiv(a, b int) int {
	return (a + b - 1) / b
}
