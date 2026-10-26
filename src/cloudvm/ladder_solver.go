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
	// QuotaVCPUs maps quotaKey(provider, region, quota name) to the vCPU limit; offers under a missing key are not planned.
	QuotaVCPUs map[string]float64
	// SpotScores maps spotScoreKey(provider, region, memory tier, GPUs per instance) to the placement score (1-10) per probed GPU count.
	SpotScores map[string]map[int]float64
}

func quotaKey(provider, region, quota string) string { return provider + "/" + region + "/" + quota }

func spotScoreKey(provider, region string, minGPUMemoryGB, gpusPerInstance int) string {
	return provider + "/" + region + "/" + strconv.Itoa(minGPUMemoryGB) + "/" + strconv.Itoa(gpusPerInstance)
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

// QuotaVCPUs returns the measured vCPU limit of a quota in a region, if the last market sync read it.
func (s *FillSolver) QuotaVCPUs(provider, region, quota string) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	limit, ok := s.limits.QuotaVCPUs[quotaKey(provider, region, quota)]
	return limit, ok
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

// plan fills targetGPUs from spot offers the placement scores support, then from on-demand offers, all within known quotas.
// Each step takes the offer with the lowest cost per GPU still needed, so whole instances larger than the remainder cost what they cost.
func (s *FillSolver) plan(minMem, targetGPUs int) FillPlan {
	var spot, onDemand []*MarketOffer
	for _, o := range s.offers {
		if o.GPUCount <= 0 || o.VCPUCount <= 0 || o.GPUMemoryGB < minMem || o.PricePerHour <= 0 {
			continue
		}
		switch o.CapacityType {
		case "spot":
			spot = append(spot, o)
		case "on-demand":
			onDemand = append(onDemand, o)
		}
	}

	p := planner{solver: s, minMem: minMem, remaining: targetGPUs,
		quotaUsed: make(map[string]float64), spotUsed: make(map[string]int), allocations: make(map[FillAllocation]*FillAllocation)}
	p.fill(spot)
	p.fill(onDemand)

	plan := FillPlan{MinGPUMemoryGB: minMem, TargetGPUs: targetGPUs}
	for _, a := range p.allocations {
		plan.Allocations = append(plan.Allocations, *a)
		plan.PlannedGPUs += a.GPUs
		plan.ExpectedGPUs += a.ExpectedGPUs
	}
	sort.Slice(plan.Allocations, func(i, j int) bool { return plan.Allocations[i].GPUs > plan.Allocations[j].GPUs })
	if useful := min(plan.PlannedGPUs, targetGPUs); useful > 0 {
		plan.PricePerGPUHour = p.cost / float64(useful)
	}
	if p.remaining > 0 {
		switch {
		case p.quotaUnknown:
			plan.LimitedReason = "no_quota_data"
		case p.quotaBound:
			plan.LimitedReason = "quota"
		default:
			plan.LimitedReason = "capacity"
		}
	}
	return plan
}

// planner holds the running state of one fill plan.
type planner struct {
	solver       *FillSolver
	minMem       int
	remaining    int
	cost         float64
	quotaUsed    map[string]float64
	spotUsed     map[string]int
	allocations  map[FillAllocation]*FillAllocation
	quotaBound   bool
	quotaUnknown bool
}

// fill repeatedly takes the offer with the lowest cost per still-needed GPU until the target is met or no offer has room.
func (p *planner) fill(offers []*MarketOffer) {
	for p.remaining > 0 {
		var best *MarketOffer
		bestInstances, bestCost := 0, math.Inf(1)
		for _, o := range offers {
			instances := p.available(o)
			// Also consider stopping short of the remainder, so smaller instances can cover the rest without overshoot.
			for _, n := range []int{instances, min(instances, p.remaining/o.GPUCount)} {
				if n == 0 {
					continue
				}
				unitCost := float64(n) * o.PricePerHour / float64(min(n*o.GPUCount, p.remaining))
				if unitCost < bestCost {
					best, bestInstances, bestCost = o, n, unitCost
				}
			}
		}
		if best == nil {
			return
		}
		p.take(best, bestInstances)
	}
}

// available returns how many instances of o the plan can still take: enough for the remainder, within spot capacity and quota.
func (p *planner) available(o *MarketOffer) int {
	instances := ceilDiv(p.remaining, o.GPUCount)
	if o.CapacityType == "spot" {
		pool := spotScoreKey(o.Provider, o.Region, p.minMem, o.GPUCount)
		instances = min(instances, (spotCapacity(p.solver.limits.SpotScores[pool])-p.spotUsed[pool])/o.GPUCount)
	}
	if instances <= 0 {
		return 0
	}
	qKey := quotaKey(o.Provider, o.Region, awsQuotaName(o.InstanceType, o.CapacityType))
	limit, ok := p.solver.limits.QuotaVCPUs[qKey]
	if !ok {
		p.quotaUnknown = true
		return 0
	}
	if allowed := int((limit - p.quotaUsed[qKey]) / float64(o.VCPUCount)); allowed < instances {
		p.quotaBound = true
		instances = max(allowed, 0)
	}
	return instances
}

func (p *planner) take(o *MarketOffer, instances int) {
	gpus := instances * o.GPUCount
	p.quotaUsed[quotaKey(o.Provider, o.Region, awsQuotaName(o.InstanceType, o.CapacityType))] += float64(instances * o.VCPUCount)
	expected := float64(gpus)
	if o.CapacityType == "spot" {
		pool := spotScoreKey(o.Provider, o.Region, p.minMem, o.GPUCount)
		p.spotUsed[pool] += gpus
		expected = float64(gpus) * scoreAt(p.solver.limits.SpotScores[pool], p.spotUsed[pool]) / 10
	}
	p.remaining -= min(gpus, p.remaining)
	p.cost += float64(instances) * o.PricePerHour
	key := FillAllocation{Provider: o.Provider, Region: o.Region, GPUModel: o.GPUModel, CapacityType: o.CapacityType}
	if p.allocations[key] == nil {
		a := key
		p.allocations[key] = &a
	}
	p.allocations[key].GPUs += gpus
	p.allocations[key].ExpectedGPUs += expected
}

// spotCapacity returns the largest probed GPU count up to which every probed count scores at least MinSpotPlacementScore, or 0.
func spotCapacity(scores map[int]float64) int {
	sizes := make([]int, 0, len(scores))
	for gpus := range scores {
		sizes = append(sizes, gpus)
	}
	sort.Ints(sizes)
	capacity := 0
	for _, gpus := range sizes {
		if scores[gpus] < MinSpotPlacementScore {
			break
		}
		capacity = gpus
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
