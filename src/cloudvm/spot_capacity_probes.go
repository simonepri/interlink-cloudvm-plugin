// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"errors"
	"log"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/smithy-go"
)

// ProbeGPUCounts are the GPU counts at which the ticker asks AWS how likely it is to get spot capacity.
var ProbeGPUCounts = []int{8, 64, 256, 1024}

// DefaultProbeMaxInstances is the largest target capacity, in instances, a probe asks about.
// AWS rejects larger targets with TargetCapacityLimitExceeded once they pass the account's exploration limit, about 50 instances.
const DefaultProbeMaxInstances = 50

// probeCapRetry is how long the ticker skips probe targets at or above one AWS rejected as too large.
// AWS sets that limit from the account's recent spot usage, so it can grow; a day later the ticker tries again.
const probeCapRetry = 24 * time.Hour

// probeCapKey identifies the probe group a learned target limit applies to.
type probeCapKey struct {
	minGPUMemoryGB  int
	gpusPerInstance int
}

// probeCap is the smallest instance target AWS rejected for a probe group, and when to stop honoring it.
type probeCap struct {
	instances int
	until     time.Time
}

// maxPlacementScoreRegions is the most regions GetSpotPlacementScores accepts and scores in one request.
const maxPlacementScoreRegions = 10

// probeGroup is the set of tracked instance types with one GPU count per instance at a memory floor.
type probeGroup struct {
	minGPUMemoryGB  int
	gpusPerInstance int
	instanceTypes   []string
}

// probeGroups splits the tracked GPU instance types by memory floor and GPUs per instance, so a target of N GPUs is N/GPUs instances.
func (t *SpotPriceTicker) probeGroups(ctx context.Context) []probeGroup {
	byKey := make(map[[2]int][]string)
	for _, itype := range t.TrackedInstanceTypes() {
		resCap := t.catalog.ResolveCapacity(ctx, nil, itype, ResourceVector{})
		if resCap.GPUs <= 0 {
			continue
		}
		for _, tier := range LadderMemoryTiers {
			if resCap.GPUMemoryGB >= tier {
				key := [2]int{tier, resCap.GPUs}
				byKey[key] = append(byKey[key], itype)
			}
		}
	}
	groups := make([]probeGroup, 0, len(byKey))
	for key, types := range byKey {
		sort.Strings(types)
		groups = append(groups, probeGroup{minGPUMemoryGB: key[0], gpusPerInstance: key[1], instanceTypes: types})
	}
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].minGPUMemoryGB != groups[j].minGPUMemoryGB {
			return groups[i].minGPUMemoryGB < groups[j].minGPUMemoryGB
		}
		return groups[i].gpusPerInstance < groups[j].gpusPerInstance
	})
	return groups
}

// ProbeSpotCapacity asks AWS for regional spot placement scores per probe group and probed GPU count, keyed like FillLimits.SpotScores.
// A probe asks about at most probeMaxInstances instances. A larger ProbeGPUCounts entry is probed at that cap and its score is stored under
// the GPU count actually probed, so a score always means "AWS rates getting this many GPUs at this likelihood"; the planner then takes no
// spot GPUs beyond the largest probed count that scores well.
// A request AWS rejects anyway, because the account's limit is lower than the cap, leaves that GPU count unscored and is logged once.
func (t *SpotPriceTicker) ProbeSpotCapacity(ctx context.Context) map[string]map[int]float64 {
	scores := make(map[string]map[int]float64)
	regions := t.Regions()
	if len(regions) == 0 {
		return scores
	}
	fetcher, err := t.fetcherFactory(ctx, t.defaultRegion)
	if err != nil {
		log.Printf("[cloudvm-ticker] Warning: spot capacity probes skipped: %v", err)
		return scores
	}

	for _, group := range t.probeGroups(ctx) {
		capKey := probeCapKey{group.minGPUMemoryGB, group.gpusPerInstance}
		probed := make(map[int]bool)
		for _, wanted := range ProbeGPUCounts {
			instances := min(ceilDiv(wanted, group.gpusPerInstance), t.maxProbeInstances())
			if probed[instances] {
				continue
			}
			probed[instances] = true
			gpus := instances * group.gpusPerInstance
			if t.probeTooLarge(capKey, instances) {
				continue
			}
			for start := 0; start < len(regions); start += maxPlacementScoreRegions {
				batch := regions[start:min(start+maxPlacementScoreRegions, len(regions))]
				callCtx, cancel := context.WithTimeout(ctx, t.regionCallTimeout)
				out, err := fetcher.GetSpotPlacementScores(callCtx, &ec2.GetSpotPlacementScoresInput{
					InstanceTypes:          group.instanceTypes,
					RegionNames:            batch,
					TargetCapacity:         aws.Int32(int32(instances)), //nolint:gosec // probe sizes are small constants
					SingleAvailabilityZone: aws.Bool(false),
				})
				cancel()
				var apiErr smithy.APIError
				if errors.As(err, &apiErr) && apiErr.ErrorCode() == "TargetCapacityLimitExceeded" {
					t.rememberProbeLimit(capKey, instances)
					break
				}
				if err != nil {
					log.Printf("[cloudvm-ticker] Spot capacity probe for %d GPUs of %d-GPU shapes at %d GB failed: %v", gpus, group.gpusPerInstance, group.minGPUMemoryGB, err) //nolint:gosec // integers and an AWS error
					continue
				}
				for _, item := range out.SpotPlacementScores {
					key := spotScoreKey("aws", aws.ToString(item.Region), group.minGPUMemoryGB, group.gpusPerInstance)
					if scores[key] == nil {
						scores[key] = make(map[int]float64)
					}
					scores[key][gpus] = float64(aws.ToInt32(item.Score))
				}
			}
		}
	}
	return scores
}

// probeTooLarge reports whether AWS recently rejected a target of this many instances, or fewer, for the probe group.
func (t *SpotPriceTicker) probeTooLarge(key probeCapKey, instances int) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	c, ok := t.probeCaps[key]
	return ok && time.Now().Before(c.until) && instances >= c.instances
}

// rememberProbeLimit records that AWS rejected a target of this many instances for the probe group, until probeCapRetry passes.
func (t *SpotPriceTicker) rememberProbeLimit(key probeCapKey, instances int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.probeCaps == nil {
		t.probeCaps = make(map[probeCapKey]probeCap)
	}
	c, ok := t.probeCaps[key]
	if !ok || instances < c.instances {
		log.Printf("[cloudvm-ticker] AWS rejected spot capacity probes of %d instances for %d-GPU shapes at %d GB as above the account limit; skipping targets at or above that size for %s", instances, key.gpusPerInstance, key.minGPUMemoryGB, probeCapRetry) //nolint:gosec // integers and a constant
	}
	if !ok || time.Now().After(c.until) || instances < c.instances {
		c = probeCap{instances: instances, until: time.Now().Add(probeCapRetry)}
	}
	t.probeCaps[key] = c
}

// SetProbeMaxInstances overrides the largest target capacity, in instances, a spot capacity probe asks about; values below 1 restore the default.
func (t *SpotPriceTicker) SetProbeMaxInstances(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.probeMaxInstances = n
}

func (t *SpotPriceTicker) maxProbeInstances() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.probeMaxInstances < 1 {
		return DefaultProbeMaxInstances
	}
	return t.probeMaxInstances
}
