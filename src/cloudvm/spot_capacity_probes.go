// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"log"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
)

// ProbeGPUCounts are the GPU counts at which the ticker asks AWS how likely it is to get spot capacity.
// AWS caps the explorable target capacity by recent spot usage; 1024 GPUs (128 8-GPU instances) already exceeds it.
var ProbeGPUCounts = []int{8, 64, 256}

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
	for _, itype := range t.instanceTypes {
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
// A request AWS rejects, for example a target above the account's spot capacity limit, leaves that GPU count unscored.
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
		for _, gpus := range ProbeGPUCounts {
			for start := 0; start < len(regions); start += maxPlacementScoreRegions {
				batch := regions[start:min(start+maxPlacementScoreRegions, len(regions))]
				callCtx, cancel := context.WithTimeout(ctx, t.regionCallTimeout)
				out, err := fetcher.GetSpotPlacementScores(callCtx, &ec2.GetSpotPlacementScoresInput{
					InstanceTypes:          group.instanceTypes,
					RegionNames:            batch,
					TargetCapacity:         aws.Int32(int32(ceilDiv(gpus, group.gpusPerInstance))), //nolint:gosec // probe sizes are small constants
					SingleAvailabilityZone: aws.Bool(false),
				})
				cancel()
				if err != nil {
					log.Printf("[cloudvm-ticker] Spot capacity probe for %d GPUs of %d-GPU shapes at %d GB failed: %v", gpus, group.gpusPerInstance, group.minGPUMemoryGB, err)
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
