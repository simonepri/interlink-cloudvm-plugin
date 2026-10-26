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
var ProbeGPUCounts = []int{8, 64, 256, 1024}

// probeInstanceGPUs is the GPU count of the shapes a probe asks about, so N GPUs is N/8 instances of any of them.
const probeInstanceGPUs = 8

// maxPlacementScoreRegions is the most regions GetSpotPlacementScores accepts and scores in one request.
const maxPlacementScoreRegions = 10

// probeShapes returns, per memory tier, the tracked 8-GPU instance types whose GPUs have at least that much memory.
func (t *SpotPriceTicker) probeShapes(ctx context.Context) map[int][]string {
	shapes := make(map[int][]string)
	for _, itype := range t.instanceTypes {
		resCap := t.catalog.ResolveCapacity(ctx, nil, itype, ResourceVector{})
		if resCap.GPUs != probeInstanceGPUs {
			continue
		}
		for _, tier := range LadderMemoryTiers {
			if resCap.GPUMemoryGB >= tier {
				shapes[tier] = append(shapes[tier], itype)
			}
		}
	}
	for tier := range shapes {
		sort.Strings(shapes[tier])
	}
	return shapes
}

// ProbeSpotCapacity asks AWS for regional spot placement scores per memory tier and probed GPU count, keyed like FillLimits.SpotScores.
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

	for tier, types := range t.probeShapes(ctx) {
		for _, gpus := range ProbeGPUCounts {
			for start := 0; start < len(regions); start += maxPlacementScoreRegions {
				batch := regions[start:min(start+maxPlacementScoreRegions, len(regions))]
				callCtx, cancel := context.WithTimeout(ctx, t.regionCallTimeout)
				out, err := fetcher.GetSpotPlacementScores(callCtx, &ec2.GetSpotPlacementScoresInput{
					InstanceTypes:          types,
					RegionNames:            batch,
					TargetCapacity:         aws.Int32(int32(ceilDiv(gpus, probeInstanceGPUs))), //nolint:gosec // probe sizes are small constants
					SingleAvailabilityZone: aws.Bool(false),
				})
				cancel()
				if err != nil {
					log.Printf("[cloudvm-ticker] Spot capacity probe for %d GPUs at %d GB failed: %v", gpus, tier, err)
					continue
				}
				for _, item := range out.SpotPlacementScores {
					key := spotScoreKey("aws", aws.ToString(item.Region), tier)
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
