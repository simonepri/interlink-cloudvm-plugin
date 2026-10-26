// Launch region selection for AWS: orders a pod's pinned region and its candidate-regions fallbacks, and decides
// which launch failures move on to the next region.
package main

import (
	"errors"
	"log"
	"sort"
	"strings"
)

// maxLaunchRegions bounds how many regions one pod launch tries, counting the pinned region.
var maxLaunchRegions = 3

// errNoInstancesLaunched reports an instant fleet that returned no instances, which EC2 uses for capacity failures.
var errNoInstancesLaunched = errors.New("no instances launched by fleet")

// RegionPlacementScorer exposes spot placement scores; SpotPriceTicker implements it.
type RegionPlacementScorer interface {
	AllScores() map[string]*PlacementScoreEntry
}

// SetRegionScorer sets the placement score source used to order candidate fallback regions.
func (p *AWSProvider) SetRegionScorer(s RegionPlacementScorer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.regionScorer = s
}

// launchRegions returns the regions to try for spec, in order. The pinned region always comes first and is the only
// one unless the pod sets candidate-regions; the candidates with a ClusterFacts launch template follow in descending
// placement score for instanceTypes, keeping annotation order for ties and unscored regions.
func (p *AWSProvider) launchRegions(spec *InstanceSpec, instanceTypes []string) []string {
	regions := []string{spec.Region}
	if len(spec.CandidateRegions) == 0 {
		return regions
	}
	if (spec.KMSCiphertext != "" || spec.MeshAuthKeyCiphertext != "") && !strings.HasPrefix(spec.KMSKeyID, "arn:") {
		log.Printf("[aws] Region fallback disabled for %s/%s: KMS key %s is not an ARN, so other regions cannot decrypt the runner token", //nolint:gosec // sanitized
			sanitizeLog(spec.PodNamespace), sanitizeLog(spec.PodName), sanitizeLog(spec.KMSKeyID))
		return regions
	}

	seen := map[string]bool{spec.Region: true}
	var fallbacks []string
	for _, r := range spec.CandidateRegions {
		if r == "" || seen[r] {
			continue
		}
		seen[r] = true
		if ltID, ltName := p.ResolveRegionalLaunchTemplate(r, spec.NumGPUs > 0); ltID == "" && ltName == "" {
			log.Printf("[aws] Skipping candidate region %s for %s/%s: no launch template in cluster facts", //nolint:gosec // sanitized
				sanitizeLog(r), sanitizeLog(spec.PodNamespace), sanitizeLog(spec.PodName))
			continue
		}
		fallbacks = append(fallbacks, r)
	}

	p.mu.RLock()
	scorer := p.regionScorer
	p.mu.RUnlock()
	if scorer != nil {
		scores := regionScores(scorer.AllScores(), instanceTypes)
		sort.SliceStable(fallbacks, func(i, j int) bool {
			return scoreOrUnscored(scores, fallbacks[i]) > scoreOrUnscored(scores, fallbacks[j])
		})
	}

	regions = append(regions, fallbacks...)
	if len(regions) > maxLaunchRegions {
		regions = regions[:maxLaunchRegions]
	}
	return regions
}

// regionScores returns each region's best placement score across instanceTypes.
func regionScores(entries map[string]*PlacementScoreEntry, instanceTypes []string) map[string]int32 {
	wanted := make(map[string]bool, len(instanceTypes))
	for _, t := range instanceTypes {
		wanted[t] = true
	}
	scores := make(map[string]int32)
	for _, e := range entries {
		if e == nil || !wanted[e.InstanceType] {
			continue
		}
		if cur, ok := scores[e.Region]; !ok || e.Score > cur {
			scores[e.Region] = e.Score
		}
	}
	return scores
}

func scoreOrUnscored(scores map[string]int32, region string) int32 {
	if s, ok := scores[region]; ok {
		return s
	}
	return -1
}

// isRegionFallbackError reports whether a launch failure is specific to the region's capacity or configuration, so
// another region may still succeed.
func isRegionFallbackError(err error) bool {
	if errors.Is(err, errNoInstancesLaunched) || errors.Is(err, errNoOfferedPlacements) {
		return true
	}
	if class := ClassifyAWSError(err); class == "no_capacity" || class == "quota" {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "UnfulfillableCapacity") || strings.Contains(msg, "InvalidFleetConfiguration")
}
