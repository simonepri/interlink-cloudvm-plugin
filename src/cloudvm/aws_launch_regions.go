// Launch region selection for AWS: orders a pod's pinned region and its candidate-regions fallbacks, and decides
// which launch failures move on to the next region.
package main

import (
	"errors"
	"log"
	"math"
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

// RegionSelectionScored is the cloudvm.vk.io/region-selection value that ranks the pinned region with its candidates.
const RegionSelectionScored = "scored"

// launchRegions returns the regions to try for spec, in order, at most maxLaunchRegions. Without candidate-regions
// only the pinned region is tried. With them, candidates need a ClusterFacts launch template and a tunnel route. By
// default the pinned region comes first and the candidates follow in descending placement score, keeping annotation
// order for ties; with region-selection "scored" the pinned region is ranked with them by rankRegions.
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
		if !spec.hasTunnelRoute(r) {
			log.Printf("[aws] Skipping candidate region %s for %s/%s: no usable tunnel route", //nolint:gosec // sanitized
				sanitizeLog(r), sanitizeLog(spec.PodNamespace), sanitizeLog(spec.PodName))
			continue
		}
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
	if spec.RegionSelection == RegionSelectionScored {
		regions = p.rankRegions(scorer, append(regions, fallbacks...), instanceTypes, spec.MarketType)
	} else {
		if scorer != nil {
			scores := regionScores(scorer.AllScores(), instanceTypes)
			sort.SliceStable(fallbacks, func(i, j int) bool {
				return scoreOrUnscored(scores, fallbacks[i]) > scoreOrUnscored(scores, fallbacks[j])
			})
		}
		regions = append(regions, fallbacks...)
	}
	if len(regions) > maxLaunchRegions {
		regions = regions[:maxLaunchRegions]
	}
	return regions
}

// regionQuotas exposes measured EC2 quota limits and account-wide usage; SpotPriceTicker implements it.
type regionQuotas interface {
	QuotaVCPUs(region, quota string) (float64, bool)
	QuotaUsedVCPUs(region, quota string) (float64, bool)
}

// regionSpotPrices exposes current spot prices; SpotPriceTicker implements it.
type regionSpotPrices interface {
	GetPrice(region, instanceType string) (*SpotPriceEntry, bool)
}

// regionRank is what rankRegions orders a region by.
type regionRank struct {
	quotaShort bool    // the remaining quota is known and cannot fit the smallest requested type
	score      int32   // best spot placement score across the requested types, -1 when unscored or on-demand
	price      float64 // cheapest hourly price across the requested types, +Inf when unknown
}

// rankRegions orders regions by expected launch success, then cost: regions whose known remaining quota cannot fit
// the smallest requested type go last; then higher spot placement score first (spot launches only); then lower
// hourly price. Ties keep the given order, which puts the pinned region first.
func (p *AWSProvider) rankRegions(scorer RegionPlacementScorer, regions, instanceTypes []string, marketType string) []string {
	capacityType := "spot"
	if marketType == "on-demand" {
		capacityType = "on-demand"
	}
	var scores map[string]int32
	if scorer != nil && capacityType == "spot" {
		scores = regionScores(scorer.AllScores(), instanceTypes)
	}
	quotas, _ := scorer.(regionQuotas)
	prices, _ := scorer.(regionSpotPrices)

	ranks := make(map[string]regionRank, len(regions))
	for _, r := range regions {
		rank := regionRank{score: scoreOrUnscored(scores, r), price: math.Inf(1)}
		rank.quotaShort = quotas != nil && !p.quotaFits(quotas, r, instanceTypes, capacityType)
		for _, t := range instanceTypes {
			price := 0.0
			if prices != nil && capacityType == "spot" {
				if e, ok := prices.GetPrice(r, t); ok {
					price = e.SpotPriceUSD
				}
			}
			if price <= 0 {
				price = GlobalOnDemandPrices.Cached(r, t)
			}
			if price > 0 && price < rank.price {
				rank.price = price
			}
		}
		ranks[r] = rank
	}
	out := append([]string(nil), regions...)
	sort.SliceStable(out, func(i, j int) bool {
		a, b := ranks[out[i]], ranks[out[j]]
		if a.quotaShort != b.quotaShort {
			return !a.quotaShort
		}
		if a.score != b.score {
			return a.score > b.score
		}
		return a.price < b.price
	})
	log.Printf("[aws] Ranked launch regions %s by quota, placement score and price", sanitizeLog(strings.Join(out, ","))) //nolint:gosec // sanitized
	return out
}

// quotaFits reports whether some requested type fits the region's remaining quota: the measured limit less the
// account-wide usage AWS reports, or less this plugin's live fleet when that usage could not be read. A quota the
// ticker has not measured fits.
func (p *AWSProvider) quotaFits(quotas regionQuotas, region string, instanceTypes []string, capacityType string) bool {
	usage := p.quotaUsage
	if usage == nil {
		usage = GlobalFleetTracker.QuotaVCPUsInUse
	}
	for _, t := range instanceTypes {
		quota := awsQuotaName(t, capacityType)
		limit, ok := quotas.QuotaVCPUs(region, quota)
		if !ok {
			return true
		}
		inUse, known := quotas.QuotaUsedVCPUs(region, quota)
		if !known {
			inUse = usage("aws", region, quota)
		}
		capacity := CapacityForInstanceType(t, ResourceVector{})
		if limit-inUse >= float64(capacity.CPU.Value()) {
			return true
		}
	}
	return false
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
	if errors.Is(err, errNoInstancesLaunched) || errors.Is(err, errNoOfferedPlacements) || errors.Is(err, errNoLaunchTemplateFits) {
		return true
	}
	if class := ClassifyAWSError(err); class == "no_capacity" || class == "quota" {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "UnfulfillableCapacity") || strings.Contains(msg, "InvalidFleetConfiguration")
}
