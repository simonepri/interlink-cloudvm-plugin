// Chooses the offering a NodeClaim launches with: the cheapest compatible offerings, spread across claims by name.

package cloudprovider

import (
	"cmp"
	"hash/fnv"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// candidate is one launchable offering of an instance type.
type candidate struct {
	it       *cloudprovider.InstanceType
	offering *cloudprovider.Offering
}

// compatibleOfferings returns the launchable offerings compatible with reqs among the instance types the requirements
// allow, cheapest first, breaking price ties by instance type, zone and capacity type for a stable order.
func compatibleOfferings(its []*cloudprovider.InstanceType, reqs scheduling.Requirements) []candidate {
	var out []candidate
	allowed := reqs.Get(corev1.LabelInstanceTypeStable)
	for _, it := range its {
		if !allowed.Has(it.Name) {
			continue
		}
		for _, o := range it.Offerings.Launchable().Compatible(reqs) {
			out = append(out, candidate{it: it, offering: o})
		}
	}
	slices.SortFunc(out, func(a, b candidate) int {
		return cmp.Or(
			cmp.Compare(a.offering.Price, b.offering.Price),
			strings.Compare(a.it.Name, b.it.Name),
			strings.Compare(a.offering.Zone(), b.offering.Zone()),
			strings.Compare(a.offering.CapacityType(), b.offering.CapacityType()),
		)
	})
	return out
}

// spreadOffering picks the offering a claim launches with from candidates sorted cheapest first. It keeps at most
// spread.MaxOfferings of the cheapest candidates priced within spread.PriceTolerance of the cheapest one, and chooses
// among them by a hash of the claim name. Claims created together then try several offerings at once instead of all
// trying the cheapest, so one round of launches learns which of them have capacity; a claim may cost up to the
// tolerance more than the cheapest offering in exchange.
func spreadOffering(candidates []candidate, claimName string, spread OfferingSpread) (candidate, bool) {
	if len(candidates) == 0 {
		return candidate{}, false
	}
	ceiling := candidates[0].offering.Price * (1 + spread.PriceTolerance)
	n := 1
	for n < len(candidates) && n < spread.MaxOfferings && candidates[n].offering.Price <= ceiling {
		n++
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(claimName))
	return candidates[h.Sum64()%uint64(n)], true //nolint:gosec // n is a positive slice length
}
