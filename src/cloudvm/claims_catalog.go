// Instance catalog of the NodeClaim API: the hardware of every tracked instance type with one priced offering per
// region, zone and capacity type, from the embedded ticker's spot prices and the on-demand list prices.
package main

import (
	"context"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

// catalogDescribeTimeout bounds the DescribeInstanceTypes calls of one catalog request.
const catalogDescribeTimeout = 20 * time.Second

// handleCatalog serves GET /v1/catalog?regions=a,b.
func (s *Server) handleCatalog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.mu.RLock()
	ticker := s.spotTicker
	s.mu.RUnlock()
	if ticker == nil {
		writeClaimError(w, apiError(http.StatusServiceUnavailable, nodeclaim.ReasonCatalogUnavailable, "the catalog needs the embedded spot ticker (ENABLE_TICKER=true)"))
		return
	}
	var regions []string
	if raw := r.URL.Query().Get("regions"); raw != "" {
		regions = parseCommaList(raw)
	} else {
		regions = ticker.Regions()
	}
	ctx, cancel := context.WithTimeout(r.Context(), catalogDescribeTimeout)
	defer cancel()
	writeClaimJSON(w, http.StatusOK, s.buildCatalog(ctx, ticker, regions))
}

// launchableTypeLister is implemented by providers that boot instance types only from launch templates.
type launchableTypeLister interface {
	LaunchableInstanceTypes(ctx context.Context, region string, facts map[string]InstanceTypeFacts) map[string]bool
}

// buildCatalog lists the ticker's tracked types and the default CPU types in the given regions. A type gets a spot
// offering in each zone the ticker prices it in, or one region-wide spot offering at the on-demand price when the
// ticker does not track it, and one region-wide on-demand offering at its list price. A region offers only the types its
// launch templates accept. Offerings that recently failed for lack of capacity are unavailable.
func (s *Server) buildCatalog(ctx context.Context, ticker *SpotPriceTicker, regions []string) *nodeclaim.Catalog {
	names := ticker.TrackedInstanceTypes()
	for _, n := range DefaultCPUInstanceTypes {
		if !slices.Contains(names, n) {
			names = append(names, n)
		}
	}
	var describer InstanceTypeDescriber
	if p, ok := s.defaultProv.(*AWSProvider); ok {
		if cl, err := p.getClient(ctx, s.defaultRegion); err == nil {
			describer, _ = cl.(InstanceTypeDescriber)
		}
	}
	facts := globalInstanceFacts.resolve(ctx, describer, names)
	// A type boots in a region only from a launch template that accepts it, so the catalog offers it only there.
	launchable := make(map[string]map[string]bool, len(regions))
	if lister, ok := s.defaultProv.(launchableTypeLister); ok {
		for _, region := range regions {
			launchable[region] = lister.LaunchableInstanceTypes(ctx, region, facts)
		}
	}

	spot := make(map[string]map[string]map[string]float64) // type -> region -> zone -> price
	for _, e := range ticker.AllPrices() {
		if e == nil || e.SpotPriceUSD <= 0 || e.AvailabilityZone == "" {
			continue
		}
		if spot[e.InstanceType] == nil {
			spot[e.InstanceType] = make(map[string]map[string]float64)
		}
		if spot[e.InstanceType][e.Region] == nil {
			spot[e.InstanceType][e.Region] = make(map[string]float64)
		}
		spot[e.InstanceType][e.Region][e.AvailabilityZone] = e.SpotPriceUSD
	}

	now := time.Now()
	s.claims.mu.Lock()
	unavailable := make(map[string]bool, len(s.claims.unavailable))
	for key, until := range s.claims.unavailable {
		unavailable[key] = now.Before(until)
	}
	s.claims.mu.Unlock()
	offer := func(itype, region, zone, capacityType string, price float64) nodeclaim.Offering {
		return nodeclaim.Offering{
			Region: region, Zone: zone, CapacityType: capacityType, PricePerHour: price,
			Available: !unavailable[offeringKey(itype, region, zone, capacityType)],
		}
	}

	out := &nodeclaim.Catalog{InstanceTypes: make([]nodeclaim.InstanceType, 0, len(facts))}
	for _, name := range names {
		f, ok := facts[name]
		if !ok || f.VCPUs == 0 {
			continue
		}
		it := catalogInstanceType(f)
		for _, region := range regions {
			if set, ok := launchable[region]; ok && !set[name] {
				continue
			}
			onDemand := GlobalOnDemandPrices.Cached(region, name)
			zones := spot[name][region]
			for _, zone := range slices.Sorted(maps.Keys(zones)) {
				it.Offerings = append(it.Offerings, offer(name, region, zone, nodeclaim.CapacityTypeSpot, zones[zone]))
			}
			if onDemand > 0 {
				if len(zones) == 0 {
					// A spot price never exceeds the on-demand price, so the list price bounds an untracked spot type.
					it.Offerings = append(it.Offerings, offer(name, region, "", nodeclaim.CapacityTypeSpot, onDemand))
				}
				it.Offerings = append(it.Offerings, offer(name, region, "", nodeclaim.CapacityTypeOnDemand, onDemand))
			}
		}
		if len(it.Offerings) > 0 {
			out.InstanceTypes = append(out.InstanceTypes, it)
		}
	}
	return out
}

// catalogInstanceType renders the hardware of an instance type in the API shape.
func catalogInstanceType(f InstanceTypeFacts) nodeclaim.InstanceType {
	family, size, _ := strings.Cut(f.Name, ".")
	_, generation := splitFamily(family)
	it := nodeclaim.InstanceType{
		Name:         f.Name,
		Provider:     string(ProviderAWS),
		CPU:          strconv.Itoa(f.VCPUs),
		Memory:       strconv.FormatInt(f.MemoryMiB, 10) + "Mi",
		Architecture: f.Arch,
		Family:       family,
		Generation:   generation,
		Size:         size,
	}
	if f.GPUs > 0 {
		it.GPUCount = f.GPUs
		it.GPUModel = f.GPUName
		it.GPUMemoryGB = roundGPUMemoryGB(f.GPUMemoryMiB)
		it.GPUManufacturer = f.GPUManufacturer
	}
	return it
}
