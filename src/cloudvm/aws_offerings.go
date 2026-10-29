// Availability Zone offering filter for EC2 Fleet overrides: drops (instance type, subnet) pairs whose subnet AZ
// does not offer the instance type, using cached DescribeSubnets and DescribeInstanceTypeOfferings results.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// defaultOfferingTTL bounds how long an instance type's AZ offerings stay cached; AWS changes them rarely.
const defaultOfferingTTL = time.Hour

// errNoOfferedPlacements reports that no requested instance type is offered in any configured subnet's AZ.
var errNoOfferedPlacements = errors.New("no requested instance type is offered in the availability zones of the configured subnets")

type offeringEntry struct {
	zones     map[string]bool
	fetchedAt time.Time
}

// offeringCache caches subnet AZs (immutable) and per-region instance type AZ offerings (refreshed after ttl).
type offeringCache struct {
	mu       sync.Mutex
	ttl      time.Duration
	now      func() time.Time
	subnetAZ map[string]string        // region/subnet -> AZ name
	offered  map[string]offeringEntry // region/instance type -> offering AZ names
}

func newOfferingCache(ttl time.Duration) *offeringCache {
	return &offeringCache{
		ttl:      ttl,
		now:      time.Now,
		subnetAZ: make(map[string]string),
		offered:  make(map[string]offeringEntry),
	}
}

func (c *offeringCache) subnetZones(ctx context.Context, client EC2FleetClient, region string, subnetIDs []string) (map[string]string, error) {
	c.mu.Lock()
	zones := make(map[string]string, len(subnetIDs))
	var missing []string
	for _, id := range subnetIDs {
		if az, ok := c.subnetAZ[region+"/"+id]; ok {
			zones[id] = az
		} else {
			missing = append(missing, id)
		}
	}
	c.mu.Unlock()
	if len(missing) == 0 {
		return zones, nil
	}

	out, err := client.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{SubnetIds: missing})
	if err != nil {
		return nil, fmt.Errorf("ec2:DescribeSubnets failed: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, sn := range out.Subnets {
		id, az := aws.ToString(sn.SubnetId), aws.ToString(sn.AvailabilityZone)
		if id == "" || az == "" {
			continue
		}
		c.subnetAZ[region+"/"+id] = az
		zones[id] = az
	}
	return zones, nil
}

func (c *offeringCache) typeZones(ctx context.Context, client EC2FleetClient, region string, instanceTypes []string) (map[string]map[string]bool, error) {
	c.mu.Lock()
	now := c.now()
	result := make(map[string]map[string]bool, len(instanceTypes))
	var stale []string
	for _, itype := range instanceTypes {
		if e, ok := c.offered[region+"/"+itype]; ok && now.Sub(e.fetchedAt) < c.ttl {
			result[itype] = e.zones
		} else {
			stale = append(stale, itype)
		}
	}
	c.mu.Unlock()
	if len(stale) == 0 {
		return result, nil
	}

	fetched := make(map[string]map[string]bool, len(stale))
	for _, itype := range stale {
		fetched[itype] = make(map[string]bool)
	}
	pager := ec2.NewDescribeInstanceTypeOfferingsPaginator(client, &ec2.DescribeInstanceTypeOfferingsInput{
		LocationType: ec2types.LocationTypeAvailabilityZone,
		Filters:      []ec2types.Filter{{Name: aws.String("instance-type"), Values: stale}},
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("ec2:DescribeInstanceTypeOfferings failed: %w", err)
		}
		for _, o := range page.InstanceTypeOfferings {
			if zones, ok := fetched[string(o.InstanceType)]; ok {
				zones[aws.ToString(o.Location)] = true
			}
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for itype, zones := range fetched {
		c.offered[region+"/"+itype] = offeringEntry{zones: zones, fetchedAt: now}
		result[itype] = zones
	}
	return result, nil
}

// offeredFleetOverrides builds the fleet overrides for instanceTypes x subnetIDs, keeping only pairs whose subnet AZ
// offers the instance type. Pairs whose AZ cannot be resolved are kept, and a failed lookup returns every pair, so a
// lookup outage degrades to the unfiltered request instead of blocking launches. A non-empty zone keeps only the
// subnets known to be in that zone, and fails when none is.
func (p *AWSProvider) offeredFleetOverrides(ctx context.Context, client EC2FleetClient, region, zone string, instanceTypes, subnetIDs []string) ([]ec2types.FleetLaunchTemplateOverridesRequest, error) {
	all := BuildFleetOverrides(instanceTypes, subnetIDs)
	if len(subnetIDs) == 0 && zone == "" {
		return all, nil
	}
	cache := p.offerings()
	cleanRegion := sanitizeLog(region)

	subnetAZ, err := cache.subnetZones(ctx, client, region, subnetIDs)
	if zone != "" {
		if err != nil {
			return nil, fmt.Errorf("resolve the zones of subnets %s in %s for zone %s: %w", strings.Join(subnetIDs, ","), region, zone, err)
		}
		var inZone []string
		for _, subnet := range subnetIDs {
			if subnetAZ[subnet] == zone {
				inZone = append(inZone, subnet)
			}
		}
		if len(inZone) == 0 {
			return nil, fmt.Errorf("%w: no subnet of %s in %s is in zone %s", errNoOfferedPlacements, strings.Join(subnetIDs, ","), region, zone)
		}
		subnetIDs = inZone
		all = BuildFleetOverrides(instanceTypes, subnetIDs)
	}
	if err != nil {
		log.Printf("[aws] Warning: skipping AZ offering filter in %s: %v", cleanRegion, err) //nolint:gosec // region sanitized
		return all, nil
	}
	if len(subnetAZ) == 0 {
		return all, nil
	}
	typeAZ, err := cache.typeZones(ctx, client, region, instanceTypes)
	if err != nil {
		log.Printf("[aws] Warning: skipping AZ offering filter in %s: %v", cleanRegion, err) //nolint:gosec // region sanitized
		return all, nil
	}

	kept := make([]ec2types.FleetLaunchTemplateOverridesRequest, 0, len(all))
	var dropped []string
	for _, ov := range all {
		subnet := aws.ToString(ov.SubnetId)
		az, known := subnetAZ[subnet]
		if known && !typeAZ[string(ov.InstanceType)][az] {
			dropped = append(dropped, fmt.Sprintf("%s@%s", ov.InstanceType, az))
			continue
		}
		kept = append(kept, ov)
	}
	if len(dropped) > 0 {
		sort.Strings(dropped)
		log.Printf("[aws] Dropped %d fleet overrides not offered in %s: %s", len(dropped), cleanRegion, sanitizeLog(strings.Join(dropped, ","))) //nolint:gosec // sanitized
	}
	if len(kept) == 0 {
		return nil, fmt.Errorf("%w in %s (instance types %s; subnets %s)", errNoOfferedPlacements, region,
			strings.Join(instanceTypes, ","), strings.Join(subnetIDs, ","))
	}
	return kept, nil
}

func (p *AWSProvider) offerings() *offeringCache {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.offeringCache == nil {
		p.offeringCache = newOfferingCache(defaultOfferingTTL)
	}
	return p.offeringCache
}
