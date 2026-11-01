// Transfer profiles: what each NodePool's VMs outside the home region pull from and exchange with it, for the catalog's
// egress-aware offering prices, and each VM's recent transfer spend for the spend gauge.
package main

import (
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

const (
	// transferProfileMinAge is how long a VM must have reported before its traffic counts in a profile, so its image
	// pulls are done and its hourly rate is not a burst.
	transferProfileMinAge = 10 * time.Minute
	// transferProfileFinished is how many VMs that stopped reporting a NodePool's profile keeps.
	transferProfileFinished = 200
	// transferSpendWindow is how far back a VM's transfer spend rate looks.
	transferSpendWindow = 15 * time.Minute
)

// vmTransferProfile is one VM's traffic with the home region while it reported.
type vmTransferProfile struct {
	nodePool    string
	remote      bool
	first, last time.Time
	// pullBytes is what its image pulls received from the home region.
	pullBytes int64
	// homeTx and homeRx are all its bytes to and from the home region, image pulls included, directly or tunneled.
	homeTx, homeRx int64
}

func (p *vmTransferProfile) addHome(direction string, delta int64) {
	if direction == directionTransmit {
		p.homeTx += delta
	} else {
		p.homeRx += delta
	}
}

func (p vmTransferProfile) counts() bool {
	return p.nodePool != "" && p.remote && p.last.Sub(p.first) >= transferProfileMinAge
}

func (v *meteredVM) addCost(now time.Time, usd float64) {
	minute := now.Unix() / 60
	if n := len(v.costs); n > 0 && v.costs[n-1].minute == minute {
		v.costs[n-1].usd += usd
		return
	}
	v.costs = append(v.costs, minuteCost{minute: minute, usd: usd})
	oldest := minute - int64(transferSpendWindow/time.Minute)
	v.costs = slices.DeleteFunc(v.costs, func(c minuteCost) bool { return c.minute <= oldest })
}

// retire keeps the profile of a VM that stopped reporting for its NodePool.
func (t *TransferMeter) retire(p vmTransferProfile) {
	if !p.counts() {
		return
	}
	t.finished[p.nodePool] = append(t.finished[p.nodePool], p)
	if done := t.finished[p.nodePool]; len(done) > transferProfileFinished {
		t.finished[p.nodePool] = done[len(done)-transferProfileFinished:]
	}
}

// SpendPerHour returns a VM's priced transfer per hour over the last transferSpendWindow.
func (t *TransferMeter) SpendPerHour(instanceID string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	v := t.vms[instanceID]
	if v == nil {
		return 0
	}
	oldest := t.now().Unix()/60 - int64(transferSpendWindow/time.Minute)
	var usd float64
	for _, c := range v.costs {
		if c.minute > oldest {
			usd += c.usd
		}
	}
	return usd / transferSpendWindow.Hours()
}

// NodePoolTransfers returns each NodePool's average image pulls from the home region per VM, its other traffic with
// the home region per VM-hour, from its live and recently finished VMs outside the home region, and the average
// lifetime of the finished ones.
func (t *TransferMeter) NodePoolTransfers() []nodeclaim.NodePoolTransfer {
	t.mu.Lock()
	defer t.mu.Unlock()
	byPool := make(map[string][]vmTransferProfile, len(t.finished))
	for pool, done := range t.finished {
		byPool[pool] = append(byPool[pool], done...)
	}
	for _, v := range t.vms {
		if v.profile.counts() {
			byPool[v.profile.nodePool] = append(byPool[v.profile.nodePool], v.profile)
		}
	}
	out := make([]nodeclaim.NodePoolTransfer, 0, len(byPool))
	for pool, profiles := range byPool {
		var pull, tx, rx int64
		var hours float64
		for _, p := range profiles {
			pull += p.pullBytes
			tx += p.homeTx
			// The VM's receive from home includes its image pulls, which the pull average already counts.
			rx += max(p.homeRx-p.pullBytes, 0)
			hours += p.last.Sub(p.first).Hours()
		}
		profile := nodeclaim.NodePoolTransfer{
			NodePool:              pool,
			VMs:                   len(profiles),
			ImagePullGBPerVM:      float64(pull) / bytesPerGB / float64(len(profiles)),
			HomeTransmitGBPerHour: float64(tx) / bytesPerGB / hours,
			HomeReceiveGBPerHour:  float64(rx) / bytesPerGB / hours,
		}
		if done := t.finished[pool]; len(done) > 0 {
			var lived float64
			for _, p := range done {
				lived += p.last.Sub(p.first).Hours()
			}
			profile.LifetimeHours = lived / float64(len(done))
		}
		out = append(out, profile)
	}
	slices.SortFunc(out, func(a, b nodeclaim.NodePoolTransfer) int { return strings.Compare(a.NodePool, b.NodePool) })
	return out
}

// CatalogTransfer returns the transfer rates between each provider's regions and the home region, and the measured
// NodePool profiles, or nil without a home region.
func (t *TransferMeter) CatalogTransfer(regions map[string][]string) *nodeclaim.CatalogTransfer {
	home, ok := t.cost.Home()
	if !ok {
		return nil
	}
	out := &nodeclaim.CatalogTransfer{HomeProvider: home.Provider, HomeRegion: home.Region, NodePools: t.NodePoolTransfers()}
	for _, provider := range slices.Sorted(maps.Keys(regions)) {
		for _, region := range slices.Compact(slices.Sorted(slices.Values(regions[provider]))) {
			loc := TransferLocation{Provider: provider, Region: region}
			rt := nodeclaim.RegionTransfer{Provider: provider, Region: region}
			if rate, ok := t.cost.Rate(home, loc); ok {
				rt.FromHomeUSDPerGB = &rate
			}
			if rate, ok := t.cost.Rate(loc, home); ok {
				rt.ToHomeUSDPerGB = &rate
			}
			out.Regions = append(out.Regions, rt)
		}
	}
	return out
}

// karpenterNodePoolLabel is the label Karpenter puts on a NodeClaim, and the plugin receives with its launch, naming
// the NodePool it belongs to.
const karpenterNodePoolLabel = "karpenter.sh/nodepool"

func init() {
	GlobalFleetTracker.transferSpend = GlobalTransferMeter.SpendPerHour
}
