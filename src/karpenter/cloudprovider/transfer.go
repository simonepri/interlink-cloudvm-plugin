// Prices the data transfer an offering's VM exchanges with the home region, from the catalog's rates and the
// NodePool's configured or measured transfer profile.

package cloudprovider

import (
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

// transferPricer adds one NodePool's transfer cost to offerings by region.
type transferPricer struct {
	rates    map[string]nodeclaim.RegionTransfer
	profile  TransferProfile
	lifetime time.Duration
}

// transferPricerFor resolves a NodePool's transfer profile: the plugin's measurements once at least
// MinMeasuredVMs VMs back them, else the configured profile, and the expected lifetime from the configuration, then
// the measured lifetime of finished VMs, then DefaultVMLifetime.
func (c *CloudProvider) transferPricerFor(catalog *nodeclaim.Catalog, nodePool string) transferPricer {
	p := transferPricer{profile: c.config.TransferProfileFor(nodePool)}
	p.lifetime = p.profile.ExpectedLifetime.Duration
	if catalog.Transfer == nil {
		return p
	}
	p.rates = make(map[string]nodeclaim.RegionTransfer, len(catalog.Transfer.Regions))
	for _, r := range catalog.Transfer.Regions {
		p.rates[r.Provider+"/"+r.Region] = r
	}
	for _, m := range catalog.Transfer.NodePools {
		if m.NodePool != nodePool {
			continue
		}
		if least := c.config.TransferCost.MinMeasuredVMs; least > 0 && m.VMs >= least {
			p.profile.ImagePullGB = m.ImagePullGBPerVM
			p.profile.HomeTransmitGBPerHour = m.HomeTransmitGBPerHour
			p.profile.HomeReceiveGBPerHour = m.HomeReceiveGBPerHour
		}
		if p.lifetime == 0 && m.LifetimeHours > 0 {
			p.lifetime = time.Duration(m.LifetimeHours * float64(time.Hour))
		}
	}
	if p.lifetime == 0 {
		p.lifetime = DefaultVMLifetime
	}
	return p
}

// perHour returns the USD per hour a VM in the region adds for transfer with the home region: its image pulls,
// billed to the home region and spread over the VM's lifetime, and its steady traffic, each direction at its sender's
// rate. A region without a known rate adds nothing for that direction.
func (p transferPricer) perHour(provider, region string) float64 {
	r, ok := p.rates[provider+"/"+region]
	if !ok {
		return 0
	}
	var fromHome, toHome float64
	if r.FromHomeUSDPerGB != nil {
		fromHome = *r.FromHomeUSDPerGB
	}
	if r.ToHomeUSDPerGB != nil {
		toHome = *r.ToHomeUSDPerGB
	}
	pull := p.profile.ImagePullGB * fromHome / p.lifetime.Hours()
	return pull + p.profile.HomeTransmitGBPerHour*toHome + p.profile.HomeReceiveGBPerHour*fromHome
}
