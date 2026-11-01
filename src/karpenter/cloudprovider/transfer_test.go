// Tests of egress-aware offering prices: amortized image pulls, steady traffic, measured profiles, and the spread.

package cloudprovider

import (
	"context"
	"math"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

func usdPerGB(v float64) *float64 { return &v }

// testTransfer prices the test catalog's regions against a us-east-1 home: eu-west-1 at 0.02 per GB each way, the
// home region free, ap-south-1 unknown, and us-central1 at 0.005 per GB sent home.
func testTransfer() *nodeclaim.CatalogTransfer {
	return &nodeclaim.CatalogTransfer{
		HomeProvider: "aws",
		HomeRegion:   "us-east-1",
		Regions: []nodeclaim.RegionTransfer{
			{Provider: "aws", Region: "us-east-1", FromHomeUSDPerGB: usdPerGB(0), ToHomeUSDPerGB: usdPerGB(0)},
			{Provider: "aws", Region: "eu-west-1", FromHomeUSDPerGB: usdPerGB(0.02), ToHomeUSDPerGB: usdPerGB(0.02)},
			{Provider: "aws", Region: "ap-south-1"},
			{Provider: "gcp", Region: "us-central1", ToHomeUSDPerGB: usdPerGB(0.005)},
		},
	}
}

// g6Prices returns the g6.2xlarge offering prices by region and capacity type.
func g6Prices(t *testing.T, its []*cloudprovider.InstanceType) map[string]float64 {
	t.Helper()
	prices := map[string]float64{}
	for _, o := range findType(t, its, "g6.2xlarge").Offerings {
		prices[o.Requirements.Get(corev1.LabelTopologyRegion).Any()+"/"+o.CapacityType()] = o.Price
	}
	return prices
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestTransferCostAmortizesPullsAndPricesSteadyTraffic(t *testing.T) {
	h := newHarness(t, &Config{TransferCost: TransferCost{Default: TransferProfile{
		ImagePullGB:           100, // 100 GB at 0.02 over 4 hours: 0.50 per hour
		ExpectedLifetime:      Duration{4 * time.Hour},
		HomeTransmitGBPerHour: 5, // 0.10 per hour
		HomeReceiveGBPerHour:  5, // 0.10 per hour, billed to the home region
	}}})
	h.api.catalog.Transfer = testTransfer()
	its, err := h.cp.GetInstanceTypes(context.Background(), testNodePool(nil))
	if err != nil {
		t.Fatal(err)
	}
	prices := g6Prices(t, its)
	for key, want := range map[string]float64{"us-east-1/spot": 0.40, "us-east-1/on-demand": 0.98, "eu-west-1/spot": 0.30 + 0.50 + 0.10 + 0.10} {
		if got := prices[key]; !near(got, want) {
			t.Errorf("price %s = %v, want %v", key, got, want)
		}
	}
}

func TestTransferCostUsesMeasuredProfileAndLifetime(t *testing.T) {
	h := newHarness(t, &Config{TransferCost: TransferCost{
		Default:        TransferProfile{ImagePullGB: 1000, HomeTransmitGBPerHour: 1000},
		NodePools:      map[string]TransferProfile{"other": {ImagePullGB: 5}},
		MinMeasuredVMs: 3,
	}})
	transfer := testTransfer()
	transfer.NodePools = []nodeclaim.NodePoolTransfer{
		// 20 GB at 0.02 over a measured 2 hours is 0.20 per hour; 10 GB an hour home is 0.20 more.
		{NodePool: "cloudvm-gpu", VMs: 3, ImagePullGBPerVM: 20, HomeTransmitGBPerHour: 10, LifetimeHours: 2},
		{NodePool: "other", VMs: 1, ImagePullGBPerVM: 500, LifetimeHours: 1},
	}
	h.api.catalog.Transfer = transfer
	its, err := h.cp.GetInstanceTypes(context.Background(), testNodePool(nil))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := g6Prices(t, its)["eu-west-1/spot"], 0.30+0.20+0.20; !near(got, want) {
		t.Errorf("measured eu-west-1 price = %v, want %v", got, want)
	}

	// Too few measured VMs keep the NodePool's configured entry; the measured lifetime still amortizes its pulls.
	other := testNodePool(nil)
	other.Name = "other"
	its, err = h.cp.GetInstanceTypes(context.Background(), other)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := g6Prices(t, its)["eu-west-1/spot"], 0.30+5*0.02/1; !near(got, want) {
		t.Errorf("configured eu-west-1 price = %v, want %v", got, want)
	}
}

func TestTransferCostWithoutRatesKeepsInstancePrices(t *testing.T) {
	h := newHarness(t, &Config{TransferCost: TransferCost{Default: TransferProfile{HomeTransmitGBPerHour: 10}}})
	its, err := h.cp.GetInstanceTypes(context.Background(), testNodePool(nil))
	if err != nil {
		t.Fatal(err)
	}
	if got := g6Prices(t, its)["eu-west-1/spot"]; !near(got, 0.30) {
		t.Errorf("price without catalog transfer rates = %v, want the instance price", got)
	}
	p := transferPricer{rates: map[string]nodeclaim.RegionTransfer{"aws/ap-south-1": {Provider: "aws", Region: "ap-south-1"}}, profile: TransferProfile{HomeTransmitGBPerHour: 10}, lifetime: time.Hour}
	if got := p.perHour("aws", "ap-south-1"); got != 0 {
		t.Errorf("region with unknown rates adds %v", got)
	}
}

func TestTransferCostShapesTheSpread(t *testing.T) {
	// Without transfer, eu-west-1 spot (0.30) is cheapest and us-east-1 spot (0.40) is outside a 10% tolerance. With
	// 0.11 an hour of transfer, eu-west-1 costs 0.41 and us-east-1 becomes the cheapest, with eu-west-1 within 10%.
	h := newHarness(t, &Config{TransferCost: TransferCost{Default: TransferProfile{HomeTransmitGBPerHour: 5.5}}})
	h.api.catalog.Transfer = testTransfer()
	its, err := h.cp.GetInstanceTypes(context.Background(), testNodePool(nil))
	if err != nil {
		t.Fatal(err)
	}
	candidates := compatibleOfferings(its, testNodeClaimRequirements("g6.2xlarge"))
	if len(candidates) < 2 || candidates[0].offering.Zone() != "us-east-1a" || candidates[1].offering.Zone() != "eu-west-1a" {
		t.Fatalf("candidates are not ordered by the transfer-adjusted price: %v", candidateZones(candidates))
	}
	regions := map[string]bool{}
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		chosen, _ := spreadOffering(candidates, name, h.cp.config.OfferingSpread)
		regions[chosen.offering.Zone()] = true
	}
	if !regions["us-east-1a"] || !regions["eu-west-1a"] || len(regions) != 2 {
		t.Errorf("claims spread over %v, want the two offerings within the tolerance", regions)
	}
}

func testNodeClaimRequirements(instanceType string) scheduling.Requirements {
	return scheduling.NewNodeSelectorRequirementsWithMinValues(req(corev1.LabelInstanceTypeStable, instanceType))
}

func candidateZones(cs []candidate) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.offering.Zone()+"/"+c.offering.CapacityType())
	}
	return out
}
