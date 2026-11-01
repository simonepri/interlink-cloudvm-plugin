// Unit tests of NodePool price cap filtering, boundary conditions and metrics.

package cloudprovider

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	"github.com/simonepri/interlink-cloudvm-plugin/karpenter/apis/v1alpha1"
	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

func counterVal(c prometheus.Counter) float64 {
	var m dto.Metric
	_ = c.Write(&m)
	return m.GetCounter().GetValue()
}

func floatPtr(v float64) *float64 {
	return &v
}

func TestPriceCapOffByDefaultKeepsAllOfferings(t *testing.T) {
	// Baseline harness with no price cap configured.
	cfg := &Config{
		GPUClasses: []GPUClass{{Class: "l4", Models: []string{"l4"}, MemoryGB: 24}},
	}
	h := newHarness(t, cfg)
	pool := testNodePool(nil)
	beforeDrops := counterVal(OfferingsDropped.WithLabelValues(pool.Name))

	its, err := h.cp.GetInstanceTypes(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}

	g6 := findType(t, its, "g6.2xlarge")
	// All 3 offerings inside footprint (us-east-1a spot, us-east-1b on-demand, eu-west-1a spot) are kept.
	if len(g6.Offerings) != 3 {
		t.Fatalf("expected 3 offerings with no cap configured, got %d", len(g6.Offerings))
	}

	afterDrops := counterVal(OfferingsDropped.WithLabelValues(pool.Name))
	if afterDrops != beforeDrops {
		t.Errorf("expected 0 offerings dropped, got %f", afterDrops-beforeDrops)
	}
}

func TestPriceCapMaxPricePerGPUHourBoundary(t *testing.T) {
	// g6.2xlarge is a 1-GPU instance.
	// In testCatalog:
	// - eu-west-1a: spot, 0.30
	// - us-east-1a: spot, 0.40
	// - us-east-1b: on-demand, 0.98
	// Setting maxPricePerGPUHour = 0.40 must:
	// - keep 0.30 (< cap)
	// - keep 0.40 (exactly at cap)
	// - drop 0.98 (above cap)
	// - increment OfferingsDropped by 1
	cfg := &Config{
		GPUClasses: []GPUClass{{Class: "l4", Models: []string{"l4"}, MemoryGB: 24}},
		PriceCap: PriceCap{
			Default: PriceCapProfile{
				MaxPricePerGPUHour: floatPtr(0.40),
			},
		},
	}
	h := newHarness(t, cfg)
	pool := testNodePool(nil)
	beforeDrops := counterVal(OfferingsDropped.WithLabelValues(pool.Name))

	its, err := h.cp.GetInstanceTypes(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}

	g6 := findType(t, its, "g6.2xlarge")
	if len(g6.Offerings) != 2 {
		t.Fatalf("expected 2 offerings after cap, got %d", len(g6.Offerings))
	}

	prices := map[string]float64{}
	for _, o := range g6.Offerings {
		prices[o.Zone()+"/"+o.CapacityType()] = o.Price
	}

	if p, ok := prices["eu-west-1a/spot"]; !ok || p != 0.30 {
		t.Errorf("expected eu-west-1a/spot at 0.30 kept, got %v (exists: %v)", p, ok)
	}
	// Offering exactly at cap must be kept:
	if p, ok := prices["us-east-1a/spot"]; !ok || p != 0.40 {
		t.Errorf("expected us-east-1a/spot at 0.40 kept (exactly at cap), got %v (exists: %v)", p, ok)
	}
	// Offering above cap must be dropped:
	if _, ok := prices["us-east-1b/on-demand"]; ok {
		t.Errorf("expected us-east-1b/on-demand at 0.98 to be dropped")
	}

	afterDrops := counterVal(OfferingsDropped.WithLabelValues(pool.Name))
	if afterDrops != beforeDrops+1 {
		t.Errorf("expected dropped metric to increase by 1, got increase of %f", afterDrops-beforeDrops)
	}
}

func TestPriceCapMaxPriceAboveCheapestPercentBoundary(t *testing.T) {
	// Construct a controlled catalog with 3 offerings for the same GPU class (l4) and capacity type (spot):
	// - us-east-1a: 0.40 (cheapest reference price)
	// - eu-west-1a: 0.50 (exactly 25% above 0.40, since 0.40 * 1.25 = 0.50)
	// - us-central1-a: 0.60 (50% above 0.40, which exceeds 25%)
	cfg := &Config{
		GPUClasses: []GPUClass{{Class: "l4", Models: []string{"l4"}, MemoryGB: 24}},
		PriceCap: PriceCap{
			NodePools: map[string]PriceCapProfile{
				"cloudvm-gpu": {
					MaxPriceAboveCheapestPercent: floatPtr(25.0),
				},
			},
		},
	}
	h := newHarness(t, cfg)
	h.api.catalog = nodeclaim.Catalog{
		InstanceTypes: []nodeclaim.InstanceType{
			{
				Name: "g6.2xlarge", Provider: "aws", CPU: "8", Memory: "32Gi", GPUCount: 1, GPUModel: "l4",
				GPUMemoryGB: 24, GPUManufacturer: "NVIDIA", Architecture: "x86_64", Family: "g6", Generation: "6", Size: "2xlarge",
				Offerings: []nodeclaim.Offering{
					{Region: "us-east-1", Zone: "us-east-1a", CapacityType: "spot", PricePerHour: 0.40, Available: true},
					{Region: "eu-west-1", Zone: "eu-west-1a", CapacityType: "spot", PricePerHour: 0.50, Available: true},
				},
			},
			{
				Name: "g2-standard-8", Provider: "gcp", CPU: "8", Memory: "32Gi", GPUCount: 1, GPUModel: "l4",
				GPUMemoryGB: 24, GPUManufacturer: "nvidia", Architecture: "amd64", Family: "g2",
				Offerings: []nodeclaim.Offering{
					{Region: "us-central1", Zone: "us-central1-a", CapacityType: "spot", PricePerHour: 0.60, Available: true},
				},
			},
		},
	}

	pool := testNodePool(nil)
	beforeDrops := counterVal(OfferingsDropped.WithLabelValues(pool.Name))

	its, err := h.cp.GetInstanceTypes(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}

	// g6.2xlarge should keep both 0.40 and 0.50 (0.50 is exactly at cap)
	g6 := findType(t, its, "g6.2xlarge")
	if len(g6.Offerings) != 2 {
		t.Fatalf("expected 2 offerings for g6.2xlarge, got %d", len(g6.Offerings))
	}
	prices := map[string]float64{}
	for _, o := range g6.Offerings {
		prices[o.Zone()] = o.Price
	}
	if p, ok := prices["us-east-1a"]; !ok || p != 0.40 {
		t.Errorf("cheapest offering 0.40 missing: %v", prices)
	}
	if p, ok := prices["eu-west-1a"]; !ok || p != 0.50 {
		t.Errorf("offering at exactly cap (0.50) missing: %v", prices)
	}

	// g2-standard-8 offering at 0.60 is dropped, so g2-standard-8 has 0 offerings and is omitted entirely
	for _, it := range its {
		if it.Name == "g2-standard-8" {
			t.Errorf("expected g2-standard-8 to be omitted because its only offering was above cap, but got %+v", it.Offerings)
		}
	}

	afterDrops := counterVal(OfferingsDropped.WithLabelValues(pool.Name))
	if afterDrops != beforeDrops+1 {
		t.Errorf("expected dropped metric to increase by 1, got %f", afterDrops-beforeDrops)
	}
}

func TestPriceCapComparesAgainstOfferingPriceWithTransferCost(t *testing.T) {
	// Base price of offering is 0.35. Transfer cost adds 0.10 => total offering price is 0.45.
	// With maxPricePerGPUHour = 0.40, the offering must be dropped because 0.45 > 0.40.
	cfg := &Config{
		GPUClasses: []GPUClass{{Class: "l4", Models: []string{"l4"}, MemoryGB: 24}},
		TransferCost: TransferCost{
			Default: TransferProfile{HomeTransmitGBPerHour: 5},
		},
		PriceCap: PriceCap{
			Default: PriceCapProfile{
				MaxPricePerGPUHour: floatPtr(0.40),
			},
		},
	}
	h := newHarness(t, cfg)
	// testTransfer sets eu-west-1 ToHomeUSDPerGB: 0.02.
	// 5 GB/hr * 0.02 = 0.10 USD/hr transfer cost.
	h.api.catalog.Transfer = testTransfer()
	h.api.catalog.InstanceTypes = []nodeclaim.InstanceType{
		{
			Name: "g6.2xlarge", Provider: "aws", CPU: "8", Memory: "32Gi", GPUCount: 1, GPUModel: "l4",
			GPUMemoryGB: 24, GPUManufacturer: "NVIDIA", Architecture: "x86_64", Family: "g6", Generation: "6", Size: "2xlarge",
			Offerings: []nodeclaim.Offering{
				// 0.35 + 0.10 = 0.45 > 0.40 -> dropped
				{Region: "eu-west-1", Zone: "eu-west-1a", CapacityType: "spot", PricePerHour: 0.35, Available: true},
				// 0.25 + 0.10 = 0.35 <= 0.40 -> kept
				{Region: "eu-west-1", Zone: "eu-west-1b", CapacityType: "spot", PricePerHour: 0.25, Available: true},
			},
		},
	}

	pool := testNodePool(nil)
	its, err := h.cp.GetInstanceTypes(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}

	g6 := findType(t, its, "g6.2xlarge")
	if len(g6.Offerings) != 1 {
		t.Fatalf("expected 1 offering, got %d", len(g6.Offerings))
	}
	if g6.Offerings[0].Zone() != "eu-west-1b" || g6.Offerings[0].Price != 0.35 {
		t.Errorf("unexpected offering kept: zone=%s price=%f", g6.Offerings[0].Zone(), g6.Offerings[0].Price)
	}
}

func TestPriceCapNodePoolOverrideWinsOverDefault(t *testing.T) {
	cfg := &Config{
		GPUClasses: []GPUClass{{Class: "l4", Models: []string{"l4"}, MemoryGB: 24}},
		PriceCap: PriceCap{
			Default: PriceCapProfile{
				MaxPricePerGPUHour: floatPtr(1.00),
			},
			NodePools: map[string]PriceCapProfile{
				"tight-pool": {
					MaxPricePerGPUHour: floatPtr(0.35),
				},
			},
		},
	}
	h := newHarness(t, cfg)

	tightPool := &karpv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "tight-pool"},
		Spec: karpv1.NodePoolSpec{Template: karpv1.NodeClaimTemplate{Spec: karpv1.NodeClaimTemplateSpec{
			NodeClassRef: &karpv1.NodeClassReference{Group: v1alpha1.Group, Kind: "CloudVMNodeClass", Name: "gpu"},
		}}},
	}
	defaultPool := &karpv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "default-pool"},
		Spec: karpv1.NodePoolSpec{Template: karpv1.NodeClaimTemplate{Spec: karpv1.NodeClaimTemplateSpec{
			NodeClassRef: &karpv1.NodeClassReference{Group: v1alpha1.Group, Kind: "CloudVMNodeClass", Name: "gpu"},
		}}},
	}

	tightIts, err := h.cp.GetInstanceTypes(context.Background(), tightPool)
	if err != nil {
		t.Fatal(err)
	}
	tightG6 := findType(t, tightIts, "g6.2xlarge")
	// Only eu-west-1a at 0.30 <= 0.35 is kept. us-east-1a (0.40) and us-east-1b (0.98) are dropped.
	if len(tightG6.Offerings) != 1 {
		t.Fatalf("expected 1 offering in tight-pool, got %d", len(tightG6.Offerings))
	}

	defaultIts, err := h.cp.GetInstanceTypes(context.Background(), defaultPool)
	if err != nil {
		t.Fatal(err)
	}
	defaultG6 := findType(t, defaultIts, "g6.2xlarge")
	// Default cap 1.00 keeps all 3 offerings (0.30, 0.40, 0.98)
	if len(defaultG6.Offerings) != 3 {
		t.Fatalf("expected 3 offerings in default-pool, got %d", len(defaultG6.Offerings))
	}
}
