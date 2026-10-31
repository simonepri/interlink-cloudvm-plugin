// Tests of offering choice for a scale-up: claims spread across the cheapest offerings within the price tolerance.

package cloudprovider

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/awslabs/operatorpkg/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"

	"github.com/simonepri/interlink-cloudvm-plugin/karpenter/apis/v1alpha1"
	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

// scaleUpRegions are the regions of the scale-up fixture, cheapest first; the last is priced past the tolerance.
var scaleUpRegions = []string{"us-east-1", "us-east-2", "us-west-2", "eu-west-1", "ap-northeast-1"}

// scaleUpHarness offers one on-demand GPU type region-wide in every scale-up region, at 1.00, 1.02, 1.04, 1.06 and
// 1.50 per hour.
func scaleUpHarness(t *testing.T, cfg *Config) *harness {
	t.Helper()
	prices := []float64{1.00, 1.02, 1.04, 1.06, 1.50}
	it := nodeclaim.InstanceType{
		Name: "g6.12xlarge", Provider: "aws", CPU: "48", Memory: "192Gi", GPUCount: 4, GPUModel: "l4",
		GPUMemoryGB: 24, GPUManufacturer: "NVIDIA", Architecture: "x86_64",
	}
	nodeClass := &v1alpha1.CloudVMNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu"},
		Status: v1alpha1.CloudVMNodeClassStatus{Conditions: []status.Condition{{
			Type: status.ConditionReady, Status: metav1.ConditionTrue, Reason: "Ready", LastTransitionTime: metav1.Now(),
		}}},
	}
	for i, region := range scaleUpRegions {
		it.Offerings = append(it.Offerings, nodeclaim.Offering{Region: region, CapacityType: "on-demand", PricePerHour: prices[i], Available: true})
		nodeClass.Spec.Footprint = append(nodeClass.Spec.Footprint, v1alpha1.FootprintRegion{Provider: "aws", Region: region})
	}
	h := newHarness(t, cfg, []client.Object{nodeClass}...)
	h.api.catalog = nodeclaim.Catalog{InstanceTypes: []nodeclaim.InstanceType{it}}
	// Every claim of a batch picks its offering before the first refusal comes back.
	h.api.refusalDelay = 200 * time.Millisecond
	return h
}

// launchBatch creates n claims concurrently, as Karpenter does for the claims of one scheduling round, and returns
// the region each claim was launched in, or "" for a claim that failed.
func launchBatch(t *testing.T, h *harness, n int) map[string]string {
	t.Helper()
	var mu sync.Mutex
	got := map[string]string{}
	var wg sync.WaitGroup
	for i := range n {
		nc := testNodeClaim(req(corev1.LabelInstanceTypeStable, "g6.12xlarge"), req(karpv1.CapacityTypeLabelKey, "on-demand"))
		nc.Name = fmt.Sprintf("cloudvm-on-demand-%03d", i)
		wg.Go(func() {
			created, err := h.cp.Create(context.Background(), nc)
			region := ""
			if err == nil {
				region = created.Labels[corev1.LabelTopologyRegion]
			} else if !cloudprovider.IsInsufficientCapacityError(err) {
				t.Errorf("Create %s: %v", nc.Name, err)
			}
			mu.Lock()
			got[nc.Name] = region
			mu.Unlock()
		})
	}
	wg.Wait()
	return got
}

func TestCreateSpreadsABatchAcrossOfferingsWithinTheTolerance(t *testing.T) {
	h := scaleUpHarness(t, nil)
	// The cheapest region has no capacity, as when every claim of a round used to go there and fail together.
	h.api.capacityErrors["g6.12xlarge/us-east-1/on-demand"] = true
	perRegion := map[string]int{}
	for _, region := range launchBatch(t, h, 40) {
		perRegion[region]++
	}
	if perRegion["ap-northeast-1"] > 0 {
		t.Errorf("launched %d claims past the price tolerance", perRegion["ap-northeast-1"])
	}
	for _, region := range scaleUpRegions[1:4] {
		if perRegion[region] == 0 {
			t.Errorf("no claim launched in %s; per region %v", region, perRegion)
		}
	}
	if perRegion[""] == 40 {
		t.Fatal("every claim of the batch failed in the one region without capacity")
	}
}

func TestCreateWithoutSpreadLaunchesEveryClaimWithTheCheapestOffering(t *testing.T) {
	h := scaleUpHarness(t, &Config{OfferingSpread: OfferingSpread{MaxOfferings: 1}})
	for name, region := range launchBatch(t, h, 10) {
		if region != "us-east-1" {
			t.Errorf("%s launched in %q, want the cheapest us-east-1", name, region)
		}
	}
}

func TestSpreadOfferingIsStablePerClaim(t *testing.T) {
	h := scaleUpHarness(t, nil)
	its, err := h.cp.GetInstanceTypes(context.Background(), testNodePool(nil))
	if err != nil {
		t.Fatal(err)
	}
	nc := testNodeClaim(req(corev1.LabelInstanceTypeStable, "g6.12xlarge"))
	reqs := scheduling.NewNodeSelectorRequirementsWithMinValues(nc.Spec.Requirements...)
	first, _ := spreadOffering(compatibleOfferings(its, reqs), nc.Name, h.cp.config.OfferingSpread)
	for range 5 {
		again, _ := spreadOffering(compatibleOfferings(its, reqs), nc.Name, h.cp.config.OfferingSpread)
		if again.offering.Zone() != first.offering.Zone() {
			t.Fatalf("claim %s chose %s, then %s", nc.Name, first.offering.Zone(), again.offering.Zone())
		}
	}
}
