// Unit tests of every CloudProvider method against the fake plugin API and a fake Kubernetes client.

package cloudprovider

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/awslabs/operatorpkg/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"

	"github.com/simonepri/interlink-cloudvm-plugin/karpenter/apis/v1alpha1"
	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

type harness struct {
	cp    *CloudProvider
	api   *fakeAPI
	clock *clocktesting.FakeClock
	kube  client.Client
}

func testNodeClass() *v1alpha1.CloudVMNodeClass {
	return &v1alpha1.CloudVMNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu"},
		Spec: v1alpha1.CloudVMNodeClassSpec{
			Footprint: []v1alpha1.FootprintRegion{
				{Provider: "aws", Region: "us-east-1", SubnetIDs: []string{"subnet-a", "subnet-b"}},
				{Provider: "aws", Region: "eu-west-1", LaunchTemplateID: "lt-eu"},
				{Provider: "gcp", Region: "us-central1"},
			},
			LaunchTemplateName: "cloudvm-worker",
			Annotations:        map[string]string{"cloudvm.vk.io/max-egress-rate": "0.50"},
		},
		Status: v1alpha1.CloudVMNodeClassStatus{Conditions: []status.Condition{{
			Type: status.ConditionReady, Status: metav1.ConditionTrue, Reason: "Ready", LastTransitionTime: metav1.Now(),
		}}},
	}
}

func newHarness(t *testing.T, cfg *Config, objs ...client.Object) *harness {
	t.Helper()
	if cfg == nil {
		cfg = &Config{}
	}
	if err := cfg.complete(); err != nil {
		t.Fatal(err)
	}
	if len(objs) == 0 {
		objs = []client.Object{testNodeClass()}
	}
	kube := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objs...).WithStatusSubresource(objs...).Build()
	api := newFakeAPI(testCatalog())
	clk := clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0))
	return &harness{cp: New(kube, api, cfg, clk), api: api, clock: clk, kube: kube}
}

func testNodePool(labels map[string]string) *karpv1.NodePool {
	return &karpv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "cloudvm-gpu", Labels: labels},
		Spec: karpv1.NodePoolSpec{Template: karpv1.NodeClaimTemplate{Spec: karpv1.NodeClaimTemplateSpec{
			NodeClassRef: &karpv1.NodeClassReference{Group: v1alpha1.Group, Kind: "CloudVMNodeClass", Name: "gpu"},
		}}},
	}
}

func testNodeClaim(reqs ...karpv1.NodeSelectorRequirementWithMinValues) *karpv1.NodeClaim {
	return &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "cloudvm-gpu-abcde", Labels: map[string]string{karpv1.NodePoolLabelKey: "cloudvm-gpu", "team": "ml"}},
		Spec: karpv1.NodeClaimSpec{
			NodeClassRef:  &karpv1.NodeClassReference{Group: v1alpha1.Group, Kind: "CloudVMNodeClass", Name: "gpu"},
			Requirements:  reqs,
			Taints:        []corev1.Taint{{Key: "virtual-node.interlink/provider", Value: "cloudvm", Effect: corev1.TaintEffectNoSchedule}},
			StartupTaints: []corev1.Taint{{Key: "cloudvm.vk.io/booting", Effect: corev1.TaintEffectNoSchedule}},
		},
	}
}

func req(key string, values ...string) karpv1.NodeSelectorRequirementWithMinValues {
	return karpv1.NodeSelectorRequirementWithMinValues{Key: key, Operator: corev1.NodeSelectorOpIn, Values: values}
}

func findType(t *testing.T, its []*cloudprovider.InstanceType, name string) *cloudprovider.InstanceType {
	t.Helper()
	for _, it := range its {
		if it.Name == name {
			return it
		}
	}
	t.Fatalf("instance type %s not returned", name)
	return nil
}

func TestGetSupportedNodeClasses(t *testing.T) {
	h := newHarness(t, nil)
	got := h.cp.GetSupportedNodeClasses()
	if len(got) != 1 {
		t.Fatalf("got %d node classes, want 1", len(got))
	}
	if _, ok := got[0].(*v1alpha1.CloudVMNodeClass); !ok {
		t.Fatalf("got %T, want *CloudVMNodeClass", got[0])
	}
}

func TestGetInstanceTypesFiltersFootprintAndDerivesLabels(t *testing.T) {
	cfg := &Config{
		GPUClasses: []GPUClass{{Class: "l4", Models: []string{"L4"}, MemoryGB: 24}},
		DataCost:   DataCost{HomeRegion: "us-east-1", DefaultUSDPerHour: 0.05, PerRegionUSDPerHour: map[string]float64{"eu-west-1": 0.20}},
	}
	h := newHarness(t, cfg)
	its, err := h.cp.GetInstanceTypes(context.Background(), testNodePool(nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(its) != 3 {
		t.Fatalf("got %d instance types, want 3", len(its))
	}
	g6 := findType(t, its, "g6.2xlarge")
	// ap-south-1 is outside the footprint.
	if len(g6.Offerings) != 3 {
		t.Fatalf("g6 has %d offerings, want 3", len(g6.Offerings))
	}
	wantLabels := map[string]string{
		corev1.LabelInstanceTypeStable: "g6.2xlarge",
		corev1.LabelArchStable:         "amd64",
		LabelGPUClass:                  "l4",
		LabelCloud:                     "aws",
		LabelInstanceFamily:            "g6",
		LabelInstanceCategory:          "g",
		LabelInstanceGeneration:        "6",
		LabelInstanceSize:              "2xlarge",
		LabelInstanceCPU:               "8",
		LabelInstanceMemory:            "32768",
		LabelInstanceGPUName:           "l4",
		LabelInstanceGPUManufacturer:   "nvidia",
		LabelInstanceGPUCount:          "1",
		LabelInstanceGPUMemory:         "24576",
		LabelNVIDIAGPUPresent:          "true",
	}
	for k, v := range wantLabels {
		if got := g6.Requirements.Get(k); !got.Has(v) || got.Len() != 1 {
			t.Errorf("g6 requirement %s = %v, want [%s]", k, got.Values(), v)
		}
	}
	if regions := g6.Requirements.Get(corev1.LabelTopologyRegion).Values(); len(regions) != 2 {
		t.Errorf("g6 regions = %v, want us-east-1 and eu-west-1", regions)
	}
	if q := g6.Capacity[nodeclaim.ResourceGPU]; q.Value() != 1 {
		t.Errorf("g6 gpu capacity = %s, want 1", q.String())
	}

	prices := map[string]float64{}
	for _, o := range g6.Offerings {
		prices[o.Zone()+"/"+o.CapacityType()] = o.Price
		if r := o.Requirements.Get(corev1.LabelTopologyRegion); r.Len() != 1 {
			t.Errorf("offering %s has region requirement %v", o.Zone(), r.Values())
		}
	}
	for key, want := range map[string]float64{"us-east-1a/spot": 0.40, "us-east-1b/on-demand": 0.98, "eu-west-1a/spot": 0.50} {
		if got := prices[key]; got < want-1e-9 || got > want+1e-9 {
			t.Errorf("price %s = %v, want %v", key, got, want)
		}
	}

	m7 := findType(t, its, "m7i.xlarge")
	for _, k := range []string{LabelGPUClass, LabelInstanceGPUName, LabelInstanceGPUCount, LabelNVIDIAGPUPresent} {
		if op := m7.Requirements.Get(k).Operator(); op != corev1.NodeSelectorOpDoesNotExist {
			t.Errorf("m7i requirement %s operator = %s, want DoesNotExist", k, op)
		}
	}
	// A DaemonSet that selects NVIDIA GPU nodes by the GPU feature discovery label fits the GPU type only.
	gpuDaemon := scheduling.NewRequirements(scheduling.NewRequirement(LabelNVIDIAGPUPresent, corev1.NodeSelectorOpIn, "true"))
	if err := g6.Requirements.Compatible(gpuDaemon, scheduling.AllowUndefinedWellKnownLabels); err != nil {
		t.Errorf("a DaemonSet selecting NVIDIA GPU nodes does not fit g6: %v", err)
	}
	if err := m7.Requirements.Compatible(gpuDaemon, scheduling.AllowUndefinedWellKnownLabels); err == nil {
		t.Error("a DaemonSet selecting NVIDIA GPU nodes fits m7i")
	}
	if got := m7.Requirements.Get(LabelInstanceSize).Any(); got != "xlarge" {
		t.Errorf("m7i size = %q, want xlarge from the name", got)
	}
	gcp := findType(t, its, "g2-standard-8")
	if got := gcp.Offerings[0].Price; got != 0.30 {
		t.Errorf("gcp price = %v, want 0.25 + 0.05 default data cost", got)
	}
}

func TestGetInstanceTypesUnmappedGPUHasNoClass(t *testing.T) {
	h := newHarness(t, nil)
	its, err := h.cp.GetInstanceTypes(context.Background(), testNodePool(nil))
	if err != nil {
		t.Fatal(err)
	}
	g6 := findType(t, its, "g6.2xlarge")
	if op := g6.Requirements.Get(LabelGPUClass).Operator(); op != corev1.NodeSelectorOpDoesNotExist {
		t.Fatalf("gpu-class operator = %s, want DoesNotExist without a mapping", op)
	}
}

func TestGetInstanceTypesZoneRestriction(t *testing.T) {
	nc := testNodeClass()
	nc.Spec.Footprint = []v1alpha1.FootprintRegion{{Provider: "aws", Region: "us-east-1", Zones: []string{"us-east-1b"}}}
	h := newHarness(t, nil, nc)
	its, err := h.cp.GetInstanceTypes(context.Background(), testNodePool(nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(its) != 1 || its[0].Name != "g6.2xlarge" || len(its[0].Offerings) != 1 || its[0].Offerings[0].Zone() != "us-east-1b" {
		t.Fatalf("got %d types; want only g6.2xlarge in us-east-1b", len(its))
	}
}

func TestGetInstanceTypesAdvertiseThePlannedDisk(t *testing.T) {
	h := newHarness(t, nil)
	h.api.catalog.InstanceTypes[0].EphemeralStorage = "250G"
	its, err := h.cp.GetInstanceTypes(context.Background(), testNodePool(nil))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, it := range its {
		q := it.Capacity[corev1.ResourceEphemeralStorage]
		got[it.Name] = q.String()
	}
	if got["g6.2xlarge"] != "250G" || got["m7i.xlarge"] != DefaultEphemeralStorage {
		t.Errorf("ephemeral-storage = %v, want the planned 250G for g6.2xlarge and the %s default for a type without one", got, DefaultEphemeralStorage)
	}
}

func TestCreateReportsThePlannedDiskBeforeThePluginReadsIt(t *testing.T) {
	h := newHarness(t, nil)
	h.api.omitStorage = true
	h.api.catalog.InstanceTypes[0].EphemeralStorage = "250G"
	got, err := h.cp.Create(context.Background(), testNodeClaim(req(corev1.LabelInstanceTypeStable, "g6.2xlarge")))
	if err != nil {
		t.Fatal(err)
	}
	for name, list := range map[string]corev1.ResourceList{"capacity": got.Status.Capacity, "allocatable": got.Status.Allocatable} {
		if q := list[corev1.ResourceEphemeralStorage]; q.Cmp(resource.MustParse("250G")) != 0 {
			t.Errorf("%s ephemeral-storage = %s, want the 250G the instance type was planned with", name, q.String())
		}
	}
}

func TestGetInstanceTypesNodePoolSelector(t *testing.T) {
	h := newHarness(t, &Config{NodePoolSelector: map[string]string{"cloudvm-controller": "blue"}})
	its, err := h.cp.GetInstanceTypes(context.Background(), testNodePool(map[string]string{"cloudvm-controller": "green"}))
	if err != nil || len(its) != 0 {
		t.Fatalf("non-matching pool got %d types, err %v; want none", len(its), err)
	}
	its, err = h.cp.GetInstanceTypes(context.Background(), testNodePool(map[string]string{"cloudvm-controller": "blue"}))
	if err != nil || len(its) == 0 {
		t.Fatalf("matching pool got %d types, err %v; want some", len(its), err)
	}
}

func TestGetInstanceTypesMissingNodeClass(t *testing.T) {
	h := newHarness(t, nil, &v1alpha1.CloudVMNodeClass{ObjectMeta: metav1.ObjectMeta{Name: "other"}})
	its, err := h.cp.GetInstanceTypes(context.Background(), testNodePool(nil))
	if err != nil || its != nil {
		t.Fatalf("got %v, %v; want nil, nil", its, err)
	}
}

func TestGetInstanceTypesCatalogCache(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	for range 3 {
		if _, err := h.cp.GetInstanceTypes(ctx, testNodePool(nil)); err != nil {
			t.Fatal(err)
		}
	}
	if h.api.catalogCalls != 1 {
		t.Fatalf("catalog calls = %d, want 1 within the TTL", h.api.catalogCalls)
	}
	h.api.catalogErr = errors.New("plugin down")
	h.clock.Step(2 * DefaultCatalogTTL)
	if _, err := h.cp.GetInstanceTypes(ctx, testNodePool(nil)); err != nil {
		t.Fatalf("stale catalog not served during outage: %v", err)
	}
	h.clock.Step(DefaultCatalogMaxStaleness)
	if _, err := h.cp.GetInstanceTypes(ctx, testNodePool(nil)); err == nil {
		t.Fatal("expected an error once the cached catalog is too old")
	}
}

func TestCreateChoosesCheapestCompatibleOffering(t *testing.T) {
	h := newHarness(t, &Config{GPUClasses: []GPUClass{{Class: "l4", Models: []string{"l4"}}}})
	nc := testNodeClaim(
		req(corev1.LabelInstanceTypeStable, "g6.2xlarge", "g2-standard-8"),
		req(LabelGPUClass, "l4"),
		req(karpv1.CapacityTypeLabelKey, "spot", "on-demand"),
	)
	got, err := h.cp.Create(context.Background(), nc)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.api.creates) != 1 {
		t.Fatalf("plugin creates = %d, want 1", len(h.api.creates))
	}
	sent := h.api.creates[0]
	if sent.Name != nc.Name || sent.InstanceType != "g2-standard-8" || sent.Region != "us-central1" || sent.Zone != "us-central1-a" || sent.CapacityType != "spot" {
		t.Fatalf("launched %+v, want g2-standard-8 spot in us-central1-a (cheapest)", sent)
	}
	if sent.Annotations["cloudvm.vk.io/provider"] != "gcp" || sent.Annotations["cloudvm.vk.io/max-egress-rate"] != "0.50" {
		t.Errorf("launch annotations = %v", sent.Annotations)
	}
	if _, ok := sent.Annotations["cloudvm.vk.io/aws-launch-template-name"]; ok {
		t.Errorf("gcp launch carries an AWS launch template: %v", sent.Annotations)
	}
	if sent.Labels["team"] != "ml" || sent.Labels[LabelGPUClass] != "l4" || sent.Labels[LabelNVIDIAGPUPresent] != "true" || sent.Labels[corev1.LabelHostname] != nc.Name {
		t.Errorf("node labels = %v", sent.Labels)
	}
	if len(sent.Taints) != 2 {
		t.Errorf("taints = %v, want the pool taint and the startup taint", sent.Taints)
	}
	if got.Status.ProviderID != "cloudvm://gcp/us-central1/i-0001/vm" {
		t.Errorf("providerID = %q", got.Status.ProviderID)
	}
	for k, v := range map[string]string{
		corev1.LabelTopologyRegion:     "us-central1",
		corev1.LabelTopologyZone:       "us-central1-a",
		karpv1.CapacityTypeLabelKey:    "spot",
		corev1.LabelInstanceTypeStable: "g2-standard-8",
		LabelInstanceFamily:            "g2",
	} {
		if got.Labels[k] != v {
			t.Errorf("returned label %s = %q, want %q", k, got.Labels[k], v)
		}
	}
	if got.Annotations[AnnotationNodeClassHash] == "" {
		t.Error("nodeclass hash annotation missing")
	}
	if q := got.Status.Capacity[nodeclaim.ResourceGPU]; q.Value() != 1 {
		t.Errorf("capacity gpu = %s", q.String())
	}
	for name, list := range map[string]corev1.ResourceList{"capacity": got.Status.Capacity, "allocatable": got.Status.Allocatable} {
		if q := list[corev1.ResourceEphemeralStorage]; q.Cmp(resource.MustParse(fakeRootVolume)) != 0 {
			t.Errorf("%s ephemeral-storage = %s, want the VM's root volume %s", name, q.String(), fakeRootVolume)
		}
	}
}

func TestCreateHonorsRegionRequirement(t *testing.T) {
	h := newHarness(t, nil)
	nc := testNodeClaim(
		req(corev1.LabelInstanceTypeStable, "g6.2xlarge", "g2-standard-8"),
		req(corev1.LabelTopologyRegion, "us-east-1"),
	)
	if _, err := h.cp.Create(context.Background(), nc); err != nil {
		t.Fatal(err)
	}
	sent := h.api.creates[0]
	if sent.Region != "us-east-1" || sent.Zone != "us-east-1a" || sent.CapacityType != "spot" {
		t.Fatalf("launched %+v, want us-east-1a spot", sent)
	}
	if sent.Annotations["cloudvm.vk.io/subnets"] != "subnet-a,subnet-b" || sent.Annotations["cloudvm.vk.io/aws-launch-template-name"] != "cloudvm-worker" {
		t.Errorf("launch annotations = %v", sent.Annotations)
	}
}

func TestCreateCountsDataCostInChoice(t *testing.T) {
	// eu-west-1 spot is 0.30 vs us-east-1 spot 0.40, but a 0.20 data adder makes the home region cheaper.
	h := newHarness(t, &Config{DataCost: DataCost{HomeRegion: "us-east-1", PerRegionUSDPerHour: map[string]float64{"eu-west-1": 0.20}}})
	nc := testNodeClaim(req(corev1.LabelInstanceTypeStable, "g6.2xlarge"))
	if _, err := h.cp.Create(context.Background(), nc); err != nil {
		t.Fatal(err)
	}
	if got := h.api.creates[0].Region; got != "us-east-1" {
		t.Fatalf("launched in %s, want us-east-1 after the data cost adder", got)
	}
}

func TestCreateInsufficientCapacityMarksOfferingUnavailable(t *testing.T) {
	h := newHarness(t, &Config{LaunchFallback: LaunchFallback{MaxAttempts: 1}})
	h.api.capacityErrors["g6.2xlarge/eu-west-1a/spot"] = true
	ctx := context.Background()
	nc := testNodeClaim(req(corev1.LabelInstanceTypeStable, "g6.2xlarge"))
	_, err := h.cp.Create(ctx, nc)
	if !cloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("err = %v, want InsufficientCapacityError", err)
	}
	its, err := h.cp.GetInstanceTypes(ctx, testNodePool(nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range findType(t, its, "g6.2xlarge").Offerings {
		if o.Zone() == "eu-west-1a" && o.Available {
			t.Fatal("refused offering still available")
		}
	}
	if _, err := h.cp.Create(ctx, nc); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if got := h.api.creates[1].Zone; got != "us-east-1a" {
		t.Fatalf("retry launched in %s, want the next cheapest us-east-1a", got)
	}
	h.clock.Step(DefaultUnavailableOfferingTTL)
	if h.cp.unavailable.IsUnavailable("g6.2xlarge", "eu-west-1a", "spot") {
		t.Fatal("offering still unavailable after the TTL")
	}
}

func TestCreateNoCompatibleOffering(t *testing.T) {
	h := newHarness(t, nil)
	nc := testNodeClaim(req(corev1.LabelInstanceTypeStable, "g6.2xlarge"), req(corev1.LabelTopologyRegion, "ap-south-1"))
	if _, err := h.cp.Create(context.Background(), nc); !cloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("err = %v, want InsufficientCapacityError", err)
	}
	if len(h.api.creates) != 0 {
		t.Fatal("plugin was called without a compatible offering")
	}
}

func TestCreateNodeClassNotReady(t *testing.T) {
	nc := testNodeClass()
	nc.Status.Conditions[0].Status = metav1.ConditionFalse
	nc.Status.Conditions[0].Message = "broken"
	h := newHarness(t, nil, nc)
	_, err := h.cp.Create(context.Background(), testNodeClaim(req(corev1.LabelInstanceTypeStable, "g6.2xlarge")))
	if !cloudprovider.IsNodeClassNotReadyError(err) {
		t.Fatalf("err = %v, want NodeClassNotReadyError", err)
	}
}

func TestCreatePluginErrorIsNotCapacity(t *testing.T) {
	h := newHarness(t, nil)
	h.api.createErr = &nodeclaim.APIError{Status: 500, Reason: nodeclaim.ReasonInternal, Message: "boom"}
	_, err := h.cp.Create(context.Background(), testNodeClaim(req(corev1.LabelInstanceTypeStable, "g6.2xlarge")))
	if err == nil || cloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("err = %v, want a plain error", err)
	}
	if len(h.api.creates) != 1 {
		t.Fatalf("plugin creates = %d, want no other offering tried after an error that is not about capacity", len(h.api.creates))
	}
}

func TestDelete(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	nc := testNodeClaim(req(corev1.LabelInstanceTypeStable, "g6.2xlarge"))
	if _, err := h.cp.Create(ctx, nc); err != nil {
		t.Fatal(err)
	}
	if err := h.cp.Delete(ctx, nc); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	if err := h.cp.Delete(ctx, nc); !cloudprovider.IsNodeClaimNotFoundError(err) {
		t.Fatalf("second delete err = %v, want NodeClaimNotFoundError", err)
	}
}

func TestCreateLabelsRegionWideOfferingWithItsZone(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	h.api.catalog.InstanceTypes[1].Offerings = []nodeclaim.Offering{
		{Region: "us-east-1", CapacityType: "on-demand", PricePerHour: 0.20, Available: true},
	}
	h.api.launchZone = "us-east-1c"
	created, err := h.cp.Create(ctx, testNodeClaim(req(corev1.LabelInstanceTypeStable, "m7i.xlarge")))
	if err != nil {
		t.Fatal(err)
	}
	if sent := h.api.creates[0]; sent.Zone != "" {
		t.Fatalf("launch zone = %q, want none so the cloud picks it", sent.Zone)
	}
	got, err := h.cp.Get(ctx, created.Status.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	its, err := h.cp.GetInstanceTypes(ctx, testNodePool(nil))
	if err != nil {
		t.Fatal(err)
	}
	// Karpenter prices a NodeClaim by the offering whose zone and capacity type match its labels.
	it := findType(t, its, "m7i.xlarge")
	for _, nc := range []*karpv1.NodeClaim{created, got} {
		zone, capacityType := nc.Labels[corev1.LabelTopologyZone], nc.Labels[karpv1.CapacityTypeLabelKey]
		if !slices.ContainsFunc(it.Offerings, func(o *cloudprovider.Offering) bool {
			return o.Zone() == zone && o.CapacityType() == capacityType
		}) {
			t.Errorf("NodeClaim labels zone %q, capacity type %q match no offering of m7i.xlarge", zone, capacityType)
		}
	}
}

func TestGet(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	created, err := h.cp.Create(ctx, testNodeClaim(req(corev1.LabelInstanceTypeStable, "g6.2xlarge")))
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.cp.Get(ctx, created.Status.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "cloudvm-gpu-abcde" || got.Labels[corev1.LabelTopologyZone] != "eu-west-1a" || got.Status.NodeName != got.Name {
		t.Fatalf("got %+v", got.ObjectMeta)
	}
	if _, err := h.cp.Get(ctx, "cloudvm://aws/us-east-1/i-missing/vm"); !cloudprovider.IsNodeClaimNotFoundError(err) {
		t.Fatalf("missing err = %v, want NodeClaimNotFoundError", err)
	}
	if _, err := h.cp.Get(ctx, "aws:///us-east-1a/i-0123"); !cloudprovider.IsNodeClaimNotFoundError(err) {
		t.Fatalf("foreign providerID err = %v, want NodeClaimNotFoundError", err)
	}
}

func TestList(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	if _, err := h.cp.Create(ctx, testNodeClaim(req(corev1.LabelInstanceTypeStable, "g6.2xlarge"))); err != nil {
		t.Fatal(err)
	}
	got, err := h.cp.List(ctx)
	if err != nil || len(got) != 1 || got[0].Status.ProviderID == "" {
		t.Fatalf("got %v, %v; want one claim with a providerID", got, err)
	}
	h.api.listErr = errors.New("store down")
	if got, err := h.cp.List(ctx); err == nil || got != nil {
		t.Fatalf("got %v, %v; want nil and an error", got, err)
	}
}

func TestIsDrifted(t *testing.T) {
	ctx := context.Background()
	launched := func(h *harness) *karpv1.NodeClaim {
		t.Helper()
		nc := testNodeClaim(req(corev1.LabelInstanceTypeStable, "g6.2xlarge"), req(corev1.LabelTopologyRegion, "us-east-1"))
		got, err := h.cp.Create(ctx, nc)
		if err != nil {
			t.Fatal(err)
		}
		nc.Labels = got.Labels
		nc.Annotations = got.Annotations
		return nc
	}
	update := func(h *harness, mutate func(*v1alpha1.CloudVMNodeClass)) {
		t.Helper()
		cls := &v1alpha1.CloudVMNodeClass{}
		if err := h.kube.Get(ctx, client.ObjectKey{Name: "gpu"}, cls); err != nil {
			t.Fatal(err)
		}
		mutate(cls)
		if err := h.kube.Update(ctx, cls); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		name   string
		mutate func(*v1alpha1.CloudVMNodeClass)
		want   cloudprovider.DriftReason
	}{
		{"unchanged", func(*v1alpha1.CloudVMNodeClass) {}, ""},
		{"other region changed", func(c *v1alpha1.CloudVMNodeClass) { c.Spec.Footprint[1].LaunchTemplateID = "lt-new" }, ""},
		{"region added", func(c *v1alpha1.CloudVMNodeClass) {
			c.Spec.Footprint = append(c.Spec.Footprint, v1alpha1.FootprintRegion{Provider: "aws", Region: "us-west-2"})
		}, ""},
		{"region removed", func(c *v1alpha1.CloudVMNodeClass) { c.Spec.Footprint = c.Spec.Footprint[1:] }, DriftRegionRemoved},
		{"zone removed", func(c *v1alpha1.CloudVMNodeClass) { c.Spec.Footprint[0].Zones = []string{"us-east-1b"} }, DriftZoneRemoved},
		{"subnets changed", func(c *v1alpha1.CloudVMNodeClass) { c.Spec.Footprint[0].SubnetIDs = []string{"subnet-c"} }, DriftNodeClass},
		{"launch template changed", func(c *v1alpha1.CloudVMNodeClass) { c.Spec.LaunchTemplateName = "cloudvm-worker-v2" }, DriftNodeClass},
		{"passthrough annotation changed", func(c *v1alpha1.CloudVMNodeClass) { c.Spec.Annotations["cloudvm.vk.io/max-egress-rate"] = "1.00" }, DriftNodeClass},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, nil)
			nc := launched(h)
			update(h, tt.mutate)
			got, err := h.cp.IsDrifted(ctx, nc)
			if err != nil || got != tt.want {
				t.Fatalf("IsDrifted = %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}

func TestRebootRefusesEveryNodeClaim(t *testing.T) {
	h := newHarness(t, nil)
	foreign := &karpv1.NodeClaim{Status: karpv1.NodeClaimStatus{ProviderID: "aws:///us-east-1a/i-0123"}}
	if err := h.cp.Reboot(context.Background(), foreign, "op"); !cloudprovider.IsNodeRebootNotImplementedError(err) {
		t.Fatalf("err = %v, want NodeRebootNotImplementedError", err)
	}
}

func TestRepairPoliciesHaveOneDefault(t *testing.T) {
	h := newHarness(t, nil)
	defaults := 0
	for _, p := range h.cp.RepairPolicies() {
		if p.ReasonRegex == "" {
			defaults++
		}
	}
	if defaults != 1 {
		t.Fatalf("got %d default repair policies, want exactly 1", defaults)
	}
}

func TestOfferingRegionRequirementFiltersCompatibility(t *testing.T) {
	h := newHarness(t, nil)
	its, err := h.cp.GetInstanceTypes(context.Background(), testNodePool(nil))
	if err != nil {
		t.Fatal(err)
	}
	reqs := scheduling.NewRequirements(scheduling.NewRequirement(corev1.LabelTopologyRegion, corev1.NodeSelectorOpIn, "eu-west-1"))
	got := findType(t, its, "g6.2xlarge").Offerings.Compatible(reqs)
	if len(got) != 1 || got[0].Zone() != "eu-west-1a" {
		t.Fatalf("region-filtered offerings = %d, want only eu-west-1a", len(got))
	}
}

// TestNodeClaimReportsEveryAdvertisedResource checks, for every instance type the provider advertises, that each
// resource of its capacity, any of which Karpenter may request, is in the NodeClaim that Create, Get and List return
// and in the plugin's claim record. The node agent reports every resource of that record on the Node, which its
// TestNodeReportsEveryClaimResource checks.
func TestNodeClaimReportsEveryAdvertisedResource(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	its, err := h.cp.GetInstanceTypes(ctx, testNodePool(nil))
	if err != nil || len(its) == 0 {
		t.Fatalf("GetInstanceTypes = %d types, %v", len(its), err)
	}
	for _, it := range its {
		nc := testNodeClaim(req(corev1.LabelInstanceTypeStable, it.Name))
		nc.Name = "claim-" + it.Name
		created, err := h.cp.Create(ctx, nc)
		if err != nil {
			t.Fatalf("Create %s: %v", it.Name, err)
		}
		got, err := h.cp.Get(ctx, created.Status.ProviderID)
		if err != nil {
			t.Fatalf("Get %s: %v", it.Name, err)
		}
		listed, err := h.cp.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		returned := map[string]*karpv1.NodeClaim{"Create": created, "Get": got}
		for _, l := range listed {
			if l.Name == nc.Name {
				returned["List"] = l
			}
		}
		record := h.api.claims[nc.Name]
		for name := range it.Capacity {
			for method, out := range returned {
				capacity, allocatable := out.Status.Capacity[name], out.Status.Allocatable[name]
				if capacity.IsZero() || allocatable.IsZero() {
					t.Errorf("%s: %s NodeClaim reports %s capacity %s and allocatable %s", it.Name, method, name, capacity.String(), allocatable.String())
				}
			}
			if q, err := resource.ParseQuantity(record.Capacity[string(name)]); err != nil || q.IsZero() {
				t.Errorf("%s: plugin claim record reports %s = %q, which its Node would lack", it.Name, name, record.Capacity[string(name)])
			}
		}
		if len(returned) != 3 {
			t.Errorf("%s: List did not return its NodeClaim", it.Name)
		}
	}
}
