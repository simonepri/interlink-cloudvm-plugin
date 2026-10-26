// Tests instance type derivation: Karpenter label requirements and resource fit over the catalog, price order, the
// fleet override cap, and explicit candidate-instance-types.
package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	corev1 "k8s.io/api/core/v1"
)

type fakeType struct {
	vcpus             int32
	memMiB            int64
	gpus              int32
	gpuMem            int32
	gpuName, gpuMaker string
}

var fakeCatalog = map[string]fakeType{
	"g4dn.xlarge":  {4, 16384, 1, 16384, "T4", "NVIDIA"},
	"g5.xlarge":    {4, 16384, 1, 24576, "A10G", "NVIDIA"},
	"g5.2xlarge":   {8, 32768, 1, 24576, "A10G", "NVIDIA"},
	"g6.xlarge":    {4, 16384, 1, 23034, "L4", "NVIDIA"},
	"g5g.xlarge":   {4, 8192, 1, 16384, "T4g", "NVIDIA"},
	"g5.12xlarge":  {48, 196608, 4, 24576, "A10G", "NVIDIA"},
	"p4d.24xlarge": {96, 1179648, 8, 40960, "A100", "NVIDIA"},
	"m7i.large":    {2, 8192, 0, 0, "", ""},
	"c7i.xlarge":   {4, 8192, 0, 0, "", ""},
	"r7i.xlarge":   {4, 32768, 0, 0, "", ""},
}

// fakeArmTypes are the fakeCatalog types with Arm processors.
var fakeArmTypes = map[string]bool{"g5g.xlarge": true}

// catalogClient serves DescribeInstanceTypes from fakeCatalog, ignoring names it does not know like EC2 filters do.
func catalogClient(calls *int) *mockEC2FleetClient {
	return &mockEC2FleetClient{describeInstanceTypesFn: func(_ context.Context, in *ec2.DescribeInstanceTypesInput) (*ec2.DescribeInstanceTypesOutput, error) {
		*calls++
		out := &ec2.DescribeInstanceTypesOutput{}
		for _, name := range in.Filters[0].Values {
			ft, ok := fakeCatalog[name]
			if !ok {
				continue
			}
			arch := ec2types.ArchitectureTypeX8664
			if fakeArmTypes[name] {
				arch = ec2types.ArchitectureTypeArm64
			}
			info := ec2types.InstanceTypeInfo{
				InstanceType:  ec2types.InstanceType(name),
				VCpuInfo:      &ec2types.VCpuInfo{DefaultVCpus: aws.Int32(ft.vcpus)},
				MemoryInfo:    &ec2types.MemoryInfo{SizeInMiB: aws.Int64(ft.memMiB)},
				ProcessorInfo: &ec2types.ProcessorInfo{SupportedArchitectures: []ec2types.ArchitectureType{arch}, Manufacturer: aws.String("Intel")},
			}
			if ft.gpus > 0 {
				info.GpuInfo = &ec2types.GpuInfo{Gpus: []ec2types.GpuDeviceInfo{{
					Name: aws.String(ft.gpuName), Manufacturer: aws.String(ft.gpuMaker), Count: aws.Int32(ft.gpus),
					MemoryInfo: &ec2types.GpuDeviceMemoryInfo{SizeInMiB: aws.Int32(ft.gpuMem)},
				}}}
			}
			out.InstanceTypes = append(out.InstanceTypes, info)
		}
		return out, nil
	}}
}

// fakeMarket tracks every GPU type of fakeCatalog and serves fixed spot prices.
type fakeMarket map[string]float64

func (m fakeMarket) AllScores() map[string]*PlacementScoreEntry { return nil }

func (m fakeMarket) TrackedInstanceTypes() []string {
	var out []string
	for name, ft := range fakeCatalog {
		if ft.gpus > 0 {
			out = append(out, name)
		}
	}
	return out
}

func (m fakeMarket) AllPrices() map[string]*SpotPriceEntry {
	out := map[string]*SpotPriceEntry{}
	for name, price := range m {
		for i, zone := range []string{"us-west-1a", "us-west-1c"} {
			out[name+zone] = &SpotPriceEntry{Region: "us-west-1", AvailabilityZone: zone, InstanceType: name, SpotPriceUSD: price + float64(i)}
		}
	}
	return out
}

func derivationProvider(t *testing.T) (*AWSProvider, *int) {
	t.Helper()
	globalInstanceFacts = &instanceFactsCache{facts: make(map[string]InstanceTypeFacts)}
	t.Setenv("AWS_INSTANCE_TYPES", "")
	p := newTestProvider(t)
	calls := 0
	p.SetRegionClient("us-west-1", catalogClient(&calls))
	p.SetRegionScorer(fakeMarket{"g5g.xlarge": 0.10, "g4dn.xlarge": 0.20, "g6.xlarge": 0.30, "g5.xlarge": 0.40, "g5.2xlarge": 0.50, "g5.12xlarge": 2.0, "p4d.24xlarge": 9.0})
	return p, &calls
}

func derivationSpec(t *testing.T, pod *Pod) *InstanceSpec {
	t.Helper()
	pod.Metadata.Name, pod.Metadata.Namespace = "burst", "research"
	pod.Metadata.Annotations = map[string]string{"cloudvm.vk.io/region": "us-west-1"}
	spec, err := PodToInstanceSpec(pod, "test-cluster")
	if err != nil {
		t.Fatalf("PodToInstanceSpec: %v", err)
	}
	return spec
}

func gpuPod(gpus int, cpu, memory string) *Pod {
	limits := map[string]string{"cpu": cpu, "memory": memory}
	if gpus > 0 {
		limits["nvidia.com/gpu"] = fmt.Sprint(gpus)
	}
	return &Pod{Spec: PodSpecWrapper{
		NodeSelector: map[string]string{"node.kubernetes.io/instance-type": "clvm", "type": "virtual-kubelet"},
		Containers:   []ContainerSpec{{Name: "main", Image: "alpine", Resources: ContainerResourceRequirements{Limits: limits}}},
	}}
}

func TestDeriveInstanceTypes_GPUPodGetsEveryFittingTypeCheapestFirst(t *testing.T) {
	p, _ := derivationProvider(t)
	spec := derivationSpec(t, gpuPod(1, "2", "8Gi"))
	if spec.InstanceType != "" {
		t.Fatalf("the virtual node's instance-type label must not become the VM type, got %q", spec.InstanceType)
	}
	got, err := p.launchInstanceTypes(context.Background(), spec)
	if err != nil {
		t.Fatalf("launchInstanceTypes: %v", err)
	}
	if want := "g4dn.xlarge,g6.xlarge,g5.xlarge,g5.2xlarge"; strings.Join(got, ",") != want {
		t.Fatalf("types = %v, want %s (exact GPU count, by spot price)", got, want)
	}
}

func TestDeriveInstanceTypes_RequestsAndKarpenterLabelsFilter(t *testing.T) {
	p, _ := derivationProvider(t)
	pod := gpuPod(1, "6", "16Gi")
	pod.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchExpressions: []corev1.NodeSelectorRequirement{
				{Key: "karpenter.k8s.aws/instance-gpu-memory", Operator: corev1.NodeSelectorOpGt, Values: []string{"23040"}},
				{Key: "karpenter.k8s.aws/instance-gpu-manufacturer", Operator: corev1.NodeSelectorOpIn, Values: []string{"nvidia"}},
			},
		}}},
	}}
	got, err := p.launchInstanceTypes(context.Background(), derivationSpec(t, pod))
	if err != nil {
		t.Fatalf("launchInstanceTypes: %v", err)
	}
	if want := "g5.2xlarge"; strings.Join(got, ",") != want {
		t.Fatalf("types = %v, want %s: only it has 6 vCPUs and more than 23040 MiB of GPU memory", got, want)
	}
}

func TestDeriveInstanceTypes_GPUCountRequirementAllowsLargerTypes(t *testing.T) {
	p, _ := derivationProvider(t)
	pod := gpuPod(1, "2", "8Gi")
	pod.Spec.NodeSelector["karpenter.k8s.aws/instance-gpu-name"] = "a10g"
	pod.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "karpenter.k8s.aws/instance-gpu-count", Operator: corev1.NodeSelectorOpGt, Values: []string{"0"}}},
		}}},
	}}
	got, err := p.launchInstanceTypes(context.Background(), derivationSpec(t, pod))
	if err != nil {
		t.Fatalf("launchInstanceTypes: %v", err)
	}
	if want := "g5.xlarge,g5.2xlarge,g5.12xlarge"; strings.Join(got, ",") != want {
		t.Fatalf("types = %v, want %s", got, want)
	}
}

func TestDeriveInstanceTypes_CPUPodGetsNoGPUTypes(t *testing.T) {
	p, _ := derivationProvider(t)
	got, err := p.launchInstanceTypes(context.Background(), derivationSpec(t, gpuPod(0, "3", "16Gi")))
	if err != nil {
		t.Fatalf("launchInstanceTypes: %v", err)
	}
	if want := "r7i.xlarge"; strings.Join(got, ",") != want {
		t.Fatalf("types = %v, want %s: the only CPU type with 3 vCPUs and 16 GiB", got, want)
	}
}

func TestDeriveInstanceTypes_OnDemandFromKarpenterCapacityType(t *testing.T) {
	pod := gpuPod(1, "2", "8Gi")
	pod.Spec.NodeSelector["karpenter.sh/capacity-type"] = "on-demand"
	if spec := derivationSpec(t, pod); spec.MarketType != "on-demand" {
		t.Fatalf("MarketType = %q, want on-demand", spec.MarketType)
	}
}

func TestDeriveInstanceTypes_FailsClearlyWhenNothingFits(t *testing.T) {
	p, _ := derivationProvider(t)
	_, err := p.Launch(context.Background(), derivationSpec(t, gpuPod(3, "2", "8Gi")))
	if err == nil || !strings.Contains(err.Error(), errNoInstanceTypeFits.Error()) {
		t.Fatalf("Launch error = %v, want errNoInstanceTypeFits", err)
	}
}

func TestDeriveInstanceTypes_CapsOverridesPerCreateFleet(t *testing.T) {
	p, _ := derivationProvider(t)
	spec := derivationSpec(t, gpuPod(1, "2", "8Gi"))
	for i := range 100 {
		spec.SubnetIDs = append(spec.SubnetIDs, fmt.Sprintf("subnet-%d", i))
	}
	got, err := p.launchInstanceTypes(context.Background(), spec)
	if err != nil {
		t.Fatalf("launchInstanceTypes: %v", err)
	}
	if want := "g4dn.xlarge,g6.xlarge,g5.xlarge"; strings.Join(got, ",") != want {
		t.Fatalf("types = %v, want the 3 cheapest so 100 subnets stay within %d overrides", got, maxFleetOverrides)
	}
}

func TestDeriveInstanceTypes_CachesDescribeInstanceTypes(t *testing.T) {
	p, calls := derivationProvider(t)
	for range 2 {
		if _, err := p.launchInstanceTypes(context.Background(), derivationSpec(t, gpuPod(1, "2", "8Gi"))); err != nil {
			t.Fatalf("launchInstanceTypes: %v", err)
		}
	}
	if *calls != 1 {
		t.Fatalf("DescribeInstanceTypes called %d times, want 1", *calls)
	}
}

func TestLaunchInstanceTypes_ExplicitCandidatesAreAuthoritative(t *testing.T) {
	p, calls := derivationProvider(t)
	pod := gpuPod(1, "2", "8Gi")
	spec := derivationSpec(t, pod)
	spec.CandidateInstanceTypes = []string{"p4d.24xlarge", "g5.12xlarge"}
	got, err := p.launchInstanceTypes(context.Background(), spec)
	if err != nil {
		t.Fatalf("launchInstanceTypes: %v", err)
	}
	if strings.Join(got, ",") != "p4d.24xlarge,g5.12xlarge" || *calls != 0 {
		t.Fatalf("types = %v after %d catalog calls, want the annotation list untouched", got, *calls)
	}
}

func TestPodInstanceRequirements_KeepsOnlyInstanceLabels(t *testing.T) {
	reqs := podInstanceRequirements(map[string]string{
		"node.kubernetes.io/instance-type":    "clvm",
		"type":                                "virtual-kubelet",
		"karpenter.k8s.aws/instance-gpu-name": "a10g",
	}, &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{Weight: 1, Preference: corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "karpenter.k8s.aws/instance-family", Operator: corev1.NodeSelectorOpIn, Values: []string{"g6"}}},
		}}},
	}})
	if len(reqs) != 1 || reqs[0].Key != "karpenter.k8s.aws/instance-gpu-name" {
		t.Fatalf("requirements = %+v, want only the GPU name; preferences and virtual node labels are not requirements", reqs)
	}
	if reqs := podInstanceRequirements(map[string]string{"node.kubernetes.io/instance-type": "g5.xlarge"}, nil); len(reqs) != 1 {
		t.Fatalf("a real EC2 instance type must stay a requirement, got %+v", reqs)
	}
}

func TestDeriveInstanceTypes_LaunchesOnlyArchitecturesWithAnAMI(t *testing.T) {
	p, _ := derivationProvider(t)
	// The Arm g5g.xlarge is the cheapest 1-GPU type, but the region's only template, the default one, has no image
	// architecture EC2 reports, so it counts as amd64.
	got, err := p.launchInstanceTypes(context.Background(), derivationSpec(t, gpuPod(1, "2", "8Gi")))
	if err != nil {
		t.Fatalf("launchInstanceTypes: %v", err)
	}
	if strings.Contains(strings.Join(got, ","), "g5g") {
		t.Fatalf("types = %v, want no Arm types for a pod that names no architecture", got)
	}

	pod := gpuPod(1, "2", "8Gi")
	pod.Spec.NodeSelector["kubernetes.io/arch"] = "arm64"
	if _, err := p.Launch(context.Background(), derivationSpec(t, pod)); err == nil || !strings.Contains(err.Error(), errNoInstanceTypeFits.Error()) {
		t.Fatalf("Launch error = %v, want errNoInstanceTypeFits for an arm64 pod", err)
	}
}

// armProvider is derivationProvider with an x86 and an Arm launch template in us-west-1.
func armProvider(t *testing.T) *AWSProvider {
	t.Helper()
	p, _ := derivationProvider(t)
	client, err := p.getClient(context.Background(), "us-west-1")
	if err != nil {
		t.Fatalf("getClient: %v", err)
	}
	client.(*mockEC2FleetClient).templateImages = map[string]ec2types.ArchitectureValues{
		"lt-x86": ec2types.ArchitectureValuesX8664, "lt-arm": ec2types.ArchitectureValuesArm64,
	}
	p.SetClusterFacts(&ClusterFacts{Regions: map[string]RegionalClusterFact{
		"us-west-1": {LaunchTemplates: []LaunchTemplateFact{{ID: "lt-x86"}, {ID: "lt-arm"}}, SubnetIDs: []string{"subnet-a"}},
	}})
	globalTemplateArch = &templateArchCache{entries: make(map[string]templateArchEntry)}
	return p
}

func TestDeriveInstanceTypes_ArmTemplateEnablesArmTypes(t *testing.T) {
	p := armProvider(t)

	got, err := p.launchInstanceTypes(context.Background(), derivationSpec(t, gpuPod(1, "2", "8Gi")))
	if err != nil {
		t.Fatalf("launchInstanceTypes: %v", err)
	}
	if got[0] != "g5g.xlarge" {
		t.Errorf("types = %v, want the cheapest g5g.xlarge first now that an Arm template exists", got)
	}

	pod := gpuPod(1, "2", "8Gi")
	pod.Spec.NodeSelector["kubernetes.io/arch"] = "arm64"
	got, err = p.launchInstanceTypes(context.Background(), derivationSpec(t, pod))
	if err != nil || strings.Join(got, ",") != "g5g.xlarge" {
		t.Errorf("arm64 pod types = %v (err %v), want g5g.xlarge", got, err)
	}
}

func TestLaunch_GroupsCandidatesByTemplate(t *testing.T) {
	p := armProvider(t)
	client, _ := p.getClient(context.Background(), "us-west-1")
	mock := client.(*mockEC2FleetClient)
	var configs []ec2types.FleetLaunchTemplateConfigRequest
	mock.createFleetFn = func(_ context.Context, in *ec2.CreateFleetInput) (*ec2.CreateFleetOutput, error) {
		configs = in.LaunchTemplateConfigs
		return &ec2.CreateFleetOutput{}, nil
	}
	spec := derivationSpec(t, gpuPod(1, "2", "8Gi"))
	spec.PodUID = "uid-arm"

	_, _ = p.Launch(context.Background(), spec)

	got := map[string][]string{}
	for _, c := range configs {
		for _, o := range c.Overrides {
			id := aws.ToString(c.LaunchTemplateSpecification.LaunchTemplateId)
			got[id] = append(got[id], string(o.InstanceType))
		}
	}
	if strings.Join(got["lt-arm"], ",") != "g5g.xlarge" || len(got["lt-x86"]) == 0 || strings.Contains(strings.Join(got["lt-x86"], ","), "g5g") {
		t.Errorf("fleet configs = %v, want g5g.xlarge on lt-arm and the x86 types on lt-x86", got)
	}
}

func TestGroupByTemplate_DeclaredRequirementsRouteGPUSlices(t *testing.T) {
	slice := InstanceTypeFacts{Name: "g6f.xlarge", Arch: "amd64", VCPUs: 4, MemoryMiB: 16384, GPUs: 1, GPUName: "l4", GPUManufacturer: "nvidia", GPUMemoryMiB: 2861}
	whole := InstanceTypeFacts{Name: "g6.xlarge", Arch: "amd64", VCPUs: 4, MemoryMiB: 16384, GPUs: 1, GPUName: "l4", GPUManufacturer: "nvidia", GPUMemoryMiB: 22888}
	family := func(op corev1.NodeSelectorOperator) []corev1.NodeSelectorRequirement {
		return []corev1.NodeSelectorRequirement{{Key: labelInstanceFamily, Operator: op, Values: []string{"g6f", "gr6f"}}}
	}
	templates := resolveTemplates(context.Background(), nil, "us-west-1", []LaunchTemplateFact{
		{ID: "lt-gpu", Requirements: family(corev1.NodeSelectorOpNotIn)},
		{ID: "lt-grid", Requirements: family(corev1.NodeSelectorOpIn)},
	})

	groups := groupByTemplate(templates, []string{"g6f.xlarge", "g6.xlarge"}, map[string]InstanceTypeFacts{"g6f.xlarge": slice, "g6.xlarge": whole})

	got := map[string]string{}
	for _, g := range groups {
		got[g.ID] = strings.Join(g.InstanceTypes, ",")
	}
	if got["lt-grid"] != "g6f.xlarge" || got["lt-gpu"] != "g6.xlarge" {
		t.Errorf("groups = %v, want the slice on lt-grid and the whole L4 on lt-gpu", got)
	}
}

func TestInstanceFactsFromInfo_GPUSliceCountsAsOneGPU(t *testing.T) {
	f := instanceFactsFromInfo(ec2types.InstanceTypeInfo{
		InstanceType: "g6f.xlarge",
		GpuInfo: &ec2types.GpuInfo{Gpus: []ec2types.GpuDeviceInfo{{
			Name: aws.String("L4"), Count: aws.Int32(0), MemoryInfo: &ec2types.GpuDeviceMemoryInfo{SizeInMiB: aws.Int32(2861)},
		}}},
	})
	if f.GPUs != 1 || f.GPUMemoryMiB != 2861 {
		t.Errorf("facts = %d GPUs with %d MiB, want 1 GPU with 2861 MiB", f.GPUs, f.GPUMemoryMiB)
	}
}
