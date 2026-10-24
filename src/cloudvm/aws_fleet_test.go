package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

func TestFleetOverridesWeightedCapacity(t *testing.T) {
	instanceTypes := []string{"g5.xlarge", "g5.12xlarge", "p4d.24xlarge"}
	subnets := []string{"subnet-1"}

	overrides := BuildFleetOverrides(instanceTypes, subnets)
	if len(overrides) != 3 {
		t.Fatalf("expected 3 overrides, got %d", len(overrides))
	}

	// In 1 Pod = 1 Dedicated VM model, every candidate shape carries weight 1.0
	for _, ov := range overrides {
		itype := string(ov.InstanceType)
		if ov.WeightedCapacity == nil || *ov.WeightedCapacity != 1.0 {
			t.Errorf("expected weight 1.0 for %s in 1:1 pod VM model, got %v", itype, ov.WeightedCapacity)
		}
	}
}

func TestGPUCountForInstanceType(t *testing.T) {
	cases := []struct {
		itype    string
		expected int
	}{
		{"p5.48xlarge", 8},
		{"p4d.24xlarge", 8},
		{"g5.48xlarge", 8},
		{"g4dn.metal", 8},
		{"g5.12xlarge", 4},
		{"g6.24xlarge", 4},
		{"p3.8xlarge", 4},
		{"g5.xlarge", 1},
		{"g6.2xlarge", 1},
		{"g4dn.xlarge", 1},
		// CPU types must strictly return 0
		{"c6i.48xlarge", 0},
		{"c6i.24xlarge", 0},
		{"m6i.12xlarge", 0},
		{"r6i.32xlarge", 0},
		{"t3.medium", 0},
	}

	for _, tc := range cases {
		got := GPUCountForInstanceType(tc.itype)
		if got != tc.expected {
			t.Errorf("GPUCountForInstanceType(%q) = %d; expected %d", tc.itype, got, tc.expected)
		}
	}
}

type mockEC2FleetClient struct {
	createFleetFn           func(ctx context.Context, params *ec2.CreateFleetInput) (*ec2.CreateFleetOutput, error)
	describeInstancesFn     func(ctx context.Context, params *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error)
	terminateInstancesFn    func(ctx context.Context, params *ec2.TerminateInstancesInput) (*ec2.TerminateInstancesOutput, error)
	describeInstanceTypesFn func(ctx context.Context, params *ec2.DescribeInstanceTypesInput) (*ec2.DescribeInstanceTypesOutput, error)
}

func (m *mockEC2FleetClient) CreateFleet(ctx context.Context, params *ec2.CreateFleetInput, _ ...func(*ec2.Options)) (*ec2.CreateFleetOutput, error) {
	if m.createFleetFn != nil {
		return m.createFleetFn(ctx, params)
	}
	return &ec2.CreateFleetOutput{}, nil
}

func (m *mockEC2FleetClient) DescribeInstances(ctx context.Context, params *ec2.DescribeInstancesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	if m.describeInstancesFn != nil {
		return m.describeInstancesFn(ctx, params)
	}
	return &ec2.DescribeInstancesOutput{}, nil
}

func (m *mockEC2FleetClient) TerminateInstances(ctx context.Context, params *ec2.TerminateInstancesInput, _ ...func(*ec2.Options)) (*ec2.TerminateInstancesOutput, error) {
	if m.terminateInstancesFn != nil {
		return m.terminateInstancesFn(ctx, params)
	}
	return &ec2.TerminateInstancesOutput{}, nil
}

func (m *mockEC2FleetClient) DescribeInstanceTypes(ctx context.Context, params *ec2.DescribeInstanceTypesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error) {
	if m.describeInstanceTypesFn != nil {
		return m.describeInstanceTypesFn(ctx, params)
	}
	return &ec2.DescribeInstanceTypesOutput{}, nil
}

func TestAWSProvider_ListActiveInstances_PartialRegionFailureDoesNotBlockOtherRegions(t *testing.T) {
	t.Setenv("REGION_STORE_PATH", filepath.Join(t.TempDir(), "regions.json"))

	awsProv := NewAWSProvider("us-east-1", "us-west-2")

	// us-east-1 returns 1 healthy running instance
	eastClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			return &ec2.DescribeInstancesOutput{
				Reservations: []ec2types.Reservation{
					{
						Instances: []ec2types.Instance{
							{
								InstanceId:   aws.String("i-east-1"),
								InstanceType: ec2types.InstanceTypeG5Xlarge,
								State: &ec2types.InstanceState{
									Name: ec2types.InstanceStateNameRunning,
								},
								PrivateIpAddress: aws.String("10.0.1.10"),
								LaunchTime:       aws.Time(time.Now()),
								Tags: []ec2types.Tag{
									{Key: aws.String("ClusterTag"), Value: aws.String("test-cluster")},
									{Key: aws.String("PodUID"), Value: aws.String("uid-east-1")},
									{Key: aws.String("PodName"), Value: aws.String("pod-east-1")},
									{Key: aws.String("Namespace"), Value: aws.String("default")},
								},
							},
						},
					},
				},
			}, nil
		},
	}
	awsProv.SetRegionClient("us-east-1", eastClient)

	// us-west-2 returns throttling error (RequestLimitExceeded)
	westClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			return nil, fmt.Errorf("RequestLimitExceeded: EC2 API rate limit exceeded")
		},
	}
	awsProv.SetRegionClient("us-west-2", westClient)

	// 1. Direct ListActiveInstances check: returns healthy region instances instead of failing completely
	instances, err := awsProv.ListActiveInstances(context.Background(), "test-cluster")
	if err != nil {
		t.Fatalf("expected successful response with instances from healthy regions, got error: %v", err)
	}
	if len(instances) != 1 || instances[0].ID != "i-east-1" {
		t.Fatalf("expected 1 instance i-east-1 from us-east-1, got %v", instances)
	}

	// 2. Reconciler test: healthy region reconciles, failed region skips without false evictions
	mockTracker := &mockTrackerWrapper{
		activePods: map[string]string{
			"uid-east-1": "i-east-1",
			"uid-east-2": "i-east-vanished",
			"uid-west-1": "i-west-healthy-pod",
		},
		podRegions: map[string]string{
			"uid-east-1": "us-east-1",
			"uid-east-2": "us-east-1",
			"uid-west-1": "us-west-2",
		},
	}
	reconciler := NewReconciler(awsProv, mockTracker, nil, "test-cluster", 10*time.Minute)
	reconciler.SetLaunchGracePeriod(0)
	result, recErr := reconciler.ReconcileOnce(context.Background())
	if recErr != nil {
		t.Fatalf("reconciler failed: %v", recErr)
	}

	// uid-east-1 is alive -> not evicted
	// uid-east-2 is vanished from us-east-1 -> evicted
	// uid-west-1 in failing region us-west-2 -> SKIPPED (protected from false eviction)
	if len(result.EvictedPods) != 1 || result.EvictedPods[0] != "uid-east-2" {
		t.Fatalf("expected only uid-east-2 to be evicted, got: %v", result.EvictedPods)
	}
	for _, f := range mockTracker.failedCalls {
		if f == "uid-west-1" {
			t.Fatalf("pod in failing region uid-west-1 was erroneously evicted!")
		}
	}
}

func TestAWSProvider_ListActiveInstances_AllRegionsFailureReturnsError(t *testing.T) {
	t.Setenv("REGION_STORE_PATH", filepath.Join(t.TempDir(), "regions.json"))
	awsProv := NewAWSProvider("us-west-2")
	westClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			return nil, fmt.Errorf("AuthFailure: AWS credentials invalid")
		},
	}
	awsProv.SetRegionClient("us-west-2", westClient)

	instances, err := awsProv.ListActiveInstances(context.Background(), "test-cluster")
	if err == nil {
		t.Fatalf("expected error when all regions fail, got nil with instances: %v", instances)
	}
}

func TestAWSProvider_StaleSpotReclaimedInstancesPrunedFromRegionStore(t *testing.T) {
	t.Setenv("REGION_STORE_PATH", filepath.Join(t.TempDir(), "regions.json"))
	awsProv := NewAWSProvider("us-east-1")

	// Pre-populate an instance in region store
	if err := awsProv.regionStore.Set("i-spot-reclaimed", "us-east-1"); err != nil {
		t.Fatalf("failed to set initial mapping: %v", err)
	}
	awsProv.instanceRegions["i-spot-reclaimed"] = "us-east-1"

	// us-east-1 returns 0 instances (spot instance was reclaimed and terminated)
	eastClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			return &ec2.DescribeInstancesOutput{}, nil
		},
	}
	awsProv.SetRegionClient("us-east-1", eastClient)

	// Pass 1: lagging instance is NOT pruned immediately
	_, _ = awsProv.ListActiveInstancesByRegion(context.Background(), "test-cluster")
	if reg := awsProv.instanceRegions["i-spot-reclaimed"]; reg != "us-east-1" {
		t.Fatalf("expected instance not pruned on pass 1, but got empty")
	}

	// Pass 2: still retained
	_, _ = awsProv.ListActiveInstancesByRegion(context.Background(), "test-cluster")
	if reg := awsProv.instanceRegions["i-spot-reclaimed"]; reg != "us-east-1" {
		t.Fatalf("expected instance not pruned on pass 2, but got empty")
	}

	// Pass 3: confirmed missing for 3 consecutive passes -> pruned!
	_, _ = awsProv.ListActiveInstancesByRegion(context.Background(), "test-cluster")
	if reg := awsProv.instanceRegions["i-spot-reclaimed"]; reg != "" {
		t.Fatalf("expected reclaimed instance pruned from instanceRegions after 3 misses, got %s", reg)
	}
	if reg := awsProv.regionStore.Get("i-spot-reclaimed"); reg != "" {
		t.Fatalf("expected reclaimed instance pruned from regionStore after 3 misses, got %s", reg)
	}
}

func TestAWSProvider_LaggingInstanceNotPrunedAndRegionRetained(t *testing.T) {
	t.Setenv("REGION_STORE_PATH", filepath.Join(t.TempDir(), "regions.json"))
	awsProv := NewAWSProvider("us-east-1") // us-west-2 is NOT in default configured regions

	// Simulate launching an instance in candidate region us-west-2
	awsProv.AddConfiguredRegions("us-west-2")
	awsProv.instanceRegions["i-new-lagging"] = "us-west-2"
	_ = awsProv.regionStore.Set("i-new-lagging", "us-west-2")

	eastClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			return &ec2.DescribeInstancesOutput{}, nil
		},
	}
	awsProv.SetRegionClient("us-east-1", eastClient)

	// Pass 1: us-west-2 returns empty list (EC2 eventual consistency lagging)
	westClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			return &ec2.DescribeInstancesOutput{}, nil
		},
	}
	awsProv.SetRegionClient("us-west-2", westClient)

	byReg, _ := awsProv.ListActiveInstancesByRegion(context.Background(), "test-cluster")
	// Verify us-west-2 WAS queried
	if _, queried := byReg["us-west-2"]; !queried {
		t.Fatalf("expected us-west-2 to be queried in pass 1")
	}
	// Verify mapping was NOT dropped
	if reg := awsProv.instanceRegions["i-new-lagging"]; reg != "us-west-2" {
		t.Fatalf("expected i-new-lagging mapping retained, got %s", reg)
	}

	// Pass 2: EC2 indexes the instance and returns it
	westClient.describeInstancesFn = func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
		return &ec2.DescribeInstancesOutput{
			Reservations: []ec2types.Reservation{
				{
					Instances: []ec2types.Instance{
						{
							InstanceId:   aws.String("i-new-lagging"),
							InstanceType: ec2types.InstanceTypeG5Xlarge,
							State: &ec2types.InstanceState{
								Name: ec2types.InstanceStateNameRunning,
							},
							PrivateIpAddress: aws.String("10.0.2.20"),
							LaunchTime:       aws.Time(time.Now()),
							Tags: []ec2types.Tag{
								{Key: aws.String("ClusterTag"), Value: aws.String("test-cluster")},
								{Key: aws.String("PodUID"), Value: aws.String("uid-lagging")},
								{Key: aws.String("PodName"), Value: aws.String("pod-lagging")},
							},
						},
					},
				},
			},
		}, nil
	}

	byReg2, _ := awsProv.ListActiveInstancesByRegion(context.Background(), "test-cluster")
	if len(byReg2["us-west-2"]) != 1 || byReg2["us-west-2"][0].ID != "i-new-lagging" {
		t.Fatalf("expected i-new-lagging visible in pass 2, got: %v", byReg2["us-west-2"])
	}
	if awsProv.missingCount["i-new-lagging"] != 0 {
		t.Fatalf("expected missingCount reset to 0, got %d", awsProv.missingCount["i-new-lagging"])
	}
}

func TestAWSProvider_RegionPersistenceAcrossReload(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "instance-regions.json")
	t.Setenv("REGION_STORE_PATH", storePath)

	p1 := NewAWSProvider("us-east-1")
	regStore, err := NewRegionStore(storePath)
	if err != nil {
		t.Fatalf("failed to create region store: %v", err)
	}
	p1.SetRegionStore(regStore)

	// Save instance region mapping
	if err := p1.regionStore.Set("i-west-persist", "us-west-2"); err != nil {
		t.Fatalf("failed to set region: %v", err)
	}

	// Recreate provider from scratch (simulating daemon restart)
	p2 := NewAWSProvider("us-east-1")
	if reg := p2.instanceRegions["i-west-persist"]; reg != "us-west-2" {
		t.Fatalf("expected restored region us-west-2, got %s", reg)
	}

	// Terminate should remove from store
	westClient := &mockEC2FleetClient{
		terminateInstancesFn: func(_ context.Context, _ *ec2.TerminateInstancesInput) (*ec2.TerminateInstancesOutput, error) {
			return &ec2.TerminateInstancesOutput{}, nil
		},
	}
	p2.SetRegionClient("us-west-2", westClient)
	if err := p2.Terminate(context.Background(), "i-west-persist"); err != nil {
		t.Fatalf("terminate failed: %v", err)
	}

	p3 := NewAWSProvider("us-east-1")
	if reg := p3.instanceRegions["i-west-persist"]; reg != "" {
		t.Fatalf("expected empty region after termination reload, got %s", reg)
	}
}

func TestAWSFleetCandidateTypesAndSubnets(t *testing.T) {
	pod := &Pod{
		Metadata: PodMeta{
			Name:      "test-burst-pod",
			Namespace: "team-research-workloads",
			UID:       "uid-test-123",
			Annotations: map[string]string{
				"cloudvm.vk.io/subnets":                  "subnet-1,subnet-2",
				"cloudvm.vk.io/candidate-instance-types": "g5.xlarge,g6.xlarge",
				"cloudvm.vk.io/instance-type":            "g5.2xlarge",
			},
		},
	}

	spec, err := PodToInstanceSpec(pod, "cell-aws-usw2", "us-west-2")
	if err != nil {
		t.Fatalf("PodToInstanceSpec failed: %v", err)
	}

	if len(spec.SubnetIDs) != 2 || spec.SubnetIDs[0] != "subnet-1" || spec.SubnetIDs[1] != "subnet-2" {
		t.Fatalf("unexpected subnetIDs: %v", spec.SubnetIDs)
	}
	if len(spec.CandidateInstanceTypes) != 2 || spec.CandidateInstanceTypes[0] != "g5.xlarge" {
		t.Fatalf("unexpected candidateInstanceTypes: %v", spec.CandidateInstanceTypes)
	}

	// Verify fleet launch receives candidate types and reports errors on zero instances
	var capturedOverrides []ec2types.FleetLaunchTemplateOverridesRequest
	client := &mockEC2FleetClient{
		createFleetFn: func(_ context.Context, input *ec2.CreateFleetInput) (*ec2.CreateFleetOutput, error) {
			if len(input.LaunchTemplateConfigs) > 0 {
				capturedOverrides = input.LaunchTemplateConfigs[0].Overrides
			}
			return &ec2.CreateFleetOutput{
				Errors: []ec2types.CreateFleetError{
					{
						ErrorCode:    aws.String("UnfulfillableCapacity"),
						ErrorMessage: aws.String("Insufficient capacity in AZ"),
					},
				},
			}, nil
		},
	}

	p := NewAWSProvider("us-west-2")
	p.SetRegionClient("us-west-2", client)

	_, err = p.Launch(context.Background(), spec)
	if err == nil {
		t.Fatalf("expected launch error on unfulfillable capacity, got nil")
	}
	if !strings.Contains(err.Error(), "Insufficient capacity in AZ") {
		t.Fatalf("expected error message to contain fleet error details, got: %v", err)
	}

	// Overrides should contain g5.2xlarge, g5.xlarge, g6.xlarge crossed with subnet-1, subnet-2 (6 total)
	if len(capturedOverrides) != 6 {
		t.Fatalf("expected 6 overrides (3 types x 2 subnets), got %d", len(capturedOverrides))
	}
}
