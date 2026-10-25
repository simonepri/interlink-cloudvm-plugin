package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	"golang.org/x/time/rate"
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

func TestAWSFleetUserDataGzipCompression(t *testing.T) {
	// Generate large cloud-init content (> 25KB)
	largeData := strings.Repeat("# comment line with filler data\n", 1000)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err := gz.Write([]byte(largeData))
	if err != nil {
		t.Fatalf("failed to gzip: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("failed to close gzip: %v", err)
	}
	encoded := base64.StdEncoding.EncodeToString(buf.Bytes())

	if len(encoded) >= 25600 {
		t.Fatalf("compressed user data exceeds 25600 bytes limit: %d", len(encoded))
	}

	// Verify decompression
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("failed to decode base64: %v", err)
	}
	r, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("failed to create gzip reader: %v", err)
	}
	decompressed, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed to read decompressed: %v", err)
	}
	if string(decompressed) != largeData {
		t.Fatalf("decompressed data does not match original")
	}
}

func TestAWSProvider_LaunchResolvesPrivateIP(t *testing.T) {
	t.Setenv("REGION_STORE_PATH", filepath.Join(t.TempDir(), "regions.json"))
	p := NewAWSProvider("us-west-2")
	expectedIP := "10.136.3.116"

	client := &mockEC2FleetClient{
		createFleetFn: func(_ context.Context, _ *ec2.CreateFleetInput) (*ec2.CreateFleetOutput, error) {
			return &ec2.CreateFleetOutput{
				FleetId: aws.String("fleet-1234"),
				Instances: []ec2types.CreateFleetInstance{
					{
						InstanceIds:  []string{"i-0123456789abcdef0"},
						InstanceType: ec2types.InstanceTypeC5Xlarge,
					},
				},
			}, nil
		},
		describeInstancesFn: func(_ context.Context, params *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			if len(params.InstanceIds) > 0 && params.InstanceIds[0] == "i-0123456789abcdef0" {
				return &ec2.DescribeInstancesOutput{
					Reservations: []ec2types.Reservation{
						{
							Instances: []ec2types.Instance{
								{
									InstanceId:       aws.String("i-0123456789abcdef0"),
									InstanceType:     ec2types.InstanceTypeC5Xlarge,
									PrivateIpAddress: aws.String(expectedIP),
									State: &ec2types.InstanceState{
										Name: ec2types.InstanceStateNameRunning,
									},
								},
							},
						},
					},
				}, nil
			}
			return &ec2.DescribeInstancesOutput{}, nil
		},
	}

	p.SetRegionClient("us-west-2", client)

	spec := &InstanceSpec{
		PodName:                "smoke-test",
		PodNamespace:           "default",
		PodUID:                 "uid-1234",
		Region:                 "us-west-2",
		CandidateInstanceTypes: []string{"c5.xlarge"},
		ClusterTag:             "test-cluster",
		LaunchTemplateID:       "lt-1234",
	}

	inst, err := p.Launch(context.Background(), spec)
	if err != nil {
		t.Fatalf("Launch failed: %v", err)
	}
	if inst.IP != expectedIP {
		t.Fatalf("expected inst.IP %s, got %s", expectedIP, inst.IP)
	}
}

func TestDescribeAllInstances_Pagination(t *testing.T) {
	var requestedTokens []string
	callCount := 0

	mockClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, params *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			callCount++
			token := ""
			if params != nil && params.NextToken != nil {
				token = *params.NextToken
			}
			requestedTokens = append(requestedTokens, token)

			switch callCount {
			case 1:
				return &ec2.DescribeInstancesOutput{
					NextToken: aws.String("token-page-2"),
					Reservations: []ec2types.Reservation{
						{
							Instances: []ec2types.Instance{
								{InstanceId: aws.String("i-page1-inst1")},
								{InstanceId: aws.String("i-page1-inst2")},
							},
						},
					},
				}, nil
			case 2:
				return &ec2.DescribeInstancesOutput{
					NextToken: aws.String("token-page-3"),
					Reservations: []ec2types.Reservation{
						{
							Instances: []ec2types.Instance{
								{InstanceId: aws.String("i-page2-inst1")},
							},
						},
					},
				}, nil
			case 3:
				return &ec2.DescribeInstancesOutput{
					NextToken: nil,
					Reservations: []ec2types.Reservation{
						{
							Instances: []ec2types.Instance{
								{InstanceId: aws.String("i-page3-inst1")},
								{InstanceId: aws.String("i-page3-inst2")},
							},
						},
					},
				}, nil
			default:
				t.Fatalf("unexpected call %d to DescribeInstances", callCount)
				return nil, nil
			}
		},
	}

	out, err := DescribeAllInstances(context.Background(), mockClient, &ec2.DescribeInstancesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("instance-state-name"), Values: []string{"running"}},
		},
	})
	if err != nil {
		t.Fatalf("DescribeAllInstances failed: %v", err)
	}

	if callCount != 3 {
		t.Fatalf("expected 3 calls, got %d", callCount)
	}

	expectedTokens := []string{"", "token-page-2", "token-page-3"}
	for i, exp := range expectedTokens {
		if requestedTokens[i] != exp {
			t.Errorf("call %d token = %q, expected %q", i+1, requestedTokens[i], exp)
		}
	}

	if len(out.Reservations) != 3 {
		t.Fatalf("expected 3 reservations, got %d", len(out.Reservations))
	}
	var totalInsts int
	for _, res := range out.Reservations {
		totalInsts += len(res.Instances)
	}
	if totalInsts != 5 {
		t.Fatalf("expected 5 total instances, got %d", totalInsts)
	}
}

func TestDescribeAllInstances_Error(t *testing.T) {
	callCount := 0
	mockClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			callCount++
			if callCount == 1 {
				return &ec2.DescribeInstancesOutput{
					NextToken: aws.String("token-page-2"),
					Reservations: []ec2types.Reservation{
						{Instances: []ec2types.Instance{{InstanceId: aws.String("i-1")}}},
					},
				}, nil
			}
			return nil, fmt.Errorf("network connection error")
		},
	}

	out, err := DescribeAllInstances(context.Background(), mockClient, nil)
	if err == nil {
		t.Fatalf("expected error from page 2, got output: %v", out)
	}
	if !strings.Contains(err.Error(), "network connection error") {
		t.Fatalf("unexpected error message: %v", err)
	}
	if callCount != 2 {
		t.Fatalf("expected 2 calls, got %d", callCount)
	}
}

func TestAWSProvider_ListActiveInstances_Paginated(t *testing.T) {
	t.Setenv("REGION_STORE_PATH", filepath.Join(t.TempDir(), "regions.json"))
	p := NewAWSProvider("us-east-1")

	callCount := 0
	client := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			callCount++
			if callCount == 1 {
				return &ec2.DescribeInstancesOutput{
					NextToken: aws.String("page-2"),
					Reservations: []ec2types.Reservation{
						{
							Instances: []ec2types.Instance{
								{
									InstanceId:       aws.String("i-paginated-1"),
									InstanceType:     ec2types.InstanceTypeG5Xlarge,
									PrivateIpAddress: aws.String("10.0.1.10"),
									State:            &ec2types.InstanceState{Name: ec2types.InstanceStateNameRunning},
									LaunchTime:       aws.Time(time.Now()),
									Tags: []ec2types.Tag{
										{Key: aws.String("ClusterTag"), Value: aws.String("paginated-cluster")},
										{Key: aws.String("PodUID"), Value: aws.String("uid-p1")},
										{Key: aws.String("PodName"), Value: aws.String("pod-p1")},
									},
								},
							},
						},
					},
				}, nil
			}
			return &ec2.DescribeInstancesOutput{
				NextToken: nil,
				Reservations: []ec2types.Reservation{
					{
						Instances: []ec2types.Instance{
							{
								InstanceId:       aws.String("i-paginated-2"),
								InstanceType:     ec2types.InstanceTypeG52xlarge,
								PrivateIpAddress: aws.String("10.0.1.20"),
								State:            &ec2types.InstanceState{Name: ec2types.InstanceStateNameRunning},
								LaunchTime:       aws.Time(time.Now()),
								Tags: []ec2types.Tag{
									{Key: aws.String("ClusterTag"), Value: aws.String("paginated-cluster")},
									{Key: aws.String("PodUID"), Value: aws.String("uid-p2")},
									{Key: aws.String("PodName"), Value: aws.String("pod-p2")},
								},
							},
						},
					},
				},
			}, nil
		},
	}

	p.SetRegionClient("us-east-1", client)

	instances, err := p.ListActiveInstances(context.Background(), "paginated-cluster")
	if err != nil {
		t.Fatalf("ListActiveInstances failed: %v", err)
	}

	if callCount != 2 {
		t.Fatalf("expected 2 DescribeInstances calls, got %d", callCount)
	}
	if len(instances) != 2 {
		t.Fatalf("expected 2 instances across 2 pages, got %d", len(instances))
	}
	if instances[0].ID != "i-paginated-1" || instances[1].ID != "i-paginated-2" {
		t.Fatalf("unexpected instances returned: %v, %v", instances[0].ID, instances[1].ID)
	}
}

func TestCreateFleet_RateLimiting(t *testing.T) {
	ResetFleetRateLimiters()
	defer ResetFleetRateLimiters()

	// Configure a tight rate limiter: 5 calls/second, burst 1
	testLimiter := rate.NewLimiter(rate.Limit(5), 1)
	SetFleetRateLimiter("us-east-rate", testLimiter)

	var callTimes []time.Time
	client := &mockEC2FleetClient{
		createFleetFn: func(_ context.Context, _ *ec2.CreateFleetInput) (*ec2.CreateFleetOutput, error) {
			callTimes = append(callTimes, time.Now())
			return &ec2.CreateFleetOutput{
				FleetId: aws.String("fleet-rate-test"),
				Instances: []ec2types.CreateFleetInstance{
					{
						InstanceIds:  []string{"i-rate-1"},
						InstanceType: ec2types.InstanceTypeG5Xlarge,
					},
				},
			}, nil
		},
	}

	req := &FleetRequest{
		Region:        "us-east-rate",
		PodUID:        "uid-rate-1",
		PodName:       "pod-rate-1",
		PodNamespace:  "default",
		InstanceTypes: []string{"g5.xlarge"},
	}

	start := time.Now()
	// First call consumes the burst token immediately
	_, err := LaunchFleetWithClient(context.Background(), client, req, nil)
	if err != nil {
		t.Fatalf("first launch failed: %v", err)
	}

	// Second call must wait ~200ms for next token (5 calls/s = 200ms interval)
	req.PodUID = "uid-rate-2"
	_, err = LaunchFleetWithClient(context.Background(), client, req, nil)
	if err != nil {
		t.Fatalf("second launch failed: %v", err)
	}

	elapsed := time.Since(start)
	if elapsed < 150*time.Millisecond {
		t.Fatalf("expected rate limiting delay >= 150ms, elapsed was: %v", elapsed)
	}
	if len(callTimes) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(callTimes))
	}
}

func TestCreateFleet_RateLimiting_ContextCanceled(t *testing.T) {
	ResetFleetRateLimiters()
	defer ResetFleetRateLimiters()

	// Drain the limiter completely
	testLimiter := rate.NewLimiter(rate.Limit(1), 1)
	if !testLimiter.Allow() {
		t.Fatalf("expected first token allowed")
	}
	SetFleetRateLimiter("us-east-rate-cancel", testLimiter)

	client := &mockEC2FleetClient{}
	req := &FleetRequest{
		Region:        "us-east-rate-cancel",
		PodUID:        "uid-rate-cancel",
		PodName:       "pod-rate-cancel",
		PodNamespace:  "default",
		InstanceTypes: []string{"g5.xlarge"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel before call

	_, err := LaunchFleetWithClient(ctx, client, req, nil)
	if err == nil {
		t.Fatalf("expected error on canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}

func TestCreateFleet_BackoffRetry_SuccessOnRetry(t *testing.T) {
	ResetFleetRateLimiters()
	defer ResetFleetRateLimiters()
	SetFleetRetryParams(5*time.Millisecond, 20*time.Millisecond, 5)
	defer ResetFleetRetryParams()

	calls := 0
	client := &mockEC2FleetClient{
		createFleetFn: func(_ context.Context, _ *ec2.CreateFleetInput) (*ec2.CreateFleetOutput, error) {
			calls++
			if calls < 3 {
				return nil, &smithy.GenericAPIError{
					Code:    "RequestLimitExceeded",
					Message: "Request limit exceeded.",
				}
			}
			return &ec2.CreateFleetOutput{
				FleetId: aws.String("fleet-retry-ok"),
				Instances: []ec2types.CreateFleetInstance{
					{
						InstanceIds:  []string{"i-retry-success"},
						InstanceType: ec2types.InstanceTypeG5Xlarge,
					},
				},
			}, nil
		},
	}

	req := &FleetRequest{
		Region:        "us-west-2",
		PodUID:        "uid-retry-ok",
		PodName:       "pod-retry-ok",
		PodNamespace:  "default",
		InstanceTypes: []string{"g5.xlarge"},
	}

	resp, err := LaunchFleetWithClient(context.Background(), client, req, nil)
	if err != nil {
		t.Fatalf("LaunchFleetWithClient failed: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls (2 throttles + 1 success), got %d", calls)
	}
	if resp.FleetID != "fleet-retry-ok" {
		t.Fatalf("expected fleetId fleet-retry-ok, got %s", resp.FleetID)
	}
	if len(resp.Instances) != 1 || resp.Instances[0].ID != "i-retry-success" {
		t.Fatalf("expected instance i-retry-success, got %v", resp.Instances)
	}
}

func TestCreateFleet_BackoffRetry_ExceedsMaxRetries(t *testing.T) {
	ResetFleetRateLimiters()
	defer ResetFleetRateLimiters()
	maxRetries := 3
	SetFleetRetryParams(5*time.Millisecond, 20*time.Millisecond, maxRetries)
	defer ResetFleetRetryParams()

	calls := 0
	client := &mockEC2FleetClient{
		createFleetFn: func(_ context.Context, _ *ec2.CreateFleetInput) (*ec2.CreateFleetOutput, error) {
			calls++
			return nil, &smithy.GenericAPIError{
				Code:    "RequestLimitExceeded",
				Message: "Request limit exceeded.",
			}
		},
	}

	req := &FleetRequest{
		Region:        "us-west-2",
		PodUID:        "uid-retry-fail",
		PodName:       "pod-retry-fail",
		PodNamespace:  "default",
		InstanceTypes: []string{"g5.xlarge"},
	}

	_, err := LaunchFleetWithClient(context.Background(), client, req, nil)
	if err == nil {
		t.Fatalf("expected failure after exceeding max retries, got nil")
	}
	// Initial call + 3 retries = 4 calls total
	if calls != maxRetries+1 {
		t.Fatalf("expected %d calls, got %d", maxRetries+1, calls)
	}
	if !strings.Contains(err.Error(), "RequestLimitExceeded") {
		t.Fatalf("expected error mentioning RequestLimitExceeded, got: %v", err)
	}
}

func TestCreateFleet_BackoffRetry_NonRetryableError(t *testing.T) {
	ResetFleetRateLimiters()
	defer ResetFleetRateLimiters()
	SetFleetRetryParams(5*time.Millisecond, 20*time.Millisecond, 5)
	defer ResetFleetRetryParams()

	calls := 0
	client := &mockEC2FleetClient{
		createFleetFn: func(_ context.Context, _ *ec2.CreateFleetInput) (*ec2.CreateFleetOutput, error) {
			calls++
			return nil, fmt.Errorf("InvalidParameterValue: Value (ami-nonexistent) is invalid")
		},
	}

	req := &FleetRequest{
		Region:        "us-west-2",
		PodUID:        "uid-non-retryable",
		PodName:       "pod-non-retryable",
		PodNamespace:  "default",
		InstanceTypes: []string{"g5.xlarge"},
	}

	_, err := LaunchFleetWithClient(context.Background(), client, req, nil)
	if err == nil {
		t.Fatalf("expected non-retryable error, got nil")
	}
	// Must fail immediately without retry!
	if calls != 1 {
		t.Fatalf("expected exactly 1 call for non-retryable error, got %d", calls)
	}
	if !strings.Contains(err.Error(), "InvalidParameterValue") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCreateFleet_BackoffRetry_ContextCanceledDuringBackoff(t *testing.T) {
	ResetFleetRateLimiters()
	defer ResetFleetRateLimiters()
	// Set longer backoff so context timeout fires during sleep
	SetFleetRetryParams(2*time.Second, 5*time.Second, 5)
	defer ResetFleetRetryParams()

	calls := 0
	client := &mockEC2FleetClient{
		createFleetFn: func(_ context.Context, _ *ec2.CreateFleetInput) (*ec2.CreateFleetOutput, error) {
			calls++
			return nil, &smithy.GenericAPIError{
				Code:    "RequestLimitExceeded",
				Message: "Request limit exceeded.",
			}
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	req := &FleetRequest{
		Region:        "us-west-2",
		PodUID:        "uid-ctx-cancel",
		PodName:       "pod-ctx-cancel",
		PodNamespace:  "default",
		InstanceTypes: []string{"g5.xlarge"},
	}

	_, err := LaunchFleetWithClient(ctx, client, req, nil)
	if err == nil {
		t.Fatalf("expected context error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("expected deadline exceeded, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call before context cancellation during sleep, got %d", calls)
	}
}

func TestAWSProvider_ClusterFactsAndClientPool(t *testing.T) {
	p := NewAWSProvider("us-east-1", "us-west-2")

	// 1. EC2 client pool
	pool := p.EC2Clients()
	if len(pool) != 0 {
		t.Fatalf("expected empty initial ec2 client pool, got %d", len(pool))
	}

	concreteClient := &ec2.Client{}
	p.SetEC2Client("us-west-2", concreteClient)

	gotClient, err := p.GetEC2Client(context.Background(), "us-west-2")
	if err != nil {
		t.Fatalf("GetEC2Client failed: %v", err)
	}
	if gotClient != concreteClient {
		t.Fatalf("expected retrieved client to match registered client")
	}

	pool = p.EC2Clients()
	if len(pool) != 1 || pool["us-west-2"] != concreteClient {
		t.Fatalf("expected us-west-2 in client pool")
	}

	// 2. ClusterFacts configuration
	facts := &ClusterFacts{
		ClusterTag: "prod-cluster",
		Regions: map[string]RegionalClusterFact{
			"us-west-2": {
				LaunchTemplateID: "lt-west-001",
				SubnetIDs:        []string{"subnet-west-a", "subnet-west-b"},
			},
			"us-east-1": {
				LaunchTemplateName: "interlink-east-template",
				SubnetIDs:          []string{"subnet-east-a"},
			},
		},
	}
	p.SetClusterFacts(facts)

	retrievedFacts := p.ClusterFacts()
	if retrievedFacts == nil || retrievedFacts.ClusterTag != "prod-cluster" {
		t.Fatalf("unexpected retrieved facts: %+v", retrievedFacts)
	}

	// Dynamic resolution
	ltID, ltName := p.ResolveRegionalLaunchTemplate("us-west-2")
	if ltID != "lt-west-001" || ltName != "" {
		t.Errorf("expected lt-west-001, got id=%s name=%s", ltID, ltName)
	}
	subnets := p.ResolveRegionalSubnets("us-west-2")
	if len(subnets) != 2 || subnets[0] != "subnet-west-a" || subnets[1] != "subnet-west-b" {
		t.Errorf("unexpected subnets: %v", subnets)
	}

	// Update regional facts dynamically
	p.SetRegionalLaunchTemplate("eu-west-1", "lt-eu-999")
	p.SetRegionalSubnets("eu-west-1", []string{"subnet-eu-1"})
	euLTID, _ := p.ResolveRegionalLaunchTemplate("eu-west-1")
	if euLTID != "lt-eu-999" {
		t.Errorf("expected lt-eu-999 for eu-west-1, got %s", euLTID)
	}

	// 3. Launch with regional fact resolution
	var capturedOverrides []ec2types.FleetLaunchTemplateOverridesRequest
	var capturedLT *ec2types.FleetLaunchTemplateSpecificationRequest
	mockClient := &mockEC2FleetClient{
		createFleetFn: func(_ context.Context, input *ec2.CreateFleetInput) (*ec2.CreateFleetOutput, error) {
			if len(input.LaunchTemplateConfigs) > 0 {
				capturedOverrides = input.LaunchTemplateConfigs[0].Overrides
				capturedLT = input.LaunchTemplateConfigs[0].LaunchTemplateSpecification
			}
			return &ec2.CreateFleetOutput{
				FleetId: aws.String("fleet-facts-123"),
				Instances: []ec2types.CreateFleetInstance{
					{
						InstanceIds:  []string{"i-facts-node"},
						InstanceType: ec2types.InstanceTypeG5Xlarge,
					},
				},
			}, nil
		},
		describeInstancesFn: func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			return &ec2.DescribeInstancesOutput{
				Reservations: []ec2types.Reservation{
					{
						Instances: []ec2types.Instance{
							{
								InstanceId:       aws.String("i-facts-node"),
								InstanceType:     ec2types.InstanceTypeG5Xlarge,
								PrivateIpAddress: aws.String("10.2.1.20"),
							},
						},
					},
				},
			}, nil
		},
	}
	p.SetRegionClient("us-west-2", mockClient)

	// Launch with NO LaunchTemplate and NO SubnetIDs specified in spec
	spec := &InstanceSpec{
		PodName:      "test-fact-pod",
		PodNamespace: "default",
		PodUID:       "uid-fact-1",
		Region:       "us-west-2",
		InstanceType: "g5.xlarge",
		ClusterTag:   "prod-cluster",
	}

	inst, err := p.Launch(context.Background(), spec)
	if err != nil {
		t.Fatalf("Launch failed: %v", err)
	}
	if inst.ID != "i-facts-node" {
		t.Fatalf("expected instance i-facts-node, got %s", inst.ID)
	}

	if capturedLT == nil || aws.ToString(capturedLT.LaunchTemplateId) != "lt-west-001" {
		t.Fatalf("expected launch template lt-west-001 from facts, got: %+v", capturedLT)
	}

	if len(capturedOverrides) != 2 {
		t.Fatalf("expected 2 overrides (1 shape x 2 subnets from facts), got %d", len(capturedOverrides))
	}
}
