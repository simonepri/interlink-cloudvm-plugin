// Tests AZ offering filtering of fleet overrides and the bounded candidate-region fallback of AWS launches.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// regionMock is an EC2 client for one region that records CreateFleet calls and serves fixed subnet AZs and offerings.
type regionMock struct {
	mu             sync.Mutex
	subnetAZ       map[string]string
	offered        map[string][]string // instance type -> AZs
	offeringsErr   error
	createErr      error
	fleetErrorCode string
	instanceID     string
	createCalls    int
	offeringCalls  int
	lastOverrides  []ec2types.FleetLaunchTemplateOverridesRequest
	lastTemplateID string
	lastUserData   string
}

func (m *regionMock) client() *mockEC2FleetClient {
	return &mockEC2FleetClient{
		createFleetFn: func(_ context.Context, in *ec2.CreateFleetInput) (*ec2.CreateFleetOutput, error) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.createCalls++
			m.lastOverrides = in.LaunchTemplateConfigs[0].Overrides
			m.lastTemplateID = aws.ToString(in.LaunchTemplateConfigs[0].LaunchTemplateSpecification.LaunchTemplateId)
			m.lastUserData = aws.ToString(in.LaunchTemplateConfigs[0].LaunchTemplateSpecification.LaunchTemplateSpecificationUserData)
			if m.createErr != nil {
				return nil, m.createErr
			}
			if m.instanceID == "" {
				return &ec2.CreateFleetOutput{Errors: []ec2types.CreateFleetError{{
					ErrorCode:    aws.String(m.fleetErrorCode),
					ErrorMessage: aws.String("There is no Spot capacity available that matches your request."),
				}}}, nil
			}
			return &ec2.CreateFleetOutput{
				FleetId:   aws.String("fleet-" + m.instanceID),
				Instances: []ec2types.CreateFleetInstance{{InstanceIds: []string{m.instanceID}, InstanceType: in.LaunchTemplateConfigs[0].Overrides[0].InstanceType}},
			}, nil
		},
		describeSubnetsFn: func(_ context.Context, in *ec2.DescribeSubnetsInput) (*ec2.DescribeSubnetsOutput, error) {
			out := &ec2.DescribeSubnetsOutput{}
			for _, id := range in.SubnetIds {
				if az, ok := m.subnetAZ[id]; ok {
					out.Subnets = append(out.Subnets, ec2types.Subnet{SubnetId: aws.String(id), AvailabilityZone: aws.String(az)})
				}
			}
			return out, nil
		},
		describeOfferingsFn: func(_ context.Context, in *ec2.DescribeInstanceTypeOfferingsInput) (*ec2.DescribeInstanceTypeOfferingsOutput, error) {
			m.mu.Lock()
			m.offeringCalls++
			m.mu.Unlock()
			if in.LocationType != ec2types.LocationTypeAvailabilityZone {
				return nil, errors.New("unexpected location type")
			}
			if m.offeringsErr != nil {
				return nil, m.offeringsErr
			}
			out := &ec2.DescribeInstanceTypeOfferingsOutput{}
			for _, itype := range in.Filters[0].Values {
				for _, az := range m.offered[itype] {
					out.InstanceTypeOfferings = append(out.InstanceTypeOfferings, ec2types.InstanceTypeOffering{
						InstanceType: ec2types.InstanceType(itype), Location: aws.String(az), LocationType: ec2types.LocationTypeAvailabilityZone,
					})
				}
			}
			return out, nil
		},
	}
}

func (m *regionMock) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.createCalls
}

func overridePairs(ovs []ec2types.FleetLaunchTemplateOverridesRequest) []string {
	var out []string
	for _, ov := range ovs {
		out = append(out, string(ov.InstanceType)+"@"+aws.ToString(ov.SubnetId))
	}
	sort.Strings(out)
	return out
}

func newTestProvider(t *testing.T) *AWSProvider {
	t.Helper()
	t.Setenv("REGION_STORE_PATH", filepath.Join(t.TempDir(), "regions.json"))
	t.Setenv("CLUSTER_FACTS", "")
	return NewAWSProvider("us-west-1")
}

func fallbackSpec(candidates ...string) *InstanceSpec {
	return &InstanceSpec{
		PodName:                "burst",
		PodNamespace:           "research",
		PodUID:                 "uid-burst",
		Region:                 "us-west-1",
		CandidateRegions:       candidates,
		InstanceType:           "g5.xlarge",
		CandidateInstanceTypes: []string{"g6.xlarge"},
		NumGPUs:                1,
	}
}

func TestLaunchDropsOverridesTheAZDoesNotOffer(t *testing.T) {
	p := newTestProvider(t)
	p.SetClusterFacts(&ClusterFacts{Regions: map[string]RegionalClusterFact{
		"us-west-1": {LaunchTemplateID: "lt-usw1", SubnetIDs: []string{"subnet-a", "subnet-c"}},
	}})
	usw1 := &regionMock{
		subnetAZ:   map[string]string{"subnet-a": "us-west-1a", "subnet-c": "us-west-1c"},
		offered:    map[string][]string{"g5.xlarge": {"us-west-1c"}, "g6.xlarge": {"us-west-1a", "us-west-1c"}},
		instanceID: "i-usw1",
	}
	p.SetRegionClient("us-west-1", usw1.client())

	if _, err := p.Launch(context.Background(), fallbackSpec()); err != nil {
		t.Fatalf("Launch failed: %v", err)
	}
	got := strings.Join(overridePairs(usw1.lastOverrides), " ")
	want := "g5.xlarge@subnet-c g6.xlarge@subnet-a g6.xlarge@subnet-c"
	if got != want {
		t.Fatalf("overrides = %q, want %q", got, want)
	}
}

func TestLaunchFailsClearlyWhenNoOverrideIsOffered(t *testing.T) {
	p := newTestProvider(t)
	p.SetClusterFacts(&ClusterFacts{Regions: map[string]RegionalClusterFact{
		"us-west-1": {LaunchTemplateID: "lt-usw1", SubnetIDs: []string{"subnet-a"}},
	}})
	usw1 := &regionMock{subnetAZ: map[string]string{"subnet-a": "us-west-1a"}, offered: map[string][]string{}, instanceID: "i-usw1"}
	p.SetRegionClient("us-west-1", usw1.client())

	_, err := p.Launch(context.Background(), fallbackSpec())
	if !errors.Is(err, errNoOfferedPlacements) {
		t.Fatalf("Launch error = %v, want errNoOfferedPlacements", err)
	}
	if !strings.Contains(err.Error(), "us-west-1") || !strings.Contains(err.Error(), "g5.xlarge") {
		t.Errorf("error %q does not name the region and instance types", err)
	}
	if usw1.calls() != 0 {
		t.Errorf("CreateFleet called %d times, want 0", usw1.calls())
	}
}

func TestOfferingLookupIsCachedUntilTTL(t *testing.T) {
	p := newTestProvider(t)
	usw1 := &regionMock{
		subnetAZ: map[string]string{"subnet-a": "us-west-1a"},
		offered:  map[string][]string{"g5.xlarge": {"us-west-1a"}},
	}
	client := usw1.client()
	cache := newOfferingCache(time.Hour)
	now := time.Unix(1_700_000_000, 0)
	cache.now = func() time.Time { return now }
	p.offeringCache = cache

	for range 2 {
		if _, err := p.offeredFleetOverrides(context.Background(), client, "us-west-1", []string{"g5.xlarge"}, []string{"subnet-a"}); err != nil {
			t.Fatalf("offeredFleetOverrides failed: %v", err)
		}
	}
	if usw1.offeringCalls != 1 {
		t.Fatalf("DescribeInstanceTypeOfferings called %d times within TTL, want 1", usw1.offeringCalls)
	}
	now = now.Add(time.Hour + time.Second)
	if _, err := p.offeredFleetOverrides(context.Background(), client, "us-west-1", []string{"g5.xlarge"}, []string{"subnet-a"}); err != nil {
		t.Fatalf("offeredFleetOverrides failed: %v", err)
	}
	if usw1.offeringCalls != 2 {
		t.Fatalf("DescribeInstanceTypeOfferings called %d times after TTL, want 2", usw1.offeringCalls)
	}
}

func TestOfferingLookupFailureKeepsAllOverrides(t *testing.T) {
	p := newTestProvider(t)
	usw1 := &regionMock{subnetAZ: map[string]string{"subnet-a": "us-west-1a"}, offeringsErr: errors.New("UnauthorizedOperation")}
	ovs, err := p.offeredFleetOverrides(context.Background(), usw1.client(), "us-west-1", []string{"g5.xlarge", "g6.xlarge"}, []string{"subnet-a"})
	if err != nil {
		t.Fatalf("offeredFleetOverrides failed: %v", err)
	}
	if len(ovs) != 2 {
		t.Fatalf("got %d overrides, want 2 unfiltered", len(ovs))
	}
}

type staticScorer map[string]*PlacementScoreEntry

func (s staticScorer) AllScores() map[string]*PlacementScoreEntry { return s }

func TestLaunchFallsBackToBestScoredCandidateRegion(t *testing.T) {
	p := newTestProvider(t)
	p.SetClusterFacts(&ClusterFacts{Regions: map[string]RegionalClusterFact{
		"us-west-1": {LaunchTemplateID: "lt-usw1", SubnetIDs: []string{"subnet-w"}},
		"us-east-1": {LaunchTemplateID: "lt-use1", SubnetIDs: []string{"subnet-e1"}},
		"us-east-2": {LaunchTemplateID: "lt-use2", GPULaunchTemplateID: "lt-use2-gpu", SubnetIDs: []string{"subnet-e2"}},
	}})
	p.SetRegionScorer(staticScorer{
		"us-east-1//g5.xlarge": {Region: "us-east-1", InstanceType: "g5.xlarge", Score: 3},
		"us-east-2//g6.xlarge": {Region: "us-east-2", InstanceType: "g6.xlarge", Score: 9},
		"us-east-1//p5.48xl":   {Region: "us-east-1", InstanceType: "p5.48xlarge", Score: 10},
	})
	usw1 := &regionMock{fleetErrorCode: "UnfulfillableCapacity"}
	use1 := &regionMock{instanceID: "i-use1"}
	use2 := &regionMock{instanceID: "i-use2"}
	p.SetRegionClient("us-west-1", usw1.client())
	p.SetRegionClient("us-east-1", use1.client())
	p.SetRegionClient("us-east-2", use2.client())

	inst, err := p.Launch(context.Background(), fallbackSpec("us-east-1", "us-east-2"))
	if err != nil {
		t.Fatalf("Launch failed: %v", err)
	}
	if inst.ID != "i-use2" || inst.Region != "us-east-2" {
		t.Fatalf("launched %s in %s, want i-use2 in us-east-2", inst.ID, inst.Region)
	}
	if use1.calls() != 0 {
		t.Errorf("lower-scored us-east-1 was tried %d times before us-east-2", use1.calls())
	}
	if use2.lastTemplateID != "lt-use2-gpu" || overridePairs(use2.lastOverrides)[0] != "g5.xlarge@subnet-e2" {
		t.Errorf("fallback used template %q and overrides %v, want the us-east-2 cluster facts", use2.lastTemplateID, overridePairs(use2.lastOverrides))
	}
	if inst.Metadata["launchRegion"] != "us-east-2" || inst.Metadata["requestedRegion"] != "us-west-1" ||
		!strings.Contains(inst.Metadata["regionFallbacks"], "us-west-1: no instances launched") {
		t.Errorf("unexpected launch region metadata: %v", inst.Metadata)
	}
	if st, err := p.Status(context.Background(), "i-use2"); err == nil && st.Region != "us-east-2" {
		t.Errorf("Status resolved region %s, want us-east-2", st.Region)
	}
}

func TestLaunchKeepsPinnedRegionWithoutCandidateRegions(t *testing.T) {
	p := newTestProvider(t)
	p.SetClusterFacts(&ClusterFacts{Regions: map[string]RegionalClusterFact{
		"us-west-1": {LaunchTemplateID: "lt-usw1"},
		"us-east-1": {LaunchTemplateID: "lt-use1"},
	}})
	usw1 := &regionMock{fleetErrorCode: "InsufficientInstanceCapacity"}
	use1 := &regionMock{instanceID: "i-use1"}
	p.SetRegionClient("us-west-1", usw1.client())
	p.SetRegionClient("us-east-1", use1.client())

	_, err := p.Launch(context.Background(), fallbackSpec())
	if !errors.Is(err, errNoInstancesLaunched) || !strings.Contains(err.Error(), "InsufficientInstanceCapacity") {
		t.Fatalf("Launch error = %v, want the pinned region's capacity error", err)
	}
	if use1.calls() != 0 {
		t.Errorf("launch left the pinned region without candidate-regions")
	}
}

func TestLaunchDoesNotFallBackOnNonCapacityError(t *testing.T) {
	p := newTestProvider(t)
	p.SetClusterFacts(&ClusterFacts{Regions: map[string]RegionalClusterFact{
		"us-west-1": {LaunchTemplateID: "lt-usw1"},
		"us-east-1": {LaunchTemplateID: "lt-use1"},
	}})
	usw1 := &regionMock{createErr: &smithy.GenericAPIError{Code: "UnauthorizedOperation", Message: "not authorized"}}
	use1 := &regionMock{instanceID: "i-use1"}
	p.SetRegionClient("us-west-1", usw1.client())
	p.SetRegionClient("us-east-1", use1.client())

	if _, err := p.Launch(context.Background(), fallbackSpec("us-east-1")); err == nil || !strings.Contains(err.Error(), "UnauthorizedOperation") {
		t.Fatalf("Launch error = %v, want UnauthorizedOperation", err)
	}
	if use1.calls() != 0 {
		t.Errorf("launch fell back after a non-capacity error")
	}
}

func TestLaunchBoundsRegionAttemptsAndSkipsUnconfiguredRegions(t *testing.T) {
	p := newTestProvider(t)
	facts := map[string]RegionalClusterFact{}
	mocks := map[string]*regionMock{}
	for _, r := range []string{"us-west-1", "us-east-1", "us-east-2", "eu-west-1", "eu-central-1"} {
		if r != "us-east-1" {
			facts[r] = RegionalClusterFact{LaunchTemplateID: "lt-" + r}
		}
		mocks[r] = &regionMock{createErr: &smithy.GenericAPIError{Code: "InvalidFleetConfiguration", Message: "g5.xlarge is not supported in your requested Availability Zone"}}
		p.SetRegionClient(r, mocks[r].client())
	}
	p.SetClusterFacts(&ClusterFacts{Regions: facts})

	_, err := p.Launch(context.Background(), fallbackSpec("us-east-1", "us-east-2", "eu-west-1", "eu-central-1"))
	if err == nil || !strings.Contains(err.Error(), "3 of 3 candidate regions") || !strings.Contains(err.Error(), "InvalidFleetConfiguration") {
		t.Fatalf("Launch error = %v, want a bounded 3-region failure", err)
	}
	for r, want := range map[string]int{"us-west-1": 1, "us-east-1": 0, "us-east-2": 1, "eu-west-1": 1, "eu-central-1": 0} {
		if got := mocks[r].calls(); got != want {
			t.Errorf("CreateFleet calls in %s = %d, want %d", r, got, want)
		}
	}
}

func TestLaunchRegionsDisablesFallbackForBareKMSKey(t *testing.T) {
	p := newTestProvider(t)
	p.SetClusterFacts(&ClusterFacts{Regions: map[string]RegionalClusterFact{"us-east-1": {LaunchTemplateID: "lt-use1"}}})
	spec := fallbackSpec("us-east-1")
	spec.KMSCiphertext = "Y2lwaGVy"
	spec.KMSKeyID = "alias/cloudvm"
	if got := p.launchRegions(spec, []string{"g5.xlarge"}); len(got) != 1 {
		t.Fatalf("launchRegions = %v, want only the pinned region for a bare KMS key", got)
	}
	spec.KMSKeyID = "arn:aws:kms:us-west-1:123456789012:key/abc"
	if got := p.launchRegions(spec, []string{"g5.xlarge"}); len(got) != 2 {
		t.Fatalf("launchRegions = %v, want fallback for a KMS key ARN", got)
	}
}

type recordedEvent struct{ namespace, name, reason, message string }

type fakeEventRecorder struct{ events []recordedEvent }

func (f *fakeEventRecorder) RecordPodEvent(_ context.Context, namespace, name, _, _, reason, message string) error {
	f.events = append(f.events, recordedEvent{namespace, name, reason, message})
	return nil
}

func TestRecordLaunchRegionEventNamesFallback(t *testing.T) {
	s := NewServer(NewMockProvider(), "test-cluster")
	rec := &fakeEventRecorder{}
	s.SetPodEventRecorder(rec)
	inst := &Instance{ID: "i-use2", InstanceType: "g5.xlarge", Metadata: map[string]string{
		"launchRegion": "us-east-2", "requestedRegion": "us-west-1", "regionFallbacks": "us-west-1: no instances launched",
	}}
	s.recordLaunchRegionEvent(context.Background(), fallbackSpec(), inst)
	if len(rec.events) != 1 {
		t.Fatalf("recorded %d events, want 1", len(rec.events))
	}
	ev := rec.events[0]
	if ev.reason != "CloudVMRegionSelected" || ev.namespace != "research" || ev.name != "burst" ||
		ev.message != "Launched cloud VM i-use2 (g5.xlarge) in us-east-2 after falling back from us-west-1: us-west-1: no instances launched" {
		t.Fatalf("unexpected event %+v", ev)
	}
}

func TestKubePodEventRecorderCreatesPodEvent(t *testing.T) {
	cs := fake.NewClientset()
	rec := NewKubePodEventRecorder(cs)
	if err := rec.RecordPodEvent(context.Background(), "research", "burst", "uid-burst", "Normal", "CloudVMRegionSelected", "Launched in us-east-2"); err != nil {
		t.Fatalf("RecordPodEvent failed: %v", err)
	}
	list, err := cs.CoreV1().Events("research").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(list.Items) != 1 {
		t.Fatalf("listed %v events (err %v), want 1", len(list.Items), err)
	}
	ev := list.Items[0]
	if ev.InvolvedObject.Kind != "Pod" || ev.InvolvedObject.Name != "burst" || string(ev.InvolvedObject.UID) != "uid-burst" || ev.Reason != "CloudVMRegionSelected" {
		t.Fatalf("unexpected event %+v", ev)
	}
}

func decodeUserData(t *testing.T, encoded string) string {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("user data is not base64: %v", err)
	}
	r, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("user data is not gzip: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read user data: %v", err)
	}
	return string(out)
}

func TestLaunchFallbackUsesTheLaunchRegionsTunnelRoute(t *testing.T) {
	p := newTestProvider(t)
	p.SetClusterFacts(&ClusterFacts{Regions: map[string]RegionalClusterFact{
		"us-west-1": {LaunchTemplateID: "lt-usw1"},
		"eu-west-1": {LaunchTemplateID: "lt-euw1"},
	}})
	usw1 := &regionMock{fleetErrorCode: "InsufficientInstanceCapacity"}
	euw1 := &regionMock{instanceID: "i-euw1"}
	p.SetRegionClient("us-west-1", usw1.client())
	p.SetRegionClient("eu-west-1", euw1.client())

	server := NewServer(p, "test-cluster")
	server.EnableTunnel(TunnelSettings{
		Endpoint:       "tunnel.example.internal:443",
		Transports:     TunnelTransports{"us-west-1": TunnelTransportRouter, "eu-west-1": TunnelTransportPublic},
		GatewayIP:      "192.0.2.10",
		PublicEndpoint: "tunnel.example.com:443",
	})
	spec := fallbackSpec("eu-west-1")
	spec.RunnerAuthToken = "runner-secret"
	if err := server.applyTunnelSpec(spec); err != nil {
		t.Fatalf("applyTunnelSpec: %v", err)
	}

	if _, err := p.Launch(context.Background(), spec); err != nil {
		t.Fatalf("Launch failed: %v", err)
	}
	pinned := decodeUserData(t, usw1.lastUserData)
	if !strings.Contains(pinned, "Router transport") || !strings.Contains(pinned, "tunnel.example.internal:443") {
		t.Errorf("pinned us-west-1 attempt must use the router route")
	}
	fallback := decodeUserData(t, euw1.lastUserData)
	if strings.Contains(fallback, "Router transport") || strings.Contains(fallback, "192.0.2.10") {
		t.Errorf("eu-west-1 fallback must not use the pinned region's router route")
	}
	if !strings.Contains(fallback, `CLOUDVM_TUNNEL_ENDPOINT="tunnel.example.com:443"`) || !strings.Contains(fallback, "instance-identity/rsa2048") {
		t.Errorf("eu-west-1 fallback must dial the public endpoint with an identity document")
	}
}

func TestLaunchRegionsSkipsCandidatesWithoutTunnelRoute(t *testing.T) {
	p := newTestProvider(t)
	p.SetClusterFacts(&ClusterFacts{Regions: map[string]RegionalClusterFact{
		"us-east-1": {LaunchTemplateID: "lt-use1"},
		"us-east-2": {LaunchTemplateID: "lt-use2"},
	}})
	spec := fallbackSpec("us-east-1", "us-east-2")
	spec.TunnelRoutes = map[string]TunnelRoute{"us-west-1": {Transport: "router"}, "us-east-2": {Transport: "public"}}
	got := strings.Join(p.launchRegions(spec, []string{"g5.xlarge"}), ",")
	if got != "us-west-1,us-east-2" {
		t.Fatalf("launchRegions = %s, want us-west-1,us-east-2", got)
	}
}
