// Tests for namespace firewalls: CLUSTER_FACTS parsing, the security group or network tag each launch attaches, the
// fail-closed refusal for unlisted namespaces, and bin-packing that keeps namespaces on separate VMs.
package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestNamespaceFirewallsFromClusterFacts(t *testing.T) {
	got, err := NamespaceFirewallsFromClusterFacts(`{"regions":{
		"us-west-2":{"namespaceFirewalls":{"team-a":"sg-0123456789abcdef0","smoke":"sg-0fedcba98"}},
		"europe-west4":{"namespaceFirewalls":{"team-a":"clvm-ns-team-a"}},
		"eu-west-1":{"tunnelTransport":"public"}}}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := map[string]map[string]string{
		"us-west-2":    {"team-a": "sg-0123456789abcdef0", "smoke": "sg-0fedcba98"},
		"europe-west4": {"team-a": "clvm-ns-team-a"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, err := NamespaceFirewallsFromClusterFacts(""); err != nil || len(got) != 0 {
		t.Fatalf("empty CLUSTER_FACTS = %v, %v; want no firewalls", got, err)
	}
	for name, facts := range map[string]string{
		"uppercase namespace": `{"namespaceFirewalls":{"Team-A":"sg-0123456789abcdef0"}}`,
		"empty namespace":     `{"namespaceFirewalls":{"":"sg-0123456789abcdef0"}}`,
		"empty firewall":      `{"namespaceFirewalls":{"team-a":""}}`,
		"firewall with space": `{"namespaceFirewalls":{"team-a":"sg 0123"}}`,
		"firewall as a URL":   `{"namespaceFirewalls":{"team-a":"projects/p/global/firewalls/x"}}`,
	} {
		_, err := NamespaceFirewallsFromClusterFacts(`{"regions":{"us-west-2":` + facts + `}}`)
		if err == nil || !strings.Contains(err.Error(), "us-west-2") {
			t.Errorf("%s must fail naming the region, got %v", name, err)
		}
	}
}

// firewallRegionMock wraps regionMock with the instance's booted security groups and records ModifyInstanceAttribute
// and TerminateInstances calls.
type firewallRegionMock struct {
	regionMock
	bootedGroups []string
	modifyErr    error
	modified     [][]string
	terminated   []string
}

func (m *firewallRegionMock) client() *mockEC2FleetClient {
	c := m.regionMock.client()
	c.describeInstancesFn = func(_ context.Context, in *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
		inst := ec2types.Instance{InstanceId: aws.String(in.InstanceIds[0]), PrivateIpAddress: aws.String("10.0.0.7")}
		for _, g := range m.bootedGroups {
			inst.SecurityGroups = append(inst.SecurityGroups, ec2types.GroupIdentifier{GroupId: aws.String(g)})
		}
		return &ec2.DescribeInstancesOutput{Reservations: []ec2types.Reservation{{Instances: []ec2types.Instance{inst}}}}, nil
	}
	c.modifyInstanceFn = func(_ context.Context, in *ec2.ModifyInstanceAttributeInput) (*ec2.ModifyInstanceAttributeOutput, error) {
		m.modified = append(m.modified, in.Groups)
		return &ec2.ModifyInstanceAttributeOutput{}, m.modifyErr
	}
	c.terminateInstancesFn = func(_ context.Context, in *ec2.TerminateInstancesInput) (*ec2.TerminateInstancesOutput, error) {
		m.terminated = append(m.terminated, in.InstanceIds...)
		return &ec2.TerminateInstancesOutput{}, nil
	}
	return c
}

func firewallProvider(t *testing.T, firewalls map[string]string) (*AWSProvider, *firewallRegionMock) {
	t.Helper()
	p := newTestProvider(t)
	p.SetClusterFacts(&ClusterFacts{Regions: map[string]RegionalClusterFact{
		"us-west-1": {LaunchTemplateID: "lt-usw1", NamespaceFirewalls: firewalls},
	}})
	m := &firewallRegionMock{
		regionMock:   regionMock{offered: map[string][]string{"g5.xlarge": {"us-west-1a"}, "g6.xlarge": {"us-west-1a"}}, instanceID: "i-usw1"},
		bootedGroups: []string{"sg-0000000000000aaaa"},
	}
	p.SetRegionClient("us-west-1", m.client())
	return p, m
}

func TestAWSLaunchAddsNamespaceSecurityGroupToTemplateGroups(t *testing.T) {
	p, m := firewallProvider(t, map[string]string{"research": "sg-0000000000000bbbb", "other": "sg-0000000000000cccc"})
	inst, err := p.Launch(context.Background(), fallbackSpec())
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if want := [][]string{{"sg-0000000000000bbbb", "sg-0000000000000aaaa"}}; !reflect.DeepEqual(m.modified, want) {
		t.Fatalf("ModifyInstanceAttribute groups = %v, want %v", m.modified, want)
	}
	if got := inst.Metadata["namespaceFirewall"]; got != "sg-0000000000000bbbb" {
		t.Errorf("namespaceFirewall metadata = %q", got)
	}
	if len(m.terminated) != 0 {
		t.Errorf("terminated %v, want none", m.terminated)
	}
}

func TestAWSLaunchWithoutNamespaceFirewallsLeavesGroupsAlone(t *testing.T) {
	p, m := firewallProvider(t, nil)
	if _, err := p.Launch(context.Background(), fallbackSpec()); err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if len(m.modified) != 0 {
		t.Fatalf("ModifyInstanceAttribute called with %v, want no call while the region lists no namespace firewalls", m.modified)
	}
}

func TestAWSLaunchRefusesNamespaceWithoutFirewall(t *testing.T) {
	p, m := firewallProvider(t, map[string]string{"other": "sg-0000000000000cccc"})
	_, err := p.Launch(context.Background(), fallbackSpec())
	if !errors.Is(err, errNamespaceFirewallMissing) || !strings.Contains(err.Error(), `"research"`) {
		t.Fatalf("Launch error = %v, want errNamespaceFirewallMissing naming the namespace", err)
	}
	if m.calls() != 0 {
		t.Fatalf("CreateFleet called %d times, want none", m.calls())
	}
}

func TestAWSLaunchTerminatesWorkerWhoseFirewallDoesNotAttach(t *testing.T) {
	p, m := firewallProvider(t, map[string]string{"research": "sg-0000000000000bbbb"})
	m.modifyErr = errors.New("UnauthorizedOperation")
	if _, err := p.Launch(context.Background(), fallbackSpec()); err == nil || !strings.Contains(err.Error(), "UnauthorizedOperation") {
		t.Fatalf("Launch error = %v, want the attach failure", err)
	}
	if !reflect.DeepEqual(m.terminated, []string{"i-usw1"}) {
		t.Fatalf("terminated %v, want the launched worker", m.terminated)
	}
}

func TestAWSLaunchRegionsSkipCandidateWithoutNamespaceFirewall(t *testing.T) {
	p := newTestProvider(t)
	p.SetClusterFacts(&ClusterFacts{Regions: map[string]RegionalClusterFact{
		"us-west-1": {LaunchTemplateID: "lt-usw1"},
		"us-east-1": {LaunchTemplateID: "lt-use1", NamespaceFirewalls: map[string]string{"other": "sg-0000000000000cccc"}},
		"us-east-2": {LaunchTemplateID: "lt-use2", NamespaceFirewalls: map[string]string{"research": "sg-0000000000000bbbb"}},
	}})
	if got := strings.Join(p.launchRegions(fallbackSpec("us-east-1", "us-east-2"), []string{"g5.xlarge"}), ","); got != "us-west-1,us-east-2" {
		t.Errorf("launchRegions = %s, want us-east-1 skipped because it has no firewall for the namespace", got)
	}
}

func TestGCPLaunchTagsWorkerWithNamespaceTag(t *testing.T) {
	client := newMockGCPComputeService()
	p := NewGCPProvider("test-project", "europe-west4-a")
	p.SetClient(client)
	p.SetNamespaceFirewalls(map[string]map[string]string{"europe-west4": {"research": "clvm-ns-research"}})
	spec := &InstanceSpec{PodName: "burst", PodNamespace: "research", PodUID: "uid-burst", Region: "europe-west4", MarketType: "spot"}
	if _, err := p.Launch(context.Background(), spec); err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if len(client.insertCalls) != 1 || client.insertCalls[0].Tags == nil || !reflect.DeepEqual(client.insertCalls[0].Tags.Items, []string{"clvm-ns-research"}) {
		t.Fatalf("inserted instance tags = %+v, want the namespace tag", client.insertCalls[0].Tags)
	}

	spec.PodNamespace = "other"
	if _, err := p.Launch(context.Background(), spec); !errors.Is(err, errNamespaceFirewallMissing) {
		t.Fatalf("Launch error = %v, want errNamespaceFirewallMissing", err)
	}
	if len(client.insertCalls) != 1 {
		t.Fatalf("inserted %d instances, want the refused launch to insert none", len(client.insertCalls))
	}
}

func TestGCPLaunchWithoutNamespaceFirewallsSetsNoTags(t *testing.T) {
	client := newMockGCPComputeService()
	p := NewGCPProvider("test-project", "europe-west4-a")
	p.SetClient(client)
	spec := &InstanceSpec{PodName: "burst", PodNamespace: "research", PodUID: "uid-burst", Region: "europe-west4", MarketType: "spot"}
	if _, err := p.Launch(context.Background(), spec); err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if client.insertCalls[0].Tags != nil {
		t.Fatalf("inserted instance tags = %+v, want none", client.insertCalls[0].Tags)
	}
}

func TestRecordNamespaceFirewallEventWarnsOnlyForMissingFirewall(t *testing.T) {
	s := NewServer(NewMockProvider(), "test-cluster")
	rec := &fakeEventRecorder{}
	s.SetPodEventRecorder(rec)
	_, missing := namespaceFirewall(map[string]string{"other": "sg-0000000000000cccc"}, "us-west-1", "research")
	s.recordNamespaceFirewallEvent(context.Background(), fallbackSpec(), errors.New("capacity"))
	s.recordNamespaceFirewallEvent(context.Background(), fallbackSpec(), missing)
	if len(rec.events) != 1 {
		t.Fatalf("recorded %d events, want 1", len(rec.events))
	}
	if ev := rec.events[0]; ev.reason != "CloudVMNamespaceFirewallMissing" || ev.namespace != "research" || !strings.Contains(ev.message, "namespaceFirewalls") {
		t.Fatalf("unexpected event %+v", ev)
	}
}

func TestNodePool_NeverPacksAcrossNamespaces(t *testing.T) {
	pool := NewNodePool(time.Minute)
	capacity := ResourceVector{CPU: resource.MustParse("32"), Memory: resource.MustParse("64Gi")}
	req := ResourceVector{CPU: resource.MustParse("1"), Memory: resource.MustParse("1Gi")}

	launched := NewCloudVMNode("i-team-a", ProviderAWS, "us-west-2", "10.0.0.1", capacity)
	pool.RegisterAndAllocate(launched, req, "uid-a1", "team-a", "a1")

	if node, _, _, err := pool.Allocate(req, "us-west-2", "uid-b1", "team-b", "b1"); err == nil {
		t.Fatalf("team-b pod packed onto %s, want a bin-pack miss", node.InstanceID)
	}
	if node, _, _, err := pool.Allocate(req, "us-west-2", "uid-x", "", "x"); err == nil {
		t.Fatalf("pod without a namespace packed onto %s, want a bin-pack miss", node.InstanceID)
	}
	node, _, _, err := pool.Allocate(req, "us-west-2", "uid-a2", "team-a", "a2")
	if err != nil || node.InstanceID != "i-team-a" {
		t.Fatalf("team-a pod got %v, %v; want it packed onto i-team-a", node, err)
	}
}
