package main

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
)

func TestAWSTerminateBatch_GroupsByRegionAndFallsBackPerInstance(t *testing.T) {
	var (
		mu    sync.Mutex
		calls = map[string][]int{}
	)
	clientFor := func(region string) *mockEC2FleetClient {
		return &mockEC2FleetClient{terminateInstancesFn: func(_ context.Context, in *ec2.TerminateInstancesInput) (*ec2.TerminateInstancesOutput, error) {
			mu.Lock()
			calls[region] = append(calls[region], len(in.InstanceIds))
			mu.Unlock()
			if slices.Contains(in.InstanceIds, "i-gone") {
				return nil, fmt.Errorf("InvalidInstanceID.NotFound")
			}
			return &ec2.TerminateInstancesOutput{}, nil
		}}
	}
	p := NewAWSProvider("us-east-1", "eu-west-1")
	p.SetRegionClient("us-east-1", clientFor("us-east-1"))
	p.SetRegionClient("eu-west-1", clientFor("eu-west-1"))

	var ids []string
	p.mu.Lock()
	for i := range 2500 {
		id := fmt.Sprintf("i-east-%d", i)
		p.instanceRegions[id] = "us-east-1"
		ids = append(ids, id)
	}
	for i := range 3 {
		id := fmt.Sprintf("i-eu-%d", i)
		p.instanceRegions[id] = "eu-west-1"
		ids = append(ids, id)
	}
	p.instanceRegions["i-gone"] = "eu-west-1"
	p.mu.Unlock()
	ids = append(ids, "i-gone")

	errs := p.TerminateBatch(context.Background(), ids)

	if len(errs) != 1 || errs["i-gone"] == nil {
		t.Fatalf("expected only i-gone to fail, got %v", errs)
	}
	east := calls["us-east-1"]
	slices.Sort(east)
	if !slices.Equal(east, []int{500, 1000, 1000}) {
		t.Fatalf("us-east-1 calls = %v, want three calls of at most 1000 IDs", east)
	}
	// The eu-west-1 batch fails on the invalid ID, then each of its four instances is retried alone.
	if eu := calls["eu-west-1"]; len(eu) != 5 || eu[0] != 4 {
		t.Fatalf("eu-west-1 calls = %v, want one batch of 4 and four single retries", eu)
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if _, ok := p.instanceRegions["i-east-0"]; ok {
		t.Fatalf("terminated instance keeps its region mapping")
	}
}
