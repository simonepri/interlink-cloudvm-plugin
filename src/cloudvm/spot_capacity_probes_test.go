// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
)

type probeFetcher struct {
	mockEC2TickerFetcher
	mu     sync.Mutex
	inputs []*ec2.GetSpotPlacementScoresInput
	limit  int32 // largest accepted target; 0 means 32
}

func (f *probeFetcher) GetSpotPlacementScores(_ context.Context, in *ec2.GetSpotPlacementScoresInput, _ ...func(*ec2.Options)) (*ec2.GetSpotPlacementScoresOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inputs = append(f.inputs, in)
	limit := f.limit
	if limit == 0 {
		limit = 32
	}
	if aws.ToInt32(in.TargetCapacity) > limit {
		return nil, &smithy.GenericAPIError{Code: "TargetCapacityLimitExceeded", Message: "The value for targetCapacity exceeds your limit"}
	}
	out := &ec2.GetSpotPlacementScoresOutput{}
	for _, region := range in.RegionNames {
		out.SpotPlacementScores = append(out.SpotPlacementScores, ec2types.SpotPlacementScore{Region: aws.String(region), Score: aws.Int32(7)})
	}
	return out, nil
}

func TestSpotPriceTicker_ProbeSpotCapacity(t *testing.T) {
	regions := make([]string, 12)
	for i := range regions {
		regions[i] = fmt.Sprintf("region-%02d", i)
	}
	ticker := NewSpotPriceTicker(regions, []string{"p5.48xlarge", "p4d.24xlarge", "g6.48xlarge", "g6.xlarge"}, time.Minute)
	fetcher := &probeFetcher{}
	ticker.SetFetcherFactory(func(_ context.Context, _ string) (SpotPriceFetcher, error) { return fetcher, nil })

	scores := ticker.ProbeSpotCapacity(context.Background())

	for _, in := range fetcher.inputs {
		if len(in.RegionNames) > maxPlacementScoreRegions {
			t.Errorf("request scored %d regions, want at most %d", len(in.RegionNames), maxPlacementScoreRegions)
		}
		if slices.Contains(in.InstanceTypes, "g6.xlarge") && len(in.InstanceTypes) != 1 {
			t.Errorf("probe mixed the 1-GPU g6.xlarge with other GPU counts: %v", in.InstanceTypes)
		}
	}
	eightGPU := scores[spotScoreKey("aws", "region-11", 80, 8)]
	if eightGPU[8] != 7 || eightGPU[256] != 7 {
		t.Errorf("80 GB 8-GPU scores in region-11 = %v, want 7 at 8 and 256 GPUs", eightGPU)
	}
	oneGPU := scores[spotScoreKey("aws", "region-00", 24, 1)]
	if oneGPU[8] != 7 {
		t.Errorf("24 GB 1-GPU scores = %v, want 7 at 8 GPUs", oneGPU)
	}
	if _, ok := eightGPU[1024]; ok {
		t.Errorf("1024 GPUs (128 instances) was rejected by AWS but still scored: %v", eightGPU)
	}
	if eightGPU[256] != 7 {
		t.Errorf("rejected 1024-GPU probe did not back off to an accepted size: %v", eightGPU)
	}
}

func TestSpotPriceTicker_ProbeSpotCapacityLearnsTheTargetLimit(t *testing.T) {
	ticker := NewSpotPriceTicker([]string{"us-east-1"}, []string{"g6.xlarge"}, time.Minute)
	fetcher := &probeFetcher{}
	ticker.SetFetcherFactory(func(_ context.Context, _ string) (SpotPriceFetcher, error) { return fetcher, nil })
	rejected := func() int {
		n := 0
		for _, in := range fetcher.inputs {
			if aws.ToInt32(in.TargetCapacity) > 32 {
				n++
			}
		}
		return n
	}

	// 1-GPU shapes: 64 GPUs is 64 instances, which the mock rejects; the probe backs off to 32, and 256 and 1024 reuse it.
	scores := ticker.ProbeSpotCapacity(context.Background())
	if got := rejected(); got != 1 {
		t.Fatalf("first pass sent %d rejected probes, want 1", got)
	}
	if got := len(fetcher.inputs); got != 3 {
		t.Errorf("first pass sent %d requests, want 3 (8, 64, 32)", got)
	}
	if got := scores[spotScoreKey("aws", "us-east-1", 24, 1)]; got[8] != 7 || got[32] != 7 {
		t.Errorf("scores %v lack 7 at 8 and 32 GPUs", got)
	}
	ticker.ProbeSpotCapacity(context.Background())
	if got := rejected(); got != 1 {
		t.Errorf("second pass retried a probe AWS rejected: %d rejected probes in total", got)
	}

	// Once the retry window passes, the ticker asks again, so a grown limit is noticed.
	ticker.mu.Lock()
	for key, c := range ticker.probeCaps {
		c.until = time.Now().Add(-time.Second)
		ticker.probeCaps[key] = c
	}
	ticker.mu.Unlock()
	ticker.ProbeSpotCapacity(context.Background())
	if got := rejected(); got != 2 {
		t.Errorf("after the retry window, %d rejected probes in total, want 2", got)
	}
}

func TestSpotPriceTicker_ProbeSpotCapacityHalvesUntilAccepted(t *testing.T) {
	ticker := NewSpotPriceTicker([]string{"us-east-1"}, []string{"g6.xlarge"}, time.Minute)
	fetcher := &probeFetcher{limit: 5}
	ticker.SetFetcherFactory(func(_ context.Context, _ string) (SpotPriceFetcher, error) { return fetcher, nil })

	scores := ticker.ProbeSpotCapacity(context.Background())

	// 8 is rejected, 4 accepted; 64 is rejected, then 32, 16 rejected (3 backoffs) so it stays unscored beyond 4.
	got := scores[spotScoreKey("aws", "us-east-1", 24, 1)]
	if got[4] != 7 || len(got) != 1 {
		t.Errorf("scores %v, want only 4 GPUs scored", got)
	}
	if n := len(fetcher.inputs); n > len(ProbeGPUCounts)*(1+maxProbeBackoffs) {
		t.Errorf("pass sent %d requests, above the backoff budget", n)
	}
}

func TestSpotPriceTicker_ProbeSpotCapacityHasNoDefaultCap(t *testing.T) {
	ticker := NewSpotPriceTicker([]string{"us-east-1"}, []string{"p5.48xlarge"}, time.Minute)
	fetcher := &probeFetcher{limit: 1000}
	ticker.SetFetcherFactory(func(_ context.Context, _ string) (SpotPriceFetcher, error) { return fetcher, nil })

	scores := ticker.ProbeSpotCapacity(context.Background())

	got := scores[spotScoreKey("aws", "us-east-1", 80, 8)]
	if got[1024] != 7 {
		t.Errorf("scores %v lack 7 at 1024 GPUs (128 instances)", got)
	}
}

func TestSpotPriceTicker_ProbeSpotCapacityCapsTargetsAtTheConfiguredLimit(t *testing.T) {
	ticker := NewSpotPriceTicker([]string{"us-east-1"}, []string{"p5.48xlarge"}, time.Minute)
	fetcher := &probeFetcher{}
	ticker.SetFetcherFactory(func(_ context.Context, _ string) (SpotPriceFetcher, error) { return fetcher, nil })
	ticker.SetProbeMaxInstances(20)

	scores := ticker.ProbeSpotCapacity(context.Background())

	var targets []int32
	for _, in := range fetcher.inputs {
		targets = append(targets, aws.ToInt32(in.TargetCapacity))
	}
	slices.Sort(targets)
	// 8 GPUs = 1 instance, 64 GPUs = 8, and 256 and 1024 GPUs both cap at 20 instances, probed once.
	for _, want := range []int32{1, 8, 20} {
		if !slices.Contains(targets, want) {
			t.Errorf("targets %v lack %d", targets, want)
		}
	}
	for _, got := range targets {
		if got > 20 {
			t.Errorf("probe asked for %d instances, above the cap of 20", got)
		}
	}
	count := func(want int32) int {
		n := 0
		for _, got := range targets {
			if got == want {
				n++
			}
		}
		return n
	}
	if count(20) != count(8) {
		t.Errorf("capped target probed %d times, want once per group like the 8-instance probe (%d): %v", count(20), count(8), targets)
	}
	got := scores[spotScoreKey("aws", "us-east-1", 80, 8)]
	for _, gpus := range []int{8, 64, 160} {
		if got[gpus] != 7 {
			t.Errorf("scores %v lack 7 at %d GPUs", got, gpus)
		}
	}
	if _, ok := got[1024]; ok {
		t.Errorf("scores %v hold 1024 GPUs, which was never probed", got)
	}
}
