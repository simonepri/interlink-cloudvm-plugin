// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

type probeFetcher struct {
	mockEC2TickerFetcher
	mu     sync.Mutex
	inputs []*ec2.GetSpotPlacementScoresInput
}

func (f *probeFetcher) GetSpotPlacementScores(_ context.Context, in *ec2.GetSpotPlacementScoresInput, _ ...func(*ec2.Options)) (*ec2.GetSpotPlacementScoresOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inputs = append(f.inputs, in)
	if aws.ToInt32(in.TargetCapacity) > 32 {
		return nil, errors.New("target capacity exceeds the account's spot limit")
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
		if slices.Contains(in.InstanceTypes, "g6.xlarge") {
			t.Errorf("probe asked about the 1-GPU g6.xlarge: %v", in.InstanceTypes)
		}
	}
	h100 := scores[spotScoreKey("aws", "region-11", 80)]
	if h100[8] != 7 || h100[256] != 7 {
		t.Errorf("80 GB scores in region-11 = %v, want 7 at 8 and 256 GPUs", h100)
	}
	if _, ok := h100[1024]; ok {
		t.Errorf("1024 GPUs (128 instances) was rejected by AWS but still scored: %v", h100)
	}
	if _, ok := scores[spotScoreKey("aws", "region-00", 24)]; !ok {
		t.Errorf("no 24 GB scores; the g6.48xlarge, p4d, and p5 shapes all qualify")
	}
}
