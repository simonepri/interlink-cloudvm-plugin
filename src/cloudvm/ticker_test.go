package main

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

type mockEC2TickerFetcher struct {
	historyOutput *ec2.DescribeSpotPriceHistoryOutput
	scoresOutput  *ec2.GetSpotPlacementScoresOutput
	err           error
}

func (m *mockEC2TickerFetcher) DescribeSpotPriceHistory(_ context.Context, _ *ec2.DescribeSpotPriceHistoryInput, _ ...func(*ec2.Options)) (*ec2.DescribeSpotPriceHistoryOutput, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.historyOutput, nil
}

func (m *mockEC2TickerFetcher) GetSpotPlacementScores(_ context.Context, _ *ec2.GetSpotPlacementScoresInput, _ ...func(*ec2.Options)) (*ec2.GetSpotPlacementScoresOutput, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.scoresOutput != nil {
		return m.scoresOutput, nil
	}
	score := int32(9)
	return &ec2.GetSpotPlacementScoresOutput{
		SpotPlacementScores: []ec2types.SpotPlacementScore{
			{
				Region:             aws.String("us-east-1"),
				AvailabilityZoneId: aws.String("use1-az1"),
				Score:              &score,
			},
		},
	}, nil
}

func (m *mockEC2TickerFetcher) DescribeRegions(_ context.Context, _ *ec2.DescribeRegionsInput, _ ...func(*ec2.Options)) (*ec2.DescribeRegionsOutput, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &ec2.DescribeRegionsOutput{
		Regions: []ec2types.Region{
			{RegionName: aws.String("us-east-1")},
			{RegionName: aws.String("us-west-2")},
			{RegionName: aws.String("eu-west-1")},
		},
	}, nil
}

func TestSpotPriceTicker_PlacementScoresAndSavings(t *testing.T) {
	now := time.Now()
	scoreVal := int32(9)
	fetcher := &mockEC2TickerFetcher{
		historyOutput: &ec2.DescribeSpotPriceHistoryOutput{
			SpotPriceHistory: []ec2types.SpotPrice{
				{
					InstanceType:     ec2types.InstanceType("g5.xlarge"),
					AvailabilityZone: aws.String("us-east-1a"),
					SpotPrice:        aws.String("0.4024"),
					Timestamp:        aws.Time(now),
				},
			},
		},
		scoresOutput: &ec2.GetSpotPlacementScoresOutput{
			SpotPlacementScores: []ec2types.SpotPlacementScore{
				{
					Region:             aws.String("us-east-1"),
					AvailabilityZoneId: aws.String("us-east-1a"),
					Score:              &scoreVal,
				},
			},
		},
	}

	ticker := NewSpotPriceTicker([]string{"us-east-1"}, []string{"g5.xlarge"}, time.Minute)
	ticker.SetFetcherFactory(func(_ context.Context, _ string) (SpotPriceFetcher, error) {
		return fetcher, nil
	})

	ticker.PollOnce(context.Background())
	ticker.PollPlacementScores(context.Background())

	scores := ticker.AllScores()
	if len(scores) == 0 {
		t.Fatalf("expected placement scores, got empty")
	}

	routes := ticker.Routes()
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "cloudvm_spot_placement_score") {
		t.Errorf("metrics missing cloudvm_spot_placement_score")
	}
	if !strings.Contains(body, "cloudvm_spot_savings_percentage") {
		t.Errorf("metrics missing cloudvm_spot_savings_percentage")
	}
	if !strings.Contains(body, "cloudvm_ondemand_price_usd_per_hour") {
		t.Errorf("metrics missing cloudvm_ondemand_price_usd_per_hour")
	}
}

func TestSpotPriceTicker_DynamicRegionDiscovery(t *testing.T) {
	t.Setenv("AWS_REGION", "us-west-2")

	// Empty regions slice enables auto-discovery
	ticker := NewSpotPriceTicker(nil, []string{"g5.xlarge"}, 10*time.Minute)
	ticker.SetFetcherFactory(func(_ context.Context, _ string) (SpotPriceFetcher, error) {
		return &mockEC2TickerFetcher{}, nil
	})

	if !ticker.autoDiscoverRegions {
		t.Fatalf("expected autoDiscoverRegions to be true")
	}

	ticker.PollOnce(context.Background())

	ticker.mu.RLock()
	regions := ticker.regions
	ticker.mu.RUnlock()

	expected := []string{"eu-west-1", "us-east-1", "us-west-2"}
	if len(regions) != len(expected) {
		t.Fatalf("expected %d discovered regions, got %d: %v", len(expected), len(regions), regions)
	}
	for i, r := range expected {
		if regions[i] != r {
			t.Errorf("expected region %s at %d, got %s", r, i, regions[i])
		}
	}
}
