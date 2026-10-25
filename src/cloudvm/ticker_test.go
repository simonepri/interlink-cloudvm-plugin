package main

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

type mockEC2TickerFetcher struct {
	historyOutput      *ec2.DescribeSpotPriceHistoryOutput
	scoresOutput       *ec2.GetSpotPlacementScoresOutput
	regionsOutput      *ec2.DescribeRegionsOutput
	err                error
	describeRegionsErr error
	lastRegionsInput   *ec2.DescribeRegionsInput
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

func (m *mockEC2TickerFetcher) DescribeRegions(_ context.Context, params *ec2.DescribeRegionsInput, _ ...func(*ec2.Options)) (*ec2.DescribeRegionsOutput, error) {
	m.lastRegionsInput = params
	if m.describeRegionsErr != nil {
		return nil, m.describeRegionsErr
	}
	if m.err != nil {
		return nil, m.err
	}
	if m.regionsOutput != nil {
		return m.regionsOutput, nil
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

func TestSpotPriceTicker_AutoDiscoveryFlags(t *testing.T) {
	tests := []struct {
		name     string
		regions  []string
		wantAuto bool
	}{
		{"nil slice", nil, true},
		{"empty slice", []string{}, true},
		{"single empty string", []string{""}, true},
		{"single whitespace string", []string{"   "}, true},
		{"single auto string", []string{"auto"}, true},
		{"uppercase AUTO string", []string{"AUTO"}, true},
		{"mixed auto with region", []string{"us-east-1", "auto"}, true},
		{"mixed empty with region", []string{"us-east-1", ""}, true},
		{"explicit distinct regions", []string{"us-east-1", "us-west-2"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ticker := NewSpotPriceTicker(tt.regions, []string{"g5.xlarge"}, 10*time.Minute)
			if ticker.autoDiscoverRegions != tt.wantAuto {
				t.Errorf("NewSpotPriceTicker(%v).autoDiscoverRegions = %v, want %v", tt.regions, ticker.autoDiscoverRegions, tt.wantAuto)
			}
		})
	}
}

func TestSpotPriceTicker_DescribeRegionsSuccess(t *testing.T) {
	fetcher := &mockEC2TickerFetcher{
		regionsOutput: &ec2.DescribeRegionsOutput{
			Regions: []ec2types.Region{
				{RegionName: aws.String("us-west-2")},
				{RegionName: aws.String("ap-northeast-1")},
				{RegionName: aws.String("eu-west-1")},
				{RegionName: aws.String("us-east-1")},
			},
		},
	}

	ticker := NewSpotPriceTicker([]string{"auto"}, []string{"g5.xlarge"}, 10*time.Minute)
	ticker.SetFetcherFactory(func(_ context.Context, _ string) (SpotPriceFetcher, error) {
		return fetcher, nil
	})

	discovered := ticker.DiscoverRegions(context.Background())
	expected := []string{"ap-northeast-1", "eu-west-1", "us-east-1", "us-west-2"}
	if !reflect.DeepEqual(discovered, expected) {
		t.Fatalf("expected sorted regions %v, got %v", expected, discovered)
	}

	// Verify filter options sent to DescribeRegions
	if fetcher.lastRegionsInput == nil {
		t.Fatalf("expected DescribeRegions to have been called with input")
	}
	if len(fetcher.lastRegionsInput.Filters) == 0 {
		t.Fatalf("expected DescribeRegions input to have filters")
	}
	filterFound := false
	for _, f := range fetcher.lastRegionsInput.Filters {
		if aws.ToString(f.Name) == "opt-in-status" {
			filterFound = true
			wantValues := []string{"opt-in-not-required", "opted-in"}
			if !reflect.DeepEqual(f.Values, wantValues) {
				t.Errorf("expected filter values %v, got %v", wantValues, f.Values)
			}
		}
	}
	if !filterFound {
		t.Errorf("expected filter with Name opt-in-status not found")
	}

	// PollOnce and verify regions are populated and synced to gcpMarket and advisor
	ticker.PollOnce(context.Background())
	if !reflect.DeepEqual(ticker.Regions(), expected) {
		t.Errorf("ticker.Regions() = %v, want %v", ticker.Regions(), expected)
	}
	if gcp := ticker.GCPMarket(); gcp != nil {
		if !reflect.DeepEqual(gcp.Regions(), expected) {
			t.Errorf("gcpMarket.Regions() = %v, want %v", gcp.Regions(), expected)
		}
	}
	if adv := ticker.Advisor(); adv != nil {
		if !reflect.DeepEqual(adv.Regions(), expected) {
			t.Errorf("advisor.Regions() = %v, want %v", adv.Regions(), expected)
		}
	}
}

func TestSpotPriceTicker_DescribeRegionsFallback(t *testing.T) {
	fallback := []string{"us-east-1", "eu-central-1"}
	fetcher := &mockEC2TickerFetcher{
		describeRegionsErr: errors.New("AWS UnauthorizedOperation: not allowed to describe regions"),
	}

	ticker := NewSpotPriceTicker([]string{"auto"}, []string{"g5.xlarge"}, 10*time.Minute, fallback...)
	ticker.SetFetcherFactory(func(_ context.Context, _ string) (SpotPriceFetcher, error) {
		return fetcher, nil
	})

	// Test that DiscoverRegions falls back to configured fallback regions
	discovered := ticker.DiscoverRegions(context.Background())
	if !reflect.DeepEqual(discovered, fallback) {
		t.Fatalf("expected fallback regions %v, got %v", fallback, discovered)
	}

	// Test with empty regions returned by DescribeRegions
	fetcher.describeRegionsErr = nil
	fetcher.regionsOutput = &ec2.DescribeRegionsOutput{Regions: []ec2types.Region{}}
	discoveredEmpty := ticker.DiscoverRegions(context.Background())
	if !reflect.DeepEqual(discoveredEmpty, fallback) {
		t.Fatalf("expected fallback regions %v on empty DescribeRegions, got %v", fallback, discoveredEmpty)
	}
}

func TestSpotPriceTicker_RediscoveryAfterExpiration(t *testing.T) {
	fetcher := &mockEC2TickerFetcher{
		regionsOutput: &ec2.DescribeRegionsOutput{
			Regions: []ec2types.Region{
				{RegionName: aws.String("us-east-1")},
				{RegionName: aws.String("us-west-2")},
			},
		},
	}

	ticker := NewSpotPriceTicker([]string{"auto"}, []string{"g5.xlarge"}, 10*time.Minute)
	ticker.SetFetcherFactory(func(_ context.Context, _ string) (SpotPriceFetcher, error) {
		return fetcher, nil
	})

	// Initial poll
	ticker.PollOnce(context.Background())
	initial := ticker.Regions()
	expectedInitial := []string{"us-east-1", "us-west-2"}
	if !reflect.DeepEqual(initial, expectedInitial) {
		t.Fatalf("expected initial regions %v, got %v", expectedInitial, initial)
	}

	// Update mock fetcher with newly enabled region
	fetcher.regionsOutput = &ec2.DescribeRegionsOutput{
		Regions: []ec2types.Region{
			{RegionName: aws.String("eu-central-1")},
			{RegionName: aws.String("us-east-1")},
			{RegionName: aws.String("us-west-2")},
		},
	}

	// Poll immediately - should not rediscover yet
	ticker.PollOnce(context.Background())
	if !reflect.DeepEqual(ticker.Regions(), expectedInitial) {
		t.Fatalf("expected regions to remain %v before expiration, got %v", expectedInitial, ticker.Regions())
	}

	// Expire discovery timestamp (> 1 hour)
	ticker.mu.Lock()
	ticker.lastRegionDiscovery = time.Now().Add(-2 * time.Hour)
	ticker.mu.Unlock()

	// Poll again - should re-discover
	ticker.PollOnce(context.Background())
	expectedUpdated := []string{"eu-central-1", "us-east-1", "us-west-2"}
	if !reflect.DeepEqual(ticker.Regions(), expectedUpdated) {
		t.Fatalf("expected updated regions %v after expiration, got %v", expectedUpdated, ticker.Regions())
	}
}
