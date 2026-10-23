// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// SpotPriceFetcher abstracts AWS DescribeSpotPriceHistory for testing.
type SpotPriceFetcher interface {
	DescribeSpotPriceHistory(ctx context.Context, params *ec2.DescribeSpotPriceHistoryInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSpotPriceHistoryOutput, error)
	GetSpotPlacementScores(ctx context.Context, params *ec2.GetSpotPlacementScoresInput, optFns ...func(*ec2.Options)) (*ec2.GetSpotPlacementScoresOutput, error)
}

// SpotPriceEntry represents a real-time spot price record.
type PlacementScoreEntry struct {
	Region           string    `json:"region"`
	AvailabilityZone string    `json:"availabilityZone,omitempty"`
	InstanceType     string    `json:"instanceType"`
	Score            int32     `json:"score"`
	Timestamp        time.Time `json:"timestamp"`
}

type SpotPriceEntry struct {
	Region           string    `json:"region"`
	AvailabilityZone string    `json:"availabilityZone,omitempty"`
	InstanceType     string    `json:"instanceType"`
	SpotPriceUSD     float64   `json:"spotPriceUSD"`
	Timestamp        time.Time `json:"timestamp"`
}

func tickerKey(region, az, instanceType string) string {
	return fmt.Sprintf("%s/%s/%s", region, az, instanceType)
}

// SpotPriceTicker maintains a live cache of spot prices and exports Prometheus metrics.
type SpotPriceTicker struct {
	mu             sync.RWMutex
	prices         map[string]*SpotPriceEntry
	scores         map[string]*PlacementScoreEntry
	regions        []string
	instanceTypes  []string
	scrapeInterval time.Duration
	fetcherFactory func(ctx context.Context, region string) (SpotPriceFetcher, error)
}

// NewSpotPriceTicker initializes a spot price ticker.
func NewSpotPriceTicker(regions, instanceTypes []string, scrapeInterval time.Duration) *SpotPriceTicker {
	if scrapeInterval <= 0 {
		scrapeInterval = 10 * time.Minute
	}
	if len(regions) == 0 {
		regions = []string{"us-east-1", "us-west-2", "eu-west-1"}
	}
	if len(instanceTypes) == 0 {
		instanceTypes = []string{"g5.xlarge", "g5.2xlarge", "g5.12xlarge", "g5.48xlarge", "g6.xlarge", "p4d.24xlarge"}
	}

	return &SpotPriceTicker{
		prices:         make(map[string]*SpotPriceEntry),
		scores:         make(map[string]*PlacementScoreEntry),
		regions:        regions,
		instanceTypes:  instanceTypes,
		scrapeInterval: scrapeInterval,
		fetcherFactory: func(ctx context.Context, region string) (SpotPriceFetcher, error) {
			cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
			if err != nil {
				return nil, err
			}
			return ec2.NewFromConfig(cfg), nil
		},
	}
}

// SetFetcherFactory overrides the fetcher constructor (for unit testing).
func (t *SpotPriceTicker) SetFetcherFactory(fn func(ctx context.Context, region string) (SpotPriceFetcher, error)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.fetcherFactory = fn
}

// PollOnce queries DescribeSpotPriceHistory across configured regions and instance types.
func (t *SpotPriceTicker) PollOnce(ctx context.Context) {
	for _, reg := range t.regions {
		fetcher, err := t.fetcherFactory(ctx, reg)
		if err != nil {
			cleanReg := sanitizeLog(reg)
			log.Printf("[cloudvm-ticker] Error initializing EC2 client for %s: %v", cleanReg, err) //nolint:gosec // region sanitized
			continue
		}

		var ec2Types []ec2types.InstanceType
		for _, itype := range t.instanceTypes {
			ec2Types = append(ec2Types, ec2types.InstanceType(itype))
		}

		now := time.Now()
		startTime := now.Add(-30 * time.Minute)
		out, err := fetcher.DescribeSpotPriceHistory(ctx, &ec2.DescribeSpotPriceHistoryInput{
			InstanceTypes:       ec2Types,
			ProductDescriptions: []string{"Linux/UNIX", "Linux/UNIX (Amazon VPC)"},
			StartTime:           aws.Time(startTime),
			EndTime:             aws.Time(now),
		})
		if err != nil {
			cleanReg := sanitizeLog(reg)
			log.Printf("[cloudvm-ticker] Error querying spot prices in %s: %v", cleanReg, err) //nolint:gosec // region sanitized
			continue
		}

		t.mu.Lock()
		for _, ph := range out.SpotPriceHistory {
			itype := string(ph.InstanceType)
			az := aws.ToString(ph.AvailabilityZone)
			ts := aws.ToTime(ph.Timestamp)
			key := tickerKey(reg, az, itype)
			priceVal, err := strconv.ParseFloat(aws.ToString(ph.SpotPrice), 64)
			if err == nil {
				existing, exists := t.prices[key]
				if !exists || ts.After(existing.Timestamp) {
					t.prices[key] = &SpotPriceEntry{
						Region:           reg,
						AvailabilityZone: az,
						InstanceType:     itype,
						SpotPriceUSD:     priceVal,
						Timestamp:        ts,
					}
				}
			}
		}
		t.mu.Unlock()
	}
}

// PollPlacementScores queries AWS GetSpotPlacementScores for configured instance types.
func (t *SpotPriceTicker) PollPlacementScores(ctx context.Context) {
	for _, reg := range t.regions {
		fetcher, err := t.fetcherFactory(ctx, reg)
		if err != nil {
			continue
		}

		targetCapacity := int32(1)
		out, err := fetcher.GetSpotPlacementScores(ctx, &ec2.GetSpotPlacementScoresInput{
			InstanceTypes:          t.instanceTypes,
			RegionNames:            []string{reg},
			TargetCapacity:         &targetCapacity,
			SingleAvailabilityZone: aws.Bool(false),
		})
		if err != nil {
			cleanReg := sanitizeLog(reg)
			log.Printf("[cloudvm-ticker] Error querying spot placement scores in %s: %v", cleanReg, err) //nolint:gosec // region sanitized
			continue
		}

		now := time.Now()
		t.mu.Lock()
		for _, item := range out.SpotPlacementScores {
			score := aws.ToInt32(item.Score)
			regionName := aws.ToString(item.Region)
			az := aws.ToString(item.AvailabilityZoneId)
			for _, itype := range t.instanceTypes {
				key := tickerKey(regionName, az, itype)
				t.scores[key] = &PlacementScoreEntry{
					Region:           regionName,
					AvailabilityZone: az,
					InstanceType:     itype,
					Score:            score,
					Timestamp:        now,
				}
			}
		}
		t.mu.Unlock()
	}
}

// AllScores returns a snapshot of all tracked placement scores.
func (t *SpotPriceTicker) AllScores() map[string]*PlacementScoreEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]*PlacementScoreEntry, len(t.scores))
	for k, v := range t.scores {
		out[k] = v
	}
	return out
}

// OnDemandBaselineUSD returns standard AWS on-demand pricing baseline for known shapes.
func OnDemandBaselineUSD(instanceType string) float64 {
	switch instanceType {
	case "g5.xlarge":
		return 1.006
	case "g5.2xlarge":
		return 1.212
	case "g5.12xlarge":
		return 5.672
	case "g5.48xlarge":
		return 16.288
	case "g6.xlarge":
		return 0.804
	case "g6.2xlarge":
		return 0.977
	case "p4d.24xlarge":
		return 32.7726
	default:
		return 1.000
	}
}

// Start runs the background polling loop until ctx is canceled.
func (t *SpotPriceTicker) Start(ctx context.Context) {
	t.PollOnce(ctx)
	t.PollPlacementScores(ctx)
	ticker := time.NewTicker(t.scrapeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.PollOnce(ctx)
			t.PollPlacementScores(ctx)
		}
	}
}

// GetPrice returns the cheapest known spot price for a region and instance type across all AZs.
func (t *SpotPriceTicker) GetPrice(region, instanceType string) (*SpotPriceEntry, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var cheapest *SpotPriceEntry
	for _, entry := range t.prices {
		if entry.Region == region && entry.InstanceType == instanceType {
			if cheapest == nil || entry.SpotPriceUSD < cheapest.SpotPriceUSD {
				cheapest = entry
			}
		}
	}
	if cheapest != nil {
		return cheapest, true
	}
	return nil, false
}

// GetPriceAZ returns the spot price for a specific availability zone.
func (t *SpotPriceTicker) GetPriceAZ(region, az, instanceType string) (*SpotPriceEntry, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	entry, ok := t.prices[tickerKey(region, az, instanceType)]
	return entry, ok
}

// AllPrices returns a snapshot of all tracked spot prices.
func (t *SpotPriceTicker) AllPrices() map[string]*SpotPriceEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]*SpotPriceEntry, len(t.prices))
	for k, v := range t.prices {
		out[k] = v
	}
	return out
}

// Routes returns HTTP handlers for GET /prices, GET /metrics, and GET /healthz.
func (t *SpotPriceTicker) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/prices", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(t.AllPrices())
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintln(w, "# HELP cloudvm_spot_price_usd_per_hour Current spot price in USD per hour")
		_, _ = fmt.Fprintln(w, "# TYPE cloudvm_spot_price_usd_per_hour gauge")
		for _, entry := range t.AllPrices() {
			ondemand := OnDemandBaselineUSD(entry.InstanceType)
			_, _ = fmt.Fprintf(w, "cloudvm_ondemand_price_usd_per_hour{region=\"%s\",instance_type=\"%s\"} %.4f\n",
				entry.Region, entry.InstanceType, ondemand)

			savingsPct := 0.0
			if ondemand > 0 && entry.SpotPriceUSD > 0 && entry.SpotPriceUSD < ondemand {
				savingsPct = ((ondemand - entry.SpotPriceUSD) / ondemand) * 100.0
			}

			if entry.AvailabilityZone != "" {
				_, _ = fmt.Fprintf(w, "cloudvm_spot_price_usd_per_hour{region=\"%s\",availability_zone=\"%s\",instance_type=\"%s\"} %.4f\n",
					entry.Region, entry.AvailabilityZone, entry.InstanceType, entry.SpotPriceUSD)
				_, _ = fmt.Fprintf(w, "cloudvm_spot_savings_percentage{region=\"%s\",availability_zone=\"%s\",instance_type=\"%s\"} %.2f\n",
					entry.Region, entry.AvailabilityZone, entry.InstanceType, savingsPct)
			} else {
				_, _ = fmt.Fprintf(w, "cloudvm_spot_price_usd_per_hour{region=\"%s\",instance_type=\"%s\"} %.4f\n",
					entry.Region, entry.InstanceType, entry.SpotPriceUSD)
				_, _ = fmt.Fprintf(w, "cloudvm_spot_savings_percentage{region=\"%s\",instance_type=\"%s\"} %.2f\n",
					entry.Region, entry.InstanceType, savingsPct)
			}
		}

		_, _ = fmt.Fprintln(w, "# HELP cloudvm_spot_placement_score Current AWS spot placement score (1-10) indicating capacity depth")
		_, _ = fmt.Fprintln(w, "# TYPE cloudvm_spot_placement_score gauge")
		for _, entry := range t.AllScores() {
			az := entry.AvailabilityZone
			if az == "" {
				az = "all"
			}
			_, _ = fmt.Fprintf(w, "cloudvm_spot_placement_score{region=\"%s\",availability_zone=\"%s\",instance_type=\"%s\"} %d\n",
				entry.Region, az, entry.InstanceType, entry.Score)
		}
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	return mux
}
