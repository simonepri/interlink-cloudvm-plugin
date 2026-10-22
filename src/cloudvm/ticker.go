// Package main implements the interlink-provider-cloudvm daemon.
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
}

// SpotPriceEntry represents a real-time spot price record.
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
	prices         map[string]*SpotPriceEntry // key: "region/instanceType"
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

// Start runs the background polling loop until ctx is canceled.
func (t *SpotPriceTicker) Start(ctx context.Context) {
	t.PollOnce(ctx)
	ticker := time.NewTicker(t.scrapeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.PollOnce(ctx)
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
			if entry.AvailabilityZone != "" {
				_, _ = fmt.Fprintf(w, "cloudvm_spot_price_usd_per_hour{region=\"%s\",availability_zone=\"%s\",instance_type=\"%s\"} %.4f\n",
					entry.Region, entry.AvailabilityZone, entry.InstanceType, entry.SpotPriceUSD)
			} else {
				_, _ = fmt.Fprintf(w, "cloudvm_spot_price_usd_per_hour{region=\"%s\",instance_type=\"%s\"} %.4f\n",
					entry.Region, entry.InstanceType, entry.SpotPriceUSD)
			}
		}
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	return mux
}
