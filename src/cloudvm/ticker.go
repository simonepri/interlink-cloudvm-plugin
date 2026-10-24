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

// SpotPriceFetcher abstracts AWS DescribeSpotPriceHistory and GetSpotPlacementScores for testing.
type SpotPriceFetcher interface {
	DescribeSpotPriceHistory(ctx context.Context, params *ec2.DescribeSpotPriceHistoryInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSpotPriceHistoryOutput, error)
	GetSpotPlacementScores(ctx context.Context, params *ec2.GetSpotPlacementScoresInput, optFns ...func(*ec2.Options)) (*ec2.GetSpotPlacementScoresOutput, error)
}

// PlacementScoreEntry represents a placement score record.
type PlacementScoreEntry struct {
	Region           string    `json:"region"`
	AvailabilityZone string    `json:"availabilityZone,omitempty"`
	InstanceType     string    `json:"instanceType"`
	Score            int32     `json:"score"`
	Timestamp        time.Time `json:"timestamp"`
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
	prices         map[string]*SpotPriceEntry
	scores         map[string]*PlacementScoreEntry
	regions        []string
	instanceTypes  []string
	scrapeInterval time.Duration
	fetcherFactory func(ctx context.Context, region string) (SpotPriceFetcher, error)

	metrics    *ObservabilityMetrics
	advisor    *SpotAdvisorCollector
	gcpMarket  *GCPMarketCollector
	solver     *FillSolver
	catalog    *HardwareCatalog
	pricingAPI AWSPricingAPI
	quotasAPI  AWSServiceQuotasAPI
}

// NewSpotPriceTicker initializes a spot price ticker with full market observability.
func NewSpotPriceTicker(regions, instanceTypes []string, scrapeInterval time.Duration) *SpotPriceTicker {
	if scrapeInterval <= 0 {
		scrapeInterval = 10 * time.Minute
	}
	if len(regions) == 0 {
		regions = []string{"us-east-1", "us-west-2", "eu-west-1"}
	}
	if len(instanceTypes) == 0 {
		instanceTypes = DiscoverTrackedShapes(nil)
	}

	metrics := GlobalMetrics
	catalog := GlobalCatalog
	advisor := NewSpotAdvisorCollector("", nil)
	gcpMarket := NewGCPMarketCollector(regions, nil)
	solver := NewFillSolver()

	return &SpotPriceTicker{
		prices:         make(map[string]*SpotPriceEntry),
		scores:         make(map[string]*PlacementScoreEntry),
		regions:        regions,
		instanceTypes:  instanceTypes,
		scrapeInterval: scrapeInterval,
		metrics:        metrics,
		advisor:        advisor,
		gcpMarket:      gcpMarket,
		solver:         solver,
		catalog:        catalog,
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

// SetMetrics overrides the metrics registry.
func (t *SpotPriceTicker) SetMetrics(m *ObservabilityMetrics) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metrics = m
}

// SetAdvisorCollector overrides the spot advisor collector.
func (t *SpotPriceTicker) SetAdvisorCollector(a *SpotAdvisorCollector) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.advisor = a
}

// SetGCPMarketCollector overrides the GCP market collector.
func (t *SpotPriceTicker) SetGCPMarketCollector(g *GCPMarketCollector) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.gcpMarket = g
}

// SetPricingAPI overrides the AWS pricing API client.
func (t *SpotPriceTicker) SetPricingAPI(api AWSPricingAPI) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pricingAPI = api
}

// SetServiceQuotasAPI overrides the AWS service quotas API client.
func (t *SpotPriceTicker) SetServiceQuotasAPI(api AWSServiceQuotasAPI) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.quotasAPI = api
}

// Metrics returns the observability metrics collector.
func (t *SpotPriceTicker) Metrics() *ObservabilityMetrics {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.metrics
}

// Solver returns the ladder fill solver.
func (t *SpotPriceTicker) Solver() *FillSolver {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.solver
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

	// Update Prometheus metrics & market offers
	t.syncMarketMetrics(ctx)
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

	t.syncMarketMetrics(ctx)
}

// syncMarketMetrics exports prices, signals, quotas, and executes ladder solver.
func (t *SpotPriceTicker) syncMarketMetrics(ctx context.Context) {
	if t.metrics == nil {
		return
	}

	prices := t.AllPrices()
	scores := t.AllScores()

	var offers []*MarketOffer

	// 1. AWS Prices & Signals
	for _, p := range prices {
		ondemand := GetAWSOnDemandPrice(ctx, t.pricingAPI, p.Region, p.InstanceType)
		savingsPct := 0.0
		if ondemand > 0 && p.SpotPriceUSD > 0 && p.SpotPriceUSD < ondemand {
			savingsPct = ((ondemand - p.SpotPriceUSD) / ondemand) * 100.0
		}

		// Legacy metrics
		t.metrics.SetLegacySpotMetrics(p.Region, p.AvailabilityZone, p.InstanceType, p.SpotPriceUSD, ondemand, savingsPct)

		// Catalog specs
		resCap := t.catalog.ResolveCapacity(ctx, nil, p.InstanceType, ResourceVector{})
		gpuModel := resCap.GPUModel
		if gpuModel == "" {
			gpuModel = p.InstanceType
		}

		if resCap.GPUs > 0 {
			t.metrics.SetGPUPrice("aws", p.Region, p.AvailabilityZone, gpuModel, resCap.GPUMemoryGB, resCap.GPUs, "spot", p.InstanceType, p.SpotPriceUSD)
			t.metrics.SetGPUPrice("aws", p.Region, p.AvailabilityZone, gpuModel, resCap.GPUMemoryGB, resCap.GPUs, "on-demand", p.InstanceType, ondemand)
		} else {
			cpuCount, _ := strconv.Atoi(resCap.CPU.String())
			if cpuCount <= 0 {
				cpuCount = 2
			}
			t.metrics.SetVCPUPrice("aws", p.Region, p.AvailabilityZone, "spot", p.InstanceType, p.SpotPriceUSD/float64(cpuCount))
			t.metrics.SetVCPUPrice("aws", p.Region, p.AvailabilityZone, "on-demand", p.InstanceType, ondemand/float64(cpuCount))
		}

		// Add to solver offers
		pricePerGPU := p.SpotPriceUSD
		if resCap.GPUs > 0 {
			pricePerGPU = p.SpotPriceUSD / float64(resCap.GPUs)
		}
		offers = append(offers, &MarketOffer{
			Provider:        "aws",
			Region:          p.Region,
			Zone:            p.AvailabilityZone,
			InstanceType:    p.InstanceType,
			GPUModel:        gpuModel,
			GPUMemoryGB:     resCap.GPUMemoryGB,
			GPUCount:        resCap.GPUs,
			CapacityType:    "spot",
			PricePerHour:    p.SpotPriceUSD,
			PricePerGPUHour: pricePerGPU,
			Availability:    0.8,
			QuotaRemaining:  64,
		})

		onDemandPerGPU := ondemand
		if resCap.GPUs > 0 {
			onDemandPerGPU = ondemand / float64(resCap.GPUs)
		}
		offers = append(offers, &MarketOffer{
			Provider:        "aws",
			Region:          p.Region,
			Zone:            p.AvailabilityZone,
			InstanceType:    p.InstanceType,
			GPUModel:        gpuModel,
			GPUMemoryGB:     resCap.GPUMemoryGB,
			GPUCount:        resCap.GPUs,
			CapacityType:    "on-demand",
			PricePerHour:    ondemand,
			PricePerGPUHour: onDemandPerGPU,
			Availability:    1.0,
			QuotaRemaining:  64,
		})
	}

	// 2. AWS Scores
	for _, s := range scores {
		t.metrics.SetLegacyPlacementScore(s.Region, s.AvailabilityZone, s.InstanceType, float64(s.Score))
		resCap := t.catalog.ResolveCapacity(ctx, nil, s.InstanceType, ResourceVector{})
		gpuModel := resCap.GPUModel
		if gpuModel == "" {
			gpuModel = s.InstanceType
		}
		t.metrics.SetAvailabilityScore("aws", s.Region, s.AvailabilityZone, gpuModel, s.InstanceType, "aws_placement_score", 1, float64(s.Score)/10.0)
	}

	// 3. AWS Spot Advisor
	if t.advisor != nil {
		_ = t.advisor.Poll(ctx)
		now := time.Now()
		for _, reg := range t.regions {
			for _, itype := range t.instanceTypes {
				if entry, ok := t.advisor.GetEntry(reg, itype); ok {
					uptime := UptimeSecondsForAdvisorRange(entry.RangeIndex)
					resCap := t.catalog.ResolveCapacity(ctx, nil, itype, ResourceVector{})
					gpuModel := resCap.GPUModel
					if gpuModel == "" {
						gpuModel = itype
					}
					t.metrics.SetExpectedUptime("aws", reg, itype, gpuModel, "aws_spot_advisor", uptime)
					t.metrics.SetSignalAge("aws", reg, "aws_spot_advisor", now.Sub(t.advisor.LastModified()).Seconds())
				}
			}
		}
	}

	// 4. AWS Quotas
	if t.quotasAPI != nil {
		for _, reg := range t.regions {
			QueryAWSGPUQuotas(ctx, t.quotasAPI, reg, t.metrics)
		}
	}

	// 5. GCP Market Collection
	if t.gcpMarket != nil {
		t.gcpMarket.Collect(ctx, t.metrics)
		for _, reg := range t.gcpMarket.regions {
			for mtype, profile := range KnownGCPHardwareProfiles {
				if profile.GPUs <= 0 {
					continue
				}
				onDemand, spot := GetGCPPrice(mtype)
				offers = append(offers, &MarketOffer{
					Provider:        "gcp",
					Region:          reg,
					Zone:            "",
					InstanceType:    mtype,
					GPUModel:        profile.GPUModel,
					GPUMemoryGB:     profile.GPUMemoryGB,
					GPUCount:        profile.GPUs,
					CapacityType:    "spot",
					PricePerHour:    spot,
					PricePerGPUHour: spot / float64(profile.GPUs),
					Availability:    0.85,
					QuotaRemaining:  64,
				})
				offers = append(offers, &MarketOffer{
					Provider:        "gcp",
					Region:          reg,
					Zone:            "",
					InstanceType:    mtype,
					GPUModel:        profile.GPUModel,
					GPUMemoryGB:     profile.GPUMemoryGB,
					GPUCount:        profile.GPUs,
					CapacityType:    "on-demand",
					PricePerHour:    onDemand,
					PricePerGPUHour: onDemand / float64(profile.GPUs),
					Availability:    1.0,
					QuotaRemaining:  64,
				})
			}
		}
	}

	// 6. Ladder Probing and Fill Solver
	if t.solver != nil {
		t.solver.SetOffers(offers)
		t.solver.SolveAll(t.metrics)
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
		payload := struct {
			Prices map[string]*SpotPriceEntry      `json:"prices"`
			Scores map[string]*PlacementScoreEntry `json:"scores"`
			Offers []*MarketOffer                  `json:"offers,omitempty"`
		}{
			Prices: t.AllPrices(),
			Scores: t.AllScores(),
		}
		if t.solver != nil {
			payload.Offers = t.solver.AllOffers()
		}
		_ = json.NewEncoder(w).Encode(payload)
	})

	mux.Handle("/metrics", t.metrics.Handler())

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	return mux
}
