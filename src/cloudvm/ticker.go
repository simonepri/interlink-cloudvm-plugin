// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
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
	DescribeRegions(ctx context.Context, params *ec2.DescribeRegionsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeRegionsOutput, error)
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

// DefaultRegionCallTimeout bounds each per-region AWS call so one unreachable regional endpoint
// cannot stall polling for every other region.
const DefaultRegionCallTimeout = 30 * time.Second

func tickerKey(region, az, instanceType string) string {
	return fmt.Sprintf("%s/%s/%s", region, az, instanceType)
}

// SpotPriceTicker maintains a live cache of spot prices and exports Prometheus metrics.
type SpotPriceTicker struct {
	mu                  sync.RWMutex
	prices              map[string]*SpotPriceEntry
	scores              map[string]*PlacementScoreEntry
	regions             []string
	fallbackRegions     []string
	instanceTypes       []string
	scrapeInterval      time.Duration
	autoDiscoverRegions bool
	lastRegionDiscovery time.Time
	defaultRegion       string
	regionCallTimeout   time.Duration
	fetcherFactory      func(ctx context.Context, region string) (SpotPriceFetcher, error)

	metrics       *ObservabilityMetrics
	advisor       *SpotAdvisorCollector
	gcpMarket     *GCPMarketCollector
	solver        *FillSolver
	catalog       *HardwareCatalog
	onDemand      *OnDemandPriceBook
	quotasAPI     AWSServiceQuotasAPI
	quotasFactory func(region string) AWSServiceQuotasAPI
}

// NewSpotPriceTicker initializes a spot price ticker with full market observability.
func NewSpotPriceTicker(regions, instanceTypes []string, scrapeInterval time.Duration, fallbackRegions ...string) *SpotPriceTicker {
	if scrapeInterval <= 0 {
		scrapeInterval = 10 * time.Minute
	}

	autoDiscover := false
	if len(regions) == 0 {
		autoDiscover = true
	} else {
		for _, r := range regions {
			trimmed := strings.TrimSpace(r)
			if trimmed == "" || strings.EqualFold(trimmed, "auto") {
				autoDiscover = true
				break
			}
		}
	}

	defaultReg := "us-west-2"
	if raw := os.Getenv("AWS_REGION"); raw != "" {
		defaultReg = raw
	}

	var cleanFallbacks []string
	seenFallbacks := make(map[string]bool)
	for _, r := range fallbackRegions {
		r = strings.TrimSpace(r)
		if r != "" && !strings.EqualFold(r, "auto") && !seenFallbacks[r] {
			cleanFallbacks = append(cleanFallbacks, r)
			seenFallbacks[r] = true
		}
	}

	var explicitRegions []string
	for _, r := range regions {
		r = strings.TrimSpace(r)
		if r != "" && !strings.EqualFold(r, "auto") {
			explicitRegions = append(explicitRegions, r)
			if !seenFallbacks[r] {
				cleanFallbacks = append(cleanFallbacks, r)
				seenFallbacks[r] = true
			}
		}
	}

	if len(cleanFallbacks) == 0 {
		cleanFallbacks = []string{defaultReg}
	}

	if len(explicitRegions) > 0 {
		defaultReg = explicitRegions[0]
	} else if len(cleanFallbacks) > 0 {
		defaultReg = cleanFallbacks[0]
	}

	initialRegions := explicitRegions
	if autoDiscover || len(initialRegions) == 0 {
		initialRegions = make([]string, len(cleanFallbacks))
		copy(initialRegions, cleanFallbacks)
	}

	if len(instanceTypes) == 0 {
		instanceTypes = DiscoverTrackedShapes(nil)
	}

	metrics := GlobalMetrics
	catalog := GlobalCatalog
	advisor := NewSpotAdvisorCollector("", nil)
	gcpMarket := NewGCPMarketCollector(nil, nil)
	solver := NewFillSolver()

	return &SpotPriceTicker{
		prices:              make(map[string]*SpotPriceEntry),
		scores:              make(map[string]*PlacementScoreEntry),
		regions:             initialRegions,
		fallbackRegions:     cleanFallbacks,
		instanceTypes:       instanceTypes,
		scrapeInterval:      scrapeInterval,
		autoDiscoverRegions: autoDiscover,
		defaultRegion:       defaultReg,
		regionCallTimeout:   DefaultRegionCallTimeout,
		metrics:             metrics,
		advisor:             advisor,
		gcpMarket:           gcpMarket,
		solver:              solver,
		catalog:             catalog,
		onDemand:            GlobalOnDemandPrices,
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

// SetRegionCallTimeout overrides the per-region AWS call timeout (for unit testing).
func (t *SpotPriceTicker) SetRegionCallTimeout(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.regionCallTimeout = d
}

// forEachRegion runs fn concurrently for every region, each under its own timeout, and waits for all.
func (t *SpotPriceTicker) forEachRegion(ctx context.Context, regions []string, fn func(ctx context.Context, region string)) {
	t.mu.RLock()
	timeout := t.regionCallTimeout
	t.mu.RUnlock()
	var wg sync.WaitGroup
	for _, reg := range regions {
		wg.Add(1)
		go func(region string) {
			defer wg.Done()
			regionCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			fn(regionCtx, region)
		}(reg)
	}
	wg.Wait()
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
	if a != nil && len(t.regions) > 0 {
		a.SetRegions(t.regions)
	}
}

// SetGCPMarketCollector overrides the GCP market collector.
func (t *SpotPriceTicker) SetGCPMarketCollector(g *GCPMarketCollector) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.gcpMarket = g
	if g != nil && len(t.regions) > 0 {
		g.SetRegions(t.regions)
	}
}

// Regions returns a copy of current monitored regions.
func (t *SpotPriceTicker) Regions() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]string, len(t.regions))
	copy(out, t.regions)
	return out
}

// FallbackRegions returns a copy of fallback regions.
func (t *SpotPriceTicker) FallbackRegions() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]string, len(t.fallbackRegions))
	copy(out, t.fallbackRegions)
	return out
}

// SetFallbackRegions updates the fallback regions for the ticker.
func (t *SpotPriceTicker) SetFallbackRegions(regions ...string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var clean []string
	seen := make(map[string]bool)
	for _, r := range regions {
		r = strings.TrimSpace(r)
		if r != "" && !strings.EqualFold(r, "auto") && !seen[r] {
			clean = append(clean, r)
			seen[r] = true
		}
	}
	t.fallbackRegions = clean
}

// Advisor returns the spot advisor collector.
func (t *SpotPriceTicker) Advisor() *SpotAdvisorCollector {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.advisor
}

// GCPMarket returns the GCP market collector.
func (t *SpotPriceTicker) GCPMarket() *GCPMarketCollector {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.gcpMarket
}

// SetPricingAPI gives this ticker its own on-demand price book backed by api instead of GlobalOnDemandPrices.
func (t *SpotPriceTicker) SetPricingAPI(api AWSPricingAPI) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.onDemand = NewOnDemandPriceBook(api)
}

// SetServiceQuotasAPI overrides the AWS service quotas client used for every region.
func (t *SpotPriceTicker) SetServiceQuotasAPI(api AWSServiceQuotasAPI) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.quotasAPI = api
}

// SetServiceQuotasFactory sets how the ticker builds a service quotas client for each region it reads quotas in.
func (t *SpotPriceTicker) SetServiceQuotasFactory(factory func(region string) AWSServiceQuotasAPI) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.quotasFactory = factory
}

// quotasClient returns the service quotas client for a region, or nil when none is configured.
func (t *SpotPriceTicker) quotasClient(region string) AWSServiceQuotasAPI {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.quotasAPI != nil {
		return t.quotasAPI
	}
	if t.quotasFactory != nil {
		return t.quotasFactory(region)
	}
	return nil
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

func (t *SpotPriceTicker) fallbackOrCurrentRegions() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(t.fallbackRegions) > 0 {
		out := make([]string, len(t.fallbackRegions))
		copy(out, t.fallbackRegions)
		return out
	}
	out := make([]string, len(t.regions))
	copy(out, t.regions)
	return out
}

// DiscoverRegions queries EC2 DescribeRegions to find all accessible regions for the account.
func (t *SpotPriceTicker) DiscoverRegions(ctx context.Context) []string {
	t.mu.RLock()
	defReg := t.defaultRegion
	factory := t.fetcherFactory
	timeout := t.regionCallTimeout
	t.mu.RUnlock()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if factory == nil {
		log.Printf("[cloudvm-ticker] Warning: no fetcher factory configured, falling back to configured regions")
		return t.fallbackOrCurrentRegions()
	}

	fetcher, err := factory(ctx, defReg)
	if err != nil {
		cleanReg := sanitizeLog(defReg)
		log.Printf("[cloudvm-ticker] Warning: failed to initialize EC2 client for region discovery in %s: %v, falling back to configured regions", cleanReg, err) //nolint:gosec // region sanitized
		return t.fallbackOrCurrentRegions()
	}

	out, err := fetcher.DescribeRegions(ctx, &ec2.DescribeRegionsInput{
		Filters: []ec2types.Filter{
			{
				Name:   aws.String("opt-in-status"),
				Values: []string{"opt-in-not-required", "opted-in"},
			},
		},
	})
	if err != nil {
		log.Printf("[cloudvm-ticker] Warning: DescribeRegions failed: %v, falling back to configured regions", err)
		return t.fallbackOrCurrentRegions()
	}
	if out == nil || len(out.Regions) == 0 {
		log.Printf("[cloudvm-ticker] Warning: DescribeRegions returned empty region list, falling back to configured regions")
		return t.fallbackOrCurrentRegions()
	}

	var res []string
	for _, r := range out.Regions {
		if r.RegionName != nil && *r.RegionName != "" {
			res = append(res, *r.RegionName)
		}
	}
	if len(res) == 0 {
		log.Printf("[cloudvm-ticker] Warning: DescribeRegions returned no valid region names, falling back to configured regions")
		return t.fallbackOrCurrentRegions()
	}
	sort.Strings(res)
	log.Printf("[cloudvm-ticker] Discovered %d allowed regions: %v", len(res), res)
	return res
}

func (t *SpotPriceTicker) refreshDiscoveredRegions(ctx context.Context) {
	discovered := t.DiscoverRegions(ctx)
	if len(discovered) > 0 {
		t.mu.Lock()
		t.regions = discovered
		t.lastRegionDiscovery = time.Now()
		if t.advisor != nil {
			t.advisor.SetRegions(discovered)
		}
		t.mu.Unlock()
	}
}

// PollOnce queries DescribeSpotPriceHistory across configured regions and instance types.
func (t *SpotPriceTicker) PollOnce(ctx context.Context) {
	if t.autoDiscoverRegions {
		t.mu.RLock()
		needsDiscovery := t.lastRegionDiscovery.IsZero() || time.Since(t.lastRegionDiscovery) > 1*time.Hour
		t.mu.RUnlock()
		if needsDiscovery {
			t.refreshDiscoveredRegions(ctx)
		}
	}
	t.mu.RLock()
	regionsToPoll := make([]string, len(t.regions))
	copy(regionsToPoll, t.regions)
	t.mu.RUnlock()

	t.forEachRegion(ctx, regionsToPoll, func(ctx context.Context, reg string) {
		fetcher, err := t.fetcherFactory(ctx, reg)
		if err != nil {
			cleanReg := sanitizeLog(reg)
			log.Printf("[cloudvm-ticker] Error initializing EC2 client for %s: %v", cleanReg, err) //nolint:gosec // region sanitized
			return
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
		if err != nil || out == nil {
			if err != nil {
				cleanReg := sanitizeLog(reg)
				log.Printf("[cloudvm-ticker] Error querying spot prices in %s: %v", cleanReg, err) //nolint:gosec // region sanitized
			}
			return
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
	})

	// Update Prometheus metrics & market offers
	t.syncMarketMetrics(ctx)
}

// PollPlacementScores queries AWS GetSpotPlacementScores for configured instance types.
func (t *SpotPriceTicker) PollPlacementScores(ctx context.Context) {
	t.mu.RLock()
	regionsToPoll := make([]string, len(t.regions))
	copy(regionsToPoll, t.regions)
	t.mu.RUnlock()

	t.forEachRegion(ctx, regionsToPoll, func(ctx context.Context, reg string) {
		fetcher, err := t.fetcherFactory(ctx, reg)
		if err != nil {
			return
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
			return
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
	})

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

	t.mu.RLock()
	onDemandPrices := t.onDemand
	t.mu.RUnlock()

	// 1. AWS Prices & Signals
	for _, p := range prices {
		ondemand := onDemandPrices.Price(ctx, p.Region, p.InstanceType)
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

		accelerator := "none"
		if resCap.TPUs > 0 {
			accelerator = "tpu"
		} else if resCap.GPUs > 0 {
			accelerator = "gpu"
		}

		if resCap.GPUs > 0 {
			t.metrics.SetGPUPrice("aws", p.Region, p.AvailabilityZone, accelerator, gpuModel, resCap.GPUMemoryGB, resCap.GPUs, "spot", p.InstanceType, p.SpotPriceUSD)
			if ondemand > 0 {
				t.metrics.SetGPUPrice("aws", p.Region, p.AvailabilityZone, accelerator, gpuModel, resCap.GPUMemoryGB, resCap.GPUs, "on-demand", p.InstanceType, ondemand)
			}
		} else {
			cpuCount, _ := strconv.Atoi(resCap.CPU.String())
			if cpuCount <= 0 {
				cpuCount = 2
			}
			t.metrics.SetVCPUPrice("aws", p.Region, p.AvailabilityZone, "spot", p.InstanceType, p.SpotPriceUSD/float64(cpuCount))
			if ondemand > 0 {
				t.metrics.SetVCPUPrice("aws", p.Region, p.AvailabilityZone, "on-demand", p.InstanceType, ondemand/float64(cpuCount))
			}
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

		if ondemand <= 0 {
			continue
		}
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
		accelerator := "gpu"
		if resCap.TPUs > 0 {
			accelerator = "tpu"
		} else if resCap.GPUs == 0 {
			accelerator = "none"
		}
		t.metrics.SetAvailabilityScore("aws", s.Region, s.AvailabilityZone, accelerator, gpuModel, s.InstanceType, "aws_placement_score", 1, float64(s.Score)/10.0)
	}

	// 3. AWS Spot Advisor
	if t.advisor != nil {
		if err := t.advisor.Poll(ctx); err != nil {
			log.Printf("[cloudvm-ticker] Warning: failed to poll spot advisor: %v", err)
		}
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
					accelerator := "gpu"
					if resCap.TPUs > 0 {
						accelerator = "tpu"
					} else if resCap.GPUs == 0 {
						accelerator = "none"
					}
					t.metrics.SetExpectedUptime("aws", reg, accelerator, itype, gpuModel, "aws_spot_advisor", uptime)
					t.metrics.SetSignalAge("aws", reg, "aws_spot_advisor", now.Sub(t.advisor.LastModified()).Seconds())
				}
			}
		}
	}

	// 4. AWS Quotas
	t.forEachRegion(ctx, t.Regions(), func(ctx context.Context, reg string) {
		QueryAWSGPUQuotas(ctx, t.quotasClient(reg), reg, t.metrics)
	})

	// 5. GCP Market Collection
	if t.gcpMarket != nil {
		t.gcpMarket.Collect(ctx, t.metrics)
		for _, reg := range t.gcpMarket.Regions() {
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

	t.metrics.SweepMarketGauges()
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

// OnDemandBaselineUSD returns the AWS on-demand rate from the pricing catalog, or 0 when the catalog lacks the shape.
func OnDemandBaselineUSD(instanceType string) float64 {
	if GlobalPricingCatalog != nil {
		if rate, ok := GlobalPricingCatalog.GetAWSRate(instanceType); ok {
			return rate
		}
	}
	return 0
}

// Start runs the background polling loop until ctx is canceled.
func (t *SpotPriceTicker) Start(ctx context.Context) {
	if t.autoDiscoverRegions {
		t.refreshDiscoveredRegions(ctx)
	}
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
