// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/pricing"
	pricingtypes "github.com/aws/aws-sdk-go-v2/service/pricing/types"
	"github.com/aws/aws-sdk-go-v2/service/servicequotas"
)

// AWSPricingAPI abstracts the AWS Price List Service API.
type AWSPricingAPI interface {
	GetProducts(ctx context.Context, params *pricing.GetProductsInput, optFns ...func(*pricing.Options)) (*pricing.GetProductsOutput, error)
}

// AWSServiceQuotasAPI abstracts the AWS Service Quotas API.
type AWSServiceQuotasAPI interface {
	GetServiceQuota(ctx context.Context, params *servicequotas.GetServiceQuotaInput, optFns ...func(*servicequotas.Options)) (*servicequotas.GetServiceQuotaOutput, error)
}

// HTTPGetter abstracts simple HTTP GET requests for testing.
type HTTPGetter interface {
	Get(url string) (*http.Response, error)
}

// SpotAdvisorEntry describes an instance type's interruption and savings in a region.
type SpotAdvisorEntry struct {
	SavingsPct int `json:"s"`
	RangeIndex int `json:"r"`
}

// SpotAdvisorData describes the payload returned by AWS Spot Advisor.
type SpotAdvisorData struct {
	SpotAdvisor map[string]map[string]SpotAdvisorEntry `json:"spot_advisor"`
}

// SpotAdvisorCollector fetches and caches the AWS Spot Advisor interruption frequency feed.
type SpotAdvisorCollector struct {
	mu           sync.RWMutex
	regions      []string
	lastData     map[string]map[string]SpotAdvisorEntry // region -> instanceType -> entry
	lastModified time.Time
	feedURL      string
	httpClient   HTTPGetter
}

// NewSpotAdvisorCollector creates a collector for AWS Spot Advisor data.
func NewSpotAdvisorCollector(feedURL string, client HTTPGetter) *SpotAdvisorCollector {
	if feedURL == "" {
		feedURL = "https://spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json"
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &SpotAdvisorCollector{
		feedURL:    feedURL,
		httpClient: client,
	}
}

// UptimeSecondsForAdvisorRange maps AWS Spot Advisor range indices to conservative expected uptime in seconds.
func UptimeSecondsForAdvisorRange(rangeIndex int) float64 {
	switch rangeIndex {
	case 0: // <5% interruption rate
		return 86400 // 24 hours
	case 1: // 5-10% interruption rate
		return 50400 // 14 hours
	case 2: // 10-15% interruption rate
		return 28800 // 8 hours
	case 3: // 15-20% interruption rate
		return 18000 // 5 hours
	case 4: // >20% interruption rate
		return 10800 // 3 hours
	default:
		return 10800
	}
}

// Poll fetches the latest advisor feed and caches it. If download fails, previous data is retained.
func (c *SpotAdvisorCollector) Poll(_ context.Context) error {
	resp, err := c.httpClient.Get(c.feedURL)
	if err != nil {
		return fmt.Errorf("failed to fetch spot advisor feed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("spot advisor feed returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read spot advisor feed body: %w", err)
	}

	var raw struct {
		SpotAdvisor map[string]map[string]json.RawMessage `json:"spot_advisor"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return fmt.Errorf("failed to decode spot advisor json: %w", err)
	}

	normalized := make(map[string]map[string]SpotAdvisorEntry)
	for region, subMap := range raw.SpotAdvisor {
		regionEntries := make(map[string]SpotAdvisorEntry)
		if linuxRaw, ok := subMap["Linux"]; ok {
			var linuxEntries map[string]SpotAdvisorEntry
			if err := json.Unmarshal(linuxRaw, &linuxEntries); err == nil {
				for itype, entry := range linuxEntries {
					regionEntries[itype] = entry
				}
			}
		} else {
			for itype, entryRaw := range subMap {
				var entry SpotAdvisorEntry
				if err := json.Unmarshal(entryRaw, &entry); err == nil {
					regionEntries[itype] = entry
				}
			}
		}
		if len(regionEntries) > 0 {
			normalized[region] = regionEntries
		}
	}

	lastMod := time.Now()
	if headerVal := resp.Header.Get("Last-Modified"); headerVal != "" {
		if t, err := http.ParseTime(headerVal); err == nil {
			lastMod = t
		}
	}

	c.mu.Lock()
	c.lastData = normalized
	c.lastModified = lastMod
	c.mu.Unlock()

	return nil
}

// GetEntry returns the advisor record for a region and instance type if available.
func (c *SpotAdvisorCollector) GetEntry(region, instanceType string) (SpotAdvisorEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.lastData == nil {
		return SpotAdvisorEntry{}, false
	}
	regData, ok := c.lastData[region]
	if !ok {
		return SpotAdvisorEntry{}, false
	}
	entry, ok := regData[instanceType]
	return entry, ok
}

// LastModified returns the timestamp of the current advisor data.
func (c *SpotAdvisorCollector) LastModified() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastModified
}

// SetRegions updates the target regions for the advisor collector.
func (c *SpotAdvisorCollector) SetRegions(regions []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.regions = make([]string, len(regions))
	copy(c.regions, regions)
}

// Regions returns a copy of configured advisor regions.
func (c *SpotAdvisorCollector) Regions() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, len(c.regions))
	copy(out, c.regions)
	return out
}

// GetAWSOnDemandPrice queries the AWS Price List API for the current Linux shared on-demand price.
// If the API call fails or returns no products, it falls back to OnDemandBaselineUSD, which is 0 for shapes the catalog lacks.
func GetAWSOnDemandPrice(ctx context.Context, client AWSPricingAPI, region, instanceType string) float64 {
	if price, ok := listAWSOnDemandPrice(ctx, client, region, instanceType); ok {
		return price
	}
	return OnDemandBaselineUSD(instanceType)
}

// listAWSOnDemandPrice returns the Price List API price and whether the API supplied one.
func listAWSOnDemandPrice(ctx context.Context, client AWSPricingAPI, region, instanceType string) (float64, bool) {
	if client == nil {
		return 0, false
	}

	// Filter on Linux, Shared tenancy, NA preinstalled software, Used capacity
	input := &pricing.GetProductsInput{
		ServiceCode: aws.String("AmazonEC2"),
		Filters: []pricingtypes.Filter{
			{Type: pricingtypes.FilterTypeTermMatch, Field: aws.String("operatingSystem"), Value: aws.String("Linux")},
			{Type: pricingtypes.FilterTypeTermMatch, Field: aws.String("tenancy"), Value: aws.String("Shared")},
			{Type: pricingtypes.FilterTypeTermMatch, Field: aws.String("preInstalledSw"), Value: aws.String("NA")},
			{Type: pricingtypes.FilterTypeTermMatch, Field: aws.String("capacitystatus"), Value: aws.String("Used")},
			{Type: pricingtypes.FilterTypeTermMatch, Field: aws.String("instanceType"), Value: aws.String(instanceType)},
			{Type: pricingtypes.FilterTypeTermMatch, Field: aws.String("regionCode"), Value: aws.String(region)},
		},
	}

	out, err := client.GetProducts(ctx, input)
	if err != nil || len(out.PriceList) == 0 {
		return 0, false
	}

	for _, itemJSON := range out.PriceList {
		if price := parsePricingJSONPrice(itemJSON); price > 0 {
			return price, true
		}
	}

	return 0, false
}

// parsePricingJSONPrice traverses AWS Pricing product JSON to extract the hourly USD price.
func parsePricingJSONPrice(raw string) float64 {
	var payload struct {
		Terms struct {
			OnDemand map[string]struct {
				PriceDimensions map[string]struct {
					PricePerUnit map[string]string `json:"pricePerUnit"`
				} `json:"priceDimensions"`
			} `json:"OnDemand"`
		} `json:"terms"`
	}

	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return 0
	}

	for _, term := range payload.Terms.OnDemand {
		for _, dim := range term.PriceDimensions {
			if usdStr, ok := dim.PricePerUnit["USD"]; ok && usdStr != "" {
				if val, err := strconv.ParseFloat(usdStr, 64); err == nil && val > 0 {
					return val
				}
			}
		}
	}

	return 0
}

// DefaultAWSGPUQuotaCodes maps standard EC2 quota codes for GPU families.
var DefaultAWSGPUQuotaCodes = map[string]string{
	"g_vt_spot":         "L-3819A6DF", // All G and VT Spot Instance Requests
	"p_spot":            "L-7295265B", // All P Spot Instance Requests
	"standard_spot":     "L-34B43A08", // All Standard (A, C, D, I, M, R, T, Z) Spot
	"g_vt_ondemand":     "L-DB2E81BA", // Running On-Demand G and VT instances
	"p_ondemand":        "L-417A185B", // Running On-Demand P instances
	"standard_ondemand": "L-1216C47A", // Running On-Demand Standard instances
}

// KnownAWSGPUQuotaCodes contains active EC2 quota codes, overridable via AWS_GPU_QUOTA_CODES.
var KnownAWSGPUQuotaCodes = DefaultAWSGPUQuotaCodes

// Conversion ratios from EC2 quota vCPUs to approximate GPU instances.
var (
	AWSVCPUPerGPUG = 4.0
	AWSVCPUPerGPUP = 12.0
)

func init() {
	if raw := os.Getenv("AWS_VCPU_PER_GPU_G"); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil && v > 0 {
			AWSVCPUPerGPUG = v
		}
	}
	if raw := os.Getenv("AWS_VCPU_PER_GPU_P"); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil && v > 0 {
			AWSVCPUPerGPUP = v
		}
	}
	if raw := os.Getenv("AWS_GPU_QUOTA_CODES"); raw != "" {
		codes := make(map[string]string)
		for k, v := range DefaultAWSGPUQuotaCodes {
			codes[k] = v
		}
		for _, pair := range strings.Split(raw, ",") {
			parts := strings.SplitN(strings.TrimSpace(pair), ":", 2)
			if len(parts) == 2 {
				k := strings.TrimSpace(parts[0])
				v := strings.TrimSpace(parts[1])
				if k != "" && v != "" {
					codes[k] = v
				}
			}
		}
		KnownAWSGPUQuotaCodes = codes
	}
}

// QueryAWSQuota retrieves an EC2 Service Quota limit in vCPUs and estimates GPU quota.
func QueryAWSQuota(ctx context.Context, client AWSServiceQuotasAPI, quotaCode string) (float64, error) {
	if client == nil {
		return 0, fmt.Errorf("service quotas client not configured")
	}
	out, err := client.GetServiceQuota(ctx, &servicequotas.GetServiceQuotaInput{
		ServiceCode: aws.String("ec2"),
		QuotaCode:   aws.String(quotaCode),
	})
	if err != nil {
		return 0, err
	}
	if out.Quota == nil || out.Quota.Value == nil {
		return 0, nil
	}
	return *out.Quota.Value, nil
}

// QueryAWSGPUQuotas queries configured GPU quota codes and exports them.
func QueryAWSGPUQuotas(ctx context.Context, client AWSServiceQuotasAPI, region string, metrics *ObservabilityMetrics) {
	if client == nil || metrics == nil {
		return
	}
	for name, code := range KnownAWSGPUQuotaCodes {
		val, err := QueryAWSQuota(ctx, client, code)
		if err == nil && val >= 0 {
			// Convert vCPUs to approx GPUs using configurable ratios
			gpus := val / AWSVCPUPerGPUG
			if strings.HasPrefix(name, "p_") {
				gpus = val / AWSVCPUPerGPUP
			}
			metrics.SetQuota("aws", region, "gpu", name, gpus, 0)
		}
	}
}

// EC2PlacementScoreFetcher abstracts GetSpotPlacementScores.
type EC2PlacementScoreFetcher interface {
	GetSpotPlacementScores(ctx context.Context, params *ec2.GetSpotPlacementScoresInput, optFns ...func(*ec2.Options)) (*ec2.GetSpotPlacementScoresOutput, error)
}

// QueryAWSPlacementScores queries both regional and single-AZ placement scores for specified instance types.
func QueryAWSPlacementScores(ctx context.Context, client EC2PlacementScoreFetcher, region string, instanceTypes []string, targetCapacity int32, singleAZ bool) (map[string]float64, error) {
	scores := make(map[string]float64)
	if client == nil || len(instanceTypes) == 0 {
		return scores, nil
	}

	out, err := client.GetSpotPlacementScores(ctx, &ec2.GetSpotPlacementScoresInput{
		InstanceTypes:          instanceTypes,
		RegionNames:            []string{region},
		TargetCapacity:         &targetCapacity,
		SingleAvailabilityZone: aws.Bool(singleAZ),
	})
	if err != nil {
		return nil, err
	}

	for _, item := range out.SpotPlacementScores {
		score := float64(aws.ToInt32(item.Score)) / 10.0 // Normalize 1-10 to 0.1-1.0
		az := aws.ToString(item.AvailabilityZoneId)
		if az == "" {
			az = "all"
		}
		scores[az] = score
	}

	return scores, nil
}
