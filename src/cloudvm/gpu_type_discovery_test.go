// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

func gpuInfo(name string, arch ec2types.ArchitectureType, gpuName string, count, memMiB int32) ec2types.InstanceTypeInfo {
	info := ec2types.InstanceTypeInfo{
		InstanceType:  ec2types.InstanceType(name),
		VCpuInfo:      &ec2types.VCpuInfo{DefaultVCpus: aws.Int32(8)},
		MemoryInfo:    &ec2types.MemoryInfo{SizeInMiB: aws.Int64(32768)},
		ProcessorInfo: &ec2types.ProcessorInfo{SupportedArchitectures: []ec2types.ArchitectureType{arch}},
	}
	if gpuName != "" {
		info.GpuInfo = &ec2types.GpuInfo{Gpus: []ec2types.GpuDeviceInfo{{
			Name: aws.String(gpuName), Count: aws.Int32(count), MemoryInfo: &ec2types.GpuDeviceMemoryInfo{SizeInMiB: aws.Int32(memMiB)},
		}}}
	}
	return info
}

// pagedTypesFetcher serves DescribeInstanceTypes in two pages.
type pagedTypesFetcher struct {
	mockEC2TickerFetcher
}

func (pagedTypesFetcher) DescribeInstanceTypes(_ context.Context, in *ec2.DescribeInstanceTypesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error) {
	if aws.ToString(in.NextToken) == "" {
		return &ec2.DescribeInstanceTypesOutput{
			InstanceTypes: []ec2types.InstanceTypeInfo{
				gpuInfo("g7e.2xlarge", ec2types.ArchitectureTypeX8664, "RTX PRO Server 6000", 1, 98304),
				gpuInfo("m7i.large", ec2types.ArchitectureTypeX8664, "", 0, 0),
			},
			NextToken: aws.String("page-2"),
		}, nil
	}
	return &ec2.DescribeInstanceTypesOutput{InstanceTypes: []ec2types.InstanceTypeInfo{
		gpuInfo("g5g.xlarge", ec2types.ArchitectureTypeArm64, "T4g", 1, 16384),
		gpuInfo("g6f.large", ec2types.ArchitectureTypeX8664, "L4", 0, 2861),
		gpuInfo("gr6.4xlarge", ec2types.ArchitectureTypeX8664, "L4", 1, 22888),
	}}, nil
}

func TestSpotPriceTicker_DiscoverGPUTypes(t *testing.T) {
	ticker := NewSpotPriceTicker([]string{"us-east-1"}, nil, time.Minute)
	ticker.catalog = NewHardwareCatalog()
	ticker.SetFetcherFactory(func(_ context.Context, _ string) (SpotPriceFetcher, error) { return &pagedTypesFetcher{}, nil })

	ticker.discoverGPUTypes(context.Background())

	// GPU types of both pages and both architectures, GPU slices included; no CPU type.
	if got := strings.Join(ticker.TrackedInstanceTypes(), ","); got != "g5g.xlarge,g6f.large,g7e.2xlarge,gr6.4xlarge" {
		t.Errorf("tracked = %s, want g5g.xlarge,g6f.large,g7e.2xlarge,gr6.4xlarge", got)
	}
	// g6f.large holds 2861 of the 22888 MiB an L4 has: one eighth, counted as one GPU.
	if got := ticker.catalog.ResolveCapacity(context.Background(), nil, "g6f.large", ResourceVector{}); got.GPUModel != "L4 1/8 3GB" || got.GPUs != 1 {
		t.Errorf("g6f.large GPU = %q x%d, want L4 1/8 3GB x1", got.GPUModel, got.GPUs)
	}
	if got := ticker.catalog.ResolveCapacity(context.Background(), nil, "gr6.4xlarge", ResourceVector{}).GPUModel; got != "L4 24GB" {
		t.Errorf("gr6.4xlarge GPU = %q, want the L4 AWS reports", got)
	}
	if got := ticker.catalog.ResolveCapacity(context.Background(), nil, "g7e.2xlarge", ResourceVector{}).GPUModel; got != "RTX PRO Server 6000 96GB" {
		t.Errorf("g7e.2xlarge GPU = %q, want RTX PRO Server 6000 96GB", got)
	}
}

func TestSpotPriceTicker_ExplicitTypesSkipDiscovery(t *testing.T) {
	ticker := NewSpotPriceTicker([]string{"us-east-1"}, []string{"g5.xlarge"}, time.Minute)
	ticker.SetFetcherFactory(func(_ context.Context, _ string) (SpotPriceFetcher, error) { return &pagedTypesFetcher{}, nil })

	ticker.PollOnce(context.Background())

	if got := strings.Join(ticker.TrackedInstanceTypes(), ","); got != "g5.xlarge" {
		t.Errorf("tracked = %s, want the explicit g5.xlarge only", got)
	}
}

// pagedPricesFetcher serves DescribeSpotPriceHistory in two pages.
type pagedPricesFetcher struct {
	mockEC2TickerFetcher
}

func (pagedPricesFetcher) DescribeSpotPriceHistory(_ context.Context, in *ec2.DescribeSpotPriceHistoryInput, _ ...func(*ec2.Options)) (*ec2.DescribeSpotPriceHistoryOutput, error) {
	price := func(itype string) ec2types.SpotPrice {
		return ec2types.SpotPrice{InstanceType: ec2types.InstanceType(itype), AvailabilityZone: aws.String("us-east-1a"), SpotPrice: aws.String("0.50"), Timestamp: aws.Time(time.Now())}
	}
	if aws.ToString(in.NextToken) == "" {
		return &ec2.DescribeSpotPriceHistoryOutput{SpotPriceHistory: []ec2types.SpotPrice{price("g5.xlarge")}, NextToken: aws.String("page-2")}, nil
	}
	return &ec2.DescribeSpotPriceHistoryOutput{SpotPriceHistory: []ec2types.SpotPrice{price("g6.xlarge")}}, nil
}

func TestSpotPriceTicker_ReadsEverySpotPricePage(t *testing.T) {
	ticker := NewSpotPriceTicker([]string{"us-east-1"}, []string{"g5.xlarge", "g6.xlarge"}, time.Minute)
	ticker.SetFetcherFactory(func(_ context.Context, _ string) (SpotPriceFetcher, error) { return &pagedPricesFetcher{}, nil })

	ticker.PollOnce(context.Background())

	if _, ok := ticker.GetPrice("us-east-1", "g6.xlarge"); !ok {
		t.Errorf("no price for g6.xlarge, which only the second page holds")
	}
}

// regionTypesFetcher lists one GPU type per region, or fails for regions in failing.
type regionTypesFetcher struct {
	mockEC2TickerFetcher
	region  string
	failing map[string]bool
}

func (f *regionTypesFetcher) DescribeInstanceTypes(_ context.Context, _ *ec2.DescribeInstanceTypesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error) {
	if f.failing[f.region] {
		return nil, errors.New("RequestLimitExceeded")
	}
	name := map[string]string{"us-east-1": "p5en.48xlarge", "us-west-2": "g7e.2xlarge"}[f.region]
	return &ec2.DescribeInstanceTypesOutput{InstanceTypes: []ec2types.InstanceTypeInfo{gpuInfo(name, ec2types.ArchitectureTypeX8664, "H200", 8, 144384)}}, nil
}

func TestSpotPriceTicker_DiscoverGPUTypesKeepsTypesOfFailedRegions(t *testing.T) {
	ticker := NewSpotPriceTicker([]string{"us-east-1", "us-west-2"}, nil, time.Minute)
	ticker.catalog = NewHardwareCatalog()
	failing := map[string]bool{}
	ticker.SetFetcherFactory(func(_ context.Context, region string) (SpotPriceFetcher, error) {
		return &regionTypesFetcher{region: region, failing: failing}, nil
	})
	ticker.discoverGPUTypes(context.Background())
	ticker.mu.Lock()
	ticker.prices[tickerKey("us-east-1", "us-east-1a", "g4dn.xlarge")] = &SpotPriceEntry{Region: "us-east-1", InstanceType: "g4dn.xlarge", SpotPriceUSD: 0.2}
	ticker.mu.Unlock()

	// us-west-2 fails: its g7e stays tracked, and the next read comes within the hour instead of a day.
	failing["us-west-2"] = true
	ticker.discoverGPUTypes(context.Background())

	if got := strings.Join(ticker.TrackedInstanceTypes(), ","); got != "g7e.2xlarge,p5en.48xlarge" {
		t.Errorf("tracked = %s, want both types although us-west-2 failed", got)
	}
	ticker.mu.RLock()
	due := time.Since(ticker.lastTypeDiscovery) > gpuTypeDiscoveryInterval-2*gpuTypeDiscoveryRetry
	_, stale := ticker.prices[tickerKey("us-east-1", "us-east-1a", "g4dn.xlarge")]
	ticker.mu.RUnlock()
	if !due {
		t.Errorf("a partial read waits the full day before retrying")
	}
	if stale {
		t.Errorf("the price of g4dn.xlarge, which is no longer tracked, was kept")
	}
}

func TestSpotPriceTicker_PartialFirstDiscoveryKeepsBuiltInTypes(t *testing.T) {
	ticker := NewSpotPriceTicker([]string{"us-east-1", "us-west-2"}, nil, time.Minute)
	ticker.catalog = NewHardwareCatalog()
	ticker.SetFetcherFactory(func(_ context.Context, region string) (SpotPriceFetcher, error) {
		return &regionTypesFetcher{region: region, failing: map[string]bool{"us-west-2": true}}, nil
	})

	ticker.discoverGPUTypes(context.Background())

	// g5.xlarge comes from the built-in list; a region that failed may offer it, so it stays tracked.
	if !slices.Contains(ticker.TrackedInstanceTypes(), "g5.xlarge") {
		t.Errorf("g5.xlarge from the built-in list was dropped after a partial first discovery")
	}
}
