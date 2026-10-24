// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"k8s.io/apimachinery/pkg/api/resource"
)

type mockDescriber struct{}

func (m *mockDescriber) DescribeInstanceTypes(_ context.Context, params *ec2.DescribeInstanceTypesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error) {
	itype := string(params.InstanceTypes[0])
	switch itype {
	case "g5.48xlarge":
		return &ec2.DescribeInstanceTypesOutput{
			InstanceTypes: []ec2types.InstanceTypeInfo{
				{
					InstanceType: ec2types.InstanceType(itype),
					VCpuInfo: &ec2types.VCpuInfo{
						DefaultVCpus: aws.Int32(192),
					},
					MemoryInfo: &ec2types.MemoryInfo{
						SizeInMiB: aws.Int64(786432), // 768 GiB
					},
					GpuInfo: &ec2types.GpuInfo{
						Gpus: []ec2types.GpuDeviceInfo{
							{
								Count: aws.Int32(8),
								Name:  aws.String("A10G"),
							},
						},
					},
				},
			},
		}, nil
	default:
		return &ec2.DescribeInstanceTypesOutput{}, nil
	}
}

type errorMockDescriber struct {
	err error
}

func (m *errorMockDescriber) DescribeInstanceTypes(_ context.Context, _ *ec2.DescribeInstanceTypesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error) {
	return nil, m.err
}

func TestHardwareCatalog_ResolveCapacity(t *testing.T) {
	cat := NewHardwareCatalog()
	ctx := context.Background()
	mock := &mockDescriber{}

	fallback, _ := NewResourceVector("2", "4Gi", 0, 0)
	resolved := cat.ResolveCapacity(ctx, mock, "g5.48xlarge", fallback)

	if resolved.GPUs != 8 {
		t.Errorf("expected 8 GPUs, got %d", resolved.GPUs)
	}
	if resolved.CPU.Cmp(resource.MustParse("192")) != 0 {
		t.Errorf("expected 192 vCPU, got %s", resolved.CPU.String())
	}
	if resolved.Memory.Cmp(resource.MustParse("768Gi")) != 0 {
		t.Errorf("expected 768Gi RAM, got %s", resolved.Memory.String())
	}

	// Verify cached
	cached, ok := cat.Get("g5.48xlarge")
	if !ok || cached.GPUs != 8 {
		t.Errorf("expected cached capacity with 8 GPUs")
	}
}

func TestHardwareCatalog_TransientErrorDoesNotPoisonCache(t *testing.T) {
	cat := NewHardwareCatalog()
	ctx := context.Background()
	errMock := &errorMockDescriber{err: fmt.Errorf("ec2 rate limit exceeded")}

	fallback, _ := NewResourceVector("2", "4Gi", 0, 0)
	// First call errors - should return static fallback without caching
	res1 := cat.ResolveCapacity(ctx, errMock, "g5.48xlarge", fallback)
	if res1.GPUs != 8 { // static table for g5.48xlarge has 8
		t.Errorf("expected fallback resolution, got %d GPUs", res1.GPUs)
	}

	if _, ok := cat.Get("g5.48xlarge"); ok {
		t.Errorf("expected catalog cache NOT to be poisoned on transient error")
	}

	// Subsequent call with working mock populates cache
	mock := &mockDescriber{}
	res2 := cat.ResolveCapacity(ctx, mock, "g5.48xlarge", fallback)
	if res2.GPUs != 8 {
		t.Errorf("expected successful resolution, got %d GPUs", res2.GPUs)
	}
	if _, ok := cat.Get("g5.48xlarge"); !ok {
		t.Errorf("expected catalog cache to be populated after successful API call")
	}
}

type mockSpotPriceFetcher struct{}

func (m *mockSpotPriceFetcher) DescribeSpotPriceHistory(_ context.Context, _ *ec2.DescribeSpotPriceHistoryInput, _ ...func(*ec2.Options)) (*ec2.DescribeSpotPriceHistoryOutput, error) {
	return &ec2.DescribeSpotPriceHistoryOutput{
		SpotPriceHistory: []ec2types.SpotPrice{
			{
				InstanceType: ec2types.InstanceType("g5.xlarge"),
				SpotPrice:    aws.String("0.3018"),
				Timestamp:    aws.Time(time.Now()),
			},
		},
	}, nil
}

func TestSpotPriceTicker_PollAndMetrics(t *testing.T) {
	ticker := NewSpotPriceTicker([]string{"us-east-1"}, []string{"g5.xlarge"}, 10*time.Minute)
	ticker.SetFetcherFactory(func(_ context.Context, _ string) (SpotPriceFetcher, error) {
		return &mockSpotPriceFetcher{}, nil
	})

	ticker.PollOnce(context.Background())
	entry, ok := ticker.GetPrice("us-east-1", "g5.xlarge")
	if !ok || entry.SpotPriceUSD != 0.3018 {
		t.Fatalf("expected spot price 0.3018, got %+v", entry)
	}

	rec := httptest.NewRecorder()
	ticker.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /metrics, got %d", rec.Code)
	}
	expected := "cloudvm_spot_price_usd_per_hour"
	if !bytes.Contains(rec.Body.Bytes(), []byte(expected)) || !bytes.Contains(rec.Body.Bytes(), []byte("g5.xlarge")) || !bytes.Contains(rec.Body.Bytes(), []byte("0.3018")) {
		t.Errorf("metrics response missing expected gauge: %s", rec.Body.String())
	}
}

type mockMultiAZSpotFetcher struct{}

func (m *mockMultiAZSpotFetcher) DescribeSpotPriceHistory(_ context.Context, _ *ec2.DescribeSpotPriceHistoryInput, _ ...func(*ec2.Options)) (*ec2.DescribeSpotPriceHistoryOutput, error) {
	t1 := time.Now().Add(-10 * time.Minute)
	t2 := time.Now().Add(-5 * time.Minute)
	t3 := time.Now()
	return &ec2.DescribeSpotPriceHistoryOutput{
		SpotPriceHistory: []ec2types.SpotPrice{
			{
				InstanceType:     ec2types.InstanceType("g5.2xlarge"),
				AvailabilityZone: aws.String("us-east-1a"),
				SpotPrice:        aws.String("0.4500"),
				Timestamp:        aws.Time(t1),
			},
			{
				InstanceType:     ec2types.InstanceType("g5.2xlarge"),
				AvailabilityZone: aws.String("us-east-1b"),
				SpotPrice:        aws.String("0.3800"),
				Timestamp:        aws.Time(t2),
			},
			{
				InstanceType:     ec2types.InstanceType("g5.2xlarge"),
				AvailabilityZone: aws.String("us-east-1a"),
				SpotPrice:        aws.String("0.4200"),
				Timestamp:        aws.Time(t3),
			},
		},
	}, nil
}

func TestSpotPriceTicker_AZKeyingAndTimestampOrdering(t *testing.T) {
	ticker := NewSpotPriceTicker([]string{"us-east-1"}, []string{"g5.2xlarge"}, 10*time.Minute)
	ticker.SetFetcherFactory(func(_ context.Context, _ string) (SpotPriceFetcher, error) {
		return &mockMultiAZSpotFetcher{}, nil
	})

	ticker.PollOnce(context.Background())

	entry1a, ok := ticker.GetPriceAZ("us-east-1", "us-east-1a", "g5.2xlarge")
	if !ok || entry1a.SpotPriceUSD != 0.4200 {
		t.Fatalf("expected newer price 0.4200 for us-east-1a, got %+v", entry1a)
	}

	entry1b, ok := ticker.GetPriceAZ("us-east-1", "us-east-1b", "g5.2xlarge")
	if !ok || entry1b.SpotPriceUSD != 0.3800 {
		t.Fatalf("expected price 0.3800 for us-east-1b, got %+v", entry1b)
	}

	cheapest, ok := ticker.GetPrice("us-east-1", "g5.2xlarge")
	if !ok || cheapest.SpotPriceUSD != 0.3800 {
		t.Fatalf("expected cheapest price 0.3800 across region, got %+v", cheapest)
	}
}
func (m *mockSpotPriceFetcher) GetSpotPlacementScores(_ context.Context, _ *ec2.GetSpotPlacementScoresInput, _ ...func(*ec2.Options)) (*ec2.GetSpotPlacementScoresOutput, error) {
	return &ec2.GetSpotPlacementScoresOutput{}, nil
}
func (m *mockMultiAZSpotFetcher) GetSpotPlacementScores(_ context.Context, _ *ec2.GetSpotPlacementScoresInput, _ ...func(*ec2.Options)) (*ec2.GetSpotPlacementScoresOutput, error) {
	return &ec2.GetSpotPlacementScoresOutput{}, nil
}
