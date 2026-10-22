// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"bytes"
	"context"
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
	expected := "cloudvm_spot_price_usd_per_hour{region=\"us-east-1\",instance_type=\"g5.xlarge\"} 0.3018"
	if !bytes.Contains(rec.Body.Bytes(), []byte(expected)) {
		t.Errorf("metrics response missing expected gauge: %s", rec.Body.String())
	}
}
