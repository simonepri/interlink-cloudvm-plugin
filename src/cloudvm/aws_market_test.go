// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/pricing"
	"github.com/aws/aws-sdk-go-v2/service/servicequotas"
	quotatypes "github.com/aws/aws-sdk-go-v2/service/servicequotas/types"
)

type mockPricingClient struct {
	priceOutput *pricing.GetProductsOutput
	err         error
}

func (m *mockPricingClient) GetProducts(_ context.Context, _ *pricing.GetProductsInput, _ ...func(*pricing.Options)) (*pricing.GetProductsOutput, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.priceOutput, nil
}

func TestGetAWSOnDemandPrice_MockAndFallback(t *testing.T) {
	ctx := context.Background()

	// 1. Successful API call parsing priceDimensions
	samplePriceJSON := `{
		"terms": {
			"OnDemand": {
				"XYZ": {
					"priceDimensions": {
						"XYZ.123": {
							"pricePerUnit": {"USD": "1.0060000000"}
						}
					}
				}
			}
		}
	}`

	mockClient := &mockPricingClient{
		priceOutput: &pricing.GetProductsOutput{
			PriceList: []string{samplePriceJSON},
		},
	}

	price := GetAWSOnDemandPrice(ctx, mockClient, "us-west-2", "g5.xlarge")
	if price != 1.006 {
		t.Errorf("expected parsed price 1.006, got %f", price)
	}

	// 2. Error returns fallback baseline
	errClient := &mockPricingClient{err: fmt.Errorf("pricing service rate exceeded")}
	fallbackPrice := GetAWSOnDemandPrice(ctx, errClient, "us-west-2", "g5.xlarge")
	if fallbackPrice != 1.006 {
		t.Errorf("expected fallback baseline 1.006, got %f", fallbackPrice)
	}
}

type mockHTTPClient struct {
	resp *http.Response
	err  error
}

func (m *mockHTTPClient) Get(_ string) (*http.Response, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.resp, nil
}

func TestSpotAdvisorCollector_PollAndMapUptime(t *testing.T) {
	ctx := context.Background()
	sampleJSON := `{
		"spot_advisor": {
			"us-west-2": {
				"g5.xlarge": {"s": 60, "r": 1},
				"g6.xlarge": {"s": 70, "r": 0},
				"p4d.24xlarge": {"s": 45, "r": 4}
			}
		}
	}`

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(sampleJSON)),
		Header:     http.Header{"Last-Modified": []string{"Tue, 24 Oct 2026 12:00:00 GMT"}},
	}

	collector := NewSpotAdvisorCollector("http://example.com/feed.json", &mockHTTPClient{resp: resp})
	if err := collector.Poll(ctx); err != nil {
		t.Fatalf("unexpected poll error: %v", err)
	}

	entry, ok := collector.GetEntry("us-west-2", "g5.xlarge")
	if !ok {
		t.Fatalf("expected entry for us-west-2 g5.xlarge")
	}
	if entry.SavingsPct != 60 || entry.RangeIndex != 1 {
		t.Errorf("expected s=60 r=1, got %+v", entry)
	}

	uptime := UptimeSecondsForAdvisorRange(entry.RangeIndex)
	if uptime != 50400 {
		t.Errorf("expected 50400s uptime, got %f", uptime)
	}

	if collector.LastModified().IsZero() {
		t.Errorf("expected non-zero LastModified")
	}
}

func TestSpotAdvisorCollector_PollAndMapUptime_NestedOS(t *testing.T) {
	ctx := context.Background()
	sampleJSON := `{
		"spot_advisor": {
			"us-west-2": {
				"Linux": {
					"g5.xlarge": {"s": 46, "r": 4},
					"g6.xlarge": {"s": 41, "r": 0}
				},
				"Windows": {
					"g5.xlarge": {"s": 30, "r": 4}
				}
			}
		}
	}`

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(sampleJSON)),
		Header:     http.Header{"Last-Modified": []string{"Tue, 24 Oct 2026 12:00:00 GMT"}},
	}

	collector := NewSpotAdvisorCollector("http://example.com/feed.json", &mockHTTPClient{resp: resp})
	if err := collector.Poll(ctx); err != nil {
		t.Fatalf("unexpected poll error: %v", err)
	}

	entry, ok := collector.GetEntry("us-west-2", "g5.xlarge")
	if !ok {
		t.Fatalf("expected entry for us-west-2 g5.xlarge")
	}
	if entry.SavingsPct != 46 || entry.RangeIndex != 4 {
		t.Errorf("expected s=46 r=4, got %+v", entry)
	}

	uptime := UptimeSecondsForAdvisorRange(entry.RangeIndex)
	if uptime != 10800 {
		t.Errorf("expected 10800s uptime, got %f", uptime)
	}

	entryG6, ok := collector.GetEntry("us-west-2", "g6.xlarge")
	if !ok {
		t.Fatalf("expected entry for us-west-2 g6.xlarge")
	}
	if entryG6.SavingsPct != 41 || entryG6.RangeIndex != 0 {
		t.Errorf("expected s=41 r=0, got %+v", entryG6)
	}
	uptimeG6 := UptimeSecondsForAdvisorRange(entryG6.RangeIndex)
	if uptimeG6 != 86400 {
		t.Errorf("expected 86400s uptime, got %f", uptimeG6)
	}
}

type mockServiceQuotasClient struct {
	val float64
	err error
}

func (m *mockServiceQuotasClient) GetServiceQuota(_ context.Context, _ *servicequotas.GetServiceQuotaInput, _ ...func(*servicequotas.Options)) (*servicequotas.GetServiceQuotaOutput, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &servicequotas.GetServiceQuotaOutput{
		Quota: &quotatypes.ServiceQuota{
			Value: aws.Float64(m.val),
		},
	}, nil
}

func TestQueryAWSGPUQuotas(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockServiceQuotasClient{val: 64.0}
	metrics := NewObservabilityMetrics()

	QueryAWSGPUQuotas(ctx, mockClient, "us-west-2", metrics)

	val, err := QueryAWSQuota(ctx, mockClient, "L-3819A6DF")
	if err != nil || val != 64.0 {
		t.Errorf("expected quota 64.0, got %f (err: %v)", val, err)
	}
}

type mockEC2ScoresClient struct{}

func (m *mockEC2ScoresClient) GetSpotPlacementScores(_ context.Context, _ *ec2.GetSpotPlacementScoresInput, _ ...func(*ec2.Options)) (*ec2.GetSpotPlacementScoresOutput, error) {
	score := int32(8)
	return &ec2.GetSpotPlacementScoresOutput{
		SpotPlacementScores: []ec2types.SpotPlacementScore{
			{
				Region:             aws.String("us-west-2"),
				AvailabilityZoneId: aws.String("usw2-az1"),
				Score:              &score,
			},
		},
	}, nil
}

func TestQueryAWSPlacementScores(t *testing.T) {
	ctx := context.Background()
	client := &mockEC2ScoresClient{}

	scores, err := QueryAWSPlacementScores(ctx, client, "us-west-2", []string{"g5.xlarge"}, 8, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if score, ok := scores["usw2-az1"]; !ok || score != 0.8 {
		t.Errorf("expected normalized score 0.8 for usw2-az1, got %f (map: %+v)", score, scores)
	}
}
