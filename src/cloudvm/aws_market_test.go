// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
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

func (m *mockServiceQuotasClient) GetServiceQuota(_ context.Context, params *servicequotas.GetServiceQuotaInput, _ ...func(*servicequotas.Options)) (*servicequotas.GetServiceQuotaOutput, error) {
	if m.err != nil {
		return nil, m.err
	}
	code := aws.ToString(params.QuotaCode)
	return &servicequotas.GetServiceQuotaOutput{
		Quota: &quotatypes.ServiceQuota{
			Value: aws.Float64(m.val),
			UsageMetric: &quotatypes.MetricInfo{
				MetricNamespace:  aws.String("AWS/Usage"),
				MetricName:       aws.String("ResourceCount"),
				MetricDimensions: map[string]string{"Class": code, "Resource": "vCPU", "Service": "EC2", "Type": "Resource"},
			},
		},
	}, nil
}

// mockUsageClient reports usedVCPUs for the quota whose usage metric has Class usedClass, and no datapoints for the others.
type mockUsageClient struct {
	usedClass string
	usedVCPUs float64
	err       error
}

func (m *mockUsageClient) GetMetricData(_ context.Context, params *cloudwatch.GetMetricDataInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error) {
	if m.err != nil {
		return nil, m.err
	}
	out := &cloudwatch.GetMetricDataOutput{}
	for _, q := range params.MetricDataQueries {
		result := cwtypes.MetricDataResult{Id: q.Id}
		for _, d := range q.MetricStat.Metric.Dimensions {
			if aws.ToString(d.Name) == "Class" && aws.ToString(d.Value) == m.usedClass {
				result.Values = []float64{m.usedVCPUs, 1}
			}
		}
		out.MetricDataResults = append(out.MetricDataResults, result)
	}
	return out, nil
}

func TestQueryAWSGPUQuotas(t *testing.T) {
	ctx := context.Background()
	quotas := &mockServiceQuotasClient{val: 768}
	metrics := NewObservabilityMetrics()

	// The P spot quota (L-7212CCBC) has 192 vCPUs in use; the other quotas have no recent usage datapoints.
	free := QueryAWSGPUQuotas(ctx, quotas, &mockUsageClient{usedClass: "L-7212CCBC", usedVCPUs: 192}, "us-west-2", metrics)

	if free["p_spot"] != 576 || free["g_vt_spot"] != 768 {
		t.Errorf("free vCPUs = %v, want 576 for p_spot and 768 for g_vt_spot", free)
	}
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	if !strings.Contains(body, `cloudvm_quota_used_gpus{accelerator="gpu",provider="aws",quota="p_spot",region="us-west-2"} 16`) {
		t.Errorf("P spot usage not exported as 192/12 = 16 GPUs:\n%s", body)
	}
}

func TestQueryAWSGPUQuotas_UnreadableUsageLeavesLimitFree(t *testing.T) {
	metrics := NewObservabilityMetrics()

	free := QueryAWSGPUQuotas(context.Background(), &mockServiceQuotasClient{val: 768}, &mockUsageClient{err: errors.New("AccessDenied")}, "us-west-2", metrics)

	if free["p_spot"] != 768 {
		t.Errorf("free p_spot = %v, want the full 768 when usage cannot be read", free["p_spot"])
	}
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if strings.Contains(rec.Body.String(), "cloudvm_quota_used_gpus{") {
		t.Errorf("exported quota usage although it could not be read")
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
