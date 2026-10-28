// Tests for the AWS data transfer price book: Price List parsing, tiered internet prices, caching, and the gauge.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/pricing"
)

// fakeTransferProduct is one AWSDataTransfer product; each tier is a beginRange and a pricePerUnit map.
type fakeTransferProduct struct {
	transferType, from, to, toLocation string
	tiers                              []fakeTransferTier
}

type fakeTransferTier struct {
	begin string
	price map[string]string
}

func usd(price string) map[string]string { return map[string]string{"USD": price} }

func (p fakeTransferProduct) json(t *testing.T) string {
	t.Helper()
	dims := make(map[string]any, len(p.tiers))
	for i, tier := range p.tiers {
		dims["SKU.T."+strconv.Itoa(i)] = map[string]any{"beginRange": tier.begin, "endRange": "Inf", "unit": "GB", "pricePerUnit": tier.price}
	}
	raw, err := json.Marshal(map[string]any{
		"product": map[string]any{"attributes": map[string]string{
			"transferType": p.transferType, "fromRegionCode": p.from, "toRegionCode": p.to, "toLocation": p.toLocation,
		}},
		"terms": map[string]any{"OnDemand": map[string]any{"SKU.T": map[string]any{"priceDimensions": dims}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// fakeTransferPricing serves products matching the transferType and fromRegionCode filters, pageSize per page.
type fakeTransferPricing struct {
	t        *testing.T
	products []fakeTransferProduct
	pageSize int

	mu    sync.Mutex
	calls int
	err   error
}

func (f *fakeTransferPricing) GetProducts(_ context.Context, in *pricing.GetProductsInput, _ ...func(*pricing.Options)) (*pricing.GetProductsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if aws.ToString(in.ServiceCode) != "AWSDataTransfer" {
		f.t.Errorf("ServiceCode = %q, want AWSDataTransfer", aws.ToString(in.ServiceCode))
	}
	filters := make(map[string]string)
	for _, fl := range in.Filters {
		filters[aws.ToString(fl.Field)] = aws.ToString(fl.Value)
	}
	var matched []string
	for _, p := range f.products {
		if p.transferType == filters["transferType"] && p.from == filters["fromRegionCode"] {
			matched = append(matched, p.json(f.t))
		}
	}
	start := 0
	if in.NextToken != nil {
		start, _ = strconv.Atoi(*in.NextToken)
	}
	size := f.pageSize
	if size <= 0 {
		size = len(matched) + 1
	}
	end := min(start+size, len(matched))
	out := &pricing.GetProductsOutput{PriceList: matched[start:end]}
	if end < len(matched) {
		out.NextToken = aws.String(strconv.Itoa(end))
	}
	return out, nil
}

func (f *fakeTransferPricing) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func internetTiers() []fakeTransferTier {
	// Listed out of order, with a free allowance, as the Price List API does not sort dimensions.
	return []fakeTransferTier{
		{begin: "10240", price: usd("0.0850000000")},
		{begin: "0", price: usd("0.0000000000")},
		{begin: "153600", price: usd("0.0500000000")},
		{begin: "100", price: usd("0.0900000000")},
	}
}

func twoRegionProducts() []fakeTransferProduct {
	return []fakeTransferProduct{
		{transferType: "InterRegion Outbound", from: "us-east-1", to: "us-east-2", tiers: []fakeTransferTier{{begin: "0", price: usd("0.0100000000")}}},
		{transferType: "InterRegion Outbound", from: "us-east-1", to: "eu-west-1", tiers: []fakeTransferTier{{begin: "0", price: usd("0.0200000000")}}},
		{transferType: "InterRegion Outbound", from: "us-east-1", to: "us-east-1-bos-1", tiers: []fakeTransferTier{{begin: "0", price: usd("0.0300000000")}}},
		{transferType: "InterRegion Outbound", from: "us-east-2", to: "us-east-1", tiers: []fakeTransferTier{{begin: "0", price: usd("0.0100000000")}}},
		{transferType: "IntraRegion", from: "us-east-1", to: "us-east-1", tiers: []fakeTransferTier{{begin: "0", price: usd("0.0100000000")}}},
		{transferType: "IntraRegion", from: "us-east-2", to: "us-east-2", tiers: []fakeTransferTier{{begin: "0", price: usd("0.0100000000")}}},
		{transferType: "AWS Outbound", from: "us-east-1", toLocation: "External", tiers: internetTiers()},
		{transferType: "AWS Outbound", from: "us-east-1", toLocation: "External-5G-Carrier", tiers: []fakeTransferTier{{begin: "0", price: usd("0.5000000000")}}},
		{transferType: "AWS Outbound", from: "us-east-2", toLocation: "External", tiers: []fakeTransferTier{{begin: "0", price: usd("0.0900000000")}}},
	}
}

func TestTransferPriceBook_ReadsRoutesBetweenLaunchRegions(t *testing.T) {
	client := &fakeTransferPricing{t: t, products: twoRegionProducts(), pageSize: 1}
	book := NewTransferPriceBook(client, "us-east-1", "us-east-2")
	book.Refresh(context.Background())

	want := []TransferRoutePrice{
		{Source: "us-east-1", Destination: "us-east-2", Kind: TransferCrossRegion, USDPerGB: 0.01},
		{Source: "us-east-1", Destination: "us-east-1", Kind: TransferCrossZone, USDPerGB: 0.01},
		{Source: "us-east-1", Destination: "internet", Kind: TransferInternet, USDPerGB: 0.09},
		{Source: "us-east-2", Destination: "us-east-1", Kind: TransferCrossRegion, USDPerGB: 0.01},
		{Source: "us-east-2", Destination: "us-east-2", Kind: TransferCrossZone, USDPerGB: 0.01},
		{Source: "us-east-2", Destination: "internet", Kind: TransferInternet, USDPerGB: 0.09},
	}
	got := book.Prices()
	if len(got) != len(want) {
		t.Fatalf("Prices() = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Prices()[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
	if _, ok := book.TransferPrice("us-east-1", "eu-west-1", TransferCrossRegion); ok {
		t.Error("priced a destination outside the launch regions")
	}
	if price, ok := book.TransferPrice("us-east-1", "us-east-2", TransferCrossRegion); !ok || price != 0.01 {
		t.Errorf("TransferPrice(us-east-1, us-east-2) = %v, %v, want 0.01, true", price, ok)
	}
}

func TestTransferPriceBook_SkipsDimensionsWithoutUSD(t *testing.T) {
	client := &fakeTransferPricing{t: t, products: []fakeTransferProduct{
		{transferType: "IntraRegion", from: "cn-north-1", to: "cn-north-1", tiers: []fakeTransferTier{{begin: "0", price: map[string]string{"CNY": "0.0670000000"}}}},
		{transferType: "InterRegion Outbound", from: "cn-north-1", to: "us-east-1", tiers: []fakeTransferTier{
			{begin: "0", price: map[string]string{"CNY": "0.6000000000"}},
			{begin: "0", price: usd("0.0900000000")},
		}},
	}}
	book := NewTransferPriceBook(client, "cn-north-1", "us-east-1")
	book.Refresh(context.Background())

	if price, ok := book.TransferPrice("cn-north-1", "cn-north-1", TransferCrossZone); ok {
		t.Errorf("TransferPrice = %v, want no price for a product priced only in CNY", price)
	}
	if price, ok := book.TransferPrice("cn-north-1", "us-east-1", TransferCrossRegion); !ok || price != 0.09 {
		t.Errorf("TransferPrice = %v, %v, want the USD dimension 0.09", price, ok)
	}
}

func TestFirstPaidTierUSD(t *testing.T) {
	cases := []struct {
		name  string
		dims  []awsPriceDimension
		price float64
		ok    bool
	}{
		{"skips free allowance and sorts tiers", []awsPriceDimension{
			{BeginRange: "10240", PricePerUnit: usd("0.085")}, {BeginRange: "0", PricePerUnit: usd("0")}, {BeginRange: "100", PricePerUnit: usd("0.09")},
		}, 0.09, true},
		{"first tier already paid", []awsPriceDimension{{BeginRange: "51200", PricePerUnit: usd("0.07")}, {BeginRange: "0", PricePerUnit: usd("0.114")}}, 0.114, true},
		{"every tier free", []awsPriceDimension{{BeginRange: "0", PricePerUnit: usd("0")}}, 0, true},
		{"no USD", []awsPriceDimension{{BeginRange: "0", PricePerUnit: map[string]string{"CNY": "0.9"}}}, 0, false},
		{"no dimensions", nil, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			price, ok := firstPaidTierUSD(tc.dims)
			if price != tc.price || ok != tc.ok {
				t.Errorf("firstPaidTierUSD = %v, %v, want %v, %v", price, ok, tc.price, tc.ok)
			}
		})
	}
}

func TestTransferPriceBook_CachesAndKeepsPricesOnFailure(t *testing.T) {
	client := &fakeTransferPricing{t: t, products: twoRegionProducts()}
	book := NewTransferPriceBook(client, "us-east-1")
	now := time.Now()
	book.now = func() time.Time { return now }

	book.Refresh(context.Background())
	calls := client.callCount()
	if calls != len(awsTransferTypes) {
		t.Fatalf("GetProducts called %d times, want one per transfer type", calls)
	}
	book.Refresh(context.Background())
	if client.callCount() != calls {
		t.Fatal("refreshed prices before they expired")
	}

	now = now.Add(onDemandPriceTTL)
	client.err = errors.New("throttled")
	book.Refresh(context.Background())
	if price, ok := book.TransferPrice("us-east-1", "internet", TransferInternet); !ok || price != 0.09 {
		t.Errorf("TransferPrice after a failed refresh = %v, %v, want the previous 0.09", price, ok)
	}

	calls = client.callCount()
	now = now.Add(onDemandPriceRetry - time.Minute)
	book.Refresh(context.Background())
	if client.callCount() != calls {
		t.Error("retried a failed refresh before the retry interval")
	}
	now = now.Add(time.Minute)
	book.Refresh(context.Background())
	if client.callCount() == calls {
		t.Error("did not retry a failed refresh after the retry interval")
	}
}

func TestTransferPriceBook_LookupMissRefreshesInBackground(t *testing.T) {
	client := &fakeTransferPricing{t: t, products: twoRegionProducts()}
	book := NewTransferPriceBook(client, "us-east-1")

	book.TransferPrice("us-east-1", "us-east-1", TransferCrossZone)
	book.Refresh(context.Background()) // waits for the background refresh the miss started
	if client.callCount() != len(awsTransferTypes) {
		t.Errorf("GetProducts called %d times, want one refresh", client.callCount())
	}
	if price, ok := book.TransferPrice("us-east-1", "us-east-1", TransferCrossZone); !ok || price != 0.01 {
		t.Errorf("TransferPrice = %v, %v, want 0.01, true", price, ok)
	}
	if _, ok := book.TransferPrice("eu-west-1", "eu-west-1", TransferCrossZone); ok || client.callCount() != len(awsTransferTypes) {
		t.Error("looked up a source outside the launch regions")
	}
}

func TestSpotPriceTicker_ExportsTransferPrices(t *testing.T) {
	metrics := NewObservabilityMetrics()
	ticker := NewSpotPriceTicker([]string{"us-east-1"}, []string{"p5.48xlarge"}, time.Minute)
	ticker.SetMetrics(metrics)
	ticker.SetFetcherFactory(func(_ context.Context, _ string) (SpotPriceFetcher, error) { return &mockEC2TickerFetcher{}, nil })
	ticker.SetPricingAPI(&fakeTransferPricing{t: t, products: twoRegionProducts()})
	ticker.TransferPrices().SetRegions([]string{"us-east-1", "us-east-2"})
	ticker.SetAdvisorCollector(NewSpotAdvisorCollector("", &mockHTTPClient{err: errors.New("offline")}))

	ticker.syncMarketMetrics(context.Background())

	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, line := range []string{
		`cloudvm_transfer_price_usd_per_gb{destination_region="us-east-2",provider="aws",source_region="us-east-1",transfer_kind="cross_region"} 0.01`,
		`cloudvm_transfer_price_usd_per_gb{destination_region="us-east-1",provider="aws",source_region="us-east-1",transfer_kind="cross_zone"} 0.01`,
		`cloudvm_transfer_price_usd_per_gb{destination_region="internet",provider="aws",source_region="us-east-1",transfer_kind="internet"} 0.09`,
		`cloudvm_transfer_price_usd_per_gb{destination_region="us-east-1",provider="aws",source_region="us-east-2",transfer_kind="cross_region"} 0.01`,
	} {
		if !strings.Contains(body, line) {
			t.Errorf("missing %s in:\n%s", line, body)
		}
	}
	if got := strings.Count(body, "\ncloudvm_transfer_price_usd_per_gb{"); got != 6 {
		t.Errorf("exported %d transfer prices, want 6", got)
	}
	if price, ok := ticker.TransferPrice("us-east-2", "internet", TransferInternet); !ok || price != 0.09 {
		t.Errorf("ticker TransferPrice = %v, %v, want 0.09, true", price, ok)
	}
}
