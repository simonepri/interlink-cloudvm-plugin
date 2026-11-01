// Tests for the transfer cost model: rate table rules, the AWS price book fallback, and config parsing.
package main

import (
	"strings"
	"testing"
)

// fakeTransferBook answers TransferPrice from a fixed map keyed by source, destination, and kind.
type fakeTransferBook map[string]float64

func (b fakeTransferBook) TransferPrice(src, dst string, kind TransferKind) (float64, bool) {
	p, ok := b[src+">"+dst+">"+string(kind)]
	return p, ok
}

func floatPtr(v float64) *float64 { return &v }

func testTransferCostConfig() TransferCostConfig {
	return TransferCostConfig{
		HomeProvider: "aws",
		HomeRegion:   "us-east-1",
		Rates: map[string]TransferRates{
			"aws": {
				SameRegionUSDPerGB: floatPtr(0),
				CrossRegion: []TransferRouteRate{
					{From: "us-east-1", To: "us-east-2", USDPerGB: 0.01},
					{From: "eu-*", To: "*", USDPerGB: 0.02},
				},
				Internet: []TransferRouteRate{{From: "eu-*", USDPerGB: 0.09}},
			},
			"gcp": {
				CrossRegion: []TransferRouteRate{{From: "europe-*", To: "europe-*", USDPerGB: 0.02}},
			},
		},
	}
}

func TestTransferCostModel_Rate(t *testing.T) {
	book := fakeTransferBook{
		"us-east-1>eu-south-2>cross_region": 0.02,
		"us-east-1>us-east-2>cross_region":  0.5, // the table's rule wins over the price list
		"us-east-1>internet>internet":       0.09,
	}
	m := NewTransferCostModel(testTransferCostConfig(), book)
	aws := func(r string) TransferLocation { return TransferLocation{Provider: "aws", Region: r} }
	gcp := func(r string) TransferLocation { return TransferLocation{Provider: "gcp", Region: r} }
	tests := []struct {
		name     string
		src, dst TransferLocation
		want     float64
		ok       bool
	}{
		{"same region from the table", aws("eu-south-2"), aws("eu-south-2"), 0, true},
		{"cross region rule", aws("eu-south-2"), aws("us-east-1"), 0.02, true},
		{"wildcard destination prices an unknown region", aws("eu-south-2"), TransferLocation{Provider: "aws"}, 0.02, true},
		{"table rule wins over the price list", aws("us-east-1"), aws("us-east-2"), 0.01, true},
		{"price list fills a route the table lacks", aws("us-east-1"), aws("eu-south-2"), 0.02, true},
		{"unknown destination without a wildcard rule", aws("us-east-1"), TransferLocation{Provider: "aws"}, 0, false},
		{"internet rule", aws("eu-central-2"), TransferLocation{}, 0.09, true},
		{"another cloud is the internet", aws("eu-central-2"), gcp("europe-west4"), 0.09, true},
		{"internet from the price list", aws("us-east-1"), TransferLocation{}, 0.09, true},
		{"gcp continent rule", gcp("europe-west4"), gcp("europe-west1"), 0.02, true},
		{"gcp route no rule covers", gcp("europe-west4"), gcp("us-central1"), 0, false},
		{"gcp same region without a rate", gcp("europe-west4"), gcp("europe-west4"), 0, false},
		{"gcp internet without a rate", gcp("europe-west4"), TransferLocation{}, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := m.Rate(tt.src, tt.dst)
			if ok != tt.ok || got != tt.want {
				t.Errorf("Rate(%v, %v) = %v, %v; want %v, %v", tt.src, tt.dst, got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestParseTransferCostConfig(t *testing.T) {
	cfg, err := ParseTransferCostConfig(`{"homeProvider":"aws","homeRegion":"us-east-1","rates":{"aws":{"sameRegionUSDPerGB":0,"crossRegion":[{"from":"eu-*","to":"*","usdPerGB":0.02}],"internet":[{"from":"eu-*","usdPerGB":0.09}]}}}`)
	if err != nil {
		t.Fatal(err)
	}
	m := NewTransferCostModel(cfg, nil)
	if home, ok := m.Home(); !ok || home.Key() != "aws/us-east-1" {
		t.Errorf("home = %v, %v", home, ok)
	}
	if rate, ok := m.Rate(TransferLocation{"aws", "eu-west-1"}, TransferLocation{"aws", "us-east-1"}); !ok || rate != 0.02 {
		t.Errorf("parsed cross-region rate = %v, %v", rate, ok)
	}
	if cfg, err := ParseTransferCostConfig(""); err != nil || cfg.Rates != nil {
		t.Errorf("empty config = %+v, %v", cfg, err)
	}
	for _, raw := range []string{
		`{"rates":{"aws":{"crossRegion":[{"from":"[","to":"*","usdPerGB":0.02}]}}}`,
		`{"rates":{"aws":{"internet":[{"from":"eu-*","usdPerGB":-1}]}}}`,
		`{"rates":{"aws":{"sameRegionUSDPerGB":-0.01}}}`,
		`{`,
	} {
		if _, err := ParseTransferCostConfig(raw); err == nil {
			t.Errorf("ParseTransferCostConfig(%s) accepted a bad config", raw)
		}
	}
}

func TestTransferCostModel_NoHome(t *testing.T) {
	m := NewTransferCostModel(TransferCostConfig{}, nil)
	if _, ok := m.Home(); ok {
		t.Error("an empty config has a home")
	}
	if key := m.HomeKey(); key != "" {
		t.Errorf("HomeKey = %q", key)
	}
	if !strings.Contains(TransferLocation{"aws", "eu-south-2"}.Key(), "/") {
		t.Error("location key has no separator")
	}
}
