// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"testing"
)

func TestFillSolver_CheapestFillAndRegionCap(t *testing.T) {
	solver := NewFillSolver()

	// 3 offers across 2 regions
	offers := []*MarketOffer{
		{
			Provider:        "aws",
			Region:          "us-west-2",
			Zone:            "us-west-2a",
			InstanceType:    "g5.xlarge",
			GPUModel:        "A10G 24GB",
			GPUMemoryGB:     24,
			GPUCount:        1,
			CapacityType:    "spot",
			PricePerHour:    0.40,
			PricePerGPUHour: 0.40,
			QuotaRemaining:  10,
		},
		{
			Provider:        "aws",
			Region:          "us-east-1",
			Zone:            "us-east-1a",
			InstanceType:    "g5.xlarge",
			GPUModel:        "A10G 24GB",
			GPUMemoryGB:     24,
			GPUCount:        1,
			CapacityType:    "spot",
			PricePerHour:    0.30, // Cheapest
			PricePerGPUHour: 0.30,
			QuotaRemaining:  4, // Limited to 4 GPUs in us-east-1
		},
		{
			Provider:        "gcp",
			Region:          "us-central1",
			Zone:            "",
			InstanceType:    "g2-standard-4",
			GPUModel:        "L4 24GB",
			GPUMemoryGB:     24,
			GPUCount:        1,
			CapacityType:    "spot",
			PricePerHour:    0.23, // Cheapest overall
			PricePerGPUHour: 0.23,
			QuotaRemaining:  64,
		},
	}
	solver.SetOffers(offers)

	// Test solving for 8 GPUs, minMem=24GB, maxRegions="1"
	res1 := solver.solveOne(24, 8, "1", "spot")
	if res1.TargetGPUs != 8 {
		t.Errorf("expected target 8 GPUs, got %d", res1.TargetGPUs)
	}
	// With maxRegions=1, cheapest single region that can fulfill 8 GPUs is us-central1 (64 quota @ 0.23)
	// (since us-east-1 is limited to 4 GPUs)
	if res1.FulfilledGPUs != 8 {
		t.Errorf("expected 8 fulfilled GPUs, got %d", res1.FulfilledGPUs)
	}
	if res1.BlendedPriceUSD != 0.23 {
		t.Errorf("expected blended price 0.23, got %f", res1.BlendedPriceUSD)
	}
	if res1.LimitedReason != "" {
		t.Errorf("expected not limited, got %s", res1.LimitedReason)
	}

	// Test solving for 1000 GPUs where total quota is 10 + 4 + 64 = 78
	resLimited := solver.solveOne(24, 1000, "unlimited", "spot")
	if resLimited.FulfilledGPUs != 78 {
		t.Errorf("expected 78 fulfilled GPUs, got %d", resLimited.FulfilledGPUs)
	}
	if resLimited.LimitedReason != "quota" {
		t.Errorf("expected quota limited reason, got %s", resLimited.LimitedReason)
	}

	// Test memory floor filtering: asking for ≥40GB should return 0 when only 24GB shapes exist
	res40GB := solver.solveOne(40, 8, "unlimited", "spot")
	if res40GB.FulfilledGPUs != 0 {
		t.Errorf("expected 0 fulfilled GPUs for 40GB floor, got %d", res40GB.FulfilledGPUs)
	}
	if res40GB.LimitedReason != "capacity" {
		t.Errorf("expected capacity limited reason, got %s", res40GB.LimitedReason)
	}
}

func TestFillSolver_SolveAllExportsMetrics(t *testing.T) {
	solver := NewFillSolver()
	solver.SetOffers([]*MarketOffer{
		{
			Provider:        "aws",
			Region:          "us-west-2",
			InstanceType:    "p4d.24xlarge",
			GPUModel:        "A100 40GB",
			GPUMemoryGB:     40,
			GPUCount:        8,
			CapacityType:    "spot",
			PricePerHour:    10.0,
			PricePerGPUHour: 1.25,
			QuotaRemaining:  64,
		},
	})

	metrics := NewObservabilityMetrics()
	results := solver.SolveAll(metrics)
	if len(results) == 0 {
		t.Fatalf("expected solved ladder results")
	}
}
