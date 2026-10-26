// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// spotOffer returns an 8-GPU, 80 GB spot offer, the shape of every probed tier in these tests.
func spotOffer(region, itype, model string, vcpus int, pricePerGPU float64) *MarketOffer {
	return &MarketOffer{
		Provider: "aws", Region: region, InstanceType: itype, GPUModel: model, GPUMemoryGB: 80,
		GPUCount: 8, VCPUCount: vcpus, CapacityType: "spot",
		PricePerHour: pricePerGPU * 8, PricePerGPUHour: pricePerGPU,
	}
}

func onDemandOffer(region, itype, model string, vcpus int, pricePerGPU float64) *MarketOffer {
	o := spotOffer(region, itype, model, vcpus, pricePerGPU)
	o.CapacityType = "on-demand"
	return o
}

func TestFillSolver_SpotUpToScoredCapacityThenOnDemand(t *testing.T) {
	solver := NewFillSolver()
	solver.SetOffers([]*MarketOffer{
		spotOffer("us-east-1", "p5.48xlarge", "H100 80GB", 192, 2.0),
		onDemandOffer("us-west-2", "p5.48xlarge", "H100 80GB", 192, 6.0),
	})
	solver.SetLimits(FillLimits{SpotScores: map[string]map[int]float64{
		// Spot is likely up to 64 GPUs and unlikely at 256, so the plan counts on 64.
		spotScoreKey("aws", "us-east-1", 80): {8: 9, 64: 5, 256: 2},
	}})

	plan := solver.plan(80, 128)

	if plan.PlannedGPUs != 128 || plan.LimitedReason != "" {
		t.Fatalf("planned %d GPUs (limited %q), want 128 unlimited", plan.PlannedGPUs, plan.LimitedReason)
	}
	got := map[string]int{}
	for _, a := range plan.Allocations {
		got[a.Region+"/"+a.CapacityType] = a.GPUs
	}
	if got["us-east-1/spot"] != 64 || got["us-west-2/on-demand"] != 64 {
		t.Errorf("allocations = %v, want 64 spot in us-east-1 and 64 on-demand in us-west-2", got)
	}
	// 64 spot GPUs at score 5 (the 64-GPU probe) count as 32; on-demand counts in full.
	if plan.ExpectedGPUs != 96 {
		t.Errorf("expected GPUs = %v, want 96", plan.ExpectedGPUs)
	}
	if want := (64*2.0 + 64*6.0) / 128; math.Abs(plan.PricePerGPUHour-want) > 1e-9 {
		t.Errorf("price = %v, want %v", plan.PricePerGPUHour, want)
	}
}

func TestFillSolver_NoSpotWithoutPlacementScores(t *testing.T) {
	solver := NewFillSolver()
	solver.SetOffers([]*MarketOffer{spotOffer("us-east-1", "p5.48xlarge", "H100 80GB", 192, 2.0)})

	plan := solver.plan(80, 8)

	if plan.PlannedGPUs != 0 || plan.LimitedReason != "capacity" {
		t.Errorf("planned %d GPUs (limited %q), want 0 limited by capacity", plan.PlannedGPUs, plan.LimitedReason)
	}
}

func TestFillSolver_QuotaCapsWholeInstances(t *testing.T) {
	solver := NewFillSolver()
	solver.SetOffers([]*MarketOffer{onDemandOffer("us-east-1", "p5.48xlarge", "H100 80GB", 192, 6.0)})
	// 400 vCPUs of P on-demand quota fit two 192-vCPU instances.
	solver.SetLimits(FillLimits{QuotaVCPUs: map[string]float64{quotaKey("aws", "us-east-1", "p_ondemand"): 400}})

	plan := solver.plan(80, 64)

	if plan.PlannedGPUs != 16 || plan.LimitedReason != "quota" {
		t.Errorf("planned %d GPUs (limited %q), want 16 limited by quota", plan.PlannedGPUs, plan.LimitedReason)
	}
}

func TestFillSolver_QuotaIsSharedAcrossOffers(t *testing.T) {
	solver := NewFillSolver()
	solver.SetOffers([]*MarketOffer{
		onDemandOffer("us-east-1", "p5.48xlarge", "H100 80GB", 192, 6.0),
		onDemandOffer("us-east-1", "p4de.24xlarge", "A100 80GB", 96, 7.0),
	})
	solver.SetLimits(FillLimits{QuotaVCPUs: map[string]float64{quotaKey("aws", "us-east-1", "p_ondemand"): 288}})

	plan := solver.plan(80, 64)

	// One p5 (192 vCPUs) leaves 96 vCPUs, enough for one p4de.
	if plan.PlannedGPUs != 16 {
		t.Errorf("planned %d GPUs, want 16 from one p5 and one p4de sharing the P quota", plan.PlannedGPUs)
	}
}

func TestFillSolver_SolveAllExportsPlans(t *testing.T) {
	solver := NewFillSolver()
	solver.SetLadderSizes([]int{8, 1024})
	solver.SetMemoryTiers([]int{80})
	solver.SetOffers([]*MarketOffer{spotOffer("us-east-1", "p5.48xlarge", "H100 80GB", 192, 2.0)})
	solver.SetLimits(FillLimits{SpotScores: map[string]map[int]float64{spotScoreKey("aws", "us-east-1", 80): {8: 8}}})
	metrics := NewObservabilityMetrics()

	solver.SolveAll(metrics)

	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`cloudvm_fill_price_usd_per_gpu_hour{gpus="8",min_gpu_memory_gb="80"} 2`,
		`cloudvm_fill_expected_gpus{gpus="8",min_gpu_memory_gb="80"} 6.4`,
		`cloudvm_fill_gpus{capacity_type="spot",gpu_model="H100 80GB",gpus="8",min_gpu_memory_gb="80",provider="aws",region="us-east-1"} 8`,
		`cloudvm_fill_planned_gpus{gpus="1024",min_gpu_memory_gb="80"} 8`,
		`cloudvm_fill_limited{gpus="1024",min_gpu_memory_gb="80",reason="capacity"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %s\n%s", want, body)
		}
	}
}
