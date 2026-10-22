package main

import (
	"testing"
)

func TestGCPCapacityForMachineType(t *testing.T) {
	fallback, _ := NewResourceVector("2", "4Gi", 0, 0)

	// Test A2 highgpu-1g
	vec, ok := GCPCapacityForMachineType("a2-highgpu-1g", fallback)
	if !ok {
		t.Fatalf("expected a2-highgpu-1g to be recognized")
	}
	if vec.GPUs != 1 {
		t.Errorf("expected 1 GPU, got %d", vec.GPUs)
	}
	if vec.CPU.String() != "12" {
		t.Errorf("expected 12 vCPUs, got %s", vec.CPU.String())
	}

	// Test G2 standard-4
	vec, ok = GCPCapacityForMachineType("g2-standard-4", fallback)
	if !ok {
		t.Fatalf("expected g2-standard-4 to be recognized")
	}
	if vec.GPUs != 1 {
		t.Errorf("expected 1 GPU, got %d", vec.GPUs)
	}

	// Test Cloud TPU v4-8
	vec, ok = GCPCapacityForMachineType("v4-8", fallback)
	if !ok {
		t.Fatalf("expected v4-8 to be recognized")
	}
	if vec.TPUs != 8 {
		t.Errorf("expected 8 TPUs, got %d", vec.TPUs)
	}

	// Unknown machine type falls back
	vec, ok = GCPCapacityForMachineType("custom-unknown", fallback)
	if ok {
		t.Errorf("expected unknown machine type not to be recognized")
	}
	if vec.CPU.String() != "2" {
		t.Errorf("expected fallback 2 CPU, got %s", vec.CPU.String())
	}
}
