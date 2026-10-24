// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
)

// GCPHardwareProfile describes standard resource specifications for Google Cloud machine types.
type GCPHardwareProfile struct {
	VCPU        string
	Memory      string
	GPUs        int
	TPUs        int
	GPUModel    string
	GPUMemoryGB int
}

// KnownGCPHardwareProfiles maps standard GCE GPU and Cloud TPU machine types to specifications.
var KnownGCPHardwareProfiles = map[string]GCPHardwareProfile{
	// NVIDIA A100 GPU instances (40GB)
	"a2-highgpu-1g":  {VCPU: "12", Memory: "85Gi", GPUs: 1, GPUModel: "A100 40GB", GPUMemoryGB: 40},
	"a2-highgpu-2g":  {VCPU: "24", Memory: "170Gi", GPUs: 2, GPUModel: "A100 40GB", GPUMemoryGB: 40},
	"a2-highgpu-4g":  {VCPU: "48", Memory: "340Gi", GPUs: 4, GPUModel: "A100 40GB", GPUMemoryGB: 40},
	"a2-highgpu-8g":  {VCPU: "96", Memory: "680Gi", GPUs: 8, GPUModel: "A100 40GB", GPUMemoryGB: 40},
	"a2-megagpu-16g": {VCPU: "96", Memory: "1360Gi", GPUs: 16, GPUModel: "A100 40GB", GPUMemoryGB: 40},

	// NVIDIA A100 GPU instances (80GB)
	"a2-ultragpu-1g": {VCPU: "12", Memory: "170Gi", GPUs: 1, GPUModel: "A100 80GB", GPUMemoryGB: 80},
	"a2-ultragpu-2g": {VCPU: "24", Memory: "340Gi", GPUs: 2, GPUModel: "A100 80GB", GPUMemoryGB: 80},
	"a2-ultragpu-4g": {VCPU: "48", Memory: "680Gi", GPUs: 4, GPUModel: "A100 80GB", GPUMemoryGB: 80},
	"a2-ultragpu-8g": {VCPU: "96", Memory: "1360Gi", GPUs: 8, GPUModel: "A100 80GB", GPUMemoryGB: 80},

	// NVIDIA H100 GPU instances (80GB)
	"a3-highgpu-8g": {VCPU: "208", Memory: "1872Gi", GPUs: 8, GPUModel: "H100 80GB", GPUMemoryGB: 80},
	"a3-megagpu-8g": {VCPU: "208", Memory: "1872Gi", GPUs: 8, GPUModel: "H100 80GB", GPUMemoryGB: 80},
	"a3-edgegpu-8g": {VCPU: "208", Memory: "1872Gi", GPUs: 8, GPUModel: "H100 80GB", GPUMemoryGB: 80},

	// NVIDIA L4 GPU instances (24GB)
	"g2-standard-4":  {VCPU: "4", Memory: "16Gi", GPUs: 1, GPUModel: "L4 24GB", GPUMemoryGB: 24},
	"g2-standard-8":  {VCPU: "8", Memory: "32Gi", GPUs: 1, GPUModel: "L4 24GB", GPUMemoryGB: 24},
	"g2-standard-12": {VCPU: "12", Memory: "48Gi", GPUs: 1, GPUModel: "L4 24GB", GPUMemoryGB: 24},
	"g2-standard-16": {VCPU: "16", Memory: "64Gi", GPUs: 1, GPUModel: "L4 24GB", GPUMemoryGB: 24},
	"g2-standard-24": {VCPU: "24", Memory: "96Gi", GPUs: 2, GPUModel: "L4 24GB", GPUMemoryGB: 24},
	"g2-standard-32": {VCPU: "32", Memory: "128Gi", GPUs: 1, GPUModel: "L4 24GB", GPUMemoryGB: 24},
	"g2-standard-48": {VCPU: "48", Memory: "192Gi", GPUs: 4, GPUModel: "L4 24GB", GPUMemoryGB: 24},
	"g2-standard-96": {VCPU: "96", Memory: "384Gi", GPUs: 8, GPUModel: "L4 24GB", GPUMemoryGB: 24},

	// NVIDIA T4 GPU instances (16GB)
	"n1-standard-4-1t4":  {VCPU: "4", Memory: "15Gi", GPUs: 1, GPUModel: "T4 16GB", GPUMemoryGB: 16},
	"n1-standard-8-1t4":  {VCPU: "8", Memory: "30Gi", GPUs: 1, GPUModel: "T4 16GB", GPUMemoryGB: 16},
	"n1-standard-16-2t4": {VCPU: "16", Memory: "60Gi", GPUs: 2, GPUModel: "T4 16GB", GPUMemoryGB: 16},
	"n1-standard-32-4t4": {VCPU: "32", Memory: "120Gi", GPUs: 4, GPUModel: "T4 16GB", GPUMemoryGB: 16},

	// Cloud TPU v4 slices
	"v4-8":  {VCPU: "64", Memory: "128Gi", TPUs: 8},
	"v4-16": {VCPU: "128", Memory: "256Gi", TPUs: 16},
	"v4-32": {VCPU: "256", Memory: "512Gi", TPUs: 32},

	// Cloud TPU v5e slices
	"v5litepod-4": {VCPU: "32", Memory: "64Gi", TPUs: 4},
	"v5litepod-8": {VCPU: "64", Memory: "128Gi", TPUs: 8},
	"v5e-4":       {VCPU: "32", Memory: "64Gi", TPUs: 4},
	"v5e-8":       {VCPU: "64", Memory: "128Gi", TPUs: 8},
}

// GCPCapacityForMachineType returns the resource vector for a GCE machine type.
func GCPCapacityForMachineType(mtype string, fallback ResourceVector) (ResourceVector, bool) {
	clean := strings.ToLower(strings.TrimSpace(mtype))
	if profile, ok := KnownGCPHardwareProfiles[clean]; ok {
		cpu := resource.MustParse(profile.VCPU)
		mem := resource.MustParse(profile.Memory)
		gpus := profile.GPUs
		tpus := profile.TPUs
		if fallback.CPU.Cmp(cpu) > 0 {
			cpu = fallback.CPU.DeepCopy()
		}
		if fallback.Memory.Cmp(mem) > 0 {
			mem = fallback.Memory.DeepCopy()
		}
		if fallback.GPUs > gpus {
			gpus = fallback.GPUs
		}
		if fallback.TPUs > tpus {
			tpus = fallback.TPUs
		}
		gpuModel := profile.GPUModel
		if gpuModel == "" && fallback.GPUModel != "" {
			gpuModel = fallback.GPUModel
		}
		gpuMem := profile.GPUMemoryGB
		if gpuMem == 0 && fallback.GPUMemoryGB != 0 {
			gpuMem = fallback.GPUMemoryGB
		}
		return ResourceVector{
			CPU:         cpu,
			Memory:      mem,
			GPUs:        gpus,
			TPUs:        tpus,
			GPUModel:    gpuModel,
			GPUMemoryGB: gpuMem,
		}, true
	}
	return fallback, false
}
