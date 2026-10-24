// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// ResourceVector wraps standard Kubernetes resource quantities for exact arithmetic and comparisons.
type ResourceVector struct {
	CPU         resource.Quantity `json:"cpu"`
	Memory      resource.Quantity `json:"memory"`
	GPUs        int               `json:"gpus"`
	TPUs        int               `json:"tpus"`
	GPUModel    string            `json:"gpuModel,omitempty"`
	GPUMemoryGB int               `json:"gpuMemoryGB,omitempty"`
}

// NewResourceVector parses human-readable quantities (e.g., "4", "16Gi") into exact Kubernetes quantities.
func NewResourceVector(cpuStr, memStr string, gpus, tpus int) (ResourceVector, error) {
	var cpu, mem resource.Quantity
	var err error

	if cpuStr != "" {
		cpu, err = resource.ParseQuantity(cpuStr)
		if err != nil {
			return ResourceVector{}, fmt.Errorf("invalid cpu quantity %q: %w", cpuStr, err)
		}
	}
	if memStr != "" {
		mem, err = resource.ParseQuantity(memStr)
		if err != nil {
			return ResourceVector{}, fmt.Errorf("invalid memory quantity %q: %w", memStr, err)
		}
	}

	return ResourceVector{
		CPU:    cpu,
		Memory: mem,
		GPUs:   gpus,
		TPUs:   tpus,
	}, nil
}

// ResourceVectorFromPod extracts cumulative resource requirements from a pod spec.
func ResourceVectorFromPod(pod *Pod) ResourceVector {
	var totalCPU, totalMem resource.Quantity
	gpus := 0
	tpus := 0

	for _, c := range pod.Spec.Containers {
		if c.Resources.Limits != nil {
			if q, err := resource.ParseQuantity(c.Resources.Limits[string(corev1.ResourceCPU)]); err == nil {
				totalCPU.Add(q)
			}
			if q, err := resource.ParseQuantity(c.Resources.Limits[string(corev1.ResourceMemory)]); err == nil {
				totalMem.Add(q)
			}
			if gStr, ok := c.Resources.Limits["nvidia.com/gpu"]; ok {
				if n, err := strconv.Atoi(gStr); err == nil {
					gpus += n
				}
			}
			if tStr, ok := c.Resources.Limits["google.com/tpu"]; ok {
				if n, err := strconv.Atoi(tStr); err == nil {
					tpus += n
				}
			}
		} else if c.Resources.Requests != nil {
			if q, err := resource.ParseQuantity(c.Resources.Requests[string(corev1.ResourceCPU)]); err == nil {
				totalCPU.Add(q)
			}
			if q, err := resource.ParseQuantity(c.Resources.Requests[string(corev1.ResourceMemory)]); err == nil {
				totalMem.Add(q)
			}
			if gStr, ok := c.Resources.Requests["nvidia.com/gpu"]; ok {
				if n, err := strconv.Atoi(gStr); err == nil {
					gpus += n
				}
			}
			if tStr, ok := c.Resources.Requests["google.com/tpu"]; ok {
				if n, err := strconv.Atoi(tStr); err == nil {
					tpus += n
				}
			}
		}
	}

	// Defaults if unspecified
	if totalCPU.IsZero() {
		totalCPU = resource.MustParse("2")
	}
	if totalMem.IsZero() {
		totalMem = resource.MustParse("4Gi")
	}

	return ResourceVector{
		CPU:    totalCPU,
		Memory: totalMem,
		GPUs:   gpus,
		TPUs:   tpus,
	}
}

// Add returns the vector sum of two resource allocations.
func (r ResourceVector) Add(other ResourceVector) ResourceVector {
	cpu := r.CPU.DeepCopy()
	cpu.Add(other.CPU)
	mem := r.Memory.DeepCopy()
	mem.Add(other.Memory)
	gpuModel := r.GPUModel
	if gpuModel == "" {
		gpuModel = other.GPUModel
	}
	gpuMem := r.GPUMemoryGB
	if gpuMem == 0 {
		gpuMem = other.GPUMemoryGB
	}
	return ResourceVector{
		CPU:         cpu,
		Memory:      mem,
		GPUs:        r.GPUs + other.GPUs,
		TPUs:        r.TPUs + other.TPUs,
		GPUModel:    gpuModel,
		GPUMemoryGB: gpuMem,
	}
}

// Sub returns the vector difference (r - other).
func (r ResourceVector) Sub(other ResourceVector) ResourceVector {
	cpu := r.CPU.DeepCopy()
	cpu.Sub(other.CPU)
	mem := r.Memory.DeepCopy()
	mem.Sub(other.Memory)
	gpus := r.GPUs - other.GPUs
	if gpus < 0 {
		gpus = 0
	}
	tpus := r.TPUs - other.TPUs
	if tpus < 0 {
		tpus = 0
	}
	return ResourceVector{
		CPU:         cpu,
		Memory:      mem,
		GPUs:        gpus,
		TPUs:        tpus,
		GPUModel:    r.GPUModel,
		GPUMemoryGB: r.GPUMemoryGB,
	}
}

// Fits returns true if the request does not exceed capacity in any dimension.
func (r ResourceVector) Fits(capacity ResourceVector) bool {
	if r.CPU.Cmp(capacity.CPU) > 0 {
		return false
	}
	if r.Memory.Cmp(capacity.Memory) > 0 {
		return false
	}
	if r.GPUs > capacity.GPUs {
		return false
	}
	if r.TPUs > capacity.TPUs {
		return false
	}
	return true
}

func (r ResourceVector) String() string {
	if r.GPUModel != "" {
		return fmt.Sprintf("CPU=%s, Mem=%s, GPUs=%d (%s), TPUs=%d", r.CPU.String(), r.Memory.String(), r.GPUs, r.GPUModel, r.TPUs)
	}
	return fmt.Sprintf("CPU=%s, Mem=%s, GPUs=%d, TPUs=%d", r.CPU.String(), r.Memory.String(), r.GPUs, r.TPUs)
}

// CapacityForInstanceType returns the hardware capacity vector (CPU, Memory, GPUs, TPUs) for a given cloud instance type.
func CapacityForInstanceType(itype string, fallback ResourceVector) ResourceVector {
	if gcpVec, ok := GCPCapacityForMachineType(itype, fallback); ok {
		return gcpVec
	}
	gpus := GPUCountForInstanceType(itype)
	var cpuStr, memStr string

	switch {
	case strings.HasSuffix(itype, ".48xlarge") || strings.HasSuffix(itype, ".metal"):
		cpuStr, memStr = "192", "768Gi"
	case strings.HasSuffix(itype, ".24xlarge"):
		cpuStr, memStr = "96", "384Gi"
	case strings.HasSuffix(itype, ".16xlarge"):
		cpuStr, memStr = "64", "256Gi"
	case strings.HasSuffix(itype, ".12xlarge"):
		cpuStr, memStr = "48", "192Gi"
	case strings.HasSuffix(itype, ".8xlarge"):
		cpuStr, memStr = "32", "128Gi"
	case strings.HasSuffix(itype, ".4xlarge"):
		cpuStr, memStr = "16", "64Gi"
	case strings.HasSuffix(itype, ".2xlarge"):
		cpuStr, memStr = "8", "32Gi"
	case strings.HasSuffix(itype, ".xlarge"):
		cpuStr, memStr = "4", "16Gi"
	default:
		cpu := fallback.CPU.DeepCopy()
		mem := fallback.Memory.DeepCopy()
		if gpus < fallback.GPUs {
			gpus = fallback.GPUs
		}
		gpuModel := fallback.GPUModel
		gpuMem := fallback.GPUMemoryGB
		return ResourceVector{
			CPU:         cpu,
			Memory:      mem,
			GPUs:        gpus,
			TPUs:        fallback.TPUs,
			GPUModel:    gpuModel,
			GPUMemoryGB: gpuMem,
		}
	}

	cpu := resource.MustParse(cpuStr)
	if fallback.CPU.Cmp(cpu) > 0 {
		cpu = fallback.CPU.DeepCopy()
	}
	mem := resource.MustParse(memStr)
	if fallback.Memory.Cmp(mem) > 0 {
		mem = fallback.Memory.DeepCopy()
	}
	if gpus < fallback.GPUs {
		gpus = fallback.GPUs
	}

	var gpuModel string
	var gpuMemGB int
	switch {
	case strings.HasPrefix(itype, "g5."):
		gpuModel = "A10G 24GB"
		gpuMemGB = 24
	case strings.HasPrefix(itype, "g6."):
		gpuModel = "L4 24GB"
		gpuMemGB = 24
	case strings.HasPrefix(itype, "g6e."):
		gpuModel = "L40S 48GB"
		gpuMemGB = 48
	case strings.HasPrefix(itype, "gr6."):
		gpuModel = "Radeon PRO V620 32GB"
		gpuMemGB = 32
	case strings.HasPrefix(itype, "p4d."):
		gpuModel = "A100 40GB"
		gpuMemGB = 40
	case strings.HasPrefix(itype, "p4de."):
		gpuModel = "A100 80GB"
		gpuMemGB = 80
	case strings.HasPrefix(itype, "p5."):
		gpuModel = "H100 80GB"
		gpuMemGB = 80
	case strings.HasPrefix(itype, "p5e."):
		gpuModel = "H200 141GB"
		gpuMemGB = 141
	case strings.HasPrefix(itype, "p3.") || strings.HasPrefix(itype, "p3dn."):
		if strings.HasPrefix(itype, "p3dn.") {
			gpuModel = "V100 32GB"
			gpuMemGB = 32
		} else {
			gpuModel = "V100 16GB"
			gpuMemGB = 16
		}
	case strings.HasPrefix(itype, "g4dn."):
		gpuModel = "T4 16GB"
		gpuMemGB = 16
	}
	if gpuModel == "" && fallback.GPUModel != "" {
		gpuModel = fallback.GPUModel
		gpuMemGB = fallback.GPUMemoryGB
	}

	return ResourceVector{
		CPU:         cpu,
		Memory:      mem,
		GPUs:        gpus,
		TPUs:        fallback.TPUs,
		GPUModel:    gpuModel,
		GPUMemoryGB: gpuMemGB,
	}
}
