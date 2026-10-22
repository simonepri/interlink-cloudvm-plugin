// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// ResourceVector wraps standard Kubernetes resource quantities for exact arithmetic and comparisons.
type ResourceVector struct {
	CPU    resource.Quantity `json:"cpu"`
	Memory resource.Quantity `json:"memory"`
	GPUs   int               `json:"gpus"`
	TPUs   int               `json:"tpus"`
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
	return ResourceVector{
		CPU:    cpu,
		Memory: mem,
		GPUs:   r.GPUs + other.GPUs,
		TPUs:   r.TPUs + other.TPUs,
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
		CPU:    cpu,
		Memory: mem,
		GPUs:   gpus,
		TPUs:   tpus,
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
	return fmt.Sprintf("CPU=%s, Mem=%s, GPUs=%d, TPUs=%d", r.CPU.String(), r.Memory.String(), r.GPUs, r.TPUs)
}
