// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"k8s.io/apimachinery/pkg/api/resource"
)

// InstanceTypeDescriber abstracts EC2 DescribeInstanceTypes for caching and testing.
type InstanceTypeDescriber interface {
	DescribeInstanceTypes(ctx context.Context, params *ec2.DescribeInstanceTypesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error)
}

// HardwareCatalog caches dynamic hardware instance capacities discovered from cloud provider APIs.
type HardwareCatalog struct {
	mu    sync.RWMutex
	cache map[string]ResourceVector
}

// GlobalCatalog is the singleton in-memory dynamic instance catalog.
var GlobalCatalog = NewHardwareCatalog()

// NewHardwareCatalog initializes an in-memory hardware catalog.
func NewHardwareCatalog() *HardwareCatalog {
	return &HardwareCatalog{
		cache: make(map[string]ResourceVector),
	}
}

// Get returns the cached capacity vector for an instance type if present.
func (c *HardwareCatalog) Get(instanceType string) (ResourceVector, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.cache[instanceType]
	return v, ok
}

// Put caches a discovered capacity vector for an instance type.
func (c *HardwareCatalog) Put(instanceType string, v ResourceVector) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache[instanceType] = v
}

// roundGPUMemoryGB converts raw GPU memory in MiB from DescribeInstanceTypes into standard marketed GB sizes.
func roundGPUMemoryGB(sizeMiB int64) int {
	switch {
	case sizeMiB <= 0:
		return 0
	case sizeMiB >= 14000 && sizeMiB <= 18000:
		return 16
	case sizeMiB >= 22000 && sizeMiB <= 26000:
		return 24
	case sizeMiB >= 30000 && sizeMiB <= 34000:
		return 32
	case sizeMiB >= 38000 && sizeMiB <= 44000:
		return 40
	case sizeMiB >= 46000 && sizeMiB <= 52000:
		return 48
	case sizeMiB >= 76000 && sizeMiB <= 88000:
		return 80
	case sizeMiB >= 92000 && sizeMiB <= 102000:
		return 96
	case sizeMiB >= 135000 && sizeMiB <= 150000:
		return 141
	case sizeMiB >= 270000 && sizeMiB <= 310000:
		return 288
	default:
		return int((sizeMiB + 512) / 1024)
	}
}

// CandidateAWSInstanceFamilies are standard AWS GPU families scanned for market tracking.
var CandidateAWSInstanceFamilies = []string{
	"g5", "g6", "g6e", "gr6", "p4d", "p4de", "p5", "p5e", "p3", "g4dn",
}

// DiscoverTrackedShapes returns the tracked GPU instance types. If an explicit list is provided,
// it is returned directly; otherwise candidates are derived from supported GPU instance families.
func DiscoverTrackedShapes(explicit []string) []string {
	if len(explicit) > 0 {
		var filtered []string
		for _, s := range explicit {
			s = strings.TrimSpace(s)
			if s != "" {
				filtered = append(filtered, s)
			}
		}
		if len(filtered) > 0 {
			return filtered
		}
	}
	return []string{
		"g5.xlarge", "g5.2xlarge", "g5.4xlarge", "g5.8xlarge", "g5.12xlarge", "g5.16xlarge", "g5.24xlarge", "g5.48xlarge",
		"g6.xlarge", "g6.2xlarge", "g6.4xlarge", "g6.8xlarge", "g6.12xlarge", "g6.16xlarge", "g6.24xlarge", "g6.48xlarge",
		"g6e.xlarge", "g6e.2xlarge", "g6e.4xlarge", "g6e.8xlarge", "g6e.12xlarge", "g6e.16xlarge", "g6e.24xlarge", "g6e.48xlarge",
		"p4d.24xlarge", "p4de.24xlarge",
		"p5.48xlarge", "p5e.48xlarge",
		"g4dn.xlarge", "g4dn.2xlarge", "g4dn.4xlarge", "g4dn.8xlarge", "g4dn.12xlarge", "g4dn.16xlarge", "g4dn.metal",
	}
}

// ResolveCapacity returns the dynamic hardware capacity vector for an instance type.
// If already cached or discovered via ec2.Client, it returns the real cloud specifications.
// Otherwise it falls back to the local instance catalog.
func (c *HardwareCatalog) ResolveCapacity(ctx context.Context, client InstanceTypeDescriber, itype string, fallback ResourceVector) ResourceVector {
	if itype == "" {
		return fallback
	}

	if vec, ok := c.Get(itype); ok {
		return vec
	}

	if client != nil {
		out, err := client.DescribeInstanceTypes(ctx, &ec2.DescribeInstanceTypesInput{
			InstanceTypes: []ec2types.InstanceType{ec2types.InstanceType(itype)},
		})
		if err == nil && len(out.InstanceTypes) > 0 {
			info := out.InstanceTypes[0]
			var vcpu int32 = 2
			if info.VCpuInfo != nil && info.VCpuInfo.DefaultVCpus != nil {
				vcpu = *info.VCpuInfo.DefaultVCpus
			}

			var memMiB int64 = 4096
			if info.MemoryInfo != nil && info.MemoryInfo.SizeInMiB != nil {
				memMiB = *info.MemoryInfo.SizeInMiB
			}

			gpus := 0
			var gpuModel string
			var gpuMemGB int
			if info.GpuInfo != nil {
				for _, g := range info.GpuInfo.Gpus {
					if g.Count != nil {
						gpus += int(*g.Count)
					}
				}
				if len(info.GpuInfo.Gpus) > 0 {
					g := info.GpuInfo.Gpus[0]
					name := aws.ToString(g.Name)
					if g.MemoryInfo != nil && g.MemoryInfo.SizeInMiB != nil {
						gpuMemGB = roundGPUMemoryGB(int64(*g.MemoryInfo.SizeInMiB))
					}
					if name != "" && gpuMemGB > 0 {
						gpuModel = fmt.Sprintf("%s %dGB", name, gpuMemGB)
					} else if name != "" {
						gpuModel = name
					}
				}
			}

			if gpuModel == "" {
				staticVec := CapacityForInstanceType(itype, fallback)
				gpuModel = staticVec.GPUModel
				gpuMemGB = staticVec.GPUMemoryGB
			}

			cpuQuantity := resource.MustParse(fmt.Sprintf("%d", vcpu))
			memBytes := memMiB * 1024 * 1024
			memQuantity := *resource.NewQuantity(memBytes, resource.BinarySI)

			resolved := ResourceVector{
				CPU:         cpuQuantity,
				Memory:      memQuantity,
				GPUs:        gpus,
				TPUs:        fallback.TPUs,
				GPUModel:    gpuModel,
				GPUMemoryGB: gpuMemGB,
			}
			c.Put(itype, resolved)
			return resolved
		}
	}

	// Fallback to static catalog table without poisoning the cache on transient cloud API failure
	return CapacityForInstanceType(itype, fallback)
}
