// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"fmt"
	"sync"

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
			if info.GpuInfo != nil {
				for _, g := range info.GpuInfo.Gpus {
					if g.Count != nil {
						gpus += int(*g.Count)
					}
				}
			}

			cpuQuantity := resource.MustParse(fmt.Sprintf("%d", vcpu))
			memBytes := memMiB * 1024 * 1024
			memQuantity := *resource.NewQuantity(memBytes, resource.BinarySI)

			resolved := ResourceVector{
				CPU:    cpuQuantity,
				Memory: memQuantity,
				GPUs:   gpus,
				TPUs:   fallback.TPUs,
			}
			c.Put(itype, resolved)
			return resolved
		}
	}

	// Fallback to static catalog table without poisoning the cache on transient cloud API failure
	return CapacityForInstanceType(itype, fallback)
}
