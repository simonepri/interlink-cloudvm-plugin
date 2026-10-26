// Package main implements the interlink-cloudvm-plugin daemon.
package main

// This file discovers every EC2 instance type with whole GPUs in the tracked regions, so the ticker tracks what AWS offers instead of a fixed list.

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"k8s.io/apimachinery/pkg/api/resource"
)

// gpuTypeDiscoveryInterval is how often the ticker re-reads the GPU instance types AWS offers; AWS adds families rarely.
const gpuTypeDiscoveryInterval = 24 * time.Hour

// launchableArchitectures are the CPU architectures the worker launch templates have an AMI for.
// NOTE(simonepri): the worker AMI is x86_64 only, so Arm GPU types are tracked for prices but never launched or planned.
var launchableArchitectures = []string{"amd64"}

// discoveredGPUType is one EC2 instance type with whole GPUs, as DescribeInstanceTypes reports it.
type discoveredGPUType struct {
	name     string
	arch     string
	capacity ResourceVector
}

// describeGPUTypes returns every instance type in the region whose GPUs AWS counts as whole devices.
// Types that share one GPU between instances report a GPU count of 0 and are left out.
func describeGPUTypes(ctx context.Context, client SpotPriceFetcher) ([]discoveredGPUType, error) {
	var out []discoveredGPUType
	input := &ec2.DescribeInstanceTypesInput{MaxResults: aws.Int32(100)}
	for {
		page, err := client.DescribeInstanceTypes(ctx, input)
		if err != nil {
			return nil, err
		}
		if page == nil {
			return out, nil
		}
		for _, info := range page.InstanceTypes {
			if t, ok := gpuTypeFromInfo(info); ok {
				out = append(out, t)
			}
		}
		if aws.ToString(page.NextToken) == "" {
			return out, nil
		}
		input.NextToken = page.NextToken
	}
}

// gpuTypeFromInfo converts an instance type description into its capacity, labeling the GPU with the name and memory AWS reports.
func gpuTypeFromInfo(info ec2types.InstanceTypeInfo) (discoveredGPUType, bool) {
	if info.GpuInfo == nil || len(info.GpuInfo.Gpus) == 0 {
		return discoveredGPUType{}, false
	}
	gpus := 0
	for _, g := range info.GpuInfo.Gpus {
		gpus += int(aws.ToInt32(g.Count))
	}
	if gpus == 0 {
		return discoveredGPUType{}, false
	}
	g := info.GpuInfo.Gpus[0]
	memGB := 0
	if g.MemoryInfo != nil {
		memGB = roundGPUMemoryGB(int64(aws.ToInt32(g.MemoryInfo.SizeInMiB)))
	}
	vcpus := int32(0)
	if info.VCpuInfo != nil {
		vcpus = aws.ToInt32(info.VCpuInfo.DefaultVCpus)
	}
	memMiB := int64(0)
	if info.MemoryInfo != nil {
		memMiB = aws.ToInt64(info.MemoryInfo.SizeInMiB)
	}
	arch := "amd64"
	if info.ProcessorInfo != nil {
		for _, a := range info.ProcessorInfo.SupportedArchitectures {
			if a == ec2types.ArchitectureTypeArm64 {
				arch = "arm64"
			}
		}
	}
	return discoveredGPUType{
		name: string(info.InstanceType),
		arch: arch,
		capacity: ResourceVector{
			CPU:         *resource.NewQuantity(int64(vcpus), resource.DecimalSI),
			Memory:      *resource.NewQuantity(memMiB*1024*1024, resource.BinarySI),
			GPUs:        gpus,
			GPUModel:    fmt.Sprintf("%s %dGB", aws.ToString(g.Name), memGB),
			GPUMemoryGB: memGB,
		},
	}, true
}

// discoverGPUTypes reads the GPU instance types of every tracked region, records their capacity in the catalog,
// and makes their union the tracked set. Regions whose read fails are skipped; if every read fails, the set is kept.
func (t *SpotPriceTicker) discoverGPUTypes(ctx context.Context) {
	found := make(map[string]discoveredGPUType)
	var mu sync.Mutex
	t.forEachRegion(ctx, t.Regions(), func(ctx context.Context, reg string) {
		fetcher, err := t.fetcherFactory(ctx, reg)
		if err != nil {
			return
		}
		types, err := describeGPUTypes(ctx, fetcher)
		if err != nil {
			log.Printf("[cloudvm-ticker] Warning: listing GPU instance types in %s failed: %v", sanitizeLog(reg), err) //nolint:gosec // region sanitized
			return
		}
		mu.Lock()
		defer mu.Unlock()
		for _, gt := range types {
			found[gt.name] = gt
		}
	})
	if len(found) == 0 {
		return
	}
	names := make([]string, 0, len(found))
	arch := make(map[string]string, len(found))
	for name, gt := range found {
		names = append(names, name)
		arch[name] = gt.arch
		t.catalog.Put(name, gt.capacity)
	}
	sort.Strings(names)
	t.mu.Lock()
	t.instanceTypes = names
	t.instanceArch = arch
	t.lastTypeDiscovery = time.Now()
	t.mu.Unlock()
	log.Printf("[cloudvm-ticker] Tracking %d GPU instance types offered in %d regions", len(names), len(t.Regions())) //nolint:gosec // logs two counts
}

// launchable reports whether the worker launch templates can boot an instance type; types discovery has not seen count as x86_64.
func (t *SpotPriceTicker) launchable(instanceType string) bool {
	t.mu.RLock()
	arch, ok := t.instanceArch[instanceType]
	t.mu.RUnlock()
	if !ok {
		arch = "amd64"
	}
	for _, a := range launchableArchitectures {
		if a == arch {
			return true
		}
	}
	return false
}
