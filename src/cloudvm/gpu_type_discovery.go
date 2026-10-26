// Package main implements the interlink-cloudvm-plugin daemon.
package main

// This file discovers every EC2 GPU instance type in the tracked regions, GPU slices included, so the ticker tracks what AWS offers instead of a fixed list.

import (
	"context"
	"fmt"
	"log"
	"math"
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

// gpuTypeDiscoveryRetry is how soon the ticker reads again after some regions failed to list their types.
const gpuTypeDiscoveryRetry = time.Hour

// discoveredGPUType is one EC2 instance type with named GPUs, as DescribeInstanceTypes reports it.
// A GPU slice, part of one GPU shared between instances, counts as one GPU.
type discoveredGPUType struct {
	name      string
	arch      string
	gpuName   string
	gpuMemMiB int64
	slice     bool
	capacity  ResourceVector
}

// describeGPUTypes returns every instance type in the region with named GPUs, GPU slices included.
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
	// AWS reports a GPU slice as zero GPUs with the slice's share of the GPU memory.
	slice := gpus == 0
	if slice {
		gpus = 1
	}
	g := info.GpuInfo.Gpus[0]
	gpuMemMiB := int64(0)
	memGB := 0
	if g.MemoryInfo != nil {
		gpuMemMiB = int64(aws.ToInt32(g.MemoryInfo.SizeInMiB))
		memGB = roundGPUMemoryGB(gpuMemMiB)
	}
	if aws.ToString(g.Name) == "" || memGB == 0 {
		return discoveredGPUType{}, false
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
		name:      string(info.InstanceType),
		arch:      arch,
		gpuName:   aws.ToString(g.Name),
		gpuMemMiB: gpuMemMiB,
		slice:     slice,
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
	failed := false
	var mu sync.Mutex
	t.forEachRegion(ctx, t.Regions(), func(ctx context.Context, reg string) {
		fetcher, err := t.fetcherFactory(ctx, reg)
		var types []discoveredGPUType
		if err == nil {
			types, err = describeGPUTypes(ctx, fetcher)
		}
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			failed = true
			log.Printf("[cloudvm-ticker] Warning: listing GPU instance types in %s failed: %v", sanitizeLog(reg), err) //nolint:gosec // region sanitized
			return
		}
		for _, gt := range types {
			found[gt.name] = gt
		}
	})

	t.mu.Lock()
	defer t.mu.Unlock()
	// A failed region may offer types no other region lists, so a partial read keeps the previous set too and
	// retries within the hour; a complete read replaces it, and a read where every region failed changes nothing.
	t.lastTypeDiscovery = time.Now()
	if failed {
		t.lastTypeDiscovery = t.lastTypeDiscovery.Add(gpuTypeDiscoveryRetry - gpuTypeDiscoveryInterval)
	}
	if len(found) == 0 {
		return
	}
	arch := make(map[string]string, len(found))
	if failed {
		for _, name := range t.instanceTypes {
			arch[name] = t.instanceArch[name]
		}
	}
	labelGPUSlices(found)
	for name, gt := range found {
		arch[name] = gt.arch
		t.catalog.Put(name, gt.capacity)
	}
	names := make([]string, 0, len(arch))
	for name := range arch {
		names = append(names, name)
	}
	sort.Strings(names)
	t.instanceTypes = names
	t.instanceArch = arch
	// Prices of types no longer tracked would keep feeding metrics, fill plans, and launch ranking.
	for key, p := range t.prices {
		if _, ok := arch[p.InstanceType]; !ok {
			delete(t.prices, key)
		}
	}
	log.Printf("[cloudvm-ticker] Tracking %d GPU instance types offered in %d regions", len(names), len(t.regions)) //nolint:gosec // logs two counts
}

// labelGPUSlices names each GPU slice like a MIG profile, "L4 1/8 3GB", by comparing its memory with a whole GPU
// of the same model; a slice whose model no discovered type offers whole keeps the plain name and memory.
func labelGPUSlices(types map[string]discoveredGPUType) {
	whole := make(map[string]int64)
	for _, gt := range types {
		if !gt.slice && gt.gpuMemMiB > whole[gt.gpuName] {
			whole[gt.gpuName] = gt.gpuMemMiB
		}
	}
	for name, gt := range types {
		full := whole[gt.gpuName]
		if !gt.slice || full == 0 || gt.gpuMemMiB == 0 {
			continue
		}
		share := int(math.Round(float64(full) / float64(gt.gpuMemMiB)))
		if share < 2 {
			continue
		}
		// AWS sells slices as fractions of the GPU's marketed memory, such as 12 GB for half of a 24 GB L4.
		gt.capacity.GPUMemoryGB = roundGPUMemoryGB(full) / share
		gt.capacity.GPUModel = fmt.Sprintf("%s 1/%d %dGB", gt.gpuName, share, gt.capacity.GPUMemoryGB)
		types[name] = gt
	}
}
