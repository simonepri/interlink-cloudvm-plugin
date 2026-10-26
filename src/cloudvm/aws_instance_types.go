// Instance type candidates for AWS launches: the pod's explicit list, or every catalog type that satisfies the pod's
// resource requests and Karpenter label requirements, cheapest first, matched with Karpenter's cloudprovider package.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// maxFleetOverrides bounds the (instance type, subnet) overrides of one CreateFleet request; it is the documented
// ceiling for EC2 Fleet launch template overrides.
const maxFleetOverrides = 300

// unpricedOffset ranks instance types without a known price after every priced one, smallest first.
const unpricedOffset = 1e9

// DefaultCPUInstanceTypes is the catalog searched for pods that request no GPU.
var DefaultCPUInstanceTypes = func() []string {
	var out []string
	for _, family := range []string{"m7i", "c7i", "r7i", "m6i", "c6i", "r6i"} {
		for _, size := range []string{"large", "xlarge", "2xlarge", "4xlarge", "8xlarge", "12xlarge", "16xlarge", "24xlarge"} {
			out = append(out, family+"."+size)
		}
	}
	return out
}()

// errNoInstanceTypeFits reports that no catalog instance type satisfies the pod; another region cannot help.
var errNoInstanceTypeFits = errors.New("no instance type satisfies the pod's requests and requirements")

// InstanceTypeFacts holds the hardware of one EC2 instance type, as DescribeInstanceTypes reports it.
type InstanceTypeFacts struct {
	Name            string
	VCPUs           int
	MemoryMiB       int64
	Arch            string
	CPUManufacturer string
	GPUs            int
	GPUName         string
	GPUManufacturer string
	GPUMemoryMiB    int64
	// GPUSlice marks a type whose GPU is part of one GPU shared between instances, such as g6f.
	GPUSlice bool
}

// instanceTypeMarket is the part of the spot ticker that knows the tracked instance types and their spot prices.
type instanceTypeMarket interface {
	TrackedInstanceTypes() []string
	AllPrices() map[string]*SpotPriceEntry
}

// instanceFactsCache caches DescribeInstanceTypes results for the process lifetime, since hardware never changes.
type instanceFactsCache struct {
	mu    sync.Mutex
	facts map[string]InstanceTypeFacts
}

var globalInstanceFacts = &instanceFactsCache{facts: make(map[string]InstanceTypeFacts)}

// resolve returns the facts of every named type that EC2 knows, describing uncached ones in batches of 100. When
// DescribeInstanceTypes fails, GPU types fall back to the static catalog and other types are left out.
func (c *instanceFactsCache) resolve(ctx context.Context, client InstanceTypeDescriber, names []string) map[string]InstanceTypeFacts {
	out := make(map[string]InstanceTypeFacts, len(names))
	var missing []string
	c.mu.Lock()
	for _, n := range names {
		if f, ok := c.facts[n]; ok {
			out[n] = f
		} else {
			missing = append(missing, n)
		}
	}
	c.mu.Unlock()

	for start := 0; start < len(missing) && client != nil; start += 100 {
		batch := missing[start:min(start+100, len(missing))]
		described, err := describeInstanceFacts(ctx, client, batch)
		if err != nil {
			log.Printf("[aws] Warning: DescribeInstanceTypes failed, using the static GPU catalog: %v", err)
			break
		}
		c.mu.Lock()
		for name, f := range described {
			c.facts[name] = f
			out[name] = f
		}
		c.mu.Unlock()
	}
	for _, n := range missing {
		if _, ok := out[n]; !ok && GPUCountForInstanceType(n) > 0 {
			out[n] = staticInstanceFacts(n)
		}
	}
	return out
}

func describeInstanceFacts(ctx context.Context, client InstanceTypeDescriber, names []string) (map[string]InstanceTypeFacts, error) {
	out := make(map[string]InstanceTypeFacts, len(names))
	input := &ec2.DescribeInstanceTypesInput{Filters: []ec2types.Filter{{Name: aws.String("instance-type"), Values: names}}}
	for {
		page, err := client.DescribeInstanceTypes(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, info := range page.InstanceTypes {
			f := instanceFactsFromInfo(info)
			out[f.Name] = f
		}
		if aws.ToString(page.NextToken) == "" {
			return out, nil
		}
		input.NextToken = page.NextToken
	}
}

func instanceFactsFromInfo(info ec2types.InstanceTypeInfo) InstanceTypeFacts {
	f := InstanceTypeFacts{Name: string(info.InstanceType), Arch: "amd64"}
	if info.VCpuInfo != nil {
		f.VCPUs = int(aws.ToInt32(info.VCpuInfo.DefaultVCpus))
	}
	if info.MemoryInfo != nil {
		f.MemoryMiB = aws.ToInt64(info.MemoryInfo.SizeInMiB)
	}
	if info.ProcessorInfo != nil {
		for _, a := range info.ProcessorInfo.SupportedArchitectures {
			if a == ec2types.ArchitectureTypeArm64 {
				f.Arch = "arm64"
			}
		}
		f.CPUManufacturer = strings.ToLower(aws.ToString(info.ProcessorInfo.Manufacturer))
	}
	if info.GpuInfo != nil && len(info.GpuInfo.Gpus) > 0 {
		g := info.GpuInfo.Gpus[0]
		for _, gpu := range info.GpuInfo.Gpus {
			f.GPUs += int(aws.ToInt32(gpu.Count))
		}
		// AWS reports a GPU slice, part of one GPU shared between instances, as zero GPUs; it serves one GPU device.
		if f.GPUs == 0 && g.MemoryInfo != nil && aws.ToInt32(g.MemoryInfo.SizeInMiB) > 0 {
			f.GPUs = 1
			f.GPUSlice = true
		}
		f.GPUName = labelValue(aws.ToString(g.Name))
		f.GPUManufacturer = labelValue(aws.ToString(g.Manufacturer))
		if g.MemoryInfo != nil {
			f.GPUMemoryMiB = int64(aws.ToInt32(g.MemoryInfo.SizeInMiB))
		}
	}
	return f
}

// staticInstanceFacts derives facts for a GPU type from the static catalog when EC2 cannot be asked.
func staticInstanceFacts(name string) InstanceTypeFacts {
	v := CapacityForInstanceType(name, ResourceVector{})
	gpuName, _, _ := strings.Cut(v.GPUModel, fmt.Sprintf(" %dGB", v.GPUMemoryGB))
	manufacturer := "nvidia"
	if strings.HasPrefix(gpuName, "Radeon") {
		manufacturer = "amd"
	}
	return InstanceTypeFacts{
		Name: name, VCPUs: int(v.CPU.Value()), MemoryMiB: v.Memory.Value() / (1 << 20), Arch: "amd64",
		GPUs: v.GPUs, GPUName: labelValue(gpuName), GPUManufacturer: manufacturer, GPUMemoryMiB: int64(v.GPUMemoryGB) * 1024,
	}
}

// labelValue lowercases a display name into a label value, such as "Radeon Pro V620" into "radeon-pro-v620".
func labelValue(name string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(name)), " ", "-")
}

// requirements returns the instance type's values for every label in instanceTypeLabels except the capacity type,
// which its offerings carry. Types without GPUs leave the GPU labels undefined, so only DoesNotExist and NotIn match.
func (f InstanceTypeFacts) requirements() scheduling.Requirements {
	family, size, _ := strings.Cut(f.Name, ".")
	category, generation := splitFamily(family)
	in := func(key, value string) *scheduling.Requirement {
		if value == "" {
			return scheduling.NewRequirement(key, corev1.NodeSelectorOpDoesNotExist)
		}
		return scheduling.NewRequirement(key, corev1.NodeSelectorOpIn, value)
	}
	gpuCount, gpuMemory := "", ""
	if f.GPUs > 0 {
		gpuCount, gpuMemory = strconv.Itoa(f.GPUs), strconv.FormatInt(f.GPUMemoryMiB, 10)
	}
	return scheduling.NewRequirements(
		in(corev1.LabelInstanceTypeStable, f.Name),
		in(corev1.LabelArchStable, f.Arch),
		in(labelInstanceCategory, category),
		in(labelInstanceFamily, family),
		in(labelInstanceGeneration, generation),
		in(labelInstanceSize, size),
		in(labelInstanceCPU, strconv.Itoa(f.VCPUs)),
		in(labelInstanceCPUManufacturer, f.CPUManufacturer),
		in(labelInstanceMemory, strconv.FormatInt(f.MemoryMiB, 10)),
		in(labelInstanceGPUName, f.GPUName),
		in(labelInstanceGPUManufacturer, f.GPUManufacturer),
		in(labelInstanceGPUCount, gpuCount),
		in(labelInstanceGPUMemory, gpuMemory),
		in(labelGPUSlice, strconv.FormatBool(f.GPUSlice)),
	)
}

// splitFamily splits an instance family such as "g4dn" into its category "g" and generation "4".
func splitFamily(family string) (string, string) {
	i := strings.IndexFunc(family, func(r rune) bool { return r >= '0' && r <= '9' })
	if i < 0 {
		return family, ""
	}
	j := i
	for j < len(family) && family[j] >= '0' && family[j] <= '9' {
		j++
	}
	return family[:i], family[i:j]
}

// capacity returns the resources a VM of this type offers the pod: its vCPUs, memory and GPUs.
func (f InstanceTypeFacts) capacity() corev1.ResourceList {
	list := corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewQuantity(int64(f.VCPUs), resource.DecimalSI),
		corev1.ResourceMemory: *resource.NewQuantity(f.MemoryMiB<<20, resource.BinarySI),
	}
	if f.GPUs > 0 {
		list[gpuResourceName] = *resource.NewQuantity(int64(f.GPUs), resource.DecimalSI)
	}
	return list
}

const gpuResourceName corev1.ResourceName = "nvidia.com/gpu"

// launchInstanceTypes lists the pod's explicit instance types, the preferred type first, or derives them from the
// catalog when the pod names none.
func (p *AWSProvider) launchInstanceTypes(ctx context.Context, spec *InstanceSpec) ([]string, error) {
	var instanceTypes []string
	if spec.InstanceType != "" {
		instanceTypes = append(instanceTypes, spec.InstanceType)
	}
	for _, cand := range spec.CandidateInstanceTypes {
		if cand != "" && cand != spec.InstanceType {
			instanceTypes = append(instanceTypes, cand)
		}
	}
	if len(instanceTypes) > 0 {
		return instanceTypes, nil
	}
	return p.deriveInstanceTypes(ctx, spec)
}

// deriveInstanceTypes returns every catalog instance type whose labels satisfy the pod's requirements and whose
// capacity fits its requests, cheapest compatible offering first, truncated so the fleet overrides of the pinned
// region stay within maxFleetOverrides. Unless the pod constrains the GPU count label itself, a GPU pod takes only
// types with exactly its GPU count and a CPU pod only types without GPUs.
func (p *AWSProvider) deriveInstanceTypes(ctx context.Context, spec *InstanceSpec) ([]string, error) {
	reqs := scheduling.NewNodeSelectorRequirementsWithMinValues(spec.InstanceRequirements...)
	gpus := spec.Requests.GPUs
	if !reqs.Has(labelInstanceGPUCount) {
		if gpus > 0 {
			reqs.Add(scheduling.NewRequirement(labelInstanceGPUCount, corev1.NodeSelectorOpIn, strconv.Itoa(gpus)))
		} else {
			reqs.Add(scheduling.NewRequirement(labelInstanceGPUCount, corev1.NodeSelectorOpDoesNotExist))
		}
	}
	capacityType := karpv1.CapacityTypeSpot
	if spec.MarketType == "on-demand" {
		capacityType = karpv1.CapacityTypeOnDemand
	}
	reqs.Add(scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, capacityType))

	var client InstanceTypeDescriber
	if c, err := p.getClient(ctx, spec.Region); err == nil {
		client = c
	}
	pool := p.instanceTypePool(gpus, reqs)
	facts := globalInstanceFacts.resolve(ctx, client, pool)
	var templates []resolvedTemplate
	if spec.LaunchTemplateID == "" && spec.LaunchTemplateName == "" {
		var tc launchTemplateClient
		if c, ok := client.(launchTemplateClient); ok {
			tc = c
		}
		templates = resolveTemplates(ctx, tc, spec.Region, p.regionTemplates(spec.Region))
	}

	requests := spec.Requests.resourceList()
	var fits cloudprovider.InstanceTypes
	for _, name := range pool {
		f, ok := facts[name]
		if !ok {
			continue
		}
		it := &cloudprovider.InstanceType{
			Name:         name,
			Requirements: f.requirements(),
			Capacity:     f.capacity(),
			Overhead:     &cloudprovider.InstanceTypeOverhead{},
		}
		if it.Requirements.Compatible(reqs, scheduling.AllowUndefinedWellKnownLabels) != nil || !resources.Fits(requests, it.Allocatable()) {
			continue
		}
		// A type the pinned region has no launch template for cannot boot there, such as an Arm type without an Arm image.
		if templates != nil {
			if _, ok := templateFor(templates, f); !ok {
				continue
			}
		}
		fits = append(fits, it)
	}
	p.priceOfferings(ctx, spec.Region, fits, facts)
	fits = fits.Compatible(reqs)
	if len(fits) == 0 {
		return nil, fmt.Errorf("%w (%d GPUs, %s CPU, %s memory, %d label requirements)", errNoInstanceTypeFits,
			gpus, spec.Requests.CPU.String(), spec.Requests.Memory.String(), len(spec.InstanceRequirements))
	}

	subnets := len(spec.SubnetIDs)
	if subnets == 0 {
		subnets = len(p.ResolveRegionalSubnets(spec.Region))
	}
	truncated, err := fits.Truncate(options.ToContext(ctx, &options.Options{MinValuesPolicy: options.MinValuesPolicyStrict}), reqs, maxFleetOverrides/max(subnets, 1))
	if err != nil {
		return nil, err
	}
	names := make([]string, len(truncated))
	for i, it := range truncated {
		names[i] = it.Name
	}
	return names, nil
}

// instanceTypePool lists the catalog to search: AWS_INSTANCE_TYPES, else the ticker's tracked GPU types or the
// default CPU types, plus every EC2 type an instance-type requirement names.
func (p *AWSProvider) instanceTypePool(gpus int, reqs scheduling.Requirements) []string {
	pool := parseCommaList(os.Getenv("AWS_INSTANCE_TYPES"))
	if len(pool) == 0 {
		switch {
		case gpus == 0:
			pool = DefaultCPUInstanceTypes
		case p.market() != nil:
			pool = p.market().TrackedInstanceTypes()
		default:
			pool = DiscoverTrackedShapes(nil)
		}
	}
	seen := make(map[string]bool, len(pool))
	var out []string
	add := func(names ...string) {
		for _, n := range names {
			if !seen[n] && isEC2InstanceType(n) {
				seen[n] = true
				out = append(out, n)
			}
		}
	}
	add(pool...)
	if r := reqs.Get(corev1.LabelInstanceTypeStable); r.Operator() == corev1.NodeSelectorOpIn {
		add(r.Values()...)
	}
	return out
}

func (p *AWSProvider) market() instanceTypeMarket {
	p.mu.RLock()
	defer p.mu.RUnlock()
	m, _ := p.regionScorer.(instanceTypeMarket)
	return m
}

// priceOfferings gives each instance type a spot offering per zone the ticker prices in region and one on-demand
// offering at the region's list price. Types without any price get offerings ranked after every priced type,
// smallest first, so the order stays deterministic.
func (p *AWSProvider) priceOfferings(ctx context.Context, region string, its cloudprovider.InstanceTypes, facts map[string]InstanceTypeFacts) {
	spot := make(map[string]map[string]float64)
	if m := p.market(); m != nil {
		for _, e := range m.AllPrices() {
			if e == nil || e.Region != region || e.SpotPriceUSD <= 0 {
				continue
			}
			if spot[e.InstanceType] == nil {
				spot[e.InstanceType] = make(map[string]float64)
			}
			spot[e.InstanceType][e.AvailabilityZone] = e.SpotPriceUSD
		}
	}

	onDemand := make([]float64, len(its))
	var wg sync.WaitGroup
	for i, it := range its {
		wg.Add(1)
		go func() {
			defer wg.Done()
			onDemand[i] = GlobalOnDemandPrices.Price(ctx, region, it.Name)
		}()
	}
	wg.Wait()

	unpriced := make([]*cloudprovider.InstanceType, 0)
	for i, it := range its {
		for zone, price := range spot[it.Name] {
			it.Offerings = append(it.Offerings, offering(karpv1.CapacityTypeSpot, zone, price))
		}
		if onDemand[i] > 0 {
			it.Offerings = append(it.Offerings, offering(karpv1.CapacityTypeOnDemand, "", onDemand[i]))
			if len(spot[it.Name]) == 0 {
				// A spot price is never above the on-demand price, so the list price bounds an untracked spot type.
				it.Offerings = append(it.Offerings, offering(karpv1.CapacityTypeSpot, "", onDemand[i]))
			}
		}
		if len(it.Offerings) == 0 {
			unpriced = append(unpriced, it)
		}
	}
	sort.Slice(unpriced, func(i, j int) bool {
		a, b := facts[unpriced[i].Name], facts[unpriced[j].Name]
		if a.VCPUs != b.VCPUs {
			return a.VCPUs < b.VCPUs
		}
		if a.MemoryMiB != b.MemoryMiB {
			return a.MemoryMiB < b.MemoryMiB
		}
		return a.Name < b.Name
	})
	for rank, it := range unpriced {
		price := unpricedOffset + float64(rank)
		it.Offerings = cloudprovider.Offerings{
			offering(karpv1.CapacityTypeSpot, "", price),
			offering(karpv1.CapacityTypeOnDemand, "", price),
		}
	}
}

func offering(capacityType, zone string, price float64) *cloudprovider.Offering {
	reqs := scheduling.NewRequirements(scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, capacityType))
	if zone != "" {
		reqs.Add(scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zone))
	}
	return &cloudprovider.Offering{Requirements: reqs, Price: price, Available: true}
}

// resourceList converts the request into the resource list Karpenter fits against instance type capacity.
func (r ResourceVector) resourceList() corev1.ResourceList {
	list := corev1.ResourceList{corev1.ResourceCPU: r.CPU.DeepCopy(), corev1.ResourceMemory: r.Memory.DeepCopy()}
	if r.GPUs > 0 {
		list[gpuResourceName] = *resource.NewQuantity(int64(r.GPUs), resource.DecimalSI)
	}
	return list
}
