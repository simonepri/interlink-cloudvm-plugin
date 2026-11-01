// Converts the plugin catalog into Karpenter instance types with per-region, per-zone, per-capacity-type offerings.

package cloudprovider

import (
	"cmp"
	"context"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/log"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"

	"github.com/simonepri/interlink-cloudvm-plugin/karpenter/apis/v1alpha1"
	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

var (
	gpuMemoryToken  = regexp.MustCompile(`(?i)^\d+\s*gb$`)
	familyCategory  = regexp.MustCompile(`^[a-z]+`)
	familyGenDigits = regexp.MustCompile(`^[a-z]+(\d+)`)
)

type gpuClassKey struct {
	class        string
	capacityType string
}

// instanceTypes builds Karpenter instance types from the catalog, keeping only offerings inside the NodeClass
// footprint, pricing each with the transfer cost of nodePool's VMs in its region, dropping offerings exceeding the
// NodePool's price cap, and marking offerings the plugin recently refused for lack of capacity as unavailable.
func (c *CloudProvider) instanceTypes(ctx context.Context, catalog *nodeclaim.Catalog, nodeClass *v1alpha1.CloudVMNodeClass, nodePool string) ([]*cloudprovider.InstanceType, error) {
	footprint := map[string]v1alpha1.FootprintRegion{}
	for _, r := range nodeClass.Spec.Footprint {
		footprint[r.Region] = r
	}
	transfer := c.transferPricerFor(catalog, nodePool)
	priceCap := c.config.PriceCapProfileFor(nodePool)

	cheapestPerGPU := map[gpuClassKey]float64{}
	if priceCap.MaxPriceAboveCheapestPercent != nil {
		for i := range catalog.InstanceTypes {
			src := &catalog.InstanceTypes[i]
			if src.GPUCount <= 0 {
				continue
			}
			class := c.config.GPUClassFor(src.GPUModel, src.GPUMemoryGB)
			if class == "" {
				continue
			}
			for _, o := range src.Offerings {
				fr, ok := footprint[o.Region]
				if !ok || fr.Provider != src.Provider {
					continue
				}
				zone := offeringZone(o)
				if len(fr.Zones) > 0 && !slices.Contains(fr.Zones, zone) {
					continue
				}
				if o.CapacityType != karpv1.CapacityTypeSpot && o.CapacityType != karpv1.CapacityTypeOnDemand {
					continue
				}
				offeringPrice := o.PricePerHour + transfer.perHour(src.Provider, o.Region)
				pricePerGPU := offeringPrice / float64(src.GPUCount)
				key := gpuClassKey{class: class, capacityType: o.CapacityType}
				if cur, ok := cheapestPerGPU[key]; !ok || pricePerGPU < cur {
					cheapestPerGPU[key] = pricePerGPU
				}
			}
		}
	}

	out := make([]*cloudprovider.InstanceType, 0, len(catalog.InstanceTypes))
	for i := range catalog.InstanceTypes {
		it, err := c.instanceType(ctx, &catalog.InstanceTypes[i], footprint, transfer, nodePool, priceCap, cheapestPerGPU)
		if err != nil {
			return nil, err
		}
		if it != nil {
			out = append(out, it)
		}
	}
	return out, nil
}

func (c *CloudProvider) instanceType(ctx context.Context, src *nodeclaim.InstanceType, footprint map[string]v1alpha1.FootprintRegion, transfer transferPricer, nodePool string, priceCap PriceCapProfile, cheapestPerGPU map[gpuClassKey]float64) (*cloudprovider.InstanceType, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	offerings := cloudprovider.Offerings{}
	zones, regions, capacityTypes := sets.New[string](), sets.New[string](), sets.New[string]()
	for _, o := range src.Offerings {
		fr, ok := footprint[o.Region]
		if !ok || fr.Provider != src.Provider {
			continue
		}
		zone := offeringZone(o)
		if len(fr.Zones) > 0 && !slices.Contains(fr.Zones, zone) {
			continue
		}
		if o.CapacityType != karpv1.CapacityTypeSpot && o.CapacityType != karpv1.CapacityTypeOnDemand {
			continue
		}
		offeringPrice := o.PricePerHour + transfer.perHour(src.Provider, o.Region)
		if priceCap.Enabled() && src.GPUCount > 0 {
			pricePerGPU := offeringPrice / float64(src.GPUCount)
			dropped := false
			var reason string
			var limit float64
			if priceCap.MaxPricePerGPUHour != nil && pricePerGPU > *priceCap.MaxPricePerGPUHour+1e-9 {
				dropped = true
				reason = "maxPricePerGPUHour"
				limit = *priceCap.MaxPricePerGPUHour
			}
			if !dropped && priceCap.MaxPriceAboveCheapestPercent != nil {
				class := c.config.GPUClassFor(src.GPUModel, src.GPUMemoryGB)
				if class != "" {
					key := gpuClassKey{class: class, capacityType: o.CapacityType}
					if minPrice, ok := cheapestPerGPU[key]; ok {
						capPrice := minPrice * (1.0 + *priceCap.MaxPriceAboveCheapestPercent/100.0)
						if pricePerGPU > capPrice+1e-9 {
							dropped = true
							reason = "maxPriceAboveCheapestPercent"
							limit = capPrice * float64(src.GPUCount)
						}
					}
				}
			}
			if dropped {
				log.FromContext(ctx).V(1).Info("dropping offering exceeding price cap",
					"nodepool", nodePool,
					"instance-type", src.Name,
					"zone", zone,
					"capacity-type", o.CapacityType,
					"price", offeringPrice,
					"reason", reason,
					"limit", limit,
				)
				OfferingsDropped.WithLabelValues(nodePool).Inc()
				continue
			}
		}

		zones.Insert(zone)
		regions.Insert(o.Region)
		capacityTypes.Insert(o.CapacityType)
		offerings = append(offerings, &cloudprovider.Offering{
			Requirements: scheduling.NewRequirements(
				scheduling.NewRequirement(corev1.LabelTopologyRegion, corev1.NodeSelectorOpIn, o.Region),
				scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zone),
				scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, o.CapacityType),
			),
			Price:     offeringPrice,
			Available: o.Available && !c.unavailable.IsUnavailable(src.Name, zone, o.CapacityType),
		})
	}
	if len(offerings) == 0 {
		return nil, nil
	}
	capacity, err := c.capacity(src)
	if err != nil {
		return nil, fmt.Errorf("instance type %s: %w", src.Name, err)
	}
	return &cloudprovider.InstanceType{
		Name:         src.Name,
		Requirements: c.requirements(src, capacity, regions, zones, capacityTypes),
		Offerings:    offerings,
		Capacity:     capacity,
		Overhead:     &cloudprovider.InstanceTypeOverhead{},
	}, nil
}

// offeringZone returns the offering's zone, or its region when the cloud chooses the zone at launch.
func offeringZone(o nodeclaim.Offering) string {
	if o.Zone == "" {
		return o.Region
	}
	return o.Zone
}

func (c *CloudProvider) capacity(src *nodeclaim.InstanceType) (corev1.ResourceList, error) {
	cpu, err := resource.ParseQuantity(src.CPU)
	if err != nil {
		return nil, fmt.Errorf("cpu: %w", err)
	}
	mem, err := resource.ParseQuantity(src.Memory)
	if err != nil {
		return nil, fmt.Errorf("memory: %w", err)
	}
	// The plugin plans the disk size of the type; the configured size covers a plugin that cannot tell.
	storage, err := resource.ParseQuantity(cmp.Or(src.EphemeralStorage, c.config.EphemeralStorage))
	if err != nil {
		return nil, fmt.Errorf("ephemeral storage: %w", err)
	}
	out := corev1.ResourceList{
		corev1.ResourceCPU:              cpu,
		corev1.ResourceMemory:           mem,
		corev1.ResourcePods:             *resource.NewQuantity(c.config.MaxPods, resource.DecimalSI),
		corev1.ResourceEphemeralStorage: storage,
	}
	if src.GPUCount > 0 {
		out[nodeclaim.ResourceGPU] = *resource.NewQuantity(int64(src.GPUCount), resource.DecimalSI)
	}
	return out, nil
}

func (c *CloudProvider) requirements(src *nodeclaim.InstanceType, capacity corev1.ResourceList, regions, zones, capacityTypes sets.Set[string]) scheduling.Requirements {
	in := func(key string, values ...string) *scheduling.Requirement {
		return scheduling.NewRequirement(key, corev1.NodeSelectorOpIn, values...)
	}
	absent := func(key string) *scheduling.Requirement {
		return scheduling.NewRequirement(key, corev1.NodeSelectorOpDoesNotExist)
	}
	reqs := scheduling.NewRequirements(
		in(corev1.LabelInstanceTypeStable, src.Name),
		in(corev1.LabelArchStable, normalizeArch(src.Architecture)),
		in(corev1.LabelOSStable, string(corev1.Linux)),
		in(corev1.LabelTopologyRegion, sets.List(regions)...),
		in(corev1.LabelTopologyZone, sets.List(zones)...),
		in(karpv1.CapacityTypeLabelKey, sets.List(capacityTypes)...),
		in(LabelCloud, src.Provider),
		in(LabelInstanceCPU, strconv.FormatInt(capacity.Cpu().Value(), 10)),
		in(LabelInstanceMemory, strconv.FormatInt(capacity.Memory().Value()/(1<<20), 10)),
	)
	family := strings.ToLower(src.Family)
	if family == "" {
		family, _, _ = strings.Cut(src.Name, ".")
	}
	reqs.Add(in(LabelInstanceFamily, family))
	if cat := familyCategory.FindString(family); cat != "" {
		reqs.Add(in(LabelInstanceCategory, cat))
	} else {
		reqs.Add(absent(LabelInstanceCategory))
	}
	generation := src.Generation
	if m := familyGenDigits.FindStringSubmatch(family); generation == "" && m != nil {
		generation = m[1]
	}
	reqs.Add(pick(generation != "", in(LabelInstanceGeneration, generation), absent(LabelInstanceGeneration)))
	size := src.Size
	if _, s, ok := strings.Cut(src.Name, "."); size == "" && ok {
		size = s
	}
	reqs.Add(pick(size != "", in(LabelInstanceSize, size), absent(LabelInstanceSize)))

	if src.GPUCount == 0 {
		for _, k := range []string{LabelInstanceGPUName, LabelInstanceGPUManufacturer, LabelInstanceGPUCount, LabelInstanceGPUMemory, LabelGPUClass, LabelNVIDIAGPUPresent, LabelNVIDIAPCIPresent} {
			reqs.Add(absent(k))
		}
		return reqs
	}
	reqs.Add(in(LabelInstanceGPUCount, strconv.Itoa(src.GPUCount)))
	reqs.Add(pick(src.GPUModel != "", in(LabelInstanceGPUName, gpuName(src.GPUModel)), absent(LabelInstanceGPUName)))
	reqs.Add(pick(src.GPUManufacturer != "", in(LabelInstanceGPUManufacturer, strings.ToLower(src.GPUManufacturer)), absent(LabelInstanceGPUManufacturer)))
	nvidia := strings.EqualFold(src.GPUManufacturer, "nvidia")
	reqs.Add(pick(nvidia, in(LabelNVIDIAGPUPresent, "true"), absent(LabelNVIDIAGPUPresent)))
	reqs.Add(pick(nvidia, in(LabelNVIDIAPCIPresent, "true"), absent(LabelNVIDIAPCIPresent)))
	reqs.Add(pick(src.GPUMemoryGB > 0, in(LabelInstanceGPUMemory, strconv.Itoa(src.GPUMemoryGB*1024)), absent(LabelInstanceGPUMemory)))
	class := c.config.GPUClassFor(src.GPUModel, src.GPUMemoryGB)
	reqs.Add(pick(class != "", in(LabelGPUClass, class), absent(LabelGPUClass)))
	return reqs
}

func pick(cond bool, a, b *scheduling.Requirement) *scheduling.Requirement {
	if cond {
		return a
	}
	return b
}

// gpuName turns a catalog GPU model such as "a100" or "A100 80GB" into the instance-gpu-name value "a100".
func gpuName(model string) string {
	fields := strings.Fields(model)
	fields = slices.DeleteFunc(fields, gpuMemoryToken.MatchString)
	return strings.ToLower(strings.Join(fields, "-"))
}

func normalizeArch(arch string) string {
	switch strings.ToLower(arch) {
	case "x86_64", "amd64", "":
		return karpv1.ArchitectureAmd64
	case "aarch64", "arm64":
		return karpv1.ArchitectureArm64
	default:
		return strings.ToLower(arch)
	}
}
