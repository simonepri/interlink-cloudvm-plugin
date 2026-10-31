// Package cloudprovider is a Karpenter CloudProvider that launches one CloudVM VM per NodeClaim through the plugin's
// NodeClaim API. It owns NodePools whose nodeClassRef is a CloudVMNodeClass and leaves every other NodePool alone.
package cloudprovider

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/awslabs/operatorpkg/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"

	"github.com/simonepri/interlink-cloudvm-plugin/karpenter/apis/v1alpha1"
	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

// Name is the provider name Karpenter reports in metrics and events.
const Name = "cloudvm"

// Drift reasons IsDrifted returns.
const (
	DriftRegionRemoved cloudprovider.DriftReason = "RegionRemoved"
	DriftZoneRemoved   cloudprovider.DriftReason = "ZoneRemoved"
	DriftNodeClass     cloudprovider.DriftReason = "NodeClassDrift"
)

// Plugin launch annotation keys the NodeClass fields map onto.
const (
	annotationProvider           = "cloudvm.vk.io/provider"
	annotationSubnets            = "cloudvm.vk.io/subnets"
	annotationLaunchTemplateID   = "cloudvm.vk.io/aws-launch-template"
	annotationLaunchTemplateName = "cloudvm.vk.io/aws-launch-template-name"
)

// CloudProvider implements cloudprovider.CloudProvider on top of the plugin's NodeClaim API.
type CloudProvider struct {
	kubeClient  client.Client
	api         nodeclaim.API
	config      *Config
	catalog     *catalogCache
	unavailable *unavailableOfferings
	poolFilter  labels.Selector
}

var _ cloudprovider.CloudProvider = (*CloudProvider)(nil)

// New returns a CloudProvider that reads NodeClasses through kubeClient and launches VMs through api.
func New(kubeClient client.Client, api nodeclaim.API, cfg *Config, clk clock.Clock) *CloudProvider {
	return &CloudProvider{
		kubeClient:  kubeClient,
		api:         api,
		config:      cfg,
		catalog:     newCatalogCache(api, clk, cfg.CatalogTTL.Duration, cfg.CatalogMaxStaleness.Duration),
		unavailable: newUnavailableOfferings(clk, cfg.UnavailableOfferingTTL.Duration),
		poolFilter:  labels.SelectorFromSet(cfg.NodePoolSelector),
	}
}

// Name returns the provider name.
func (c *CloudProvider) Name() string { return Name }

// GetSupportedNodeClasses limits this controller to NodePools and NodeClaims that reference a CloudVMNodeClass.
func (c *CloudProvider) GetSupportedNodeClasses() []status.Object {
	return []status.Object{&v1alpha1.CloudVMNodeClass{}}
}

// GetInstanceTypes returns the catalog instance types offered in the NodePool's NodeClass footprint.
func (c *CloudProvider) GetInstanceTypes(ctx context.Context, nodePool *karpv1.NodePool) ([]*cloudprovider.InstanceType, error) {
	if !c.poolFilter.Matches(labels.Set(nodePool.Labels)) {
		return nil, nil
	}
	ref := nodePool.Spec.Template.Spec.NodeClassRef
	if ref == nil {
		return nil, nil
	}
	nodeClass, err := c.nodeClass(ctx, ref.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return c.instanceTypesFor(ctx, nodeClass)
}

func (c *CloudProvider) instanceTypesFor(ctx context.Context, nodeClass *v1alpha1.CloudVMNodeClass) ([]*cloudprovider.InstanceType, error) {
	regions := make([]string, 0, len(nodeClass.Spec.Footprint))
	for _, r := range nodeClass.Spec.Footprint {
		regions = append(regions, r.Region)
	}
	catalog, err := c.catalog.Get(ctx, regions)
	if err != nil {
		return nil, err
	}
	return c.instanceTypes(catalog, nodeClass)
}

// Create launches one VM for the cheapest launchable offering compatible with the NodeClaim.
func (c *CloudProvider) Create(ctx context.Context, nc *karpv1.NodeClaim) (*karpv1.NodeClaim, error) {
	nodeClass, err := c.nodeClassFor(ctx, nc)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, cloudprovider.NewInsufficientCapacityError(fmt.Errorf("resolving nodeclass: %w", err))
		}
		return nil, fmt.Errorf("resolving nodeclass: %w", err)
	}
	if ready := nodeClass.StatusConditions().Get(status.ConditionReady); ready.IsFalse() {
		return nil, cloudprovider.NewNodeClassNotReadyError(errors.New(ready.Message))
	}
	its, err := c.instanceTypesFor(ctx, nodeClass)
	if err != nil {
		return nil, err
	}
	reqs := scheduling.NewNodeSelectorRequirementsWithMinValues(nc.Spec.Requirements...)
	it, offering := cheapestOffering(its, reqs)
	if offering == nil {
		return nil, cloudprovider.NewInsufficientCapacityError(fmt.Errorf("no launchable offering matches the nodeclaim requirements"))
	}
	region := offering.Requirements.Get(corev1.LabelTopologyRegion).Any()
	zone := offering.Zone()
	capacityType := offering.CapacityType()
	fr, _ := nodeClass.Region(region)

	nodeLabels := resolvedLabels(it, offering)
	maps.Copy(nodeLabels, nc.Labels)
	nodeLabels[corev1.LabelHostname] = nc.Name
	launch := launchAnnotations(nodeClass, fr)
	req := &nodeclaim.CreateRequest{
		Name:         nc.Name,
		InstanceType: it.Name,
		Region:       region,
		Zone:         pluginZone(region, zone),
		CapacityType: capacityType,
		Labels:       nodeLabels,
		Taints:       pluginTaints(append(slices.Clone(nc.Spec.Taints), nc.Spec.StartupTaints...)),
		Annotations:  launch,
	}
	created, err := c.api.Create(ctx, req)
	if err != nil {
		if nodeclaim.IsInsufficientCapacity(err) {
			c.unavailable.MarkUnavailable(it.Name, zone, capacityType)
			return nil, cloudprovider.NewInsufficientCapacityError(fmt.Errorf("launching %s in %s (%s): %w", it.Name, zone, capacityType, err))
		}
		return nil, fmt.Errorf("launching %s in %s (%s): %w", it.Name, zone, capacityType, err)
	}
	log.FromContext(ctx).V(1).Info("launched cloudvm instance", "instance-type", it.Name, "zone", zone, "capacity-type", capacityType, "price", offering.Price, "provider-id", created.ProviderID)

	out := c.toNodeClaim(created, it)
	resolved := resolvedLabels(it, offering)
	maps.Copy(resolved, out.Labels)
	out.Labels = resolved
	out.Annotations = map[string]string{AnnotationNodeClassHash: hashLaunch(launch)}
	return out, nil
}

// cheapestOffering returns the cheapest launchable offering compatible with reqs among the instance types the
// requirements allow, breaking price ties by instance type and zone name for a stable choice.
func cheapestOffering(its []*cloudprovider.InstanceType, reqs scheduling.Requirements) (*cloudprovider.InstanceType, *cloudprovider.Offering) {
	var bestIT *cloudprovider.InstanceType
	var best *cloudprovider.Offering
	allowed := reqs.Get(corev1.LabelInstanceTypeStable)
	for _, it := range its {
		if !allowed.Has(it.Name) {
			continue
		}
		for _, o := range it.Offerings.Launchable().Compatible(reqs) {
			if best == nil || cmp.Or(
				cmp.Compare(o.Price, best.Price),
				strings.Compare(it.Name, bestIT.Name),
				strings.Compare(o.Zone(), best.Zone()),
				strings.Compare(o.CapacityType(), best.CapacityType()),
			) < 0 {
				bestIT, best = it, o
			}
		}
	}
	return bestIT, best
}

// Delete starts terminating the NodeClaim's VM, keyed by the NodeClaim name the VM launched under; it returns
// NodeClaimNotFoundError once the VM is gone, which ends Karpenter's retries.
func (c *CloudProvider) Delete(ctx context.Context, nc *karpv1.NodeClaim) error {
	if err := c.api.Delete(ctx, nc.Name); err != nil {
		if nodeclaim.IsNotFound(err) {
			return cloudprovider.NewNodeClaimNotFoundError(err)
		}
		return fmt.Errorf("terminating instance %s: %w", nc.Name, err)
	}
	return nil
}

// Get returns the NodeClaim backed by the VM with the providerID.
func (c *CloudProvider) Get(ctx context.Context, providerID string) (*karpv1.NodeClaim, error) {
	if _, _, _, err := nodeclaim.ParseProviderID(providerID); err != nil {
		return nil, cloudprovider.NewNodeClaimNotFoundError(err)
	}
	got, err := c.api.GetByProviderID(ctx, providerID)
	if err != nil {
		if nodeclaim.IsNotFound(err) {
			return nil, cloudprovider.NewNodeClaimNotFoundError(err)
		}
		return nil, fmt.Errorf("getting instance %s: %w", providerID, err)
	}
	return c.toNodeClaim(got, nil), nil
}

// List returns a NodeClaim for every VM the plugin launched through the NodeClaim API.
func (c *CloudProvider) List(ctx context.Context) ([]*karpv1.NodeClaim, error) {
	items, err := c.api.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing instances: %w", err)
	}
	out := make([]*karpv1.NodeClaim, 0, len(items))
	for i := range items {
		out = append(out, c.toNodeClaim(&items[i], nil))
	}
	return out, nil
}

// IsDrifted reports a NodeClaim whose region or zone left the NodeClass footprint, or whose launch settings changed.
func (c *CloudProvider) IsDrifted(ctx context.Context, nc *karpv1.NodeClaim) (cloudprovider.DriftReason, error) {
	nodeClass, err := c.nodeClassFor(ctx, nc)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("resolving nodeclass: %w", err)
	}
	region := nc.Labels[corev1.LabelTopologyRegion]
	if region == "" {
		return "", nil
	}
	fr, ok := nodeClass.Region(region)
	if !ok {
		return DriftRegionRemoved, nil
	}
	if zone := nc.Labels[corev1.LabelTopologyZone]; len(fr.Zones) > 0 && zone != region && !slices.Contains(fr.Zones, zone) {
		return DriftZoneRemoved, nil
	}
	if want, ok := nc.Annotations[AnnotationNodeClassHash]; ok && want != hashLaunch(launchAnnotations(nodeClass, fr)) {
		return DriftNodeClass, nil
	}
	return "", nil
}

// Reboot is not supported: a CloudVM VM is replaced, never restarted in place. It also keeps this provider from
// acting on another provider's NodeClaims, which Karpenter's reboot controller does not filter.
func (c *CloudProvider) Reboot(context.Context, *karpv1.NodeClaim, string) error {
	return cloudprovider.NewNodeRebootNotImplementedError()
}

// RepairPolicies replaces nodes whose agent stops reporting Ready for ten minutes.
func (c *CloudProvider) RepairPolicies() []cloudprovider.RepairPolicy {
	return []cloudprovider.RepairPolicy{
		{
			ConditionType:          corev1.NodeReady,
			ConditionStatus:        corev1.ConditionFalse,
			TolerationDuration:     10 * time.Minute,
			TerminationGracePeriod: new(time.Duration(0)),
			Action:                 cloudprovider.ReplaceNode,
		},
		{
			ConditionType:          corev1.NodeReady,
			ConditionStatus:        corev1.ConditionUnknown,
			ReasonRegex:            ".*",
			TolerationDuration:     10 * time.Minute,
			TerminationGracePeriod: new(time.Duration(0)),
			Action:                 cloudprovider.ReplaceNode,
		},
	}
}

func (c *CloudProvider) nodeClass(ctx context.Context, name string) (*v1alpha1.CloudVMNodeClass, error) {
	nodeClass := &v1alpha1.CloudVMNodeClass{}
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: name}, nodeClass); err != nil {
		return nil, err
	}
	return nodeClass, nil
}

func (c *CloudProvider) nodeClassFor(ctx context.Context, nc *karpv1.NodeClaim) (*v1alpha1.CloudVMNodeClass, error) {
	if nc.Spec.NodeClassRef == nil {
		return nil, apierrors.NewNotFound(v1alpha1.GroupVersion.WithResource("cloudvmnodeclasses").GroupResource(), "")
	}
	return c.nodeClass(ctx, nc.Spec.NodeClassRef.Name)
}

// toNodeClaim converts a plugin claim; it is the shape Create, Get and List return to Karpenter.
func (c *CloudProvider) toNodeClaim(src *nodeclaim.NodeClaim, it *cloudprovider.InstanceType) *karpv1.NodeClaim {
	nodeLabels := maps.Clone(src.Labels)
	if nodeLabels == nil {
		nodeLabels = map[string]string{}
	}
	nodeLabels[corev1.LabelInstanceTypeStable] = src.InstanceType
	nodeLabels[corev1.LabelTopologyRegion] = src.Region
	// The zone label names the offering the claim launched from, which is the region for an offering whose zone
	// the cloud picks; Karpenter prices a NodeClaim by the offering its labels name.
	nodeLabels[corev1.LabelTopologyZone] = cmp.Or(nodeLabels[corev1.LabelTopologyZone], src.Zone, src.Region)
	nodeLabels[karpv1.CapacityTypeLabelKey] = src.CapacityType

	// The claim's capacity is what the node agent reports on the Node, so the NodeClaim states the same resources.
	// Nodes reserve nothing for system daemons, so allocatable matches capacity.
	capacity := corev1.ResourceList{}
	for k, v := range src.Capacity {
		if q, err := resource.ParseQuantity(v); err == nil {
			capacity[corev1.ResourceName(k)] = q
		}
	}
	if len(capacity) == 0 && it != nil {
		capacity = it.Capacity.DeepCopy()
	}
	// A claim whose plugin has not read its disk yet still has the disk its instance type was planned with; without
	// it Karpenter takes the launched node for one that cannot hold the pods it was launched for.
	if _, ok := capacity[corev1.ResourceEphemeralStorage]; !ok && it != nil {
		if q, ok := it.Capacity[corev1.ResourceEphemeralStorage]; ok {
			capacity[corev1.ResourceEphemeralStorage] = q
		}
	}
	allocatable := capacity.DeepCopy()
	return &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:              src.Name,
			Labels:            nodeLabels,
			CreationTimestamp: metav1.NewTime(src.CreatedAt),
		},
		Status: karpv1.NodeClaimStatus{
			ProviderID:  src.ProviderID,
			NodeName:    src.Name,
			Capacity:    capacity,
			Allocatable: allocatable,
		},
	}
}

// resolvedLabels returns the single-valued labels of an instance type and the chosen offering.
func resolvedLabels(it *cloudprovider.InstanceType, offering *cloudprovider.Offering) map[string]string {
	out := map[string]string{}
	for _, reqs := range []scheduling.Requirements{it.Requirements, offering.Requirements} {
		for key, r := range reqs {
			if r.Operator() == corev1.NodeSelectorOpIn && r.Len() == 1 {
				out[key] = r.Any()
			}
		}
	}
	return out
}

// launchAnnotations maps NodeClass launch settings for a region onto the plugin's cloudvm.vk.io/* launch keys.
func launchAnnotations(nodeClass *v1alpha1.CloudVMNodeClass, fr v1alpha1.FootprintRegion) map[string]string {
	out := maps.Clone(nodeClass.Spec.Annotations)
	if out == nil {
		out = map[string]string{}
	}
	set := func(k, v string) {
		if v != "" {
			out[k] = v
		}
	}
	set(annotationProvider, fr.Provider)
	set(annotationSubnets, strings.Join(fr.SubnetIDs, ","))
	if fr.LaunchTemplateID != "" {
		set(annotationLaunchTemplateID, fr.LaunchTemplateID)
	} else if fr.Provider == "aws" {
		set(annotationLaunchTemplateName, nodeClass.Spec.LaunchTemplateName)
	}
	return out
}

// hashLaunch fingerprints the launch settings a VM was started with; encoding/json sorts map keys.
func hashLaunch(launch map[string]string) string {
	raw, _ := json.Marshal(launch)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:8])
}

// pluginZone returns the zone to pin, or "" when the offering lets the cloud choose the zone.
func pluginZone(region, zone string) string {
	if zone == region {
		return ""
	}
	return zone
}

func pluginTaints(taints []corev1.Taint) []nodeclaim.Taint {
	out := make([]nodeclaim.Taint, 0, len(taints))
	for _, t := range taints {
		out = append(out, nodeclaim.Taint{Key: t.Key, Value: t.Value, Effect: string(t.Effect)})
	}
	return out
}
