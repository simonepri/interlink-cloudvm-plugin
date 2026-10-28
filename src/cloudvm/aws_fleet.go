// Package cloudvm provides the AWS EC2 Fleet engine for launching opportunistic spot and on-demand GPU instances.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
)

// FleetAllocationStrategy specifies the spot allocation behavior.
type FleetAllocationStrategy string

const (
	StrategyPriceCapacityOptimized FleetAllocationStrategy = "price-capacity-optimized"
	StrategyCapacityOptimized      FleetAllocationStrategy = "capacity-optimized"
	StrategyLowestPrice            FleetAllocationStrategy = "lowest-price"
)

// GPUCountForInstanceType returns the number of physical GPUs/accelerators for an AWS instance type.
// CPU instance families (such as c6i, m6i, r6i) return 0.
func GPUCountForInstanceType(itype string) int {
	switch {
	// 8-GPU accelerators
	case strings.HasPrefix(itype, "p5.") || strings.HasPrefix(itype, "p5e."):
		return 8
	case strings.HasPrefix(itype, "p4d.") || strings.HasPrefix(itype, "p4de."):
		return 8
	case strings.HasPrefix(itype, "p3.16xlarge") || strings.HasPrefix(itype, "p3dn.24xlarge"):
		return 8
	case strings.HasPrefix(itype, "g5.48xlarge") || strings.HasPrefix(itype, "g6.48xlarge") || strings.HasPrefix(itype, "g6e.48xlarge"):
		return 8
	case itype == "g4dn.metal":
		return 8
	// 4-GPU accelerators
	case strings.HasPrefix(itype, "p3.8xlarge"):
		return 4
	case strings.HasPrefix(itype, "g5.12xlarge") || strings.HasPrefix(itype, "g5.24xlarge"):
		return 4
	case strings.HasPrefix(itype, "g6.12xlarge") || strings.HasPrefix(itype, "g6.24xlarge"):
		return 4
	case strings.HasPrefix(itype, "g6e.12xlarge") || strings.HasPrefix(itype, "g6e.24xlarge"):
		return 4
	case strings.HasPrefix(itype, "g4dn.12xlarge"):
		return 4
	// 1-GPU accelerators
	case strings.HasPrefix(itype, "p3.2xlarge"):
		return 1
	case strings.HasPrefix(itype, "g5."):
		return 1
	case strings.HasPrefix(itype, "g6."):
		return 1
	case strings.HasPrefix(itype, "g6e."):
		return 1
	case strings.HasPrefix(itype, "g4dn."):
		return 1
	default:
		return 0
	}
}

// FleetOverride defines a single instance type / subnet combination with weighted capacity.
type FleetOverride struct {
	InstanceType     string  `json:"instanceType"`
	SubnetID         string  `json:"subnetId,omitempty"`
	WeightedCapacity float64 `json:"weightedCapacity"`
}

// FleetRequest represents a spot or on-demand request across multiple instance types and AZs.
type FleetRequest struct {
	Region             string                  `json:"region"`
	TargetCapacity     int                     `json:"targetCapacity"`
	AllocationStrategy FleetAllocationStrategy `json:"allocationStrategy"`
	InstanceTypes      []string                `json:"instanceTypes"`
	SubnetIDs          []string                `json:"subnetIds,omitempty"`
	SecurityGroupIDs   []string                `json:"securityGroupIds,omitempty"`
	ClusterTag         string                  `json:"clusterTag"`
	PodName            string                  `json:"podName"`
	PodNamespace       string                  `json:"podNamespace"`
	PodUID             string                  `json:"podUid"`
	CostCenter         string                  `json:"costCenter,omitempty"`
	JobID              string                  `json:"jobId,omitempty"`
	UserData           string                  `json:"userData,omitempty"`
	LaunchTemplateID   string                  `json:"launchTemplateId,omitempty"`
	LaunchTemplateName string                  `json:"launchTemplateName,omitempty"`
	// LaunchTemplates, when set, replaces LaunchTemplateID and LaunchTemplateName with one fleet config per template.
	LaunchTemplates []fleetTemplateGroup `json:"-"`
	MarketType      string               `json:"marketType,omitempty"`
	MaxPrice        string               `json:"maxPrice,omitempty"`
	PricePolicy     string               `json:"pricePolicy,omitempty"`
	RunnerAuthToken string               `json:"runnerAuthToken,omitempty"`
}

// FleetResponse represents the result of a fleet launch.
type FleetResponse struct {
	FleetID       string      `json:"fleetId"`
	Region        string      `json:"region"`
	InstanceIDs   []string    `json:"instanceIds"`
	TargetGPU     int         `json:"targetGpu"`
	ActiveWorkers int         `json:"activeWorkers"`
	Instances     []*Instance `json:"instances"`
	Errors        []string    `json:"errors,omitempty"`
}

// fleetTemplateConfigs returns the fleet's launch template configs: one per template group, each with the overrides of
// its instance types, or the single ltSpec with every override when the request names no groups.
func fleetTemplateConfigs(req *FleetRequest, ltSpec *ec2types.FleetLaunchTemplateSpecificationRequest, overrides []ec2types.FleetLaunchTemplateOverridesRequest) []ec2types.FleetLaunchTemplateConfigRequest {
	if len(req.LaunchTemplates) == 0 {
		return []ec2types.FleetLaunchTemplateConfigRequest{{LaunchTemplateSpecification: ltSpec, Overrides: overrides}}
	}
	var configs []ec2types.FleetLaunchTemplateConfigRequest
	for _, g := range req.LaunchTemplates {
		spec := &ec2types.FleetLaunchTemplateSpecificationRequest{
			Version:                             ltSpec.Version,
			LaunchTemplateSpecificationUserData: ltSpec.LaunchTemplateSpecificationUserData,
		}
		if g.ID != "" {
			spec.LaunchTemplateId = aws.String(g.ID)
		} else {
			spec.LaunchTemplateName = aws.String(g.Name)
		}
		types := make(map[string]bool, len(g.InstanceTypes))
		for _, t := range g.InstanceTypes {
			types[t] = true
		}
		var own []ec2types.FleetLaunchTemplateOverridesRequest
		for _, o := range overrides {
			if types[string(o.InstanceType)] {
				own = append(own, o)
			}
		}
		if len(own) > 0 {
			configs = append(configs, ec2types.FleetLaunchTemplateConfigRequest{LaunchTemplateSpecification: spec, Overrides: own})
		}
	}
	return configs
}

// RegionalClusterFact stores dynamic Launch Template and subnet configuration for a specific AWS region.
type RegionalClusterFact struct {
	LaunchTemplateID string `json:"launchTemplateId,omitempty"`
	// GPULaunchTemplateID names the template whose image ships the NVIDIA driver and container toolkit; pods requesting GPUs use it.
	GPULaunchTemplateID string `json:"gpuLaunchTemplateId,omitempty"`
	// LaunchTemplates lists the region's templates in the order launches try them, each with the instance requirements
	// a type must satisfy to boot from it; when set, it replaces launchTemplateId, launchTemplateName, and gpuLaunchTemplateId.
	LaunchTemplates    []LaunchTemplateFact `json:"launchTemplates,omitempty"`
	LaunchTemplateName string               `json:"launchTemplateName,omitempty"`
	SubnetIDs          []string             `json:"subnetIds,omitempty"`
	SecurityGroupIDs   []string             `json:"securityGroupIds,omitempty"`
	// TunnelTransport is the path the region's workers take to the tunnel gateway; empty means tailnet.
	TunnelTransport string `json:"tunnelTransport,omitempty"`
	// DirectCIDRs are the private networks the region's workers reach each other on without the tunnel.
	DirectCIDRs []string `json:"directCidrs,omitempty"`
	// ImageMirrors maps an upstream registry host to the mirror reference prefix the region's workers pull from instead.
	ImageMirrors map[string]string `json:"imageMirrors,omitempty"`
	// NamespaceFirewalls maps a pod namespace to the firewall its workers join so they reach only workers of the same
	// namespace: a security group ID on AWS, a network tag on GCP. When set, pods of unlisted namespaces do not launch.
	NamespaceFirewalls map[string]string `json:"namespaceFirewalls,omitempty"`
}

// ClusterFacts holds the regional deployment facts for dynamic Launch Template and subnet selection.
type ClusterFacts struct {
	ClusterTag string                         `json:"clusterTag,omitempty"`
	Regions    map[string]RegionalClusterFact `json:"regions,omitempty"`
}

// EC2FleetClient abstracts the AWS EC2 client for unit testing.
type EC2FleetClient interface {
	CreateFleet(ctx context.Context, params *ec2.CreateFleetInput, optFns ...func(*ec2.Options)) (*ec2.CreateFleetOutput, error)
	DescribeInstances(ctx context.Context, params *ec2.DescribeInstancesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
	TerminateInstances(ctx context.Context, params *ec2.TerminateInstancesInput, optFns ...func(*ec2.Options)) (*ec2.TerminateInstancesOutput, error)
	DescribeInstanceTypes(ctx context.Context, params *ec2.DescribeInstanceTypesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error)
	DescribeInstanceTypeOfferings(ctx context.Context, params *ec2.DescribeInstanceTypeOfferingsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstanceTypeOfferingsOutput, error)
	DescribeSubnets(ctx context.Context, params *ec2.DescribeSubnetsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error)
	DescribeLaunchTemplateVersions(ctx context.Context, params *ec2.DescribeLaunchTemplateVersionsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeLaunchTemplateVersionsOutput, error)
	DescribeImages(ctx context.Context, params *ec2.DescribeImagesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error)
	ModifyInstanceAttribute(ctx context.Context, params *ec2.ModifyInstanceAttributeInput, optFns ...func(*ec2.Options)) (*ec2.ModifyInstanceAttributeOutput, error)
}

var (
	fleetRetryMu        sync.RWMutex
	fleetInitialBackoff = 100 * time.Millisecond
	fleetMaxBackoff     = 3 * time.Second
	fleetMaxRetries     = 5
)

func getFleetRetryParams() (time.Duration, time.Duration, int) {
	fleetRetryMu.RLock()
	defer fleetRetryMu.RUnlock()
	return fleetInitialBackoff, fleetMaxBackoff, fleetMaxRetries
}

// SetFleetRetryParams allows overriding backoff and max retries for testing.
func SetFleetRetryParams(initialBackoff, maxBackoff time.Duration, maxRetries int) {
	fleetRetryMu.Lock()
	defer fleetRetryMu.Unlock()
	fleetInitialBackoff = initialBackoff
	fleetMaxBackoff = maxBackoff
	fleetMaxRetries = maxRetries
}

// ResetFleetRetryParams resets retry parameters to default values.
func ResetFleetRetryParams() {
	fleetRetryMu.Lock()
	defer fleetRetryMu.Unlock()
	fleetInitialBackoff = 100 * time.Millisecond
	fleetMaxBackoff = 3 * time.Second
	fleetMaxRetries = 5
}

func jitterDuration(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	// Add up to 25% jitter: [base, base + base/4]
	extraMax := int64(base / 4)
	if extraMax <= 0 {
		return base
	}
	n, err := rand.Int(rand.Reader, big.NewInt(extraMax))
	if err != nil {
		return base
	}
	return base + time.Duration(n.Int64())
}

func isRequestLimitExceeded(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode() == "RequestLimitExceeded" {
			return true
		}
	}
	return strings.Contains(err.Error(), "RequestLimitExceeded")
}

// DescribeAllInstances paginates through all pages of ec2.DescribeInstances using NextToken,
// aggregating all reservations across pages into a single DescribeInstancesOutput.
func DescribeAllInstances(ctx context.Context, client EC2FleetClient, input *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
	if client == nil {
		return nil, fmt.Errorf("ec2 fleet client is nil")
	}

	var req ec2.DescribeInstancesInput
	if input != nil {
		req = *input
	}

	var allReservations []ec2types.Reservation
	for {
		out, err := client.DescribeInstances(ctx, &req)
		if err != nil {
			return nil, err
		}
		if out != nil {
			allReservations = append(allReservations, out.Reservations...)
			if out.NextToken == nil || *out.NextToken == "" {
				break
			}
			req.NextToken = out.NextToken
		} else {
			break
		}
	}

	return &ec2.DescribeInstancesOutput{
		Reservations: allReservations,
	}, nil
}

// BuildFleetOverrides constructs the launch template overrides for EC2 Fleet.
// In 1 Pod = 1 VM architecture, candidate shapes represent equivalent alternative VM configurations.
func BuildFleetOverrides(instanceTypes []string, subnetIDs []string) []ec2types.FleetLaunchTemplateOverridesRequest {
	var overrides []ec2types.FleetLaunchTemplateOverridesRequest
	subnets := subnetIDs
	if len(subnets) == 0 {
		subnets = []string{""}
	}

	for _, itype := range instanceTypes {
		for _, subnet := range subnets {
			ov := ec2types.FleetLaunchTemplateOverridesRequest{
				InstanceType:     ec2types.InstanceType(itype),
				WeightedCapacity: aws.Float64(1.0),
			}
			if subnet != "" {
				ov.SubnetId = aws.String(subnet)
			}
			overrides = append(overrides, ov)
		}
	}
	return overrides
}

// LaunchFleetWithClient invokes AWS CreateFleet using a custom client.
func LaunchFleetWithClient(ctx context.Context, client EC2FleetClient, req *FleetRequest, overrides []ec2types.FleetLaunchTemplateOverridesRequest) (*FleetResponse, error) {
	if req.PodUID == "" {
		return nil, fmt.Errorf("pod UID is required for idempotent fleet creation")
	}

	strategy := req.AllocationStrategy
	if strategy == "" {
		strategy = StrategyPriceCapacityOptimized
	}

	targetCapType := ec2types.DefaultTargetCapacityTypeSpot
	var spotOpts *ec2types.SpotOptionsRequest
	var onDemandOpts *ec2types.OnDemandOptionsRequest

	if req.MarketType == "on-demand" {
		targetCapType = ec2types.DefaultTargetCapacityTypeOnDemand
		onDemandOpts = &ec2types.OnDemandOptionsRequest{
			AllocationStrategy: ec2types.FleetOnDemandAllocationStrategyLowestPrice,
		}
		if req.MaxPrice != "" {
			onDemandOpts.MaxTotalPrice = aws.String(req.MaxPrice)
		}
	} else {
		spotOpts = &ec2types.SpotOptionsRequest{
			AllocationStrategy: ec2types.SpotAllocationStrategy(strategy),
		}
		if req.MaxPrice != "" {
			spotOpts.MaxTotalPrice = aws.String(req.MaxPrice)
		}
	}

	// Put UID first so truncation never cuts off uniqueness.
	tokenBase := fmt.Sprintf("cvm-%s-%s-%s", req.PodUID, req.PodNamespace, req.PodName)
	if len(tokenBase) > 64 {
		tokenBase = tokenBase[:64]
	}
	clientToken := tokenBase

	// Target capacity is 1 instance (1 pod = 1 VM)
	targetCapacity := int32(1)

	ltSpec := &ec2types.FleetLaunchTemplateSpecificationRequest{
		Version: aws.String("$Latest"),
	}
	switch {
	case req.LaunchTemplateID != "":
		ltSpec.LaunchTemplateId = aws.String(req.LaunchTemplateID)
	case req.LaunchTemplateName != "":
		ltSpec.LaunchTemplateName = aws.String(req.LaunchTemplateName)
	default:
		ltSpec.LaunchTemplateName = aws.String(defaultLaunchTemplateName)
	}
	if req.UserData != "" {
		ltSpec.LaunchTemplateSpecificationUserData = aws.String(req.UserData)
	}

	tags := []ec2types.Tag{
		{Key: aws.String("ManagedBy"), Value: aws.String("interlink-cloudvm-plugin")},
		{Key: aws.String("ClusterTag"), Value: aws.String(req.ClusterTag)},
		{Key: aws.String("PodName"), Value: aws.String(req.PodName)},
		{Key: aws.String("Namespace"), Value: aws.String(req.PodNamespace)},
		{Key: aws.String("PodUID"), Value: aws.String(req.PodUID)},
	}
	if req.CostCenter != "" {
		tags = append(tags, ec2types.Tag{Key: aws.String("CostCenter"), Value: aws.String(req.CostCenter)})
	}
	if req.JobID != "" {
		tags = append(tags, ec2types.Tag{Key: aws.String("JobID"), Value: aws.String(req.JobID)})
	}

	input := &ec2.CreateFleetInput{
		Type:        ec2types.FleetTypeInstant,
		ClientToken: aws.String(clientToken),
		TargetCapacitySpecification: &ec2types.TargetCapacitySpecificationRequest{
			TotalTargetCapacity:       aws.Int32(targetCapacity),
			DefaultTargetCapacityType: targetCapType,
		},
		SpotOptions:           spotOpts,
		OnDemandOptions:       onDemandOpts,
		LaunchTemplateConfigs: fleetTemplateConfigs(req, ltSpec, overrides),
		TagSpecifications: []ec2types.TagSpecification{
			{
				ResourceType: ec2types.ResourceTypeInstance,
				Tags:         tags,
			},
			{
				ResourceType: ec2types.ResourceTypeVolume,
				Tags:         tags,
			},
		},
	}

	limiter := getFleetRateLimiter(req.Region)
	initBackoff, maxBackoff, maxRetries := getFleetRetryParams()
	backoff := initBackoff

	var out *ec2.CreateFleetOutput
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			sleepDur := jitterDuration(backoff)
			cleanRegion := sanitizeLog(req.Region)
			log.Printf("[aws] CreateFleet throttled for region %s (%v), attempt %d/%d; retrying after %v",
				cleanRegion, lastErr, attempt, maxRetries, sleepDur) //nolint:gosec // sanitized
			timer := time.NewTimer(sleepDur)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		if err := limiter.Wait(ctx); err != nil {
			return nil, fmt.Errorf("rate limiter wait failed: %w", err)
		}

		out, lastErr = client.CreateFleet(ctx, input)
		if lastErr == nil {
			break
		}

		if !isRequestLimitExceeded(lastErr) {
			return nil, fmt.Errorf("ec2:CreateFleet call failed: %w", lastErr)
		}
	}

	if lastErr != nil {
		return nil, fmt.Errorf("ec2:CreateFleet call failed after %d retries: %w", maxRetries, lastErr)
	}

	if out == nil {
		return nil, fmt.Errorf("ec2:CreateFleet returned nil output")
	}

	resp := &FleetResponse{
		FleetID:   aws.ToString(out.FleetId),
		Region:    req.Region,
		TargetGPU: req.TargetCapacity,
	}

	for _, errItem := range out.Errors {
		resp.Errors = append(resp.Errors, fmt.Sprintf("%s: %s",
			aws.ToString(errItem.ErrorCode), aws.ToString(errItem.ErrorMessage)))
	}

	for _, instItem := range out.Instances {
		for _, instID := range instItem.InstanceIds {
			resp.InstanceIDs = append(resp.InstanceIDs, instID)
			itype := string(instItem.InstanceType)
			resp.ActiveWorkers++
			resp.Instances = append(resp.Instances, &Instance{
				ID:           instID,
				Provider:     ProviderAWS,
				Region:       req.Region,
				InstanceType: itype,
				State:        StateRunning,
				CreatedAt:    time.Now(),
				Metadata: map[string]string{
					"fleetId":   resp.FleetID,
					"podName":   req.PodName,
					"namespace": req.PodNamespace,
					"podUID":    req.PodUID,
				},
			})
		}
	}

	return resp, nil
}

// AWSProvider implements Provider for AWS EC2 Fleet.
type AWSProvider struct {
	Region            string
	configuredRegions []string
	clients           map[string]EC2FleetClient
	ec2Clients        map[string]*ec2.Client
	clusterFacts      *ClusterFacts
	regionStore       *RegionStore
	instanceRegions   map[string]string
	missingCount      map[string]int
	offeringCache     *offeringCache
	regionScorer      RegionPlacementScorer
	quotaUsage        func(provider, region, quota string) float64
	client            EC2FleetClient
	mu                sync.RWMutex
}

// NewAWSProvider creates an AWS provider instance.
func NewAWSProvider(region string, additionalRegions ...string) *AWSProvider {
	if region == "" {
		region = "us-east-1"
	}
	storePath := os.Getenv("REGION_STORE_PATH")
	if storePath == "" {
		storePath = filepath.Join(os.TempDir(), "cloudvm-instance-regions.json")
	}
	regStore, err := NewRegionStore(storePath)
	if err != nil {
		cleanStorePath := filepath.Clean(storePath)
		log.Printf("[aws] Warning: failed to initialize region store at %s: %v, falling back to temp file", cleanStorePath, err) //nolint:gosec // local config path
		regStore, _ = NewRegionStore("")
	}

	initialRegions := make(map[string]string)
	if regStore != nil {
		initialRegions = regStore.GetAll()
	}

	var confRegions []string
	seen := make(map[string]bool)
	confRegions = append(confRegions, region)
	seen[region] = true
	for _, r := range additionalRegions {
		r = strings.TrimSpace(r)
		if r != "" && !seen[r] {
			confRegions = append(confRegions, r)
			seen[r] = true
		}
	}

	facts := &ClusterFacts{
		Regions: make(map[string]RegionalClusterFact),
	}
	if rawFacts := os.Getenv("CLUSTER_FACTS"); rawFacts != "" {
		_ = json.Unmarshal([]byte(rawFacts), facts)
	}
	if facts.Regions == nil {
		facts.Regions = make(map[string]RegionalClusterFact)
	}

	return &AWSProvider{
		Region:            region,
		configuredRegions: confRegions,
		clients:           make(map[string]EC2FleetClient),
		ec2Clients:        make(map[string]*ec2.Client),
		clusterFacts:      facts,
		regionStore:       regStore,
		instanceRegions:   initialRegions,
	}
}

// SetClusterFacts updates the regional cluster facts.
func (p *AWSProvider) SetClusterFacts(facts *ClusterFacts) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clusterFacts = facts
}

// ClusterFacts returns a copy of the current regional cluster facts.
func (p *AWSProvider) ClusterFacts() *ClusterFacts {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.clusterFacts == nil {
		return nil
	}
	cp := &ClusterFacts{
		ClusterTag: p.clusterFacts.ClusterTag,
		Regions:    make(map[string]RegionalClusterFact, len(p.clusterFacts.Regions)),
	}
	for k, v := range p.clusterFacts.Regions {
		cp.Regions[k] = v
	}
	return cp
}

// SetRegionalLaunchTemplate configures the regional launch template (ID or Name) for a region.
func (p *AWSProvider) SetRegionalLaunchTemplate(region, lt string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.clusterFacts == nil {
		p.clusterFacts = &ClusterFacts{Regions: make(map[string]RegionalClusterFact)}
	}
	if p.clusterFacts.Regions == nil {
		p.clusterFacts.Regions = make(map[string]RegionalClusterFact)
	}
	fact := p.clusterFacts.Regions[region]
	if strings.HasPrefix(lt, "lt-") {
		fact.LaunchTemplateID = lt
	} else {
		fact.LaunchTemplateName = lt
	}
	p.clusterFacts.Regions[region] = fact
}

// SetRegionalSubnets configures the regional subnets for a region.
func (p *AWSProvider) SetRegionalSubnets(region string, subnets []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.clusterFacts == nil {
		p.clusterFacts = &ClusterFacts{Regions: make(map[string]RegionalClusterFact)}
	}
	if p.clusterFacts.Regions == nil {
		p.clusterFacts.Regions = make(map[string]RegionalClusterFact)
	}
	fact := p.clusterFacts.Regions[region]
	fact.SubnetIDs = subnets
	p.clusterFacts.Regions[region] = fact
}

// configuredTemplates returns the launch templates cluster facts name for a region, in the order launches try them.
func (p *AWSProvider) configuredTemplates(region string) []LaunchTemplateFact {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.clusterFacts == nil || p.clusterFacts.Regions == nil {
		return nil
	}
	return regionTemplates(p.clusterFacts.Regions[region])
}

// defaultLaunchTemplateName is the template launches use in a region whose cluster facts name none.
const defaultLaunchTemplateName = "interlink-cloudvm-worker"

// regionTemplates returns the region's launch templates from cluster facts, in the order launches try them, or the
// default template when the facts name none.
func (p *AWSProvider) regionTemplates(region string) []LaunchTemplateFact {
	templates := p.configuredTemplates(region)
	if len(templates) == 0 {
		templates = []LaunchTemplateFact{{Name: defaultLaunchTemplateName}}
	}
	return templates
}

// ResolveRegionalSubnets resolves subnet IDs for a region from cluster facts.
func (p *AWSProvider) ResolveRegionalSubnets(region string) []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.clusterFacts == nil || p.clusterFacts.Regions == nil {
		return nil
	}
	fact, ok := p.clusterFacts.Regions[region]
	if !ok || len(fact.SubnetIDs) == 0 {
		return nil
	}
	res := make([]string, len(fact.SubnetIDs))
	copy(res, fact.SubnetIDs)
	return res
}

// SetEC2Client registers a concrete *ec2.Client into the multi-region client pool.
// namespaceFirewall returns the security group a pod of namespace joins in region: "" when the region's cluster facts
// list no namespace firewalls, and errNamespaceFirewallMissing when they list some but none for namespace.
func (p *AWSProvider) namespaceFirewall(region, namespace string) (string, error) {
	p.mu.RLock()
	var firewalls map[string]string
	if p.clusterFacts != nil {
		firewalls = p.clusterFacts.Regions[region].NamespaceFirewalls
	}
	p.mu.RUnlock()
	sg, err := namespaceFirewall(firewalls, region, namespace)
	if err != nil {
		return "", err
	}
	if sg != "" && !securityGroupIDPattern.MatchString(sg) {
		return "", fmt.Errorf("region %s: namespace firewall %q of namespace %q is not a security group ID", region, sg, namespace)
	}
	return sg, nil
}

// attachNamespaceFirewall adds sg to the security groups the instance booted with from its launch template; EC2 Fleet
// overrides cannot name security groups, so the group joins after launch.
func attachNamespaceFirewall(ctx context.Context, client EC2FleetClient, instanceID string, booted []ec2types.GroupIdentifier, sg string) error {
	groups := []string{sg}
	for _, g := range booted {
		if id := aws.ToString(g.GroupId); id != "" && id != sg {
			groups = append(groups, id)
		}
	}
	if len(groups) == 1 {
		return fmt.Errorf("instance %s reports no launch template security groups to keep beside %s", instanceID, sg)
	}
	if _, err := client.ModifyInstanceAttribute(ctx, &ec2.ModifyInstanceAttributeInput{
		InstanceId: aws.String(instanceID),
		Groups:     groups,
	}); err != nil {
		return fmt.Errorf("ec2:ModifyInstanceAttribute adding %s to %s: %w", sg, instanceID, err)
	}
	return nil
}

func (p *AWSProvider) SetEC2Client(region string, c *ec2.Client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ec2Clients == nil {
		p.ec2Clients = make(map[string]*ec2.Client)
	}
	p.ec2Clients[region] = c
	if p.clients == nil {
		p.clients = make(map[string]EC2FleetClient)
	}
	p.clients[region] = c
}

// GetEC2Client returns the concrete *ec2.Client for the given region from the pool.
func (p *AWSProvider) GetEC2Client(ctx context.Context, region string) (*ec2.Client, error) {
	if region == "" {
		region = p.Region
	}
	p.mu.RLock()
	if c, ok := p.ec2Clients[region]; ok && c != nil {
		p.mu.RUnlock()
		return c, nil
	}
	p.mu.RUnlock()

	client, err := p.getClient(ctx, region)
	if err != nil {
		return nil, err
	}
	if concrete, ok := client.(*ec2.Client); ok {
		return concrete, nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.ec2Clients[region], nil
}

// EC2Clients returns a thread-safe copy of the multi-region concrete ec2.Client pool.
func (p *AWSProvider) EC2Clients() map[string]*ec2.Client {
	p.mu.RLock()
	defer p.mu.RUnlock()
	pool := make(map[string]*ec2.Client, len(p.ec2Clients))
	for k, v := range p.ec2Clients {
		pool[k] = v
	}
	return pool
}

// SetRegionStore overrides the region store for instance-to-region mapping persistence.
func (p *AWSProvider) SetRegionStore(rs *RegionStore) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.regionStore = rs
	if rs != nil {
		p.instanceRegions = rs.GetAll()
	}
}

// AddConfiguredRegions adds additional regions to be queried during multi-region inventory scans.
func (p *AWSProvider) AddConfiguredRegions(regions ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := make(map[string]bool, len(p.configuredRegions))
	for _, r := range p.configuredRegions {
		seen[r] = true
	}
	for _, r := range regions {
		r = strings.TrimSpace(r)
		if r != "" && !seen[r] {
			p.configuredRegions = append(p.configuredRegions, r)
			seen[r] = true
		}
	}
}

// ConfiguredRegions returns a copy of configured regions monitored by the AWS provider.
func (p *AWSProvider) ConfiguredRegions() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]string, len(p.configuredRegions))
	copy(out, p.configuredRegions)
	return out
}

// SetClient overrides the internal EC2 client (used for testing).
func (p *AWSProvider) SetClient(c EC2FleetClient) {
	p.client = c
}

// SetRegionClient overrides the EC2 client for a specific region (used for testing multi-region scenarios).
func (p *AWSProvider) SetRegionClient(region string, c EC2FleetClient) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.clients == nil {
		p.clients = make(map[string]EC2FleetClient)
	}
	p.clients[region] = c
	if concrete, ok := c.(*ec2.Client); ok {
		if p.ec2Clients == nil {
			p.ec2Clients = make(map[string]*ec2.Client)
		}
		p.ec2Clients[region] = concrete
	}
}

func (p *AWSProvider) getClient(ctx context.Context, region string) (EC2FleetClient, error) {
	if region == "" {
		region = p.Region
	}
	p.mu.RLock()
	if c, ok := p.clients[region]; ok {
		p.mu.RUnlock()
		return c, nil
	}
	p.mu.RUnlock()

	if p.client != nil {
		return p.client, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.clients[region]; ok {
		return c, nil
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config for %s: %w", region, err)
	}
	cl := ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.Retryer = newEC2Retryer(cfg.RetryMode) })
	if p.clients == nil {
		p.clients = make(map[string]EC2FleetClient)
	}
	p.clients[region] = cl
	if p.ec2Clients == nil {
		p.ec2Clients = make(map[string]*ec2.Client)
	}
	p.ec2Clients[region] = cl
	return cl, nil
}

// Launch starts one VM for spec. It tries the pinned region first and, when the pod sets candidate-regions, falls
// back to the next candidate region after a capacity failure, up to maxLaunchRegions regions.
func (p *AWSProvider) Launch(ctx context.Context, spec *InstanceSpec) (*Instance, error) {
	instanceTypes, err := p.launchInstanceTypes(ctx, spec)
	if err != nil {
		return nil, err
	}
	if spec.InstanceType == "" {
		derived := *spec
		derived.InstanceType = instanceTypes[0]
		spec = &derived
	}
	if _, err := p.namespaceFirewall(spec.Region, spec.PodNamespace); err != nil {
		return nil, err
	}
	regions := p.launchRegions(spec, instanceTypes)

	var failures []string
	for _, region := range regions {
		inst, err := p.launchInRegion(ctx, spec.forLaunchRegion(region), region, region == spec.Region, instanceTypes)
		if err == nil {
			inst.Metadata["launchRegion"] = region
			if region != spec.Region {
				inst.Metadata["requestedRegion"] = spec.Region
				inst.Metadata["regionFallbacks"] = strings.Join(failures, "; ")
				log.Printf("[aws] Launched %s/%s outside its pinned region in %s after: %s", //nolint:gosec // sanitized
					sanitizeLog(spec.PodNamespace), sanitizeLog(spec.PodName), sanitizeLog(region), sanitizeLog(strings.Join(failures, "; ")))
			}
			return inst, nil
		}
		if len(regions) == 1 {
			return nil, err
		}
		failures = append(failures, fmt.Sprintf("%s: %v", region, err))
		if !isRegionFallbackError(err) || ctx.Err() != nil {
			break
		}
	}
	return nil, fmt.Errorf("launch failed in %d of %d candidate regions: %s", len(failures), len(regions), strings.Join(failures, "; "))
}

// launchInRegion runs one CreateFleet attempt in region. The pinned region honors the pod's explicit launch template
// and subnets; a fallback region uses only its ClusterFacts template and subnets, since those IDs are regional.
func (p *AWSProvider) launchInRegion(ctx context.Context, spec *InstanceSpec, region string, pinned bool, instanceTypes []string) (*Instance, error) {
	startTime := time.Now()
	capType := "spot"
	if spec.MarketType == "on-demand" {
		capType = "on-demand"
	}

	namespaceSG, err := p.namespaceFirewall(region, spec.PodNamespace)
	if err != nil {
		return nil, err
	}

	ltID, ltName := "", ""
	var subnetIDs []string
	if pinned {
		ltID, ltName = spec.LaunchTemplateID, spec.LaunchTemplateName
		subnetIDs = spec.SubnetIDs
	}
	if len(subnetIDs) == 0 {
		subnetIDs = p.ResolveRegionalSubnets(region)
	}

	encodedUserData, err := renderFleetUserData(spec, region)
	if err != nil {
		return nil, err
	}

	client, err := p.getClient(ctx, region)
	if err != nil {
		return nil, err
	}

	// A template the pod names boots every candidate; otherwise each candidate boots from the first regional
	// template it satisfies, and candidates no template accepts are left out.
	var groups []fleetTemplateGroup
	if ltID == "" && ltName == "" {
		resolved := resolveTemplates(ctx, client, region, p.regionTemplates(region))
		groups = groupByTemplate(resolved, instanceTypes, globalInstanceFacts.resolve(ctx, client, instanceTypes))
		if len(groups) == 0 {
			return nil, fmt.Errorf("%w in %s: %s", errNoLaunchTemplateFits, region, strings.Join(instanceTypes, ","))
		}
		instanceTypes = nil
		for _, g := range groups {
			instanceTypes = append(instanceTypes, g.InstanceTypes...)
		}
	}

	overrides, err := p.offeredFleetOverrides(ctx, client, region, instanceTypes, subnetIDs)
	if err != nil {
		GlobalFleetTracker.RecordLaunchAttempt("aws", region, spec.InstanceType, capType, "no_capacity")
		return nil, err
	}

	req := &FleetRequest{
		Region:             region,
		TargetCapacity:     1,
		InstanceTypes:      instanceTypes,
		SubnetIDs:          subnetIDs,
		ClusterTag:         spec.ClusterTag,
		PodName:            spec.PodName,
		PodNamespace:       spec.PodNamespace,
		PodUID:             spec.PodUID,
		CostCenter:         spec.CostCenter,
		JobID:              spec.JobID,
		UserData:           encodedUserData,
		LaunchTemplateID:   ltID,
		LaunchTemplateName: ltName,
		LaunchTemplates:    groups,
		MarketType:         spec.MarketType,
		MaxPrice:           spec.MaxPrice,
		PricePolicy:        spec.PricePolicy,
		AllocationStrategy: func() FleetAllocationStrategy {
			if spec.PricePolicy == "capacity-optimized" || strings.ToLower(os.Getenv("AWS_FLEET_ALLOCATION_STRATEGY")) == "capacity-optimized" {
				return StrategyCapacityOptimized
			}
			return StrategyPriceCapacityOptimized
		}(),
		RunnerAuthToken: spec.RunnerAuthToken,
	}

	resp, err := LaunchFleetWithClient(ctx, client, req, overrides)
	if err != nil {
		GlobalFleetTracker.RecordLaunchAttempt("aws", region, spec.InstanceType, capType, ClassifyAWSError(err))
		return nil, fmt.Errorf("launch fleet failed: %w", err)
	}
	if len(resp.Instances) == 0 {
		GlobalFleetTracker.RecordLaunchAttempt("aws", region, spec.InstanceType, capType, "no_capacity")
		if len(resp.Errors) > 0 {
			return nil, fmt.Errorf("%w in %s (fleet errors: %s)", errNoInstancesLaunched, region, strings.Join(resp.Errors, "; "))
		}
		return nil, fmt.Errorf("%w in %s", errNoInstancesLaunched, region)
	}

	inst := resp.Instances[0]
	if inst.Metadata == nil {
		inst.Metadata = make(map[string]string)
	}
	inst.Metadata["podName"] = spec.PodName
	inst.Metadata["namespace"] = spec.PodNamespace
	inst.Metadata["podUID"] = spec.PodUID
	inst.Metadata["runnerToken"] = spec.RunnerAuthToken

	// Query DescribeInstances to populate the instance IP assigned by EC2
	descOut, descErr := client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: []string{inst.ID},
	})
	var bootedGroups []ec2types.GroupIdentifier
	if descErr == nil && len(descOut.Reservations) > 0 && len(descOut.Reservations[0].Instances) > 0 {
		ec2Inst := descOut.Reservations[0].Instances[0]
		if ec2Inst.PrivateIpAddress != nil && *ec2Inst.PrivateIpAddress != "" {
			inst.IP = *ec2Inst.PrivateIpAddress
		}
		if zone := instanceZone(ec2Inst); zone != "" {
			inst.Metadata["availabilityZone"] = zone
		}
		bootedGroups = ec2Inst.SecurityGroups
	}
	if namespaceSG != "" {
		if err := attachNamespaceFirewall(ctx, client, inst.ID, bootedGroups, namespaceSG); err != nil {
			// A worker outside its namespace firewall cannot reach its peers, so it does not run the pod.
			if _, termErr := client.TerminateInstances(context.WithoutCancel(ctx), &ec2.TerminateInstancesInput{InstanceIds: []string{inst.ID}}); termErr != nil {
				log.Printf("[aws] Failed to terminate %s after its namespace firewall did not attach: %v", sanitizeLog(inst.ID), termErr) //nolint:gosec // sanitized
			}
			GlobalFleetTracker.RecordLaunchAttempt("aws", region, spec.InstanceType, capType, "namespace_firewall")
			return nil, fmt.Errorf("attach namespace firewall: %w", err)
		}
		inst.Metadata["namespaceFirewall"] = namespaceSG
	}

	p.AddConfiguredRegions(region)
	p.AddConfiguredRegions(spec.CandidateRegions...)
	p.mu.Lock()
	if p.instanceRegions == nil {
		p.instanceRegions = make(map[string]string)
	}
	p.instanceRegions[inst.ID] = region
	if p.missingCount != nil {
		delete(p.missingCount, inst.ID)
	}
	p.mu.Unlock()
	if p.regionStore != nil {
		_ = p.regionStore.Set(inst.ID, region)
	}
	GlobalFleetTracker.RecordLaunchAttempt("aws", region, spec.InstanceType, capType, "ok")
	GlobalFleetTracker.RecordLaunchDuration("aws", region, spec.InstanceType, capType, time.Since(startTime))
	return inst, nil
}

// renderFleetUserData renders the VM cloud-init for region and returns it gzip-compressed and base64-encoded.
func renderFleetUserData(spec *InstanceSpec, region string) (string, error) {
	runnerToken := spec.RunnerAuthToken
	if spec.KMSEncryptedToken || spec.KMSCiphertext != "" {
		runnerToken = ""
	}
	cloudInit, err := RenderCloudInit(&CloudInitConfig{
		PodName:               spec.PodName,
		PodNamespace:          spec.PodNamespace,
		PodUID:                spec.PodUID,
		Region:                region,
		InstanceType:          spec.InstanceType,
		NumGPUs:               spec.NumGPUs,
		AssignedGPUs:          spec.AssignedGPUs,
		RunnerAuthToken:       runnerToken,
		KMSEncryptedToken:     spec.KMSEncryptedToken,
		KMSKeyID:              spec.KMSKeyID,
		KMSCiphertext:         spec.KMSCiphertext,
		UseNativeDLAMI:        spec.AMIMode == "dlami",
		RayHeadAddress:        spec.RayHeadAddress,
		RayVersion:            spec.RayVersion,
		MeshAuthKey:           spec.MeshAuthKey,
		MeshAuthKeyCiphertext: spec.MeshAuthKeyCiphertext,
		MeshTags:              spec.MeshTags,
		TunnelEndpoint:        spec.TunnelEndpoint,
		TunnelTransport:       spec.TunnelTransport,
		TunnelGatewayIP:       spec.TunnelGatewayIP,
		TunnelDirectCIDRs:     spec.TunnelDirectCIDRs,
		TunnelCIDRs:           spec.TunnelCIDRs,
		ImageMirrors:          spec.ImageMirrors,
		RunnerDirectHTTP:      spec.RunnerDirectHTTP,
		TargetContainerImage:  spec.TargetContainerImage,
		Command:               spec.Command,
		Args:                  spec.Args,
		Env:                   spec.Env,
		EgressBandwidth:       spec.EgressBandwidth,
		EgressBurst:           spec.EgressBurst,
		CPUMillicores:         spec.CPUMillicores,
		MemoryBytes:           spec.MemoryBytes,
		RunAsUser:             spec.RunAsUser,
		RunAsGroup:            spec.RunAsGroup,
		DockerConfigJSON:      spec.DockerConfigJSON,
		RegistryMirrors:       spec.RegistryMirrors,
		OtelEndpoint:          spec.OtelEndpoint,
		NodeProblemDetector:   spec.NodeProblemDetector,
		CustomBootstrapScript: spec.CustomBootstrapScript,
		RunnerConcurrency:     spec.RunnerConcurrency,
	})
	if err != nil {
		return "", fmt.Errorf("failed to generate cloud-init: %w", err)
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write([]byte(cloudInit)); err != nil {
		return "", fmt.Errorf("failed to compress cloud-init with gzip: %w", err)
	}
	if err := gz.Close(); err != nil {
		return "", fmt.Errorf("failed to finalize gzip compression: %w", err)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

func (p *AWSProvider) Terminate(ctx context.Context, instanceID string) error {
	p.mu.RLock()
	region, ok := p.instanceRegions[instanceID]
	p.mu.RUnlock()
	if !ok || region == "" {
		region = p.Region
	}

	client, err := p.getClient(ctx, region)
	if err != nil {
		return err
	}
	_, err = client.TerminateInstances(ctx, &ec2.TerminateInstancesInput{
		InstanceIds: []string{instanceID},
	})
	if err == nil {
		p.mu.Lock()
		delete(p.instanceRegions, instanceID)
		p.mu.Unlock()
		if p.regionStore != nil {
			_ = p.regionStore.Delete(instanceID)
		}
	}
	return err
}

func (p *AWSProvider) Status(ctx context.Context, instanceID string) (*Instance, error) {
	p.mu.RLock()
	region, ok := p.instanceRegions[instanceID]
	p.mu.RUnlock()
	if !ok || region == "" {
		region = p.Region
	}

	client, err := p.getClient(ctx, region)
	if err != nil {
		return nil, err
	}
	out, err := client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: []string{instanceID},
	})
	if err != nil {
		return nil, err
	}

	for _, res := range out.Reservations {
		for _, inst := range res.Instances {
			if aws.ToString(inst.InstanceId) != instanceID {
				continue
			}
			state := StateRunning
			if inst.State != nil {
				switch inst.State.Name {
				case ec2types.InstanceStateNamePending:
					state = StateProvisioning
				case ec2types.InstanceStateNameRunning:
					state = StateRunning
				case ec2types.InstanceStateNameShuttingDown, ec2types.InstanceStateNameStopping:
					state = StateTerminating
				case ec2types.InstanceStateNameTerminated, ec2types.InstanceStateNameStopped:
					state = StateTerminated
				}
			}
			stateReasonCode := ""
			stateReasonMsg := ""
			if inst.StateReason != nil {
				stateReasonCode = aws.ToString(inst.StateReason.Code)
				stateReasonMsg = aws.ToString(inst.StateReason.Message)
			}
			marketType := "on-demand"
			if inst.InstanceLifecycle == ec2types.InstanceLifecycleTypeSpot {
				marketType = "spot"
			}
			ip := aws.ToString(inst.PrivateIpAddress)
			return &Instance{
				ID:           aws.ToString(inst.InstanceId),
				Provider:     ProviderAWS,
				Region:       region,
				InstanceType: string(inst.InstanceType),
				IP:           ip,
				State:        state,
				CreatedAt:    aws.ToTime(inst.LaunchTime),
				Metadata: map[string]string{
					"marketType":         marketType,
					"availabilityZone":   instanceZone(inst),
					"stateReasonCode":    stateReasonCode,
					"stateReasonMessage": stateReasonMsg,
				},
			}, nil
		}
	}
	return nil, fmt.Errorf("instance %s not found", instanceID)
}

// regionScanTimeout bounds each region's inventory scan so one unreachable regional endpoint
// cannot stall reconciliation for every other region.
var regionScanTimeout = 60 * time.Second

// ListActiveInstancesByRegion queries EC2 across all tracked and configured regions,
// returning instances grouped by region and recording individual region query errors.
func (p *AWSProvider) ListActiveInstancesByRegion(ctx context.Context, clusterTag string) (map[string][]*Instance, map[string]error) {
	p.mu.RLock()
	regionsMap := make(map[string]bool)
	if p.Region != "" {
		regionsMap[p.Region] = true
	}
	for _, reg := range p.configuredRegions {
		if reg != "" {
			regionsMap[reg] = true
		}
	}
	for _, reg := range p.instanceRegions {
		if reg != "" {
			regionsMap[reg] = true
		}
	}
	p.mu.RUnlock()

	instancesByRegion := make(map[string][]*Instance)
	errorsByRegion := make(map[string]error)

	type regResult struct {
		region    string
		instances []*Instance
		seenIDs   map[string]bool
		err       error
	}

	resultsChan := make(chan regResult, len(regionsMap))
	var wg sync.WaitGroup

	for reg := range regionsMap {
		wg.Add(1)
		go func(targetRegion string) {
			defer wg.Done()
			regionCtx, cancel := context.WithTimeout(ctx, regionScanTimeout)
			defer cancel()
			client, err := p.getClient(regionCtx, targetRegion)
			if err != nil {
				resultsChan <- regResult{region: targetRegion, err: fmt.Errorf("failed to get AWS client for region %s: %w", targetRegion, err)}
				return
			}
			out, err := DescribeAllInstances(regionCtx, client, &ec2.DescribeInstancesInput{
				Filters: []ec2types.Filter{
					{
						Name:   aws.String("tag:ClusterTag"),
						Values: []string{clusterTag},
					},
					{
						Name:   aws.String("instance-state-name"),
						Values: []string{"pending", "running"},
					},
				},
			})
			if err != nil {
				resultsChan <- regResult{region: targetRegion, err: fmt.Errorf("failed to describe instances in region %s: %w", targetRegion, err)}
				return
			}

			var regionInstances []*Instance
			seenInReg := make(map[string]bool)
			for _, res := range out.Reservations {
				for _, inst := range res.Instances {
					podName := ""
					podNamespace := "default"
					podUID := ""
					runnerToken := ""
					for _, tag := range inst.Tags {
						k := aws.ToString(tag.Key)
						v := aws.ToString(tag.Value)
						switch k {
						case "PodName":
							podName = v
						case "Namespace":
							podNamespace = v
						case "PodUID":
							podUID = v
						case "RunnerToken":
							runnerToken = v
						}
					}
					instID := aws.ToString(inst.InstanceId)
					seenInReg[instID] = true

					stateReasonCode := ""
					stateReasonMsg := ""
					if inst.StateReason != nil {
						stateReasonCode = aws.ToString(inst.StateReason.Code)
						stateReasonMsg = aws.ToString(inst.StateReason.Message)
					}
					marketType := "on-demand"
					if inst.InstanceLifecycle == ec2types.InstanceLifecycleTypeSpot {
						marketType = "spot"
					}

					regionInstances = append(regionInstances, &Instance{
						ID:           instID,
						Provider:     ProviderAWS,
						Region:       targetRegion,
						InstanceType: string(inst.InstanceType),
						IP:           aws.ToString(inst.PrivateIpAddress),
						State:        StateRunning,
						CreatedAt:    aws.ToTime(inst.LaunchTime),
						Metadata: map[string]string{
							"podName":            podName,
							"namespace":          podNamespace,
							"podUID":             podUID,
							"marketType":         marketType,
							"availabilityZone":   instanceZone(inst),
							"clusterTag":         clusterTag,
							"runnerToken":        runnerToken,
							"stateReasonCode":    stateReasonCode,
							"stateReasonMessage": stateReasonMsg,
						},
					})
				}
			}
			resultsChan <- regResult{region: targetRegion, instances: regionInstances, seenIDs: seenInReg}
		}(reg)
	}

	wg.Wait()
	close(resultsChan)

	var allChangedIDs []string
	var allChangedRegs []string
	var allStaleIDs []string

	for res := range resultsChan {
		if res.err != nil {
			errorsByRegion[res.region] = res.err
			continue
		}
		instancesByRegion[res.region] = res.instances

		p.mu.Lock()
		if p.instanceRegions == nil {
			p.instanceRegions = make(map[string]string)
		}
		for _, inst := range res.instances {
			if p.instanceRegions[inst.ID] != res.region {
				p.instanceRegions[inst.ID] = res.region
				allChangedIDs = append(allChangedIDs, inst.ID)
				allChangedRegs = append(allChangedRegs, res.region)
			}
		}

		if p.missingCount == nil {
			p.missingCount = make(map[string]int)
		}
		for id, mappedReg := range p.instanceRegions {
			if mappedReg == res.region {
				if res.seenIDs[id] {
					delete(p.missingCount, id)
				} else {
					p.missingCount[id]++
					if p.missingCount[id] >= 3 {
						allStaleIDs = append(allStaleIDs, id)
					}
				}
			}
		}
		for _, id := range allStaleIDs {
			delete(p.instanceRegions, id)
			delete(p.missingCount, id)
		}
		p.mu.Unlock()
	}

	if p.regionStore != nil {
		for i, id := range allChangedIDs {
			_ = p.regionStore.Set(id, allChangedRegs[i])
		}
		for _, id := range allStaleIDs {
			_ = p.regionStore.Delete(id)
		}
	}

	return instancesByRegion, errorsByRegion
}

// ListActiveInstances returns all active cloud instances across healthy regions.
// If all queried regions fail, it returns an error.
func (p *AWSProvider) ListActiveInstances(ctx context.Context, clusterTag string) ([]*Instance, error) {
	instancesByRegion, errorsByRegion := p.ListActiveInstancesByRegion(ctx, clusterTag)
	var instances []*Instance
	for _, regInsts := range instancesByRegion {
		instances = append(instances, regInsts...)
	}

	if len(instancesByRegion) == 0 && len(errorsByRegion) > 0 {
		var errMsgs []string
		for reg, err := range errorsByRegion {
			errMsgs = append(errMsgs, fmt.Sprintf("%s: %v", reg, err))
		}
		return nil, fmt.Errorf("all regions failed active instance query: %s", strings.Join(errMsgs, "; "))
	}

	for reg, err := range errorsByRegion {
		cleanReg := sanitizeLog(reg)
		log.Printf("[aws] Warning: region %s failed inventory query: %v", cleanReg, err) //nolint:gosec // region sanitized
	}

	return instances, nil
}

// DescribeAllInstances retrieves all instances for the given region using DescribeAllInstances pagination.
func (p *AWSProvider) DescribeAllInstances(ctx context.Context, region string, input *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
	if region == "" {
		region = p.Region
	}
	client, err := p.getClient(ctx, region)
	if err != nil {
		return nil, err
	}
	return DescribeAllInstances(ctx, client, input)
}

// instanceZone returns the availability zone an EC2 instance runs in, or "" when EC2 did not report it.
func instanceZone(inst ec2types.Instance) string {
	if inst.Placement == nil {
		return ""
	}
	return aws.ToString(inst.Placement.AvailabilityZone)
}
