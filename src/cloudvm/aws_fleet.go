// Package cloudvm provides the AWS EC2 Fleet engine for launching opportunistic spot and on-demand GPU instances.
package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
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
	MarketType         string                  `json:"marketType,omitempty"`
	MaxPrice           string                  `json:"maxPrice,omitempty"`
	PricePolicy        string                  `json:"pricePolicy,omitempty"`
	RunnerAuthToken    string                  `json:"runnerAuthToken,omitempty"`
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

// EC2FleetClient abstracts the AWS EC2 client for unit testing.
type EC2FleetClient interface {
	CreateFleet(ctx context.Context, params *ec2.CreateFleetInput, optFns ...func(*ec2.Options)) (*ec2.CreateFleetOutput, error)
	DescribeInstances(ctx context.Context, params *ec2.DescribeInstancesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
	TerminateInstances(ctx context.Context, params *ec2.TerminateInstancesInput, optFns ...func(*ec2.Options)) (*ec2.TerminateInstancesOutput, error)
	DescribeInstanceTypes(ctx context.Context, params *ec2.DescribeInstanceTypesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error)
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
		ltSpec.LaunchTemplateName = aws.String("interlink-cloudvm-worker")
	}
	if req.UserData != "" {
		ltSpec.LaunchTemplateSpecificationUserData = aws.String(req.UserData)
	}

	tags := []ec2types.Tag{
		{Key: aws.String("ManagedBy"), Value: aws.String("interlink-provider-cloudvm")},
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
		SpotOptions:     spotOpts,
		OnDemandOptions: onDemandOpts,
		LaunchTemplateConfigs: []ec2types.FleetLaunchTemplateConfigRequest{
			{
				LaunchTemplateSpecification: ltSpec,
				Overrides:                   overrides,
			},
		},
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

	out, err := client.CreateFleet(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("ec2:CreateFleet call failed: %w", err)
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
	regionStore       *RegionStore
	instanceRegions   map[string]string
	missingCount      map[string]int
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

	return &AWSProvider{
		Region:            region,
		configuredRegions: confRegions,
		clients:           make(map[string]EC2FleetClient),
		regionStore:       regStore,
		instanceRegions:   initialRegions,
	}
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
	cl := ec2.NewFromConfig(cfg)
	if p.clients == nil {
		p.clients = make(map[string]EC2FleetClient)
	}
	p.clients[region] = cl
	return cl, nil
}

func (p *AWSProvider) Launch(ctx context.Context, spec *InstanceSpec) (*Instance, error) {
	cloudInit, err := RenderCloudInit(&CloudInitConfig{
		PodName:              spec.PodName,
		PodNamespace:         spec.PodNamespace,
		PodUID:               spec.PodUID,
		Region:               spec.Region,
		InstanceType:         spec.InstanceType,
		NumGPUs:              spec.NumGPUs,
		AssignedGPUs:         spec.AssignedGPUs,
		RunnerAuthToken:      spec.RunnerAuthToken,
		UseNativeDLAMI:       spec.AMIMode == "dlami",
		RayHeadAddress:       spec.RayHeadAddress,
		RayVersion:           spec.RayVersion,
		MeshAuthKey:          spec.MeshAuthKey,
		TargetContainerImage: spec.TargetContainerImage,
		Command:              spec.Command,
		Args:                 spec.Args,
		Env:                  spec.Env,
		EgressBandwidth:      spec.EgressBandwidth,
		EgressBurst:          spec.EgressBurst,
		CPUMillicores:        spec.CPUMillicores,
		MemoryBytes:          spec.MemoryBytes,
		RunAsUser:            spec.RunAsUser,
		RunAsGroup:           spec.RunAsGroup,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate cloud-init: %w", err)
	}

	client, err := p.getClient(ctx, spec.Region)
	if err != nil {
		return nil, err
	}

	instanceTypes := []string{spec.InstanceType}
	if len(instanceTypes) == 0 || instanceTypes[0] == "" {
		switch {
		case spec.NumGPUs >= 8:
			instanceTypes = []string{"p5.48xlarge", "p4de.24xlarge", "g5.48xlarge"}
		case spec.NumGPUs >= 4:
			instanceTypes = []string{"g5.12xlarge", "g5.24xlarge", "g6.12xlarge"}
		default:
			instanceTypes = []string{"g5.xlarge", "g6.xlarge", "g4dn.xlarge"}
		}
	}

	encodedUserData := base64.StdEncoding.EncodeToString([]byte(cloudInit))

	req := &FleetRequest{
		Region:             spec.Region,
		TargetCapacity:     1,
		InstanceTypes:      instanceTypes,
		SubnetIDs:          spec.SubnetIDs,
		ClusterTag:         spec.ClusterTag,
		PodName:            spec.PodName,
		PodNamespace:       spec.PodNamespace,
		PodUID:             spec.PodUID,
		CostCenter:         spec.CostCenter,
		JobID:              spec.JobID,
		UserData:           encodedUserData,
		LaunchTemplateID:   spec.LaunchTemplateID,
		LaunchTemplateName: spec.LaunchTemplateName,
		MarketType:         spec.MarketType,
		MaxPrice:           spec.MaxPrice,
		PricePolicy:        spec.PricePolicy,
		AllocationStrategy: StrategyPriceCapacityOptimized,
		RunnerAuthToken:    spec.RunnerAuthToken,
	}

	overrides := BuildFleetOverrides(req.InstanceTypes, req.SubnetIDs)

	resp, err := LaunchFleetWithClient(ctx, client, req, overrides)
	if err != nil {
		return nil, fmt.Errorf("launch fleet failed: %w", err)
	}
	if len(resp.Instances) == 0 {
		return nil, fmt.Errorf("no instances launched by fleet in %s", spec.Region)
	}

	inst := resp.Instances[0]
	if inst.Metadata == nil {
		inst.Metadata = make(map[string]string)
	}
	inst.Metadata["podName"] = spec.PodName
	inst.Metadata["namespace"] = spec.PodNamespace
	inst.Metadata["podUID"] = spec.PodUID
	inst.Metadata["runnerToken"] = spec.RunnerAuthToken
	p.AddConfiguredRegions(spec.Region)
	p.AddConfiguredRegions(spec.CandidateRegions...)
	p.mu.Lock()
	if p.instanceRegions == nil {
		p.instanceRegions = make(map[string]string)
	}
	p.instanceRegions[inst.ID] = spec.Region
	if p.missingCount != nil {
		delete(p.missingCount, inst.ID)
	}
	p.mu.Unlock()
	if p.regionStore != nil {
		_ = p.regionStore.Set(inst.ID, spec.Region)
	}
	return inst, nil
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
					"stateReasonCode":    stateReasonCode,
					"stateReasonMessage": stateReasonMsg,
				},
			}, nil
		}
	}
	return nil, fmt.Errorf("instance %s not found", instanceID)
}

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

	for reg := range regionsMap {
		client, err := p.getClient(ctx, reg)
		if err != nil {
			errorsByRegion[reg] = fmt.Errorf("failed to get AWS client for region %s: %w", reg, err)
			continue
		}
		out, err := client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
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
			errorsByRegion[reg] = fmt.Errorf("failed to describe instances in region %s: %w", reg, err)
			continue
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

				p.mu.Lock()
				regChanged := false
				if p.instanceRegions == nil {
					p.instanceRegions = make(map[string]string)
				}
				if p.instanceRegions[instID] != reg {
					p.instanceRegions[instID] = reg
					regChanged = true
				}
				p.mu.Unlock()
				if regChanged && p.regionStore != nil {
					_ = p.regionStore.Set(instID, reg)
				}

				regionInstances = append(regionInstances, &Instance{
					ID:           instID,
					Provider:     ProviderAWS,
					Region:       reg,
					InstanceType: string(inst.InstanceType),
					IP:           aws.ToString(inst.PrivateIpAddress),
					State:        StateRunning,
					CreatedAt:    aws.ToTime(inst.LaunchTime),
					Metadata: map[string]string{
						"podName":            podName,
						"namespace":          podNamespace,
						"podUID":             podUID,
						"marketType":         marketType,
						"clusterTag":         clusterTag,
						"runnerToken":        runnerToken,
						"stateReasonCode":    stateReasonCode,
						"stateReasonMessage": stateReasonMsg,
					},
				})
			}
		}
		instancesByRegion[reg] = regionInstances

		// Prune stale instance mappings that belonged to this region only after >= 3 consecutive misses
		p.mu.Lock()
		if p.missingCount == nil {
			p.missingCount = make(map[string]int)
		}
		var staleIDs []string
		for id, mappedReg := range p.instanceRegions {
			if mappedReg == reg {
				if seenInReg[id] {
					delete(p.missingCount, id)
				} else {
					p.missingCount[id]++
					if p.missingCount[id] >= 3 {
						staleIDs = append(staleIDs, id)
					}
				}
			}
		}
		for _, id := range staleIDs {
			delete(p.instanceRegions, id)
			delete(p.missingCount, id)
		}
		p.mu.Unlock()
		if p.regionStore != nil {
			for _, id := range staleIDs {
				_ = p.regionStore.Delete(id)
			}
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
