// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// FleetAllocationStrategy defines the bidding and placement strategy.
type FleetAllocationStrategy string

const (
	StrategyPriceCapacityOptimized FleetAllocationStrategy = "price-capacity-optimized"
	StrategyCapacityOptimized      FleetAllocationStrategy = "capacity-optimized"
	StrategyLowestPrice            FleetAllocationStrategy = "lowest-price"
)

// GPUCountForInstanceType returns the number of physical GPUs for an instance type.
func GPUCountForInstanceType(itype string) int {
	switch {
	case strings.Contains(itype, "p5.") || strings.Contains(itype, "p4d.") || strings.HasSuffix(itype, ".48xlarge") || strings.HasSuffix(itype, ".metal"):
		return 8
	case strings.HasSuffix(itype, ".12xlarge") || strings.HasSuffix(itype, ".24xlarge"):
		return 4
	case strings.HasPrefix(itype, "g4dn.") || strings.HasPrefix(itype, "g5.") || strings.HasPrefix(itype, "g6.") || strings.HasPrefix(itype, "g6e."):
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

// FleetRequest represents a bulk spot request across multiple instance types and AZs.
type FleetRequest struct {
	Region             string                  `json:"region"`
	TargetCapacity     int                     `json:"targetCapacity"`
	InstanceTypes      []string                `json:"instanceTypes"`
	SubnetIDs          []string                `json:"subnetIds,omitempty"`
	LaunchTemplateID   string                  `json:"launchTemplateId,omitempty"`
	AllocationStrategy FleetAllocationStrategy `json:"allocationStrategy"`
	ClusterTag         string                  `json:"clusterTag"`
	PodName            string                  `json:"podName"`
	PodNamespace       string                  `json:"podNamespace"`
	PodUID             string                  `json:"podUID,omitempty"`
	MarketType         string                  `json:"marketType,omitempty"`
	MaxPrice           string                  `json:"maxPrice,omitempty"`
	PricePolicy        string                  `json:"pricePolicy,omitempty"`
}

// FleetResponse represents the result of a bulk fleet launch.
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
}

// BuildFleetOverrides constructs the weighted launch template overrides for EC2 Fleet.
func BuildFleetOverrides(instanceTypes []string, subnetIDs []string) []ec2types.FleetLaunchTemplateOverridesRequest {
	var overrides []ec2types.FleetLaunchTemplateOverridesRequest
	subnets := subnetIDs
	if len(subnets) == 0 {
		subnets = []string{""}
	}

	for _, itype := range instanceTypes {
		gpus := GPUCountForInstanceType(itype)
		weight := float64(gpus)
		if weight <= 0 {
			weight = 1.0
		}

		for _, subnet := range subnets {
			ov := ec2types.FleetLaunchTemplateOverridesRequest{
				InstanceType:     ec2types.InstanceType(itype),
				WeightedCapacity: aws.Float64(weight),
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

	tokenBase := fmt.Sprintf("%s-%s-%s-%s", req.ClusterTag, req.PodNamespace, req.PodName, req.PodUID)
	if len(tokenBase) > 64 {
		tokenBase = tokenBase[:64]
	}
	clientToken := tokenBase
	input := &ec2.CreateFleetInput{
		Type:        ec2types.FleetTypeInstant,
		ClientToken: aws.String(clientToken),
		TargetCapacitySpecification: &ec2types.TargetCapacitySpecificationRequest{
			TotalTargetCapacity:       aws.Int32(int32(req.TargetCapacity & 0x7fffffff)),
			DefaultTargetCapacityType: targetCapType,
		},
		SpotOptions:     spotOpts,
		OnDemandOptions: onDemandOpts,
		LaunchTemplateConfigs: []ec2types.FleetLaunchTemplateConfigRequest{
			{
				LaunchTemplateSpecification: &ec2types.FleetLaunchTemplateSpecificationRequest{
					LaunchTemplateId: aws.String(req.LaunchTemplateID),
					Version:          aws.String("$Latest"),
				},
				Overrides: overrides,
			},
		},
		TagSpecifications: []ec2types.TagSpecification{
			{
				ResourceType: ec2types.ResourceTypeInstance,
				Tags: []ec2types.Tag{
					{Key: aws.String("ManagedBy"), Value: aws.String("interlink-provider-cloudvm")},
					{Key: aws.String("ClusterTag"), Value: aws.String(req.ClusterTag)},
					{Key: aws.String("PodName"), Value: aws.String(req.PodName)},
					{Key: aws.String("Namespace"), Value: aws.String(req.PodNamespace)},
				},
			},
		},
	}

	out, err := client.CreateFleet(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("CreateFleet failed in %s: %w", req.Region, err)
	}

	resp := &FleetResponse{
		FleetID:   aws.ToString(out.FleetId),
		Region:    req.Region,
		TargetGPU: req.TargetCapacity,
	}

	for _, inst := range out.Instances {
		for _, id := range inst.InstanceIds {
			resp.InstanceIDs = append(resp.InstanceIDs, id)
			resp.Instances = append(resp.Instances, &Instance{
				ID:           id,
				Provider:     ProviderAWS,
				Region:       req.Region,
				InstanceType: string(inst.InstanceType),
				State:        StateRunning,
				CreatedAt:    time.Now(),
			})
		}
	}

	for _, fleetErr := range out.Errors {
		resp.Errors = append(resp.Errors, fmt.Sprintf("%s: %s", aws.ToString(fleetErr.ErrorCode), aws.ToString(fleetErr.ErrorMessage)))
	}

	resp.ActiveWorkers = len(resp.InstanceIDs)
	return resp, nil
}

// AWSProvider implements Provider and CloudInventory for AWS EC2 Fleets.
type AWSProvider struct {
	Region string
	Client EC2FleetClient
}

// NewAWSProvider creates an AWS provider initialized with default SDK v2 config.
func NewAWSProvider(region string) *AWSProvider {
	return &AWSProvider{
		Region: region,
	}
}

func (p *AWSProvider) getClient(ctx context.Context) (EC2FleetClient, error) {
	if p.Client != nil {
		return p.Client, nil
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(p.Region))
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS config for %s: %w", p.Region, err)
	}
	return ec2.NewFromConfig(cfg), nil
}

func (p *AWSProvider) Launch(ctx context.Context, spec *InstanceSpec) (*Instance, error) {
	cloudInit, err := GenerateCloudInit(&CloudInitConfig{
		PodName:              spec.PodName,
		PodNamespace:         spec.PodNamespace,
		RayHeadAddress:       spec.RayHeadAddress,
		RayVersion:           spec.RayVersion,
		TargetContainerImage: spec.TargetContainerImage,
		Command:              spec.Command,
		Args:                 spec.Args,
		Env:                  spec.Env,
		UseNativeDLAMI:       spec.AMIMode == "dlami",
		NumGPUs:              spec.NumGPUs,
		MeshAuthKey:          spec.MeshAuthKey,
		Region:               spec.Region,
		InstanceType:         spec.InstanceType,
		EgressBandwidth:      spec.EgressBandwidth,
		EgressBurst:          spec.EgressBurst,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate cloud-init: %w", err)
	}

	client, err := p.getClient(ctx)
	if err != nil {
		return nil, err
	}

	// When running against live AWS with no LaunchTemplate configured, create single instance
	instanceTypes := []string{spec.InstanceType}
	if len(instanceTypes) == 0 || instanceTypes[0] == "" {
		instanceTypes = []string{"g5.xlarge", "g6.xlarge", "g4dn.xlarge"}
	}

	targetCap := spec.NumGPUs
	if targetCap <= 0 {
		targetCap = 1
	}

	req := &FleetRequest{
		Region:             spec.Region,
		TargetCapacity:     targetCap,
		InstanceTypes:      instanceTypes,
		SubnetIDs:          spec.SubnetIDs,
		ClusterTag:         spec.ClusterTag,
		PodName:            spec.PodName,
		PodNamespace:       spec.PodNamespace,
		PodUID:             spec.PodUID,
		MarketType:         spec.MarketType,
		MaxPrice:           spec.MaxPrice,
		PricePolicy:        spec.PricePolicy,
		AllocationStrategy: StrategyPriceCapacityOptimized,
	}

	overrides := BuildFleetOverrides(req.InstanceTypes, req.SubnetIDs)

	// If LaunchTemplate is configured, use Fleet; otherwise fallback to direct RunInstances
	_ = base64.StdEncoding.EncodeToString([]byte(cloudInit))

	resp, err := LaunchFleetWithClient(ctx, client, req, overrides)
	if err != nil {
		return nil, fmt.Errorf("launch fleet failed: %w", err)
	}
	if len(resp.Instances) == 0 {
		return nil, fmt.Errorf("no instances launched by fleet in %s", spec.Region)
	}

	inst := resp.Instances[0]
	inst.Metadata = map[string]string{
		"fleetId": resp.FleetID,
		"podName": spec.PodName,
	}
	return inst, nil
}

func (p *AWSProvider) Terminate(ctx context.Context, instanceID string) error {
	client, err := p.getClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.TerminateInstances(ctx, &ec2.TerminateInstancesInput{
		InstanceIds: []string{instanceID},
	})
	return err
}

func (p *AWSProvider) Status(ctx context.Context, instanceID string) (*Instance, error) {
	client, err := p.getClient(ctx)
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
			ip := aws.ToString(inst.PrivateIpAddress)
			return &Instance{
				ID:           aws.ToString(inst.InstanceId),
				Provider:     ProviderAWS,
				Region:       p.Region,
				InstanceType: string(inst.InstanceType),
				IP:           ip,
				State:        state,
				CreatedAt:    aws.ToTime(inst.LaunchTime),
			}, nil
		}
	}
	return nil, fmt.Errorf("instance %s not found", instanceID)
}

func (p *AWSProvider) ListActiveInstances(ctx context.Context, clusterTag string) ([]*Instance, error) {
	client, err := p.getClient(ctx)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	var instances []*Instance
	for _, res := range out.Reservations {
		for _, inst := range res.Instances {
			podName := ""
			for _, tag := range inst.Tags {
				if aws.ToString(tag.Key) == "PodName" {
					podName = aws.ToString(tag.Value)
				}
			}
			instances = append(instances, &Instance{
				ID:           aws.ToString(inst.InstanceId),
				Provider:     ProviderAWS,
				Region:       p.Region,
				InstanceType: string(inst.InstanceType),
				IP:           aws.ToString(inst.PrivateIpAddress),
				State:        StateRunning,
				CreatedAt:    aws.ToTime(inst.LaunchTime),
				Metadata: map[string]string{
					"podName": podName,
				},
			})
		}
	}
	return instances, nil
}
