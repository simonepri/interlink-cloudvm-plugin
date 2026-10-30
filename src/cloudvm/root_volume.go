// Package main implements the interlink-cloudvm-plugin daemon.
package main

// This file reads the size of a launched VM's root volume, which holds its container images, writable layers and logs
// and so is the ephemeral-storage a claim's Node reports.

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"k8s.io/apimachinery/pkg/api/resource"
)

// RootVolumeSizer reports the size of a launched VM's root volume.
type RootVolumeSizer interface {
	RootVolumeSize(ctx context.Context, region, instanceID string) (resource.Quantity, error)
}

// EC2 tags EC2 sets on every instance launched from a launch template.
const (
	tagLaunchTemplateID      = "aws:ec2launchtemplate:id"
	tagLaunchTemplateVersion = "aws:ec2launchtemplate:version"
)

// rootVolumeClient is the EC2 subset that reads an instance's root volume size.
type rootVolumeClient interface {
	launchTemplateClient
	DescribeInstances(ctx context.Context, params *ec2.DescribeInstancesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
}

// rootVolumeCache holds root volume sizes by launch template version and image, both of which never change.
var rootVolumeCache sync.Map

// RootVolumeSize returns the size of the instance's root EBS volume: the size its launch template version gives the
// root device, or else the size of the root device in its image.
func (p *AWSProvider) RootVolumeSize(ctx context.Context, region, instanceID string) (resource.Quantity, error) {
	client, err := p.getClient(ctx, region)
	if err != nil {
		return resource.Quantity{}, err
	}
	gib, err := awsRootVolumeGiB(ctx, client, region, instanceID)
	if err != nil {
		return resource.Quantity{}, err
	}
	return *resource.NewQuantity(int64(gib)<<30, resource.BinarySI), nil
}

func awsRootVolumeGiB(ctx context.Context, client rootVolumeClient, region, instanceID string) (int32, error) {
	out, err := client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{instanceID}})
	if err != nil {
		return 0, fmt.Errorf("ec2:DescribeInstances: %w", err)
	}
	if len(out.Reservations) == 0 || len(out.Reservations[0].Instances) == 0 {
		return 0, fmt.Errorf("instance %s not found", instanceID)
	}
	inst := out.Reservations[0].Instances[0]
	root, image := aws.ToString(inst.RootDeviceName), aws.ToString(inst.ImageId)
	if root == "" || image == "" {
		return 0, fmt.Errorf("instance %s reports no root device or image yet", instanceID)
	}
	var templateID, version string
	for _, t := range inst.Tags {
		switch aws.ToString(t.Key) {
		case tagLaunchTemplateID:
			templateID = aws.ToString(t.Value)
		case tagLaunchTemplateVersion:
			version = aws.ToString(t.Value)
		}
	}
	key := region + "/" + templateID + "/" + version + "/" + image + "/" + root
	if v, ok := rootVolumeCache.Load(key); ok {
		return v.(int32), nil
	}
	gib, err := rootVolumeGiBFromSources(ctx, client, templateID, version, image, root)
	if err != nil {
		return 0, err
	}
	rootVolumeCache.Store(key, gib)
	return gib, nil
}

func rootVolumeGiBFromSources(ctx context.Context, client launchTemplateClient, templateID, version, image, root string) (int32, error) {
	if templateID != "" && version != "" {
		versions, err := client.DescribeLaunchTemplateVersions(ctx, &ec2.DescribeLaunchTemplateVersionsInput{
			LaunchTemplateId: aws.String(templateID), Versions: []string{version},
		})
		if err != nil {
			return 0, fmt.Errorf("ec2:DescribeLaunchTemplateVersions %s version %s: %w", templateID, version, err)
		}
		if len(versions.LaunchTemplateVersions) > 0 && versions.LaunchTemplateVersions[0].LaunchTemplateData != nil {
			for _, m := range versions.LaunchTemplateVersions[0].LaunchTemplateData.BlockDeviceMappings {
				if aws.ToString(m.DeviceName) == root && m.Ebs != nil && aws.ToInt32(m.Ebs.VolumeSize) > 0 {
					return *m.Ebs.VolumeSize, nil
				}
			}
		}
	}
	images, err := client.DescribeImages(ctx, &ec2.DescribeImagesInput{ImageIds: []string{image}})
	if err != nil {
		return 0, fmt.Errorf("ec2:DescribeImages %s: %w", image, err)
	}
	if len(images.Images) > 0 {
		if gib := imageRootGiB(images.Images[0].BlockDeviceMappings, root); gib > 0 {
			return gib, nil
		}
	}
	return 0, fmt.Errorf("neither launch template %q nor image %s sizes root device %s", templateID, image, root)
}

func imageRootGiB(mappings []ec2types.BlockDeviceMapping, root string) int32 {
	for _, m := range mappings {
		if aws.ToString(m.DeviceName) == root && m.Ebs != nil {
			return aws.ToInt32(m.Ebs.VolumeSize)
		}
	}
	return 0
}

// gcpBootDiskGB is the size of the boot disk every GCP worker launches with.
const gcpBootDiskGB = 50

// RootVolumeSize returns the size of the boot disk every GCP worker launches with.
func (p *GCPProvider) RootVolumeSize(context.Context, string, string) (resource.Quantity, error) { //nolint:unparam // RootVolumeSizer returns an error for AWS reads
	return *resource.NewQuantity(gcpBootDiskGB<<30, resource.BinarySI), nil
}
