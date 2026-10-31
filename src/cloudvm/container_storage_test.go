// Tests that an AWS VM's container storage is its NVMe instance store, else its root volume sized by its launch template
// version or its image.
package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// rootVolumeEC2 serves one instance, its instance type, its launch template versions and its image.
type rootVolumeEC2 struct {
	instance      ec2types.Instance
	storage       *ec2types.InstanceStorageInfo
	templateSizes map[string]int32 // "<template>/<version>" -> root device size the version sets
	imageSize     int32
	calls         int
}

func (c *rootVolumeEC2) DescribeInstanceTypes(_ context.Context, in *ec2.DescribeInstanceTypesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error) {
	c.calls++
	return &ec2.DescribeInstanceTypesOutput{InstanceTypes: []ec2types.InstanceTypeInfo{{InstanceType: in.InstanceTypes[0], InstanceStorageInfo: c.storage}}}, nil
}

func (c *rootVolumeEC2) DescribeInstances(context.Context, *ec2.DescribeInstancesInput, ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	c.calls++
	return &ec2.DescribeInstancesOutput{Reservations: []ec2types.Reservation{{Instances: []ec2types.Instance{c.instance}}}}, nil
}

func (c *rootVolumeEC2) DescribeLaunchTemplateVersions(_ context.Context, in *ec2.DescribeLaunchTemplateVersionsInput, _ ...func(*ec2.Options)) (*ec2.DescribeLaunchTemplateVersionsOutput, error) {
	c.calls++
	data := &ec2types.ResponseLaunchTemplateData{BlockDeviceMappings: []ec2types.LaunchTemplateBlockDeviceMapping{
		{DeviceName: aws.String("/dev/sdb"), Ebs: &ec2types.LaunchTemplateEbsBlockDevice{VolumeSize: aws.Int32(500)}},
	}}
	if size, ok := c.templateSizes[aws.ToString(in.LaunchTemplateId)+"/"+in.Versions[0]]; ok {
		data.BlockDeviceMappings = append(data.BlockDeviceMappings, ec2types.LaunchTemplateBlockDeviceMapping{
			DeviceName: aws.String("/dev/xvda"), Ebs: &ec2types.LaunchTemplateEbsBlockDevice{VolumeSize: aws.Int32(size)},
		})
	}
	return &ec2.DescribeLaunchTemplateVersionsOutput{LaunchTemplateVersions: []ec2types.LaunchTemplateVersion{{LaunchTemplateData: data}}}, nil
}

func (c *rootVolumeEC2) DescribeImages(context.Context, *ec2.DescribeImagesInput, ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error) {
	c.calls++
	if c.imageSize == 0 {
		return nil, errors.New("image not found")
	}
	return &ec2.DescribeImagesOutput{Images: []ec2types.Image{{BlockDeviceMappings: []ec2types.BlockDeviceMapping{
		{DeviceName: aws.String("/dev/xvda"), Ebs: &ec2types.EbsBlockDevice{VolumeSize: aws.Int32(c.imageSize)}},
	}}}}, nil
}

func templateInstance(id, image, template, version string) ec2types.Instance {
	inst := ec2types.Instance{
		InstanceId: aws.String(id), InstanceType: ec2types.InstanceType("ebs-only." + id), ImageId: aws.String(image),
		RootDeviceName: aws.String("/dev/xvda"),
	}
	if template != "" {
		inst.Tags = []ec2types.Tag{
			{Key: aws.String(tagLaunchTemplateID), Value: aws.String(template)},
			{Key: aws.String(tagLaunchTemplateVersion), Value: aws.String(version)},
		}
	}
	return inst
}

func TestAWSContainerStorageOnRootVolume(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		client  *rootVolumeEC2
		want    int32
		wantErr bool
	}{
		{
			name: "launch template version sizes the root device",
			client: &rootVolumeEC2{
				instance:      templateInstance("i-1", "ami-1", "lt-1", "3"),
				templateSizes: map[string]int32{"lt-1/3": 200}, imageSize: 8,
			},
			want: 200,
		},
		{
			name:   "image sizes the root device the template leaves alone",
			client: &rootVolumeEC2{instance: templateInstance("i-2", "ami-2", "lt-2", "1"), imageSize: 30},
			want:   30,
		},
		{
			name:   "instance launched without a template",
			client: &rootVolumeEC2{instance: templateInstance("i-3", "ami-3", "", ""), imageSize: 40},
			want:   40,
		},
		{
			name:    "nothing sizes the root device",
			client:  &rootVolumeEC2{instance: templateInstance("i-4", "ami-4", "lt-4", "1")},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := awsContainerStorage(ctx, tt.client, "test-region-"+t.Name(), aws.ToString(tt.client.instance.InstanceId))
			if (err != nil) != tt.wantErr || got.Value() != int64(tt.want)<<30 {
				t.Fatalf("awsContainerStorage = %s, %v; want %dGi (error %v)", got.String(), err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestAWSContainerStorageIsCachedPerInstanceTypeTemplateVersionAndImage(t *testing.T) {
	ctx := context.Background()
	client := &rootVolumeEC2{instance: templateInstance("i-cache", "ami-1", "lt-1", "7"), templateSizes: map[string]int32{"lt-1/7": 100}}
	for range 2 {
		if got, err := awsContainerStorage(ctx, client, "test-region-cache", "i-cache"); err != nil || got.String() != "100Gi" {
			t.Fatalf("awsContainerStorage = %s, %v; want 100Gi", got.String(), err)
		}
	}
	// Two instance reads, one instance type read and one template read: the second launch reuses the cached sizes.
	if client.calls != 4 {
		t.Fatalf("EC2 calls = %d, want 4", client.calls)
	}
}

func TestAWSContainerStorageOnInstanceStore(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		storage *ec2types.InstanceStorageInfo
		want    string
	}{
		{
			name:    "one NVMe disk",
			storage: &ec2types.InstanceStorageInfo{NvmeSupport: ec2types.EphemeralNvmeSupportRequired, TotalSizeInGB: aws.Int64(250)},
			want:    "250G",
		},
		{
			name:    "several NVMe disks striped into one",
			storage: &ec2types.InstanceStorageInfo{NvmeSupport: ec2types.EphemeralNvmeSupportRequired, TotalSizeInGB: aws.Int64(7600)},
			want:    "7600G",
		},
		{
			name:    "instance store without NVMe stays unmounted",
			storage: &ec2types.InstanceStorageInfo{NvmeSupport: ec2types.EphemeralNvmeSupportUnsupported, TotalSizeInGB: aws.Int64(800)},
			want:    "30Gi",
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inst := templateInstance(fmt.Sprintf("i-store-%d", i), "ami-1", "", "")
			client := &rootVolumeEC2{instance: inst, storage: tt.storage, imageSize: 30}
			got, err := awsContainerStorage(ctx, client, "test-region-store", aws.ToString(inst.InstanceId))
			if err != nil || got.String() != tt.want {
				t.Fatalf("awsContainerStorage = %s, %v; want %s", got.String(), err, tt.want)
			}
		})
	}
}

// plannedEC2 serves an instance type, a launch template's latest version and its image, for planning before launch.
type plannedEC2 struct {
	*mockEC2FleetClient
	storage     *ec2types.InstanceStorageInfo
	image       string
	templateGiB int32 // root device size the template sets, or 0 to leave it to the image
	imageGiB    int32
}

func (c *plannedEC2) DescribeInstanceTypes(_ context.Context, in *ec2.DescribeInstanceTypesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error) {
	if len(in.InstanceTypes) == 0 {
		return &ec2.DescribeInstanceTypesOutput{InstanceTypes: []ec2types.InstanceTypeInfo{{
			InstanceType: ec2types.InstanceType(in.Filters[0].Values[0]), VCpuInfo: &ec2types.VCpuInfo{DefaultVCpus: aws.Int32(4)},
		}}}, nil
	}
	return &ec2.DescribeInstanceTypesOutput{InstanceTypes: []ec2types.InstanceTypeInfo{{InstanceType: in.InstanceTypes[0], InstanceStorageInfo: c.storage}}}, nil
}

func (c *plannedEC2) DescribeLaunchTemplateVersions(_ context.Context, in *ec2.DescribeLaunchTemplateVersionsInput, _ ...func(*ec2.Options)) (*ec2.DescribeLaunchTemplateVersionsOutput, error) {
	if in.Versions[0] != "$Latest" {
		return nil, errors.New("planning must read the latest template version")
	}
	data := &ec2types.ResponseLaunchTemplateData{ImageId: aws.String(c.image)}
	if c.templateGiB > 0 {
		data.BlockDeviceMappings = []ec2types.LaunchTemplateBlockDeviceMapping{
			{DeviceName: aws.String("/dev/xvda"), Ebs: &ec2types.LaunchTemplateEbsBlockDevice{VolumeSize: aws.Int32(c.templateGiB)}},
		}
	}
	return &ec2.DescribeLaunchTemplateVersionsOutput{LaunchTemplateVersions: []ec2types.LaunchTemplateVersion{{LaunchTemplateData: data}}}, nil
}

func (c *plannedEC2) DescribeImages(context.Context, *ec2.DescribeImagesInput, ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error) {
	return &ec2.DescribeImagesOutput{Images: []ec2types.Image{{
		RootDeviceName: aws.String("/dev/xvda"),
		BlockDeviceMappings: []ec2types.BlockDeviceMapping{
			{DeviceName: aws.String("/dev/xvda"), Ebs: &ec2types.EbsBlockDevice{VolumeSize: aws.Int32(c.imageGiB)}},
		},
	}}}, nil
}

func TestPlannedContainerStorage(t *testing.T) {
	nvme := &ec2types.InstanceStorageInfo{NvmeSupport: ec2types.EphemeralNvmeSupportRequired, TotalSizeInGB: aws.Int64(900)}
	tests := []struct {
		name    string
		client  *plannedEC2
		pinned  LaunchTemplateFact
		want    string
		wantErr bool
	}{
		{"instance store", &plannedEC2{mockEC2FleetClient: &mockEC2FleetClient{}, storage: nvme, image: "ami-1", imageGiB: 30}, LaunchTemplateFact{}, "900G", false},
		{"template sizes the root device", &plannedEC2{mockEC2FleetClient: &mockEC2FleetClient{}, image: "ami-1", templateGiB: 200, imageGiB: 30}, LaunchTemplateFact{}, "200Gi", false},
		{"image sizes the root device", &plannedEC2{mockEC2FleetClient: &mockEC2FleetClient{}, image: "ami-1", imageGiB: 30}, LaunchTemplateFact{}, "30Gi", false},
		{"pinned template", &plannedEC2{mockEC2FleetClient: &mockEC2FleetClient{}, image: "ami-1", templateGiB: 80, imageGiB: 30}, LaunchTemplateFact{Name: "pinned"}, "80Gi", false},
		{"image resolved from SSM", &plannedEC2{mockEC2FleetClient: &mockEC2FleetClient{}, image: "resolve:ssm:/ami"}, LaunchTemplateFact{}, "", true},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			globalInstanceFacts = &instanceFactsCache{facts: make(map[string]InstanceTypeFacts)}
			globalTemplateArch = &templateArchCache{entries: make(map[string]templateArchEntry)}
			region := fmt.Sprintf("test-region-planned-%d", i)
			p := newTestProvider(t)
			p.SetClusterFacts(&ClusterFacts{Regions: map[string]RegionalClusterFact{region: {LaunchTemplates: []LaunchTemplateFact{{ID: "lt-planned"}}}}})
			p.SetRegionClient(region, tt.client)

			got, err := p.PlannedContainerStorage(context.Background(), region, fmt.Sprintf("planned%d.xlarge", i), tt.pinned)
			if (err != nil) != tt.wantErr || (!tt.wantErr && got.String() != tt.want) {
				t.Fatalf("PlannedContainerStorage = %s, %v; want %s (error %v)", got.String(), err, tt.want, tt.wantErr)
			}
		})
	}
}
