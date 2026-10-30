// Tests that an AWS VM's root volume size comes from its launch template version, else from its image.
package main

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// rootVolumeEC2 serves one instance, its launch template versions and its image.
type rootVolumeEC2 struct {
	instance      ec2types.Instance
	templateSizes map[string]int32 // "<template>/<version>" -> root device size the version sets
	imageSize     int32
	calls         int
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
	inst := ec2types.Instance{InstanceId: aws.String(id), ImageId: aws.String(image), RootDeviceName: aws.String("/dev/xvda")}
	if template != "" {
		inst.Tags = []ec2types.Tag{
			{Key: aws.String(tagLaunchTemplateID), Value: aws.String(template)},
			{Key: aws.String(tagLaunchTemplateVersion), Value: aws.String(version)},
		}
	}
	return inst
}

func TestAWSRootVolumeSize(t *testing.T) {
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
			got, err := awsRootVolumeGiB(ctx, tt.client, "test-region-"+t.Name(), aws.ToString(tt.client.instance.InstanceId))
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Fatalf("awsRootVolumeGiB = %d, %v; want %d (error %v)", got, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestAWSRootVolumeSizeIsCachedPerTemplateVersionAndImage(t *testing.T) {
	ctx := context.Background()
	client := &rootVolumeEC2{instance: templateInstance("i-1", "ami-1", "lt-1", "7"), templateSizes: map[string]int32{"lt-1/7": 100}}
	for range 2 {
		if got, err := awsRootVolumeGiB(ctx, client, "test-region-cache", "i-1"); err != nil || got != 100 {
			t.Fatalf("awsRootVolumeGiB = %d, %v; want 100", got, err)
		}
	}
	// Two instance reads and one template read: the second launch reuses the cached size.
	if client.calls != 3 {
		t.Fatalf("EC2 calls = %d, want 3", client.calls)
	}
}
