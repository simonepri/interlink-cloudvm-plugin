// Tests that the claim catalog offers an instance type in a region only when one of the region's launch templates
// accepts it.
package main

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	corev1 "k8s.io/api/core/v1"
)

// sliceCatalogClient describes a whole-GPU type and a GPU slice, which AWS reports with zero GPUs.
func sliceCatalogClient() *mockEC2FleetClient {
	return &mockEC2FleetClient{describeInstanceTypesFn: func(_ context.Context, in *ec2.DescribeInstanceTypesInput) (*ec2.DescribeInstanceTypesOutput, error) {
		out := &ec2.DescribeInstanceTypesOutput{}
		for _, name := range in.Filters[0].Values {
			gpus, mem := int32(1), int32(24576)
			if name == "g6f.xlarge" {
				gpus, mem = 0, 2861
			}
			out.InstanceTypes = append(out.InstanceTypes, ec2types.InstanceTypeInfo{
				InstanceType:  ec2types.InstanceType(name),
				VCpuInfo:      &ec2types.VCpuInfo{DefaultVCpus: aws.Int32(4)},
				MemoryInfo:    &ec2types.MemoryInfo{SizeInMiB: aws.Int64(16384)},
				ProcessorInfo: &ec2types.ProcessorInfo{SupportedArchitectures: []ec2types.ArchitectureType{ec2types.ArchitectureTypeX8664}},
				GpuInfo: &ec2types.GpuInfo{Gpus: []ec2types.GpuDeviceInfo{{
					Name: aws.String("L4"), Manufacturer: aws.String("NVIDIA"), Count: aws.Int32(gpus),
					MemoryInfo: &ec2types.GpuDeviceMemoryInfo{SizeInMiB: aws.Int32(mem)},
				}}},
			})
		}
		return out, nil
	}}
}

func TestClaimCatalogOffersOnlyTypesALaunchTemplateAccepts(t *testing.T) {
	sliceTemplate := LaunchTemplateFact{ID: "lt-grid", Requirements: []corev1.NodeSelectorRequirement{
		{Key: labelGPUSlice, Operator: corev1.NodeSelectorOpIn, Values: []string{"true"}},
	}}
	wholeTemplate := LaunchTemplateFact{ID: "lt-gpu", Requirements: []corev1.NodeSelectorRequirement{
		{Key: labelGPUSlice, Operator: corev1.NodeSelectorOpNotIn, Values: []string{"true"}},
	}}
	tests := []struct {
		name string
		fact *RegionalClusterFact
		want []string
	}{
		{"default template", nil, []string{"g5.xlarge"}},
		{"legacy templates", &RegionalClusterFact{LaunchTemplateID: "lt-cpu", GPULaunchTemplateID: "lt-gpu"}, []string{"g5.xlarge"}},
		{"templates without a slice template", &RegionalClusterFact{LaunchTemplates: []LaunchTemplateFact{wholeTemplate}}, []string{"g5.xlarge"}},
		{"template that accepts slices", &RegionalClusterFact{LaunchTemplates: []LaunchTemplateFact{wholeTemplate, sliceTemplate}}, []string{"g5.xlarge", "g6f.xlarge"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			globalInstanceFacts = &instanceFactsCache{facts: make(map[string]InstanceTypeFacts)}
			globalTemplateArch = &templateArchCache{entries: make(map[string]templateArchEntry)}
			p := newTestProvider(t)
			p.SetRegionClient("us-east-1", sliceCatalogClient())
			p.SetRegionClient("us-west-1", sliceCatalogClient())
			facts := &ClusterFacts{Regions: map[string]RegionalClusterFact{}}
			if tc.fact != nil {
				facts.Regions["us-east-1"] = *tc.fact
			}
			p.SetClusterFacts(facts)
			s, client, _ := newClaimServer(t, p)
			ticker := NewSpotPriceTicker([]string{"us-east-1"}, []string{"g5.xlarge", "g6f.xlarge"}, time.Hour)
			for _, name := range []string{"g5.xlarge", "g6f.xlarge"} {
				ticker.prices[tickerKey("us-east-1", "us-east-1a", name)] = &SpotPriceEntry{Region: "us-east-1", AvailabilityZone: "us-east-1a", InstanceType: name, SpotPriceUSD: 0.4}
			}
			s.SetSpotTicker(ticker)

			cat, err := client.Catalog(context.Background(), []string{"us-east-1"})
			if err != nil {
				t.Fatalf("Catalog: %v", err)
			}
			var got []string
			for _, it := range cat.InstanceTypes {
				if it.GPUCount > 0 {
					got = append(got, it.Name)
				}
			}
			slices.Sort(got)
			if !slices.Equal(got, tc.want) {
				t.Errorf("GPU types in the catalog = %v, want %v", got, tc.want)
			}
		})
	}
}
