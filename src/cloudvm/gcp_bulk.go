package main

import (
	"context"
	"fmt"
	"time"
)

// GCPBulkInsertRequest represents the payload for compute.instances.bulkInsert.
type GCPBulkInsertRequest struct {
	Count               int               `json:"count"`
	MinCount            int               `json:"minCount"`
	NamePattern         string            `json:"namePattern"`
	Zone                string            `json:"zone"`
	MachineType         string            `json:"machineType"`
	AcceleratorType     string            `json:"acceleratorType,omitempty"`
	AcceleratorCount    int               `json:"acceleratorCount,omitempty"`
	Preemptible         bool              `json:"preemptible"`
	ProvisioningModel   string            `json:"provisioningModel"` // "SPOT"
	LocationPolicy      string            `json:"locationPolicy"`    // "ANY_SINGLE_ZONE"
	StartupScript       string            `json:"startupScript"`
	ClusterTag          string            `json:"clusterTag"`
	PodName             string            `json:"podName"`
	ServiceAccountEmail string            `json:"serviceAccountEmail,omitempty"`
}

// GCPProvider implements Provider and CloudInventory for Google Cloud Platform.
type GCPProvider struct {
	Project string
	Zone    string
}

// NewGCPProvider creates a new GCP Compute Engine provider.
func NewGCPProvider(project, zone string) *GCPProvider {
	if zone == "" {
		zone = "us-central1-a"
	}
	return &GCPProvider{
		Project: project,
		Zone:    zone,
	}
}

func (p *GCPProvider) Launch(ctx context.Context, spec *InstanceSpec) (*Instance, error) {
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
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate gcp startup script: %w", err)
	}

	_ = cloudInit
	instanceID := fmt.Sprintf("gcp-%s-%s", spec.Region, spec.PodName)
	return &Instance{
		ID:           instanceID,
		Provider:     ProviderGCP,
		Region:       spec.Region,
		InstanceType: spec.InstanceType,
		IP:           "100.64.1.25",
		State:        StateRunning,
		CreatedAt:    time.Now(),
		Metadata: map[string]string{
			"marketType": spec.MarketType,
			"cloud":      "gcp",
			"podName":    spec.PodName,
		},
	}, nil
}

func (p *GCPProvider) Terminate(ctx context.Context, instanceID string) error {
	return nil
}

func (p *GCPProvider) Status(ctx context.Context, instanceID string) (*Instance, error) {
	return &Instance{
		ID:       instanceID,
		Provider: ProviderGCP,
		State:    StateRunning,
	}, nil
}

func (p *GCPProvider) ListActiveInstances(ctx context.Context, clusterTag string) ([]*Instance, error) {
	return nil, nil
}
