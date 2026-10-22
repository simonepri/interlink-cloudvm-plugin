package main

import (
	"context"
	"time"
)

// ProviderType identifies the underlying cloud or mock provider.
type ProviderType string

const (
	ProviderAWS  ProviderType = "aws"
	ProviderGCP  ProviderType = "gcp"
	ProviderMock ProviderType = "mock"
)

// InstanceState represents the lifecycle phase of an opportunistic cloud VM.
type InstanceState string

const (
	StateProvisioning InstanceState = "Provisioning"
	StateRunning      InstanceState = "Running"
	StateTerminating  InstanceState = "Terminating"
	StateTerminated   InstanceState = "Terminated"
	StateFailed       InstanceState = "Failed"
)

// InstanceSpec describes the requirements for launching an opportunistic VM worker.
type InstanceSpec struct {
	// PodName is the corresponding Kubernetes pod name.
	PodName string `json:"podName"`
	// PodNamespace is the namespace of the Kubernetes pod.
	PodNamespace string `json:"podNamespace"`
	// Region is the target cloud region (e.g. us-east-1, us-west-2).
	Region string `json:"region"`
	// CandidateRegions lists allowable regions for cross-region spot arbitrage.
	CandidateRegions []string `json:"candidateRegions,omitempty"`
	// InstanceType is the preferred or target instance type (e.g. g5.2xlarge).
	InstanceType string `json:"instanceType"`
	// MarketType indicates spot or on-demand ("spot" default).
	MarketType string `json:"marketType"`
	// NumGPUs requested by the worker pod.
	NumGPUs int `json:"numGPUs"`
	// GPUType specifies accelerator model (e.g. nvidia-a10g, nvidia-h100).
	GPUType string `json:"gpuType,omitempty"`
	// AMIMode selects "dlami" (fast native boot) or "container" (docker runner).
	AMIMode string `json:"amiMode,omitempty"`
	// MaxSpotPrice defines maximum spot price bid in USD/hr.
	MaxSpotPrice string `json:"maxSpotPrice,omitempty"`
	// TargetContainerImage is the container image to execute.
	TargetContainerImage string `json:"targetContainerImage,omitempty"`
	// Command is the command override for container execution.
	Command []string `json:"command,omitempty"`
	// Args are arguments passed to the container or worker.
	Args []string `json:"args,omitempty"`
	// Env holds container environment variables.
	Env map[string]string `json:"env,omitempty"`
	// RayHeadAddress is the reachable address of the Ray Head if running Ray.
	RayHeadAddress string `json:"rayHeadAddress,omitempty"`
	// RayVersion is the target Ray container version if running Ray.
	RayVersion string `json:"rayVersion,omitempty"`
	// MeshAuthKey is the Tailscale/WireGuard ephemeral join key.
	MeshAuthKey string `json:"meshAuthKey,omitempty"`
	// RegionalAMI maps cloud regions to pre-baked golden AMI / image IDs.
	RegionalAMI map[string]string `json:"regionalAMI,omitempty"`
	// IAMInstanceProfileArn is the AWS IAM role attached to the EC2 instance.
	IAMInstanceProfileArn string `json:"iamInstanceProfileArn,omitempty"`
	// ServiceAccountEmail is the GCP service account attached to the VM.
	ServiceAccountEmail string `json:"serviceAccountEmail,omitempty"`
	// SubnetIDs specifies target VPC subnets across Availability Zones.
	SubnetIDs []string `json:"subnetIDs,omitempty"`
	// SecurityGroupIDs specifies pre-configured security groups.
	SecurityGroupIDs []string `json:"securityGroupIDs,omitempty"`
	// ClusterTag identifies the resource owner for automated garbage collection.
	ClusterTag string `json:"clusterTag,omitempty"`
}

// Instance represents an active or terminating cloud VM instance.
type Instance struct {
	ID           string            `json:"id"`
	Provider     ProviderType      `json:"provider"`
	Region       string            `json:"region"`
	InstanceType string            `json:"instanceType"`
	IP           string            `json:"ip"`
	State        InstanceState     `json:"state"`
	CreatedAt    time.Time         `json:"createdAt"`
	TerminatedAt *time.Time        `json:"terminatedAt,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// Provider defines the interface for provisioning and managing opportunistic cloud VMs.
type Provider interface {
	// Launch provisions an opportunistic bare VM and injects bootstrap cloud-init.
	Launch(ctx context.Context, spec *InstanceSpec) (*Instance, error)
	// Terminate gracefully drains and terminates the bare VM.
	Terminate(ctx context.Context, instanceID string) error
	// Status queries the live status of an instance.
	Status(ctx context.Context, instanceID string) (*Instance, error)
}

// CloudInventory allows the background reconciler to enumerate real cloud instances to discover zombies.
type CloudInventory interface {
	// ListActiveInstances returns all cloud instances tagged with the cluster identifier.
	ListActiveInstances(ctx context.Context, clusterTag string) ([]*Instance, error)
}

// Minimal Kubernetes Pod representation for InterLink Sidecar API without external k8s.io dependencies.
type PodMeta struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace"`
	UID         string            `json:"uid,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type ContainerResourceRequirements struct {
	Limits   map[string]string `json:"limits,omitempty"`
	Requests map[string]string `json:"requests,omitempty"`
}

type ContainerSpec struct {
	Name      string                        `json:"name"`
	Image     string                        `json:"image"`
	Command   []string                      `json:"command,omitempty"`
	Args      []string                      `json:"args,omitempty"`
	Env       []EnvVar                      `json:"env,omitempty"`
	Resources ContainerResourceRequirements `json:"resources,omitempty"`
}

type PodSpecWrapper struct {
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	Containers   []ContainerSpec   `json:"containers"`
}

// Pod represents the incoming pod definition from InterLink.
type Pod struct {
	Metadata PodMeta        `json:"metadata"`
	Spec     PodSpecWrapper `json:"spec"`
}

// ContainerStatusState models standard InterLink/K8s container state.
type ContainerStatusState struct {
	Waiting    *ContainerWaitingState    `json:"waiting,omitempty"`
	Running    *ContainerRunningState    `json:"running,omitempty"`
	Terminated *ContainerTerminatedState `json:"terminated,omitempty"`
}

type ContainerWaitingState struct {
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

type ContainerRunningState struct {
	StartedAt time.Time `json:"startedAt"`
}

type ContainerTerminatedState struct {
	ExitCode    int32     `json:"exitCode"`
	Reason      string    `json:"reason,omitempty"`
	Message     string    `json:"message,omitempty"`
	FinishedAt  time.Time `json:"finishedAt"`
	ContainerID string    `json:"containerID,omitempty"`
}

type ContainerStatus struct {
	Name  string               `json:"name"`
	State ContainerStatusState `json:"state"`
	Ready bool                 `json:"ready"`
}

// PodStatusResponse represents the status response contract sent back to InterLink.
type PodStatusResponse struct {
	PodUID     string            `json:"podUID,omitempty"`
	Name       string            `json:"name"`
	Namespace  string            `json:"namespace"`
	Containers []ContainerStatus `json:"containers"`
}
