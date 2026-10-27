// Package cloudvm provides core structures and interfaces for InterLink CloudVM Provider.
package main

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// ProviderType represents supported cloud providers.
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
	// PodUID is the unique identifier of the Kubernetes pod.
	PodUID string `json:"podUID,omitempty"`
	// Provider explicitly overrides provider ("aws", "gcp").
	Provider string `json:"provider,omitempty"`
	// TickerURL is the optional URL of a central cloudvm-ticker service.
	TickerURL string `json:"tickerURL,omitempty"`
	// CostCenter is the FinOps cost allocation tag value.
	CostCenter string `json:"costCenter,omitempty"`
	// JobID is the high-level workload or Ray job identifier tag.
	JobID string `json:"jobId,omitempty"`
	// LaunchTemplateID specifies an AWS EC2 Launch Template ID.
	LaunchTemplateID string `json:"launchTemplateId,omitempty"`
	// LaunchTemplateName specifies an AWS EC2 Launch Template Name.
	LaunchTemplateName string `json:"launchTemplateName,omitempty"`
	// Region is the target cloud region (e.g. us-east-1, us-west-2).
	Region string `json:"region"`
	// CandidateRegions lists allowable regions for cross-region spot arbitrage.
	CandidateRegions []string `json:"candidateRegions,omitempty"`
	// RegionSelection is "scored" to rank Region with CandidateRegions instead of trying Region first.
	RegionSelection string `json:"regionSelection,omitempty"`
	// CandidateInstanceTypes lists alternative instance types for spot capacity diversification.
	CandidateInstanceTypes []string `json:"candidateInstanceTypes,omitempty"`
	// InstanceRequirements holds the pod's Karpenter instance type label requirements; types are derived from them
	// when the pod names no instance type.
	InstanceRequirements []karpv1.NodeSelectorRequirementWithMinValues `json:"instanceRequirements,omitempty"`
	// Requests is the pod's total CPU, memory and GPU request.
	Requests ResourceVector `json:"requests"`
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
	// RunAsUser specifies UID for container execution.
	RunAsUser *int64 `json:"runAsUser,omitempty"`
	// RunAsGroup specifies GID for container execution.
	RunAsGroup *int64 `json:"runAsGroup,omitempty"`
	// CPUMillicores specifies the CPU limit in millicores.
	CPUMillicores int64 `json:"cpuMillicores,omitempty"`
	// MemoryBytes specifies the memory limit in bytes.
	MemoryBytes int64 `json:"memoryBytes,omitempty"`
	// MaxPrice defines maximum spot or hourly price cap in USD/hr.
	MaxPrice string `json:"maxPrice,omitempty"`
	// MaxSpotPrice is an alias for MaxPrice.
	MaxSpotPrice string `json:"maxSpotPrice,omitempty"`
	// PricePolicy specifies "fail-fast" or "wait" when price exceeds cap.
	PricePolicy string `json:"pricePolicy,omitempty"`
	// MaxEgressRate defines maximum allowed egress spend rate in USD/hr (e.g. "0.50").
	MaxEgressRate string `json:"maxEgressRate,omitempty"`
	// EgressBandwidth specifies the rate limit (e.g. "100Mbps", "1Gbps").
	EgressBandwidth string `json:"egressBandwidth,omitempty"`
	// EgressBurst specifies the token bucket burst size (e.g. "10MB").
	EgressBurst string `json:"egressBurst,omitempty"`
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
	// MeshAuthKeyCiphertext holds the base64 KMS ciphertext of MeshAuthKey when KMS envelope encryption is configured.
	MeshAuthKeyCiphertext string `json:"meshAuthKeyCiphertext,omitempty"`
	// MeshTags is the comma-separated list of tailnet tags the worker advertises.
	MeshTags string `json:"meshTags,omitempty"`
	// TunnelEndpoint is the reverse tunnel gateway host:port the worker dials.
	TunnelEndpoint string `json:"tunnelEndpoint,omitempty"`
	// TunnelTransport is the path the worker takes to the gateway: tailnet, router or public.
	TunnelTransport string `json:"tunnelTransport,omitempty"`
	// TunnelGatewayIP is the gateway address a router-transport worker pins the TunnelEndpoint host to.
	TunnelGatewayIP string `json:"tunnelGatewayIP,omitempty"`
	// TunnelDirectCIDRs are the private networks the worker's agent dials directly instead of through the tunnel.
	TunnelDirectCIDRs []string `json:"tunnelDirectCidrs,omitempty"`
	// TunnelCIDRs are the networks the worker's agent sends through the tunnel; it dials every other destination itself.
	TunnelCIDRs []string `json:"tunnelCidrs,omitempty"`
	// ImageMirrors maps an upstream registry host to the mirror reference prefix the worker's runner pulls from instead.
	ImageMirrors map[string]string `json:"imageMirrors,omitempty"`
	// TunnelRoutes holds the tunnel route of every region this launch may use; the Tunnel* fields hold the pinned region's.
	TunnelRoutes map[string]TunnelRoute `json:"tunnelRoutes,omitempty"`
	// RunnerDirectHTTP keeps the in-VM runner listening on all interfaces for same-VPC direct HTTP access.
	RunnerDirectHTTP bool `json:"runnerDirectHTTP,omitempty"`
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
	// RunnerAuthToken is the shared bearer secret for the in-VM micro-runner daemon.
	RunnerAuthToken string `json:"runnerAuthToken,omitempty"`
	// KMSEncryptedToken indicates whether RunnerAuthToken is encrypted with AWS KMS.
	KMSEncryptedToken bool `json:"kmsEncryptedToken,omitempty"`
	// KMSKeyID specifies the AWS KMS key ARN or ID used to encrypt the runner token.
	KMSKeyID string `json:"kmsKeyId,omitempty"`
	// KMSCiphertext holds the base64-encoded KMS ciphertext for runner token bootstrap.
	KMSCiphertext string `json:"kmsCiphertext,omitempty"`
	// NumTPUs specifies the number of discrete Cloud TPU cores/slices requested.
	NumTPUs int `json:"numTPUs,omitempty"`
	// EnablePublicIP specifies whether a public NAT IP should be attached to the VM.
	EnablePublicIP bool `json:"enablePublicIP,omitempty"`
	// ServiceAccountScopes specifies OAuth2 scopes for the instance service account.
	ServiceAccountScopes []string `json:"serviceAccountScopes,omitempty"`
	// AssignedGPUs are the discrete GPU device indices allocated for the pod.
	AssignedGPUs []int `json:"assignedGPUs,omitempty"`
	// DockerConfigJSON holds the serialized .dockerconfigjson for private container registry authentication.
	DockerConfigJSON string `json:"dockerConfigJSON,omitempty"`
	// RegistryMirrors specifies custom container registry mirror URLs.
	RegistryMirrors []string `json:"registryMirrors,omitempty"`
	// OtelEndpoint specifies the OpenTelemetry metrics/traces exporter target.
	OtelEndpoint string `json:"otelEndpoint,omitempty"`
	// NodeProblemDetector enables standalone node-problem-detector daemon on the VM.
	NodeProblemDetector bool `json:"nodeProblemDetector,omitempty"`
	// CustomBootstrapScript holds arbitrary bash or systemd bootstrap commands appended to cloud-init.
	CustomBootstrapScript string `json:"customBootstrapScript,omitempty"`
	// RunnerConcurrency specifies the max concurrent containers executed by runner.
	RunnerConcurrency int `json:"runnerConcurrency,omitempty"`
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
	// FailureMessage is the terminal message, including any console tail, recorded when the VM died under its pods.
	FailureMessage string `json:"failureMessage,omitempty"`
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

// RegionInventory optionally allows multi-region providers to return inventory grouped by region with per-region errors.
type RegionInventory interface {
	ListActiveInstancesByRegion(ctx context.Context, clusterTag string) (map[string][]*Instance, map[string]error)
}

// InstanceStatusChecker queries individual instance status by ID (including terminated instances).
type InstanceStatusChecker interface {
	Status(ctx context.Context, instanceID string) (*Instance, error)
}

// InstanceAdopter allows re-adopting active VMs that were missed during a transient startup recovery failure.
type InstanceAdopter interface {
	AdoptInstance(ctx context.Context, inst *Instance) bool
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

// SecurityContext specifies privilege and user settings for a container.
type SecurityContext struct {
	RunAsUser  *int64 `json:"runAsUser,omitempty"`
	RunAsGroup *int64 `json:"runAsGroup,omitempty"`
}

// PodSecurityContext specifies privilege and user settings at the pod level.
type PodSecurityContext struct {
	RunAsUser  *int64 `json:"runAsUser,omitempty"`
	RunAsGroup *int64 `json:"runAsGroup,omitempty"`
}

type ContainerSpec struct {
	Name            string                        `json:"name"`
	Image           string                        `json:"image"`
	Command         []string                      `json:"command,omitempty"`
	Args            []string                      `json:"args,omitempty"`
	Env             []EnvVar                      `json:"env,omitempty"`
	Resources       ContainerResourceRequirements `json:"resources,omitempty"`
	SecurityContext *SecurityContext              `json:"securityContext,omitempty"`
}

type PodSpecWrapper struct {
	NodeName         string                        `json:"nodeName,omitempty"`
	NodeSelector     map[string]string             `json:"nodeSelector,omitempty"`
	Affinity         *corev1.Affinity              `json:"affinity,omitempty"`
	Containers       []ContainerSpec               `json:"containers"`
	SecurityContext  *PodSecurityContext           `json:"securityContext,omitempty"`
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`
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

// defaultContainerName names the workload container when the pod spec carries no container name.
const defaultContainerName = "worker"

// ContainerStatus mirrors the k8s.io/api/core/v1 ContainerStatus fields that interLink copies into the pod.
type ContainerStatus struct {
	Name  string               `json:"name"`
	Image string               `json:"image,omitempty"`
	State ContainerStatusState `json:"state"`
	Ready bool                 `json:"ready"`
}

// CreateStruct represents the response contract sent back to InterLink for /create.
type CreateStruct struct {
	PodUID string `json:"PodUID"`
	PodJID string `json:"PodJID"`
}

// PodStatusResponse represents the status response contract sent back to InterLink.
// Matches CNCF InterLink pkg/interlink/types.go PodStatus definition.
type PodStatusResponse struct {
	PodUID         string            `json:"UID"`
	Name           string            `json:"name"`
	Namespace      string            `json:"namespace"`
	JID            string            `json:"JID"`
	Containers     []ContainerStatus `json:"containers"`
	InitContainers []ContainerStatus `json:"initContainers"`
	// PodIP is the private IP of the VM running the pod; empty until the VM's IP is known.
	PodIP  string   `json:"podIP,omitempty"`
	PodIPs []string `json:"podIPs,omitempty"`
}
