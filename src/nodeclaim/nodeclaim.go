// Package nodeclaim defines the wire contract of the plugin's VM-per-NodeClaim API: one cloud VM per Karpenter
// NodeClaim, the instance catalog with priced offerings, and the API interface that the HTTP client implements.
package nodeclaim

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// API paths the plugin serves.
const (
	// PathNodeClaims lists claims (GET, optional ?providerID=) and launches one (POST).
	PathNodeClaims = "/v1/nodeclaims"
	// PathCatalog returns the instance catalog with priced offerings (GET, optional ?regions=a,b).
	PathCatalog = "/v1/catalog"
)

// ProviderIDScheme prefixes every providerID the plugin issues: cloudvm://<cloud>/<region>/<instance-id>/vm.
const ProviderIDScheme = "cloudvm://"

// providerIDSuffix ends every providerID the plugin issues so that its last path segment is not an instance ID. The
// AWS cloud controller manager reads the last segment of any providerID that does not start with aws:// as an EC2
// instance in its own account and deletes a NotReady Node whose instance it cannot find there, which would delete
// the Node of every live VM whose workers run in another account or region while its Ready condition is not True.
const providerIDSuffix = "/vm"

// Capacity types a claim launches with, the values of the karpenter.sh/capacity-type label.
const (
	CapacityTypeSpot     = "spot"
	CapacityTypeOnDemand = "on-demand"
)

// Labels and taints the plugin and the node agent agree on.
const (
	// LabelNamespace binds a claim VM to the one namespace whose pods it runs, because co-tenant pods share the
	// VM's host network.
	LabelNamespace = "cloudvm.vk.io/namespace"
	// LabelNodeAgent marks the Nodes the multiplexed node agent serves.
	LabelNodeAgent = "cloudvm.vk.io/node-agent"
	// TaintUnregistered is Karpenter's registration taint, which a new Node carries until Karpenter removes it.
	TaintUnregistered = "karpenter.sh/unregistered"
)

// IsDaemonSetOwner reports whether an owner reference with this apiVersion and kind names a DaemonSet. A claim VM
// never runs a DaemonSet pod: the DaemonSet controller creates one for every Node whose taints it tolerates, and a claim
// VM serves the namespace of the workload it was launched for.
func IsDaemonSetOwner(apiVersion, kind string) bool {
	group, _, _ := strings.Cut(apiVersion, "/")
	return group == "apps" && kind == "DaemonSet"
}

// Resource names of a claim's capacity.
const (
	ResourceCPU              = "cpu"
	ResourceMemory           = "memory"
	ResourcePods             = "pods"
	ResourceEphemeralStorage = "ephemeral-storage"
	// ResourceGPU is the extended resource name of NVIDIA GPUs.
	ResourceGPU = "nvidia.com/gpu"
	// ResourceTPU is the extended resource name of Google TPUs.
	ResourceTPU = "google.com/tpu"
)

// State is the lifecycle phase of a claim's VM.
type State string

const (
	// StateLaunching means the cloud accepted the VM but its tunnel has not connected yet.
	StateLaunching State = "Launching"
	// StateRunning means the VM's tunnel is connected and its runner takes pods.
	StateRunning State = "Running"
	// StateUnhealthy means the VM is up but cannot take pods safely, such as after it failed to remove a pod's
	// container. The node agent reports its Node NotReady so the scheduler stops using it and Karpenter replaces it.
	StateUnhealthy State = "Unhealthy"
	// StateTerminating means termination was requested and the cloud has not confirmed it. A claim whose VM is gone
	// is neither listed nor found.
	StateTerminating State = "Terminating"
)

// Error reasons the plugin returns in an ErrorResponse.
const (
	ReasonInsufficientCapacity = "InsufficientCapacity"
	ReasonInvalidRequest       = "InvalidRequest"
	ReasonNotFound             = "NotFound"
	ReasonCatalogUnavailable   = "CatalogUnavailable"
	ReasonUnauthorized         = "Unauthorized"
	ReasonForbidden            = "Forbidden"
	ReasonInternal             = "Internal"
)

// DefaultTokenAudience is the audience of the service account token a caller presents as a bearer token; the plugin
// verifies it with a TokenReview and admits only the service accounts it allows.
const DefaultTokenAudience = "cloudvm-plugin" //nolint:gosec // an audience name, not a credential

// Taint mirrors a Kubernetes node taint without depending on k8s.io/api.
type Taint struct {
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Effect string `json:"effect"`
}

// CreateRequest asks the plugin for one VM of a chosen offering for a NodeClaim.
type CreateRequest struct {
	// Name is the NodeClaim name; it keys the claim and names the Node. A repeated launch with the same name returns
	// the existing claim.
	Name string `json:"name"`
	// InstanceType is the cloud instance type, such as g5.xlarge.
	InstanceType string `json:"instanceType"`
	// Region is the cloud region the VM launches in.
	Region string `json:"region"`
	// Zone pins the availability zone; empty lets the cloud choose within the region.
	Zone string `json:"zone,omitempty"`
	// CapacityType is CapacityTypeSpot or CapacityTypeOnDemand.
	CapacityType string `json:"capacityType"`
	// Labels are copied onto the Node.
	Labels map[string]string `json:"labels,omitempty"`
	// Taints are copied onto the Node.
	Taints []Taint `json:"taints,omitempty"`
	// Annotations carry the plugin's cloudvm.vk.io/* launch options, such as cloudvm.vk.io/aws-launch-template or
	// cloudvm.vk.io/subnets, with the meaning they have on a pod. The plugin ignores keys it does not know.
	Annotations map[string]string `json:"annotations,omitempty"`
}

// Validate reports the first missing or malformed field of a launch request.
func (r *CreateRequest) Validate() error {
	switch {
	case r.Name == "":
		return fmt.Errorf("name is required")
	case r.InstanceType == "":
		return fmt.Errorf("instanceType is required")
	case r.Region == "":
		return fmt.Errorf("region is required")
	case r.CapacityType != CapacityTypeSpot && r.CapacityType != CapacityTypeOnDemand:
		return fmt.Errorf("capacityType must be %q or %q, got %q", CapacityTypeSpot, CapacityTypeOnDemand, r.CapacityType)
	}
	return nil
}

// NodeClaim is the plugin's record of the VM launched for a NodeClaim.
type NodeClaim struct {
	Name         string            `json:"name"`
	ProviderID   string            `json:"providerID"`
	InstanceType string            `json:"instanceType"`
	Region       string            `json:"region"`
	Zone         string            `json:"zone,omitempty"`
	CapacityType string            `json:"capacityType"`
	State        State             `json:"state"`
	CreatedAt    time.Time         `json:"createdAt"`
	Labels       map[string]string `json:"labels,omitempty"`
	Taints       []Taint           `json:"taints,omitempty"`
	// Capacity holds the resource quantities the VM offers pods, as Hardware.Capacity renders them, such as
	// {"cpu": "4", "memory": "16Gi", "ephemeral-storage": "200Gi", "nvidia.com/gpu": "1", "pods": "110"}. The node
	// agent reports each of them as the Node's capacity and allocatable.
	Capacity map[string]string `json:"capacity,omitempty"`
	// Interrupted is true once the cloud announced a spot interruption of the VM.
	Interrupted bool `json:"interrupted,omitempty"`
	// Pods are the pods the plugin placed on the VM, sorted by UID. The node agent calls /delete for each one that is
	// no longer bound to the claim's Node, which frees what the pod held on the VM.
	Pods []PodRef `json:"pods,omitempty"`
}

// PodRef identifies a pod placed on a claim's VM.
type PodRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
}

// Hardware is what a claim's VM offers pods.
type Hardware struct {
	CPU    string
	Memory string
	// EphemeralStorage is the size of the root volume that holds container images, writable layers and logs; empty
	// while the plugin has not read it.
	EphemeralStorage string
	GPUs             int
	TPUs             int
	Pods             int
}

// Capacity renders the hardware as a claim's Capacity; resources the VM lacks are left out.
func (h Hardware) Capacity() map[string]string {
	out := map[string]string{
		ResourceCPU:    h.CPU,
		ResourceMemory: h.Memory,
		ResourcePods:   strconv.Itoa(h.Pods),
	}
	if h.EphemeralStorage != "" {
		out[ResourceEphemeralStorage] = h.EphemeralStorage
	}
	if h.GPUs > 0 {
		out[ResourceGPU] = strconv.Itoa(h.GPUs)
	}
	if h.TPUs > 0 {
		out[ResourceTPU] = strconv.Itoa(h.TPUs)
	}
	return out
}

// List is the response of GET /v1/nodeclaims.
type List struct {
	Items []NodeClaim `json:"items"`
}

// Offering is one place and capacity type an instance type can launch with, priced per hour.
type Offering struct {
	Region       string  `json:"region"`
	Zone         string  `json:"zone"`
	CapacityType string  `json:"capacityType"`
	PricePerHour float64 `json:"pricePerHour"`
	Available    bool    `json:"available"`
}

// InstanceType describes one catalog instance type and its offerings.
type InstanceType struct {
	Name            string `json:"name"`
	Provider        string `json:"provider"`
	CPU             string `json:"cpu"`
	Memory          string `json:"memory"`
	GPUCount        int    `json:"gpuCount,omitempty"`
	GPUModel        string `json:"gpuModel,omitempty"`
	GPUMemoryGB     int    `json:"gpuMemoryGB,omitempty"`
	GPUManufacturer string `json:"gpuManufacturer,omitempty"`
	// EphemeralStorage is the size of the disk a VM of this type keeps its containers on, as the plugin plans it
	// before launch; the smallest across the requested regions. Empty when the plugin cannot tell.
	EphemeralStorage string     `json:"ephemeralStorage,omitempty"`
	Architecture     string     `json:"architecture"`
	Family           string     `json:"family,omitempty"`
	Generation       string     `json:"generation,omitempty"`
	Size             string     `json:"size,omitempty"`
	Offerings        []Offering `json:"offerings"`
}

// Catalog is the response of GET /v1/catalog.
type Catalog struct {
	InstanceTypes []InstanceType `json:"instanceTypes"`
}

// ErrorResponse is the JSON body of every non-2xx response.
type ErrorResponse struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// API is the VM-per-NodeClaim API of the plugin.
type API interface {
	// Catalog returns the instance types with offerings in the given regions, or in every region when regions is
	// empty.
	Catalog(ctx context.Context, regions []string) (*Catalog, error)
	// Create launches a VM for the request, or returns the claim already launched under its name.
	Create(ctx context.Context, req *CreateRequest) (*NodeClaim, error)
	// Get returns the claim with the name, or an error for which IsNotFound is true.
	Get(ctx context.Context, name string) (*NodeClaim, error)
	// GetByProviderID returns the claim with the providerID, or an error for which IsNotFound is true.
	GetByProviderID(ctx context.Context, providerID string) (*NodeClaim, error)
	// List returns every launching, running or terminating claim; it never returns an empty list on error.
	List(ctx context.Context) ([]NodeClaim, error)
	// Delete starts terminating the claim's VM; it returns an error for which IsNotFound is true when the claim is
	// already gone.
	Delete(ctx context.Context, name string) error
}

// FormatProviderID builds the providerID of a VM.
func FormatProviderID(cloud, region, instanceID string) string {
	return FormatLegacyProviderID(cloud, region, instanceID) + providerIDSuffix
}

// FormatLegacyProviderID builds the providerID that claims launched by earlier releases carry, without the suffix;
// such a claim keeps it because a Node's providerID never changes.
func FormatLegacyProviderID(cloud, region, instanceID string) string {
	return ProviderIDScheme + cloud + "/" + region + "/" + instanceID
}

// ParseProviderID splits a providerID, current or legacy, into its cloud, region and instance ID.
func ParseProviderID(providerID string) (cloud, region, instanceID string, err error) {
	rest, ok := strings.CutPrefix(providerID, ProviderIDScheme)
	rest = strings.TrimSuffix(rest, providerIDSuffix)
	parts := strings.Split(rest, "/")
	if !ok || len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", fmt.Errorf("providerID %q is not %s<cloud>/<region>/<instance-id>%s", providerID, ProviderIDScheme, providerIDSuffix)
	}
	return parts[0], parts[1], parts[2], nil
}
