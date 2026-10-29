// CloudVMNodeClass API: the regions and launch settings a CloudVM NodePool launches with.
// Karpenter NodePools reference it through spec.template.spec.nodeClassRef.

package v1alpha1

import (
	"github.com/awslabs/operatorpkg/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CloudVMNodeClassSpec holds the launch settings shared by every NodeClaim of the class.
type CloudVMNodeClassSpec struct {
	// Footprint lists the regions the class may launch in, each with its cloud and network references.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	// +listType=map
	// +listMapKey=region
	Footprint []FootprintRegion `json:"footprint"`
	// LaunchTemplateName names the AWS launch template the plugin launches from in every AWS region.
	// +optional
	LaunchTemplateName string `json:"launchTemplateName,omitempty"`
	// Annotations are extra cloudvm.vk.io/* launch settings passed to the plugin unchanged.
	// +kubebuilder:validation:XValidation:rule="self.all(k, k.startsWith('cloudvm.vk.io/'))",message="annotation keys must start with cloudvm.vk.io/"
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// FootprintRegion is one region the class may launch in.
type FootprintRegion struct {
	// Provider is the cloud that hosts the region.
	// +kubebuilder:validation:Enum=aws;gcp
	Provider string `json:"provider"`
	// Region is the cloud region name, such as us-east-1 or us-central1.
	// +kubebuilder:validation:MinLength=1
	Region string `json:"region"`
	// Zones restricts launches to these zones; empty allows every zone the catalog offers in the region.
	// +optional
	Zones []string `json:"zones,omitempty"`
	// SubnetIDs are the subnets the plugin launches into in this region.
	// +optional
	SubnetIDs []string `json:"subnetIDs,omitempty"`
	// LaunchTemplateID is the AWS launch template ID for this region; it overrides LaunchTemplateName.
	// +optional
	LaunchTemplateID string `json:"launchTemplateID,omitempty"`
}

// CloudVMNodeClassStatus reports whether the class is usable.
type CloudVMNodeClassStatus struct {
	// Conditions contains signals for health and readiness.
	// +optional
	Conditions []status.Condition `json:"conditions,omitempty"`
}

// CloudVMNodeClass is the Schema for the CloudVMNodeClass API.
// +kubebuilder:object:root=true
// +kubebuilder:resource:path=cloudvmnodeclasses,scope=Cluster,categories=karpenter,shortName={cvmnc,cvmncs}
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type CloudVMNodeClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec CloudVMNodeClassSpec `json:"spec"`
	// Status starts Ready: the CRD schema validates every field, so a stored class is usable.
	// +kubebuilder:default:={conditions: {{type: "Ready", status: "True", reason: "Ready", lastTransitionTime: "2026-01-01T00:00:00Z", message: ""}}}
	Status CloudVMNodeClassStatus `json:"status,omitempty"`
}

// CloudVMNodeClassList contains a list of CloudVMNodeClass.
// +kubebuilder:object:root=true
type CloudVMNodeClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CloudVMNodeClass `json:"items"`
}

// StatusConditions returns the class's Ready condition set.
func (in *CloudVMNodeClass) StatusConditions(opts ...status.ForOption) status.ConditionSet {
	return status.NewReadyConditions().For(in, opts...)
}

// GetConditions returns the status conditions.
func (in *CloudVMNodeClass) GetConditions() []status.Condition {
	return in.Status.Conditions
}

// SetConditions replaces the status conditions.
func (in *CloudVMNodeClass) SetConditions(conditions []status.Condition) {
	in.Status.Conditions = conditions
}

// Region returns the footprint entry for a region and whether the class contains it.
func (in *CloudVMNodeClass) Region(name string) (FootprintRegion, bool) {
	for _, r := range in.Spec.Footprint {
		if r.Region == name {
			return r, true
		}
	}
	return FootprintRegion{}, false
}
