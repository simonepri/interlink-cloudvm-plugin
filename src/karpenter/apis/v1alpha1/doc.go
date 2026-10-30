// Package v1alpha1 defines the CloudVMNodeClass API and registers it with the client-go scheme.
// +k8s:deepcopy-gen=package
// +groupName=karpenter.cloudvm.vk.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
)

// Group is the API group of the CloudVM NodeClass.
const Group = "karpenter.cloudvm.vk.io"

// GroupVersion is the API version of this package.
var GroupVersion = schema.GroupVersion{Group: Group, Version: "v1alpha1"}

func init() {
	metav1.AddToGroupVersion(scheme.Scheme, GroupVersion)
	scheme.Scheme.AddKnownTypes(GroupVersion, &CloudVMNodeClass{}, &CloudVMNodeClassList{})
}
