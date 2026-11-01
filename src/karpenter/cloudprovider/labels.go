// Node label and annotation keys the CloudVM provider sets, registered as Karpenter well-known labels.

package cloudprovider

import (
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	"github.com/simonepri/interlink-cloudvm-plugin/karpenter/apis/v1alpha1"
)

// Label keys the provider derives from the catalog.
const (
	// LabelGPUClass is the cluster-wide GPU class label, such as a100-80gb.
	LabelGPUClass = "gpu-class"
	// LabelCloud is the cloud hosting the VM: aws or gcp.
	LabelCloud = v1alpha1.Group + "/cloud"

	// The karpenter.k8s.aws/instance-* keys team NodePools and flavors already select on.
	LabelInstanceCategory        = "karpenter.k8s.aws/instance-category"
	LabelInstanceFamily          = "karpenter.k8s.aws/instance-family"
	LabelInstanceGeneration      = "karpenter.k8s.aws/instance-generation"
	LabelInstanceSize            = "karpenter.k8s.aws/instance-size"
	LabelInstanceCPU             = "karpenter.k8s.aws/instance-cpu"
	LabelInstanceMemory          = "karpenter.k8s.aws/instance-memory"
	LabelInstanceGPUName         = "karpenter.k8s.aws/instance-gpu-name"
	LabelInstanceGPUManufacturer = "karpenter.k8s.aws/instance-gpu-manufacturer"
	LabelInstanceGPUCount        = "karpenter.k8s.aws/instance-gpu-count"
	LabelInstanceGPUMemory       = "karpenter.k8s.aws/instance-gpu-memory"

	// LabelNVIDIAGPUPresent is "true" on types with NVIDIA GPUs, the label NVIDIA's GPU feature discovery sets on GPU
	// nodes, so DaemonSets that select GPU nodes by it, such as GPU exporters, also select CloudVM GPU Nodes.
	LabelNVIDIAGPUPresent = "nvidia.com/gpu.present"
)

// AnnotationNodeClassHash records the launch-settings hash of the NodeClass a NodeClaim launched with.
const AnnotationNodeClassHash = v1alpha1.Group + "/nodeclass-hash"

// derivedLabels are the keys every instance type defines, so pods and NodePools may select on them.
var derivedLabels = []string{
	LabelGPUClass,
	LabelCloud,
	LabelInstanceCategory,
	LabelInstanceFamily,
	LabelInstanceGeneration,
	LabelInstanceSize,
	LabelInstanceCPU,
	LabelInstanceMemory,
	LabelInstanceGPUName,
	LabelInstanceGPUManufacturer,
	LabelInstanceGPUCount,
	LabelInstanceGPUMemory,
	LabelNVIDIAGPUPresent,
}

func init() {
	karpv1.WellKnownLabels.Insert(derivedLabels...)
}
