// Instance type requirements of a pod in Karpenter's well-known label vocabulary: reads them from the pod's node
// selector and required node affinity with Karpenter's scheduling package, keeping only the labels the catalog models.
package main

import (
	"regexp"

	corev1 "k8s.io/api/core/v1"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// Instance type labels documented at https://karpenter.sh/docs/concepts/scheduling/#well-known-labels.
const (
	labelInstanceCategory        = "karpenter.k8s.aws/instance-category"
	labelInstanceFamily          = "karpenter.k8s.aws/instance-family"
	labelInstanceGeneration      = "karpenter.k8s.aws/instance-generation"
	labelInstanceSize            = "karpenter.k8s.aws/instance-size"
	labelInstanceCPU             = "karpenter.k8s.aws/instance-cpu"
	labelInstanceCPUManufacturer = "karpenter.k8s.aws/instance-cpu-manufacturer"
	labelInstanceMemory          = "karpenter.k8s.aws/instance-memory"
	labelInstanceGPUName         = "karpenter.k8s.aws/instance-gpu-name"
	labelInstanceGPUManufacturer = "karpenter.k8s.aws/instance-gpu-manufacturer"
	labelInstanceGPUCount        = "karpenter.k8s.aws/instance-gpu-count"
	labelInstanceGPUMemory       = "karpenter.k8s.aws/instance-gpu-memory"
)

// instanceTypeLabels are the labels every catalog instance type defines; requirements on other labels describe the
// virtual node rather than the VM and are ignored.
var instanceTypeLabels = map[string]bool{
	corev1.LabelInstanceTypeStable: true,
	corev1.LabelArchStable:         true,
	karpv1.CapacityTypeLabelKey:    true,
	labelInstanceCategory:          true,
	labelInstanceFamily:            true,
	labelInstanceGeneration:        true,
	labelInstanceSize:              true,
	labelInstanceCPU:               true,
	labelInstanceCPUManufacturer:   true,
	labelInstanceMemory:            true,
	labelInstanceGPUName:           true,
	labelInstanceGPUManufacturer:   true,
	labelInstanceGPUCount:          true,
	labelInstanceGPUMemory:         true,
}

var ec2InstanceTypePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*\.[a-z0-9-]+$`)

// isEC2InstanceType reports whether name has the family.size shape of an EC2 instance type.
func isEC2InstanceType(name string) bool {
	return ec2InstanceTypePattern.MatchString(name)
}

// podInstanceRequirements returns the pod's requirements on instance type labels, from its node selector and the
// first term of its required node affinity, as Karpenter reads them. A node.kubernetes.io/instance-type In
// requirement that names no EC2 instance type selects the virtual node itself and is dropped.
func podInstanceRequirements(nodeSelector map[string]string, affinity *corev1.Affinity) []karpv1.NodeSelectorRequirementWithMinValues {
	all := scheduling.NewStrictPodRequirements(&corev1.Pod{Spec: corev1.PodSpec{NodeSelector: nodeSelector, Affinity: affinity}})
	kept := scheduling.NewRequirements()
	for _, req := range all.Values() {
		if !instanceTypeLabels[req.Key] {
			continue
		}
		if req.Key == corev1.LabelInstanceTypeStable && req.Operator() == corev1.NodeSelectorOpIn && !anyEC2InstanceType(req.Values()) {
			continue
		}
		kept.Add(req)
	}
	if len(kept) == 0 {
		return nil
	}
	return kept.NodeSelectorRequirements()
}

func anyEC2InstanceType(names []string) bool {
	for _, n := range names {
		if isEC2InstanceType(n) {
			return true
		}
	}
	return false
}

// requiredMarketType returns "on-demand" when the requirements exclude spot but allow on-demand, and "" otherwise.
func requiredMarketType(reqs []karpv1.NodeSelectorRequirementWithMinValues) string {
	capacity := scheduling.NewNodeSelectorRequirementsWithMinValues(reqs...).Get(karpv1.CapacityTypeLabelKey)
	if !capacity.Has(karpv1.CapacityTypeSpot) && capacity.Has(karpv1.CapacityTypeOnDemand) {
		return "on-demand"
	}
	return ""
}
