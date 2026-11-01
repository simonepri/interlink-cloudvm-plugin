// Pod admission: refuses pods a worker cannot run as asked, such as ones with several regular containers or a sandbox
// runtime class, and decides which GPUs an NVIDIA runtime class pod that requests none may see.
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Reasons of the events and errors that refuse a pod at create time.
const (
	reasonMultipleContainersUnsupported = "MultipleContainersUnsupported"
	reasonRuntimeClassUnsupported       = "RuntimeClassUnsupported"
	// reasonNVIDIAVisibleDevicesIgnored warns that the plugin ran a pod without the GPUs its NVIDIA_VISIBLE_DEVICES named.
	reasonNVIDIAVisibleDevicesIgnored = "NVIDIAVisibleDevicesIgnored"
)

// envNVIDIAVisibleDevices is the variable the NVIDIA container runtime reads the devices a container sees from.
const envNVIDIAVisibleDevices = "NVIDIA_VISIBLE_DEVICES"

// defaultNVIDIARuntimeClasses are the RuntimeClass names the NVIDIA GPU Operator creates.
var defaultNVIDIARuntimeClasses = []string{"nvidia", "nvidia-cdi", "nvidia-legacy"}

// PodRefusal is a create-time refusal whose reason names the unsupported feature in the pod's event and status.
type PodRefusal struct {
	Reason  string
	Message string
}

func (r *PodRefusal) Error() string { return r.Reason + ": " + r.Message }

// PodAdmission holds the runtime class settings pod admission reads.
type PodAdmission struct {
	// nvidiaRuntimeClasses run like the NVIDIA container runtime: their pods may see GPUs through NVIDIA_VISIBLE_DEVICES.
	nvidiaRuntimeClasses map[string]bool
	// allGPUsNamespaces are the namespaces whose DaemonSet pods may see GPUs they do not request.
	allGPUsNamespaces map[string]bool
}

// NewPodAdmission returns the admission settings for the NVIDIA runtime class names, the defaults when empty, and the
// namespaces whose DaemonSet pods may see every GPU.
func NewPodAdmission(nvidiaRuntimeClasses, allGPUsNamespaces []string) PodAdmission {
	if len(nvidiaRuntimeClasses) == 0 {
		nvidiaRuntimeClasses = defaultNVIDIARuntimeClasses
	}
	a := PodAdmission{nvidiaRuntimeClasses: map[string]bool{}, allGPUsNamespaces: map[string]bool{}}
	for _, c := range nvidiaRuntimeClasses {
		a.nvidiaRuntimeClasses[c] = true
	}
	for _, ns := range allGPUsNamespaces {
		a.allGPUsNamespaces[ns] = true
	}
	return a
}

// PodAdmissionFromEnv reads NVIDIA_RUNTIME_CLASSES and ALL_GPUS_NAMESPACES, both comma-separated lists.
func PodAdmissionFromEnv() PodAdmission {
	return NewPodAdmission(parseCommaList(os.Getenv("NVIDIA_RUNTIME_CLASSES")), parseCommaList(os.Getenv("ALL_GPUS_NAMESPACES")))
}

// Refusal returns why the plugin cannot run the pod as it asks, or nil when it can.
func (a PodAdmission) Refusal(pod *Pod) *PodRefusal {
	if len(pod.Spec.Containers) > 1 {
		// TODO(simonepri): run sidecars and every regular container of a pod; the runner runs init containers and one
		// regular container only.
		extra := make([]string, 0, len(pod.Spec.Containers)-1)
		for _, c := range pod.Spec.Containers[1:] {
			extra = append(extra, fmt.Sprintf("%q", c.Name))
		}
		return &PodRefusal{Reason: reasonMultipleContainersUnsupported, Message: fmt.Sprintf(
			"CloudVM workers run one regular container per pod; container %q would run and %s would never start",
			pod.Spec.Containers[0].Name, strings.Join(extra, ", "))}
	}
	if rc := pod.Spec.RuntimeClassName; rc != nil && *rc != "" && !a.nvidiaRuntimeClasses[*rc] {
		return &PodRefusal{Reason: reasonRuntimeClassUnsupported, Message: fmt.Sprintf(
			"CloudVM workers have no runtime for runtime class %q; they run NVIDIA runtime classes only and never run a pod without the sandbox it asks for",
			*rc)}
	}
	return nil
}

// VisibleGPUs returns the NVIDIA_VISIBLE_DEVICES value whose GPUs the main container sees when the pod requests none,
// as the NVIDIA container runtime exposes them on a kubelet node. Only a DaemonSet pod of an ALL_GPUS_NAMESPACES
// namespace with an NVIDIA runtime class gets them; when another pod asks for devices, ignored says why.
func (a PodAdmission) VisibleGPUs(pod *Pod, env map[string]string, requestedGPUs int) (devices, ignored string) {
	rc := pod.Spec.RuntimeClassName
	if rc == nil || !a.nvidiaRuntimeClasses[*rc] || requestedGPUs > 0 {
		return "", ""
	}
	devices = strings.TrimSpace(env[envNVIDIAVisibleDevices])
	switch devices {
	case "", "none", "void":
		return "", ""
	}
	if !pod.Metadata.DaemonSetOwned() || !a.allGPUsNamespaces[pod.Metadata.Namespace] {
		return "", fmt.Sprintf("the pod runs without GPUs: %s=%s exposes GPUs the pod does not request only to DaemonSet pods of the ALL_GPUS_NAMESPACES namespaces",
			envNVIDIAVisibleDevices, devices)
	}
	return devices, ""
}

// visibleGPUDevices returns the device indexes of a VM with count GPUs that an NVIDIA_VISIBLE_DEVICES value names. The
// runner exposes them like GPUs the pod was assigned, without taking them from the pods that were.
func visibleGPUDevices(visible string, count int) ([]int, error) {
	if count <= 0 {
		return nil, fmt.Errorf("the VM has no GPUs")
	}
	var devices []int
	if visible == "all" {
		for i := range count {
			devices = append(devices, i)
		}
		return devices, nil
	}
	for _, field := range strings.Split(visible, ",") {
		i, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || i < 0 || i >= count {
			return nil, fmt.Errorf("%q is not the index of one of its %d GPUs", field, count)
		}
		devices = append(devices, i)
	}
	return devices, nil
}
