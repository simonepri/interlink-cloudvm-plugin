// Derives how the in-VM runner isolates a pod's containers the way the kubelet would: each container's effective
// security settings from the pod and container security contexts, and the pod's QoS cgroup.
package main

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// ContainerSecurity is a container's effective security context, which the runner turns into nerdctl flags.
type ContainerSecurity struct {
	// RunAsUser and RunAsGroup unset run the container as the image's user and group, as the kubelet does.
	RunAsUser    *int64 `json:"runAsUser,omitempty"`
	RunAsGroup   *int64 `json:"runAsGroup,omitempty"`
	RunAsNonRoot bool   `json:"runAsNonRoot,omitempty"`
	// SupplementalGroups holds the pod's supplementalGroups and fsGroup.
	SupplementalGroups     []int64  `json:"supplementalGroups,omitempty"`
	Privileged             bool     `json:"privileged,omitempty"`
	CapAdd                 []string `json:"capAdd,omitempty"`
	CapDrop                []string `json:"capDrop,omitempty"`
	NoNewPrivileges        bool     `json:"noNewPrivileges,omitempty"`
	ReadOnlyRootFilesystem bool     `json:"readOnlyRootFilesystem,omitempty"`
	SeccompUnconfined      bool     `json:"seccompUnconfined,omitempty"`
	AppArmorUnconfined     bool     `json:"appArmorUnconfined,omitempty"`
}

// EffectiveSecurity merges the pod's security context into the container's, the container's fields winning, as the
// kubelet's DetermineEffectiveSecurityContext does.
func EffectiveSecurity(pod *Pod, c *ContainerSpec) ContainerSecurity {
	var out ContainerSecurity
	var seccomp *corev1.SeccompProfile
	var apparmor *corev1.AppArmorProfile
	if psc := pod.Spec.SecurityContext; psc != nil {
		out.RunAsUser, out.RunAsGroup = psc.RunAsUser, psc.RunAsGroup
		out.RunAsNonRoot = psc.RunAsNonRoot != nil && *psc.RunAsNonRoot
		out.SupplementalGroups = append(out.SupplementalGroups, psc.SupplementalGroups...)
		if psc.FSGroup != nil {
			out.SupplementalGroups = append(out.SupplementalGroups, *psc.FSGroup)
		}
		seccomp, apparmor = psc.SeccompProfile, psc.AppArmorProfile
	}
	if sc := c.SecurityContext; sc != nil {
		if sc.RunAsUser != nil {
			out.RunAsUser = sc.RunAsUser
		}
		if sc.RunAsGroup != nil {
			out.RunAsGroup = sc.RunAsGroup
		}
		if sc.RunAsNonRoot != nil {
			out.RunAsNonRoot = *sc.RunAsNonRoot
		}
		out.Privileged = sc.Privileged != nil && *sc.Privileged
		if sc.Capabilities != nil {
			out.CapAdd = capabilityNames(sc.Capabilities.Add)
			out.CapDrop = capabilityNames(sc.Capabilities.Drop)
		}
		// The kubelet sets no_new_privs only when allowPrivilegeEscalation is false.
		out.NoNewPrivileges = sc.AllowPrivilegeEscalation != nil && !*sc.AllowPrivilegeEscalation
		out.ReadOnlyRootFilesystem = sc.ReadOnlyRootFilesystem != nil && *sc.ReadOnlyRootFilesystem
		if sc.SeccompProfile != nil {
			seccomp = sc.SeccompProfile
		}
		if sc.AppArmorProfile != nil {
			apparmor = sc.AppArmorProfile
		}
	}
	out.SeccompUnconfined = seccomp != nil && seccomp.Type == corev1.SeccompProfileTypeUnconfined
	out.AppArmorUnconfined = apparmor != nil && apparmor.Type == corev1.AppArmorProfileTypeUnconfined
	return out
}

func capabilityNames(caps []corev1.Capability) []string {
	out := make([]string, 0, len(caps))
	for _, c := range caps {
		out = append(out, strings.ToUpper(string(c)))
	}
	return out
}

// ValidatePodRuntime refuses a pod that asks for a runtime setting a worker cannot honor.
func ValidatePodRuntime(pod *Pod) error {
	if psc := pod.Spec.SecurityContext; psc != nil {
		if len(psc.Sysctls) > 0 {
			return fmt.Errorf("pod securityContext.sysctls are not supported: containers share the worker's host network and kernel")
		}
		if err := validateProfiles(psc.SeccompProfile, psc.AppArmorProfile); err != nil {
			return fmt.Errorf("pod securityContext: %w", err)
		}
	}
	for i := range pod.Spec.InitContainers {
		c := &pod.Spec.InitContainers[i]
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			return fmt.Errorf("init container %q: sidecar init containers (restartPolicy Always) are not supported", c.Name)
		}
	}
	for _, list := range [][]ContainerSpec{pod.Spec.InitContainers, pod.Spec.Containers} {
		for i := range list {
			if sc := list[i].SecurityContext; sc != nil {
				if err := validateProfiles(sc.SeccompProfile, sc.AppArmorProfile); err != nil {
					return fmt.Errorf("container %q: %w", list[i].Name, err)
				}
			}
		}
	}
	return nil
}

func validateProfiles(seccomp *corev1.SeccompProfile, apparmor *corev1.AppArmorProfile) error {
	if seccomp != nil && seccomp.Type == corev1.SeccompProfileTypeLocalhost {
		return fmt.Errorf("seccompProfile type Localhost is not supported: workers have no node-local profiles")
	}
	if apparmor != nil && apparmor.Type == corev1.AppArmorProfileTypeLocalhost {
		return fmt.Errorf("appArmorProfile type Localhost is not supported: workers have no node-local profiles")
	}
	return nil
}

// PodCgroupParent is the cgroup the runner puts the pod's containers under, laid out as the kubelet's cgroupfs driver
// lays out pod cgroups, so node agents map a container's /proc/<pid>/cgroup to its pod. A pod whose containers request
// or limit no CPU or memory is BestEffort; every other pod goes under burstable, Guaranteed ones included, so each path
// has a QoS segment.
func PodCgroupParent(pod *Pod, uid string) string {
	qos := "besteffort"
	for _, list := range [][]ContainerSpec{pod.Spec.InitContainers, pod.Spec.Containers} {
		for _, c := range list {
			for _, res := range []map[string]string{c.Resources.Requests, c.Resources.Limits} {
				if res[string(corev1.ResourceCPU)] != "" || res[string(corev1.ResourceMemory)] != "" {
					qos = "burstable"
				}
			}
		}
	}
	return "/kubepods/" + qos + "/pod" + uid
}
