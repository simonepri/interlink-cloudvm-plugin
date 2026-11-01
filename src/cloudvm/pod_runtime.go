// Builds the part of a runner start request that makes the worker run the pod as the kubelet would: its cgroup, host
// namespaces, volumes, init containers, and each container's environment, mounts and security settings.
package main

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// PodRuntime is how the runner runs a pod besides the main container's image, command and resources.
type PodRuntime struct {
	Volumes        []PodVolume
	Mounts         []ContainerMount
	Security       ContainerSecurity
	HostIPEnv      []string
	InitContainers []ContainerRun
	CgroupParent   string
	HostPID        bool
	HostIPC        bool
	RestartPolicy  string
	FSGroup        *int64
}

// BuildPodRuntime resolves the volumes and containers of a pod PodToInstanceSpec accepted, minting the service account tokens its volumes project,
// and returns the main container's environment, which the caller merges into the spec.
func BuildPodRuntime(ctx context.Context, pod *Pod, uid string, src *PodSources) (*PodRuntime, *ContainerEnv, []PodToken, error) {
	volumes, tokens, err := ResolvePodVolumes(ctx, pod, src)
	if err != nil {
		return nil, nil, nil, err
	}
	rt := &PodRuntime{
		Volumes:       volumes,
		CgroupParent:  PodCgroupParent(pod, uid),
		HostPID:       pod.Spec.HostPID,
		HostIPC:       pod.Spec.HostIPC,
		RestartPolicy: string(pod.Spec.RestartPolicy),
	}
	if pod.Spec.SecurityContext != nil {
		rt.FSGroup = pod.Spec.SecurityContext.FSGroup
	}
	for i := range pod.Spec.InitContainers {
		c := &pod.Spec.InitContainers[i]
		env, err := ResolveContainerEnv(ctx, pod, c, src)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("init container %q: %w", c.Name, err)
		}
		mounts, err := ContainerMounts(c, volumes)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("init container %q: %w", c.Name, err)
		}
		sec := EffectiveSecurity(pod, c)
		rt.InitContainers = append(rt.InitContainers, ContainerRun{
			Name: c.Name, Image: c.Image, Command: env.Command, Args: env.Args, Env: env.Vars, HostIPEnv: env.HostIPVars,
			CPUMillicores: quantity(c.Resources.Limits, corev1.ResourceCPU).MilliValue(),
			MemoryBytes:   quantity(c.Resources.Limits, corev1.ResourceMemory).Value(),
			Mounts:        mounts, Security: &sec,
		})
	}
	if len(pod.Spec.Containers) == 0 {
		return rt, &ContainerEnv{}, tokens, nil
	}
	main := &pod.Spec.Containers[0]
	env, err := ResolveContainerEnv(ctx, pod, main, src)
	if err != nil {
		return nil, nil, nil, err
	}
	if rt.Mounts, err = ContainerMounts(main, volumes); err != nil {
		return nil, nil, nil, err
	}
	rt.Security = EffectiveSecurity(pod, main)
	rt.HostIPEnv = env.HostIPVars
	return rt, env, tokens, nil
}

func quantity(m map[string]string, name corev1.ResourceName) *resource.Quantity {
	q, err := resource.ParseQuantity(m[string(name)])
	if err != nil {
		return resource.NewQuantity(0, resource.DecimalSI)
	}
	return &q
}

// apply copies the runtime into the runner start request.
func (rt *PodRuntime) apply(req *StartContainerRequest) {
	if rt == nil {
		return
	}
	sec := rt.Security
	req.Volumes = rt.Volumes
	req.Mounts = rt.Mounts
	req.Security = &sec
	req.HostIPEnv = rt.HostIPEnv
	req.InitContainers = rt.InitContainers
	req.CgroupParent = rt.CgroupParent
	req.HostPID = rt.HostPID
	req.HostIPC = rt.HostIPC
	req.RestartPolicy = rt.RestartPolicy
	req.FSGroup = rt.FSGroup
}
