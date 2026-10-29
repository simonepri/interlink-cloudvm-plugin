package main

import "fmt"

const (
	// rayNodeTypeLabel is set by KubeRay on every Ray pod.
	rayNodeTypeLabel = "ray.io/node-type"

	// tcpProxyPort is the port of the VM agent's local HTTP CONNECT proxy (CLOUDVM_TCP_PROXY_PORT default).
	tcpProxyPort = 13128

	// transparentProxyPort is the port of the VM agent's listener for redirected Service connections
	// (CLOUDVM_TRANSPARENT_PORT).
	transparentProxyPort = 13129
)

// injectRayProxyEnv adds the gRPC proxy settings to every container of a KubeRay pod, keeping values the pod sets.
// Only gRPC traffic uses the proxy; plain HTTP clients are unaffected.
func injectRayProxyEnv(pod *Pod) {
	if _, ok := pod.Metadata.Labels[rayNodeTypeLabel]; !ok {
		return
	}
	defaults := []EnvVar{
		{Name: "RAY_grpc_enable_http_proxy", Value: "1"},
		{Name: "grpc_proxy", Value: fmt.Sprintf("http://127.0.0.1:%d", tcpProxyPort)},
		{Name: "no_grpc_proxy", Value: "127.0.0.1,localhost"},
	}
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		have := make(map[string]bool, len(c.Env))
		for _, e := range c.Env {
			have[e.Name] = true
		}
		for _, d := range defaults {
			if !have[d.Name] {
				c.Env = append(c.Env, d)
			}
		}
	}
}
