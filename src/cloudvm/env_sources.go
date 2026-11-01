// Resolves a container's environment the way the kubelet does: the API server Service variables, then envFrom, then
// env in order, with Secret, ConfigMap, pod field and resource sources, and $(VAR) references in values and arguments.
package main

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// PodSources are where a pod's environment and volumes read their contents from.
type PodSources struct {
	// Provided are the Secrets of the create request, read before the Kubernetes API.
	Provided   []corev1.Secret
	Secrets    SecretResolver
	ConfigMaps ConfigMapResolver
	Tokens     ServiceAccountTokenIssuer
	// Base holds the variables every container gets before its own, which its own override.
	Base map[string]string

	secrets    map[string]map[string][]byte
	configMaps map[string]map[string][]byte
}

// ContainerEnv is a container's resolved environment and its arguments with variable references expanded.
type ContainerEnv struct {
	Vars    map[string]string
	Command []string
	Args    []string
	// HostIPVars name the variables set from status.podIP or status.hostIP. A container shares the worker's network,
	// so both are the worker's address, which the runner fills in together with their $(NAME) references.
	HostIPVars []string
}

// APIServerEnv returns the variables the kubelet gives every container for the default/kubernetes Service.
func APIServerEnv(host string, port int32) map[string]string {
	if host == "" || port == 0 {
		return nil
	}
	p := strconv.Itoa(int(port))
	url := "tcp://" + host + ":" + p
	prefix := "KUBERNETES_PORT_" + p + "_TCP"
	return map[string]string{
		"KUBERNETES_SERVICE_HOST":       host,
		"KUBERNETES_SERVICE_PORT":       p,
		"KUBERNETES_SERVICE_PORT_HTTPS": p,
		"KUBERNETES_PORT":               url,
		prefix:                          url,
		prefix + "_PROTO":               "tcp",
		prefix + "_PORT":                p,
		prefix + "_ADDR":                host,
	}
}

// ResolveContainerEnv returns the container's environment. A missing optional Secret, ConfigMap or key leaves its
// variables out; a missing required one, or a source the worker cannot provide, is an error.
func ResolveContainerEnv(ctx context.Context, pod *Pod, c *ContainerSpec, src *PodSources) (*ContainerEnv, error) {
	out := &ContainerEnv{Vars: map[string]string{}}
	for k, v := range src.Base {
		out.Vars[k] = v
	}
	hostIP := map[string]bool{}
	for _, from := range c.EnvFrom {
		var data map[string][]byte
		var err error
		var optional bool
		switch {
		case from.ConfigMapRef != nil:
			optional = from.ConfigMapRef.Optional != nil && *from.ConfigMapRef.Optional
			data, err = src.configMap(ctx, podNamespace(pod), from.ConfigMapRef.Name)
		case from.SecretRef != nil:
			optional = from.SecretRef.Optional != nil && *from.SecretRef.Optional
			data, err = src.secret(ctx, podNamespace(pod), from.SecretRef.Name)
		default:
			continue
		}
		if err != nil {
			if optional {
				continue
			}
			return nil, fmt.Errorf("envFrom: %w", err)
		}
		for _, k := range sortedKeys(data) {
			// The kubelet skips keys that are not valid variable names and reports them in an event.
			if name := from.Prefix + k; name != "" && !strings.Contains(name, "=") {
				out.Vars[name] = string(data[k])
				delete(hostIP, name)
			}
		}
	}
	mapping := func(name string) (string, bool) {
		v, ok := out.Vars[name]
		return v, ok && !hostIP[name]
	}
	for _, env := range c.Env {
		if env.ValueFrom == nil {
			out.Vars[env.Name] = expandEnv(env.Value, mapping)
			delete(hostIP, env.Name)
			continue
		}
		value, isHostIP, skip, err := src.envValue(ctx, pod, c, env.ValueFrom)
		if err != nil {
			return nil, fmt.Errorf("env %s: %w", env.Name, err)
		}
		if skip {
			continue
		}
		out.Vars[env.Name] = value
		if isHostIP {
			hostIP[env.Name] = true
		} else {
			delete(hostIP, env.Name)
		}
	}
	for _, a := range c.Command {
		out.Command = append(out.Command, expandEnv(a, mapping))
	}
	for _, a := range c.Args {
		out.Args = append(out.Args, expandEnv(a, mapping))
	}
	for name := range hostIP {
		out.HostIPVars = append(out.HostIPVars, name)
	}
	sort.Strings(out.HostIPVars)
	return out, nil
}

// envValue resolves one valueFrom; skip reports an optional source that is missing.
func (src *PodSources) envValue(ctx context.Context, pod *Pod, c *ContainerSpec, from *corev1.EnvVarSource) (value string, hostIP, skip bool, err error) {
	ns := podNamespace(pod)
	switch {
	case from.SecretKeyRef != nil:
		ref := from.SecretKeyRef
		return keyValue(ref.Optional, ref.Key, "secret", ns, ref.Name)(src.secret(ctx, ns, ref.Name))
	case from.ConfigMapKeyRef != nil:
		ref := from.ConfigMapKeyRef
		return keyValue(ref.Optional, ref.Key, "configmap", ns, ref.Name)(src.configMap(ctx, ns, ref.Name))
	case from.FieldRef != nil:
		switch from.FieldRef.FieldPath {
		case "status.podIP", "status.hostIP", "status.podIPs", "status.hostIPs":
			return "", true, false, nil
		}
		value, err = podFieldValue(pod, from.FieldRef.FieldPath)
		return value, false, false, err
	case from.ResourceFieldRef != nil:
		value, ok, err := resourceFieldValue(pod, c, from.ResourceFieldRef)
		return value, false, !ok, err
	}
	return "", false, true, nil
}

// keyValue returns the reader of key from a Secret or ConfigMap lookup's result.
func keyValue(optional *bool, key, kind, ns, name string) func(map[string][]byte, error) (string, bool, bool, error) {
	opt := optional != nil && *optional
	return func(data map[string][]byte, err error) (string, bool, bool, error) {
		if err != nil {
			if opt {
				return "", false, true, nil
			}
			return "", false, false, err
		}
		v, ok := data[key]
		if !ok {
			if opt {
				return "", false, true, nil
			}
			return "", false, false, fmt.Errorf("%s %s/%s has no key %q", kind, ns, name, key)
		}
		return string(v), false, false, nil
	}
}

func (src *PodSources) secret(ctx context.Context, ns, name string) (map[string][]byte, error) {
	if data, ok := src.secrets[name]; ok {
		return data, nil
	}
	data, err := pullSecretData(ctx, src.Provided, src.Secrets, ns, name)
	if err != nil {
		return nil, fmt.Errorf("secret %s/%s: %w", ns, name, err)
	}
	if src.secrets == nil {
		src.secrets = map[string]map[string][]byte{}
	}
	src.secrets[name] = data
	return data, nil
}

func (src *PodSources) configMap(ctx context.Context, ns, name string) (map[string][]byte, error) {
	if data, ok := src.configMaps[name]; ok {
		return data, nil
	}
	if src.ConfigMaps == nil {
		return nil, fmt.Errorf("configmap %s/%s: no Kubernetes API access", ns, name)
	}
	data, err := src.ConfigMaps.GetConfigMap(ctx, ns, name)
	if err != nil {
		return nil, fmt.Errorf("configmap %s/%s: %w", ns, name, err)
	}
	if src.configMaps == nil {
		src.configMaps = map[string]map[string][]byte{}
	}
	src.configMaps[name] = data
	return data, nil
}

// podFieldValue returns the pod field a fieldRef names, as the downward API renders it.
func podFieldValue(pod *Pod, path string) (string, error) {
	switch path {
	case "metadata.name":
		return pod.Metadata.Name, nil
	case "metadata.namespace":
		return podNamespace(pod), nil
	case "metadata.uid":
		return pod.Metadata.UID, nil
	case "metadata.labels":
		return formatDownwardMap(pod.Metadata.Labels), nil
	case "metadata.annotations":
		return formatDownwardMap(pod.Metadata.Annotations), nil
	case "spec.nodeName":
		return pod.Spec.NodeName, nil
	case "spec.serviceAccountName":
		return pod.Spec.ServiceAccountName, nil
	}
	for prefix, m := range map[string]map[string]string{"metadata.labels": pod.Metadata.Labels, "metadata.annotations": pod.Metadata.Annotations} {
		if key, ok := strings.CutPrefix(path, prefix+"['"); ok && strings.HasSuffix(key, "']") {
			return m[strings.TrimSuffix(key, "']")], nil
		}
	}
	return "", fmt.Errorf("fieldRef %q is not supported", path)
}

// formatDownwardMap renders labels or annotations as the kubelet writes them: one key="value" line per key, sorted.
func formatDownwardMap(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	lines := make([]string, 0, len(keys))
	for _, k := range keys {
		lines = append(lines, k+"="+strconv.Quote(m[k]))
	}
	return strings.Join(lines, "\n")
}

// resourceFieldValue returns the container resource a resourceFieldRef names, rounded up to its divisor. A limit the
// container does not set falls back to its request, since the plugin does not know the worker's allocatable resources
// that the kubelet would report; ok is false when neither is set.
func resourceFieldValue(pod *Pod, self *ContainerSpec, ref *corev1.ResourceFieldSelector) (string, bool, error) {
	c := self
	if ref.ContainerName != "" {
		c = nil
		for _, list := range [][]ContainerSpec{pod.Spec.Containers, pod.Spec.InitContainers} {
			for i := range list {
				if list[i].Name == ref.ContainerName {
					c = &list[i]
				}
			}
		}
	}
	if c == nil {
		return "", false, fmt.Errorf("resourceFieldRef %q names no container %q", ref.Resource, ref.ContainerName)
	}
	kind, name, found := strings.Cut(ref.Resource, ".")
	if !found || (kind != "limits" && kind != "requests") {
		return "", false, fmt.Errorf("resourceFieldRef %q is not supported", ref.Resource)
	}
	raw := c.Resources.Requests[name]
	if kind == "limits" && c.Resources.Limits[name] != "" {
		raw = c.Resources.Limits[name]
	}
	if raw == "" {
		return "", false, nil
	}
	q, err := resource.ParseQuantity(raw)
	if err != nil {
		return "", false, fmt.Errorf("resourceFieldRef %q: %w", ref.Resource, err)
	}
	divisor := ref.Divisor
	if divisor.IsZero() {
		divisor = resource.MustParse("1")
	}
	if name == string(corev1.ResourceCPU) {
		return strconv.FormatInt(int64(math.Ceil(float64(q.MilliValue())/float64(divisor.MilliValue()))), 10), true, nil
	}
	return strconv.FormatInt(int64(math.Ceil(float64(q.Value())/float64(divisor.Value()))), 10), true, nil
}

// expandEnv replaces each $(NAME) that mapping defines and turns $$ into $, leaving other references as written, with
// the rules of the kubelet's expansion package.
func expandEnv(input string, mapping func(string) (string, bool)) string {
	var b strings.Builder
	for i := 0; i < len(input); i++ {
		if input[i] != '$' || i+1 >= len(input) {
			b.WriteByte(input[i])
			continue
		}
		switch next := input[i+1]; next {
		case '$':
			b.WriteByte('$')
			i++
		case '(':
			end := strings.IndexByte(input[i+2:], ')')
			if end < 0 {
				b.WriteString("$(")
				i++
				continue
			}
			name := input[i+2 : i+2+end]
			if v, ok := mapping(name); ok {
				b.WriteString(v)
			} else {
				b.WriteString("$(" + name + ")")
			}
			i += 2 + end
		default:
			b.WriteByte('$')
		}
	}
	return b.String()
}

func podNamespace(pod *Pod) string {
	if pod.Metadata.Namespace == "" {
		return "default"
	}
	return pod.Metadata.Namespace
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
