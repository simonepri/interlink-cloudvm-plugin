// Worker profiling settings: the parca-agent config and flags every worker VM boots its profiler with, derived from the
// operator's in-cluster agent config so worker profiles carry the same Kubernetes labels.
package main

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"
)

// Environment variables the runner sets on every container of a profiled worker; the worker's parca-agent reads them
// from each process, since a VM has no kubelet whose pods it could look up.
const (
	profilingEnvNamespace      = "CLOUDVM_POD_NAMESPACE"
	profilingEnvPodName        = "CLOUDVM_POD_NAME"
	profilingEnvPodUID         = "CLOUDVM_POD_UID"
	profilingEnvContainerName  = "CLOUDVM_CONTAINER_NAME"
	profilingEnvContainerImage = "CLOUDVM_CONTAINER_IMAGE"
	profilingEnvNodeName       = "CLOUDVM_NODE_NAME"
	profilingEnvPodLabelPrefix = "CLOUDVM_POD_LABEL_"
)

// profilingPodMeta maps each runner-set variable to the Kubernetes meta label the in-cluster agent derives from the pod.
var profilingPodMeta = []struct{ env, meta string }{
	{profilingEnvNamespace, "__meta_kubernetes_namespace"},
	{profilingEnvPodName, "__meta_kubernetes_pod_name"},
	{profilingEnvPodUID, "__meta_kubernetes_pod_uid"},
	{profilingEnvContainerName, "__meta_kubernetes_pod_container_name"},
	{profilingEnvContainerImage, "__meta_kubernetes_pod_container_image"},
	{profilingEnvNodeName, "__meta_kubernetes_node_name"},
}

// podLabelMetaPattern finds the pod labels an agent config reads, by their sanitized label names.
var podLabelMetaPattern = regexp.MustCompile(`__meta_kubernetes_pod_label_([a-zA-Z0-9_]+)`)

// WorkerProfiling is what a worker VM needs to run parca-agent: its config, its extra flags, and the sanitized names of
// the pod labels the runner exposes to it.
type WorkerProfiling struct {
	// Config is the parca-agent config file: rules that turn the runner-set variables into Kubernetes meta labels,
	// followed by the operator's relabel rules.
	Config string `json:"config"`
	// Args are the operator's extra parca-agent flags, such as sampling settings and external labels.
	Args []string `json:"args,omitempty"`
	// PodLabels are the sanitized names of the pod labels the operator's rules read.
	PodLabels []string `json:"podLabels,omitempty"`
}

// EnvVars returns the variables parca-agent must read from each process, in a stable order.
func (p *WorkerProfiling) EnvVars() []string {
	vars := make([]string, 0, len(profilingPodMeta)+len(p.PodLabels))
	for _, m := range profilingPodMeta {
		vars = append(vars, m.env)
	}
	for _, l := range p.PodLabels {
		vars = append(vars, profilingEnvPodLabelPrefix+l)
	}
	return vars
}

// PodLabelList returns the comma-separated sanitized pod label names the runner exposes.
func (p *WorkerProfiling) PodLabelList() string {
	return strings.Join(p.PodLabels, ",")
}

// LoadWorkerProfiling reads the operator's parca-agent config at path, which may be empty, and builds the worker config.
func LoadWorkerProfiling(path string, args []string) (*WorkerProfiling, error) {
	var raw []byte
	if path != "" {
		var err error
		if raw, err = os.ReadFile(path); err != nil { //nolint:gosec // operator-configured path
			return nil, fmt.Errorf("cannot read profiling config: %w", err)
		}
	}
	return NewWorkerProfiling(raw, args)
}

// NewWorkerProfiling builds the worker config from the operator's parca-agent config, written for the in-cluster
// agent's __meta_kubernetes_* labels.
func NewWorkerProfiling(operatorConfig []byte, args []string) (*WorkerProfiling, error) {
	var parsed struct {
		RelabelConfigs []any `json:"relabel_configs"`
	}
	if err := yaml.UnmarshalStrict(operatorConfig, &parsed); err != nil {
		return nil, fmt.Errorf("invalid profiling config: %w", err)
	}
	seen := make(map[string]bool)
	var labels []string
	for _, m := range podLabelMetaPattern.FindAllStringSubmatch(string(operatorConfig), -1) {
		if !seen[m[1]] {
			seen[m[1]] = true
			labels = append(labels, m[1])
		}
	}
	sort.Strings(labels)

	rules := make([]any, 0, len(profilingPodMeta)+len(labels)+1+len(parsed.RelabelConfigs))
	for _, m := range profilingPodMeta {
		rules = append(rules, map[string]any{"source_labels": []string{"__meta_env_var_" + m.env}, "target_label": m.meta})
	}
	for _, l := range labels {
		rules = append(rules, map[string]any{
			"source_labels": []string{"__meta_env_var_" + profilingEnvPodLabelPrefix + l},
			"target_label":  "__meta_kubernetes_pod_label_" + l,
		})
	}
	// The in-cluster agent names the Kubernetes node in the node label; a VM's agent knows only its hostname.
	rules = append(rules, map[string]any{
		"source_labels": []string{"__meta_env_var_" + profilingEnvNodeName},
		"regex":         "(.+)",
		"target_label":  "node",
	})
	rules = append(rules, parsed.RelabelConfigs...)
	out, err := yaml.Marshal(map[string]any{"relabel_configs": rules})
	if err != nil {
		return nil, fmt.Errorf("cannot render profiling config: %w", err)
	}
	return &WorkerProfiling{Config: string(out), Args: args, PodLabels: labels}, nil
}
