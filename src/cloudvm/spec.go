// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// sanitizeLog strips newline and carriage-return characters to prevent log injection.
func sanitizeLog(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\n", ""), "\r", "")
}

// EgressRateToBandwidth converts an hourly budget in USD/hr to an equivalent network rate string (e.g. "22mbit").
// Uses provider baseline egress rates ($0.09/GB for AWS, $0.085/GB for GCP).
func EgressRateToBandwidth(dollarPerHour float64, provider string) string {
	if dollarPerHour <= 0 {
		return ""
	}
	costPerGB := 0.09 // default AWS
	if strings.EqualFold(provider, "gcp") {
		costPerGB = 0.085
	}
	// Megabits per second = (GB/hr * 8 * 1000) / 3600
	mbps := (dollarPerHour / costPerGB) * 8000.0 / 3600.0
	if mbps < 1.0 {
		return fmt.Sprintf("%dkbit", int(mbps*1000))
	}
	return fmt.Sprintf("%dmbit", int(mbps))
}

// PodToInstanceSpec converts an incoming InterLink Pod into an InstanceSpec.
func PodToInstanceSpec(pod *Pod, defaultClusterTag string, defaultRegion ...string) (*InstanceSpec, error) {
	// Enforce non-root containers on host network: reject runAsUser 0 or runAsGroup 0
	for _, c := range pod.Spec.Containers {
		if c.SecurityContext != nil {
			if c.SecurityContext.RunAsUser != nil && *c.SecurityContext.RunAsUser == 0 {
				return nil, fmt.Errorf("cloudvm provider enforces non-root containers; container %q specifies runAsUser 0 which is forbidden on host network", c.Name)
			}
			if c.SecurityContext.RunAsGroup != nil && *c.SecurityContext.RunAsGroup == 0 {
				return nil, fmt.Errorf("cloudvm provider enforces non-root containers; container %q specifies runAsGroup 0 which is forbidden on host network", c.Name)
			}
		}
	}
	if pod.Spec.SecurityContext != nil {
		if pod.Spec.SecurityContext.RunAsUser != nil && *pod.Spec.SecurityContext.RunAsUser == 0 {
			return nil, fmt.Errorf("cloudvm provider enforces non-root containers; pod securityContext specifies runAsUser 0 which is forbidden on host network")
		}
		if pod.Spec.SecurityContext.RunAsGroup != nil && *pod.Spec.SecurityContext.RunAsGroup == 0 {
			return nil, fmt.Errorf("cloudvm provider enforces non-root containers; pod securityContext specifies runAsGroup 0 which is forbidden on host network")
		}
	}
	ns := pod.Metadata.Namespace
	if ns == "" {
		ns = "default"
	}
	name := pod.Metadata.Name
	uid := pod.Metadata.UID
	if uid == "" {
		uid = pod.Metadata.Annotations["kubernetes.io/pod-uid"]
	}
	if uid == "" {
		uid = fmt.Sprintf("%s/%s", ns, name)
	}

	// 0. Cloud Provider selection
	prov := pod.Metadata.Annotations["cloudvm.vk.io/provider"]
	if prov == "" {
		prov = pod.Spec.NodeSelector["cloudvm.vk.io/provider"]
	}

	// 1. Region
	region := pod.Metadata.Annotations["cloudvm.vk.io/region"]
	if region == "" {
		region = pod.Spec.NodeSelector["topology.kubernetes.io/region"]
	}
	if region == "" {
		if len(defaultRegion) > 0 && defaultRegion[0] != "" {
			region = defaultRegion[0]
		} else {
			region = "us-east-1"
		}
	}

	var candidateRegions []string
	if cand := pod.Metadata.Annotations["cloudvm.vk.io/candidate-regions"]; cand != "" {
		for _, r := range strings.Split(cand, ",") {
			r = strings.TrimSpace(r)
			if r != "" {
				candidateRegions = append(candidateRegions, r)
			}
		}
	}

	// 2. Instance Type
	instanceType := pod.Metadata.Annotations["cloudvm.vk.io/instance-type"]
	if instanceType == "" {
		instanceType = pod.Spec.NodeSelector["node.kubernetes.io/instance-type"]
	}
	if instanceType == "" {
		instanceType = "g5.2xlarge"
	}

	// 3. Market / Capacity Type (spot vs on-demand)
	marketType := pod.Metadata.Annotations["cloudvm.vk.io/capacity-type"]
	if marketType == "" && pod.Metadata.Annotations["cloudvm.vk.io/spot"] == "false" {
		marketType = "on-demand"
	}
	if marketType == "" {
		marketType = pod.Spec.NodeSelector["cloudvm.vk.io/capacity-type"]
	}
	if marketType == "" {
		marketType = "spot"
	}

	// 4. AMI Mode (dlami vs container)
	amiMode := pod.Metadata.Annotations["cloudvm.vk.io/ami-mode"]
	if amiMode == "" {
		amiMode = "container"
	}

	// 5. Max Price cap & policy
	maxPrice := pod.Metadata.Annotations["cloudvm.vk.io/max-price"]
	if maxPrice == "" {
		maxPrice = pod.Metadata.Annotations["cloudvm.vk.io/max-spot-price"]
	}
	pricePolicy := pod.Metadata.Annotations["cloudvm.vk.io/price-policy"]
	if pricePolicy == "" {
		pricePolicy = pod.Metadata.Annotations["cloudvm.vk.io/fleet-allocation-strategy"]
	}
	if pricePolicy == "" {
		pricePolicy = "fail-fast"
	}

	// Cost allocation & tags
	costCenter := pod.Metadata.Annotations["cloudvm.vk.io/cost-center"]
	jobID := pod.Metadata.Annotations["cloudvm.vk.io/job-id"]
	launchTemplateID := pod.Metadata.Annotations["cloudvm.vk.io/aws-launch-template"]
	launchTemplateName := pod.Metadata.Annotations["cloudvm.vk.io/aws-launch-template-name"]

	var subnetIDs []string
	if rawSubnets := pod.Metadata.Annotations["cloudvm.vk.io/subnets"]; rawSubnets != "" {
		for _, s := range strings.Split(rawSubnets, ",") {
			if trimmed := strings.TrimSpace(s); trimmed != "" {
				subnetIDs = append(subnetIDs, trimmed)
			}
		}
	}
	if len(subnetIDs) == 0 {
		envSubnets := os.Getenv("AWS_SUBNET_IDS")
		if envSubnets == "" {
			envSubnets = os.Getenv("SUBNET_IDS")
		}
		if envSubnets != "" {
			for _, s := range strings.Split(envSubnets, ",") {
				if trimmed := strings.TrimSpace(s); trimmed != "" {
					subnetIDs = append(subnetIDs, trimmed)
				}
			}
		}
	}

	var candidateInstanceTypes []string
	if cand := pod.Metadata.Annotations["cloudvm.vk.io/candidate-instance-types"]; cand != "" {
		for _, t := range strings.Split(cand, ",") {
			if trimmed := strings.TrimSpace(t); trimmed != "" {
				candidateInstanceTypes = append(candidateInstanceTypes, trimmed)
			}
		}
	}
	if len(candidateInstanceTypes) == 0 {
		if envTypes := os.Getenv("AWS_INSTANCE_TYPES"); envTypes != "" {
			for _, t := range strings.Split(envTypes, ",") {
				if trimmed := strings.TrimSpace(t); trimmed != "" {
					candidateInstanceTypes = append(candidateInstanceTypes, trimmed)
				}
			}
		}
	}

	// KMS token encryption is controlled exclusively by provider configuration

	var registryMirrors []string
	if rawMirrors := pod.Metadata.Annotations["cloudvm.vk.io/registry-mirrors"]; rawMirrors != "" {
		for _, m := range strings.Split(rawMirrors, ",") {
			if trimmed := strings.TrimSpace(m); trimmed != "" {
				registryMirrors = append(registryMirrors, trimmed)
			}
		}
	}
	otelEndpoint := pod.Metadata.Annotations["cloudvm.vk.io/otel-endpoint"]
	enableNPD := pod.Metadata.Annotations["cloudvm.vk.io/enable-npd"] == "true" || pod.Metadata.Annotations["cloudvm.vk.io/node-problem-detector"] == "true"
	customBootstrapScript := pod.Metadata.Annotations["cloudvm.vk.io/custom-bootstrap-script"]

	runnerConcurrency := 0
	rawConcurrency := pod.Metadata.Annotations["cloudvm.vk.io/runner-concurrency"]
	if rawConcurrency == "" && pod.Spec.NodeSelector != nil {
		rawConcurrency = pod.Spec.NodeSelector["cloudvm.vk.io/runner-concurrency"]
	}
	if rawConcurrency != "" {
		if rc, err := strconv.Atoi(strings.TrimSpace(rawConcurrency)); err == nil && rc > 0 {
			runnerConcurrency = rc
		}
	}

	// 6. Network Egress Rate Limiting (Token Bucket Filter via rate or dollar/hr spend)
	maxEgressRate := pod.Metadata.Annotations["cloudvm.vk.io/max-egress-rate"]
	egressBandwidth := pod.Metadata.Annotations["cloudvm.vk.io/egress-bandwidth"]
	if egressBandwidth == "" {
		egressBandwidth = pod.Metadata.Annotations["kubernetes.io/egress-bandwidth"]
	}
	egressBurst := pod.Metadata.Annotations["cloudvm.vk.io/egress-burst"]

	// If dollar/hr rate is configured and raw bandwidth is omitted, calculate equivalent token rate
	if maxEgressRate != "" && egressBandwidth == "" {
		clean := strings.TrimPrefix(strings.TrimSpace(maxEgressRate), "$")
		if rateVal, err := strconv.ParseFloat(clean, 64); err == nil && rateVal > 0 {
			egressBandwidth = EgressRateToBandwidth(rateVal, prov)
			if egressBurst == "" {
				egressBurst = "100mb" // generous burst allowance for checkpoints
			}
		}
	}

	var targetImage string
	var command []string
	var args []string
	envMap := make(map[string]string)
	var rayHeadAddress string
	var meshAuthKey string
	numGPUs := 0
	numTPUs := 0

	if len(pod.Spec.Containers) > 0 {
		c := pod.Spec.Containers[0]
		targetImage = c.Image
		command = c.Command
		args = c.Args

		// Extract GPU count
		if gpuStr, exists := c.Resources.Limits["nvidia.com/gpu"]; exists {
			if n, err := strconv.Atoi(gpuStr); err == nil {
				numGPUs = n
			}
		}

		// Extract TPU count
		if tpuStr, exists := c.Resources.Limits["google.com/tpu"]; exists {
			if n, err := strconv.Atoi(tpuStr); err == nil {
				numTPUs = n
			}
		} else if tpuStr, exists := c.Resources.Requests["google.com/tpu"]; exists {
			if n, err := strconv.Atoi(tpuStr); err == nil {
				numTPUs = n
			}
		}

		// Extract environment variables
		for _, env := range c.Env {
			envMap[env.Name] = env.Value
			switch env.Name {
			case "RAY_HEAD_ADDRESS":
				rayHeadAddress = env.Value
			case "TAILSCALE_AUTHKEY", "MESH_AUTH_KEY":
				meshAuthKey = env.Value
			}
		}
	}

	var runAsUser *int64
	var runAsGroup *int64
	if len(pod.Spec.Containers) > 0 {
		c := pod.Spec.Containers[0]
		if c.SecurityContext != nil && c.SecurityContext.RunAsUser != nil {
			runAsUser = c.SecurityContext.RunAsUser
			runAsGroup = c.SecurityContext.RunAsGroup
		}
	}
	if runAsUser == nil && pod.Spec.SecurityContext != nil && pod.Spec.SecurityContext.RunAsUser != nil {
		runAsUser = pod.Spec.SecurityContext.RunAsUser
		runAsGroup = pod.Spec.SecurityContext.RunAsGroup
	}

	var cpuMillicores int64
	var memoryBytes int64
	if len(pod.Spec.Containers) > 0 {
		c := pod.Spec.Containers[0]
		if cpuLimit, exists := c.Resources.Limits["cpu"]; exists {
			if q, err := resource.ParseQuantity(cpuLimit); err == nil {
				cpuMillicores = q.MilliValue()
			}
		}
		if memLimit, exists := c.Resources.Limits["memory"]; exists {
			if q, err := resource.ParseQuantity(memLimit); err == nil {
				memoryBytes = q.Value()
			}
		}
	}

	if rayHeadAddress == "" {
		rayHeadAddress = pod.Metadata.Annotations["ray.io/head-address"]
	}

	return &InstanceSpec{
		PodName:                name,
		PodNamespace:           ns,
		PodUID:                 uid,
		Provider:               prov,
		CostCenter:             costCenter,
		JobID:                  jobID,
		LaunchTemplateID:       launchTemplateID,
		LaunchTemplateName:     launchTemplateName,
		Region:                 region,
		CandidateRegions:       candidateRegions,
		RegionSelection:        strings.TrimSpace(pod.Metadata.Annotations["cloudvm.vk.io/region-selection"]),
		InstanceType:           instanceType,
		MarketType:             marketType,
		AMIMode:                amiMode,
		MaxPrice:               maxPrice,
		MaxSpotPrice:           maxPrice,
		PricePolicy:            pricePolicy,
		MaxEgressRate:          maxEgressRate,
		EgressBandwidth:        egressBandwidth,
		EgressBurst:            egressBurst,
		NumGPUs:                numGPUs,
		NumTPUs:                numTPUs,
		TargetContainerImage:   targetImage,
		Command:                command,
		Args:                   args,
		Env:                    envMap,
		RayHeadAddress:         rayHeadAddress,
		MeshAuthKey:            meshAuthKey,
		RunAsUser:              runAsUser,
		RunAsGroup:             runAsGroup,
		CPUMillicores:          cpuMillicores,
		MemoryBytes:            memoryBytes,
		ClusterTag:             defaultClusterTag,
		RegistryMirrors:        registryMirrors,
		OtelEndpoint:           otelEndpoint,
		NodeProblemDetector:    enableNPD,
		CustomBootstrapScript:  customBootstrapScript,
		RunnerConcurrency:      runnerConcurrency,
		SubnetIDs:              subnetIDs,
		CandidateInstanceTypes: candidateInstanceTypes,
	}, nil
}

// parsePodFromPayload parses either InterLink standard RetrievedPodData ({"pod": ...}) or flat Pod JSON.
func parsePodFromPayload(body []byte) (*Pod, error) {
	var retrieved struct {
		Pod Pod `json:"pod"`
	}
	if err := json.Unmarshal(body, &retrieved); err == nil && retrieved.Pod.Metadata.Name != "" {
		return &retrieved.Pod, nil
	}

	var direct Pod
	if err := json.Unmarshal(body, &direct); err == nil && direct.Metadata.Name != "" {
		return &direct, nil
	}

	return nil, fmt.Errorf("invalid pod JSON payload")
}

// parseSecretsFromPayload returns the Secrets the virtual kubelet sends in a create request ({"secrets": [...]}).
func parseSecretsFromPayload(body []byte) []corev1.Secret {
	var req struct {
		Secrets []corev1.Secret `json:"secrets"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil
	}
	return req.Secrets
}
