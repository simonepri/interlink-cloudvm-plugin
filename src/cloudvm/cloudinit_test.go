package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCloudInitGeneration(t *testing.T) {
	cfg := &CloudInitConfig{
		PodName:        "test-pod",
		PodNamespace:   "default",
		RayHeadAddress: "100.64.0.1:6379",
		RayVersion:     "2.44.0",
		UseNativeDLAMI: true,
		NumGPUs:        1,
		Region:         "us-east-1",
		InstanceType:   "g5.xlarge",
	}

	script, err := GenerateCloudInit(cfg)
	if err != nil {
		t.Fatalf("GenerateCloudInit failed: %v", err)
	}

	if !bytes.Contains([]byte(script), []byte("token-ttl-seconds: 21600")) {
		t.Errorf("cloud-init missing IMDSv2 token retrieval")
	}
	if !bytes.Contains([]byte(script), []byte("/opt/pytorch/bin/ray start")) {
		t.Errorf("cloud-init missing native DLAMI ray start command")
	}
	if !bytes.Contains([]byte(script), []byte("/dev/console")) {
		t.Errorf("cloud-init missing serial console telemetry logging")
	}
}

func TestCloudInit_ContainerModeWithMicroRunner(t *testing.T) {
	testToken := fmt.Sprintf("auth-token-%d", time.Now().UnixNano())
	cfg := &CloudInitConfig{
		PodName:              "multi-tenant-pod",
		PodNamespace:         "ai-workloads",
		PodUID:               "pod-uid-456",
		RunnerAuthToken:      testToken,
		TargetContainerImage: "ghcr.io/org/worker:v1",
		Command:              []string{"python3", "worker.py"},
		UseNativeDLAMI:       false,
		NumGPUs:              2,
		AssignedGPUs:         []int{0, 1},
		CPUMillicores:        4000,
		MemoryBytes:          16 * 1024 * 1024 * 1024,
		Region:               "us-east-1",
		InstanceType:         "g5.12xlarge",
	}

	script, err := GenerateCloudInit(cfg)
	if err != nil {
		t.Fatalf("GenerateCloudInit failed: %v", err)
	}

	if bytes.Contains([]byte(script), []byte("cloudvm-workload")) {
		t.Errorf("cloud-init contains stale reference to cloudvm-workload")
	}
	if bytes.Contains([]byte(script), []byte("shell=True")) {
		t.Errorf("cloud-init runner must not execute commands with shell=True")
	}
	if bytes.Contains([]byte(script), []byte("--pid=host")) || bytes.Contains([]byte(script), []byte("--ipc=host")) {
		t.Errorf("cloud-init containers must not share pid or ipc namespace with host")
	}
	if !bytes.Contains([]byte(script), []byte(fmt.Sprintf(`AUTH_TOKEN = "%s"`, testToken))) {
		t.Errorf("cloud-init missing runner auth token configuration")
	}
	if !bytes.Contains([]byte(script), []byte("ThreadingHTTPServer")) {
		t.Errorf("cloud-init runner should use ThreadingHTTPServer for concurrent requests")
	}
	if cfg.GPUFlag() != `--gpus '"device=0,1"'` {
		t.Errorf("cloud-init missing discrete device slicing flags with CSV quotes, got: %s", cfg.GPUFlag())
	}
	if !bytes.Contains([]byte(script), []byte("--shm-size=16g")) {
		t.Errorf("cloud-init missing shared memory limit for PyTorch DataLoader")
	}
	if !bytes.Contains([]byte(script), []byte("pod-uid-456")) || !bytes.Contains([]byte(script), []byte("/containers/start")) {
		t.Errorf("cloud-init missing initial container launch via runner endpoint, got script:\n%s", script)
	}
	if !bytes.Contains([]byte(script), []byte(`docker ps -q --filter "name=cloudvm"`)) {
		t.Errorf("cloud-init missing multi-pod idle watchdog container check")
	}
}

func TestPodToInstanceSpec_PriceCapAndEgress(t *testing.T) {
	pod := &Pod{
		Metadata: PodMeta{
			Name:      "test-worker-price",
			Namespace: "default",
			Annotations: map[string]string{
				"cloudvm.vk.io/spot":             "false",
				"cloudvm.vk.io/max-price":        "2.50",
				"cloudvm.vk.io/price-policy":     "wait",
				"cloudvm.vk.io/egress-bandwidth": "100Mbps",
				"cloudvm.vk.io/egress-burst":     "15MB",
			},
		},
		Spec: PodSpecWrapper{
			Containers: []ContainerSpec{
				{
					Name:  "worker",
					Image: "test:latest",
				},
			},
		},
	}

	spec, err := PodToInstanceSpec(pod, "cluster-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.MarketType != "on-demand" {
		t.Errorf("expected marketType on-demand, got %s", spec.MarketType)
	}
	if spec.MaxPrice != "2.50" {
		t.Errorf("expected maxPrice 2.50, got %s", spec.MaxPrice)
	}
	if spec.PricePolicy != "wait" {
		t.Errorf("expected pricePolicy wait, got %s", spec.PricePolicy)
	}
	if spec.EgressBandwidth != "100Mbps" {
		t.Errorf("expected egressBandwidth 100Mbps, got %s", spec.EgressBandwidth)
	}
	if spec.EgressBurst != "15MB" {
		t.Errorf("expected egressBurst 15MB, got %s", spec.EgressBurst)
	}
}

func TestCloudInit_EgressRateLimiting(t *testing.T) {
	cfg := &CloudInitConfig{
		PodName:         "rate-limited-pod",
		PodNamespace:    "default",
		EgressBandwidth: "250Mbps",
		EgressBurst:     "20MB",
	}

	script, err := GenerateCloudInit(cfg)
	if err != nil {
		t.Fatalf("GenerateCloudInit failed: %v", err)
	}

	if !bytes.Contains([]byte(script), []byte("tc qdisc add dev \"$DEV\" parent 1:2 handle 20: tbf rate 250Mbps burst 20MB latency 50ms")) {
		t.Errorf("cloud-init missing expected tc rate limiting command, got:\n%s", script)
	}
}

func TestEgressRateToBandwidthCalculation(t *testing.T) {
	// $0.90/hr on AWS ($0.09/GB) should yield ~22mbit
	bwAWS := EgressRateToBandwidth(0.90, "aws")
	if bwAWS != "22mbit" {
		t.Errorf("expected 22mbit for $0.90/hr on AWS, got %s", bwAWS)
	}

	// $4.05/hr on AWS should yield ~100mbit
	bw100 := EgressRateToBandwidth(4.05, "aws")
	if bw100 != "100mbit" {
		t.Errorf("expected 100mbit for $4.05/hr on AWS, got %s", bw100)
	}

	// $0.85/hr on GCP ($0.085/GB) should yield ~22mbit
	bwGCP := EgressRateToBandwidth(0.85, "gcp")
	if bwGCP != "22mbit" {
		t.Errorf("expected 22mbit for $0.85/hr on GCP, got %s", bwGCP)
	}
}

func TestPodToInstanceSpec_MaxEgressRateDollarConversion(t *testing.T) {
	pod := &Pod{
		Metadata: PodMeta{
			Name:      "test-dollar-egress",
			Namespace: "default",
			Annotations: map[string]string{
				"cloudvm.vk.io/max-egress-rate": "$0.90",
			},
		},
		Spec: PodSpecWrapper{
			Containers: []ContainerSpec{
				{Name: "worker", Image: "test:latest"},
			},
		},
	}

	spec, err := PodToInstanceSpec(pod, "cluster-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.MaxEgressRate != "$0.90" {
		t.Errorf("expected MaxEgressRate $0.90, got %s", spec.MaxEgressRate)
	}
	if spec.EgressBandwidth != "22mbit" {
		t.Errorf("expected calculated EgressBandwidth 22mbit, got %s", spec.EgressBandwidth)
	}
	if spec.EgressBurst != "100mb" {
		t.Errorf("expected default burst 100mb, got %s", spec.EgressBurst)
	}
}

func TestCloudInit_RunnerPythonSyntaxAndImports(t *testing.T) {
	cfg := &CloudInitConfig{
		PodName:              "syntax-test",
		PodNamespace:         "default",
		PodUID:               "uid-syntax-1",
		RunnerAuthToken:      "test-token",
		TargetContainerImage: "rayproject/ray:2.44.0",
		RayHeadAddress:       "10.0.0.1:6379",
	}

	script, err := GenerateCloudInit(cfg)
	if err != nil {
		t.Fatalf("failed to generate cloud-init: %v", err)
	}

	// Verify Python runner imports time and threading
	if !strings.Contains(script, "import time") || !strings.Contains(script, "import threading") {
		t.Fatalf("cloud-init runner missing import time or import threading")
	}

	// Verify Ray auto-start command restored in JSON
	if !strings.Contains(script, "ray") || !strings.Contains(script, "10.0.0.1:6379") {
		t.Fatalf("cloud-init runner missing ray auto-start command in initial pod JSON")
	}

	// Verify IMDS protection and runner port protection
	if !strings.Contains(script, "169.254.169.254") || !strings.Contains(script, "--dport 9090") {
		t.Fatalf("cloud-init missing IMDS or runner port firewall rules")
	}

	// Verify container capability isolation
	if !strings.Contains(script, "--cap-drop=SETUID") || !strings.Contains(script, "--security-opt=no-new-privileges") || !strings.Contains(script, "--user=65532:65532") {
		t.Fatalf("cloud-init missing container privilege isolation flags")
	}
}

func TestPodToInstanceSpec_RootUIDRejected(t *testing.T) {
	rootUID := int64(0)
	pod := &Pod{
		Metadata: PodMeta{Name: "root-pod", Namespace: "default"},
		Spec: PodSpecWrapper{
			SecurityContext: &PodSecurityContext{
				RunAsUser: &rootUID,
			},
			Containers: []ContainerSpec{
				{Name: "main", Image: "alpine"},
			},
		},
	}

	_, err := PodToInstanceSpec(pod, "test-cluster")
	if err == nil {
		t.Fatal("expected error when pod specifies runAsUser: 0, got nil")
	}

	nonRootUID := int64(1000)
	pod.Spec.SecurityContext.RunAsUser = &nonRootUID
	pod.Spec.Containers[0].SecurityContext = &SecurityContext{
		RunAsUser: &rootUID,
	}

	_, err = PodToInstanceSpec(pod, "test-cluster")
	if err == nil {
		t.Fatal("expected error when container specifies runAsUser: 0, got nil")
	}
}

func TestPodToInstanceSpec_LimitsOnly(t *testing.T) {
	pod := &Pod{
		Metadata: PodMeta{Name: "req-pod", Namespace: "default"},
		Spec: PodSpecWrapper{
			Containers: []ContainerSpec{
				{
					Name:  "main",
					Image: "alpine",
					Resources: ContainerResourceRequirements{
						Requests: map[string]string{
							"cpu":    "2",
							"memory": "4Gi",
						},
					},
				},
			},
		},
	}

	spec, err := PodToInstanceSpec(pod, "cluster-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if spec.CPUMillicores != 0 {
		t.Fatalf("expected CPUMillicores to be 0 when only requests are set, got %d", spec.CPUMillicores)
	}
	if spec.MemoryBytes != 0 {
		t.Fatalf("expected MemoryBytes to be 0 when only requests are set, got %d", spec.MemoryBytes)
	}
}
