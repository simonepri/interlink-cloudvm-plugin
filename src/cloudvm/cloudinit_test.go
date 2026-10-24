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
	if !bytes.Contains([]byte(script), []byte(fmt.Sprintf(`echo -n "%s" > /etc/cloudvm/runner-token`, testToken))) {
		t.Errorf("cloud-init missing runner auth token configuration in /etc/cloudvm/runner-token")
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
	// Verify initial container is not launched via cloud-init to prevent credential and spec leakage
	if bytes.Contains([]byte(script), []byte("initial-pod.json")) {
		t.Errorf("cloud-init must not launch initial-pod.json via user-data")
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

	// Verify runner does not hardcode ray initial container in user-data
	if strings.Contains(script, "initial-pod.json") {
		t.Fatalf("cloud-init should not embed initial-pod.json")
	}

	// Verify IMDS protection and runner port protection
	if !strings.Contains(script, "169.254.169.254") || !strings.Contains(script, "--dport 9090") {
		t.Fatalf("cloud-init missing IMDS or runner port firewall rules")
	}

	// Verify container capability isolation
	if !strings.Contains(script, "--cap-drop=SETUID") || !strings.Contains(script, "--security-opt=no-new-privileges") || !strings.Contains(script, "--user=65532:65532") {
		t.Fatalf("cloud-init missing container privilege isolation flags")
	}

	// Verify runner concurrency throttle and shutdown check
	if !strings.Contains(script, "DOCKER_SEMAPHORE = threading.Semaphore(8)") {
		t.Fatalf("cloud-init runner missing DOCKER_SEMAPHORE definition")
	}
	if !strings.Contains(script, "if os.path.exists('/tmp/cloudvm-shutting-down'):") {
		t.Fatalf("cloud-init runner missing shutting-down check")
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

func TestCloudInit_RegistryMirrors(t *testing.T) {
	t.Run("configured mirrors", func(t *testing.T) {
		cfg := &CloudInitConfig{
			PodName:         "mirror-pod",
			PodNamespace:    "default",
			RunnerAuthToken: "token-1",
			RegistryMirrors: []string{
				"https://mirror.gcr.io",
				"https://dockerhub.mirror.local",
			},
		}

		script, err := GenerateCloudInit(cfg)
		if err != nil {
			t.Fatalf("GenerateCloudInit failed: %v", err)
		}

		if !strings.Contains(script, "/etc/docker/daemon.json") {
			t.Errorf("cloud-init missing /etc/docker/daemon.json configuration")
		}
		if !strings.Contains(script, "registry-mirrors") {
			t.Errorf("cloud-init missing registry-mirrors key")
		}
		if !strings.Contains(script, "https://mirror.gcr.io") || !strings.Contains(script, "https://dockerhub.mirror.local") {
			t.Errorf("cloud-init missing configured mirror URLs in script")
		}
		if !strings.Contains(script, "systemctl restart docker") {
			t.Errorf("cloud-init missing docker restart command after updating daemon.json")
		}
	})

	t.Run("omitted when empty", func(t *testing.T) {
		cfg := &CloudInitConfig{
			PodName:         "no-mirror-pod",
			PodNamespace:    "default",
			RunnerAuthToken: "token-2",
		}

		script, err := GenerateCloudInit(cfg)
		if err != nil {
			t.Fatalf("GenerateCloudInit failed: %v", err)
		}

		if strings.Contains(script, "registry-mirrors") {
			t.Errorf("cloud-init should not contain registry-mirrors when RegistryMirrors is empty")
		}
	})
}

func TestCloudInit_OtelEndpoint(t *testing.T) {
	t.Run("configured otel endpoint", func(t *testing.T) {
		cfg := &CloudInitConfig{
			PodName:         "otel-pod",
			PodNamespace:    "default",
			RunnerAuthToken: "token-3",
			OtelEndpoint:    "otel-collector.monitoring.svc:4317",
		}

		script, err := GenerateCloudInit(cfg)
		if err != nil {
			t.Fatalf("GenerateCloudInit failed: %v", err)
		}

		if !strings.Contains(script, "/usr/local/bin/cloudvm-otel-forwarder.sh") {
			t.Errorf("cloud-init missing otel forwarder script")
		}
		if !strings.Contains(script, "/etc/systemd/system/cloudvm-otel.service") {
			t.Errorf("cloud-init missing cloudvm-otel.service unit")
		}
		if !strings.Contains(script, "OTEL_EXPORTER_OTLP_ENDPOINT=otel-collector.monitoring.svc:4317") {
			t.Errorf("cloud-init missing OTEL_EXPORTER_OTLP_ENDPOINT environment definition")
		}
		if !strings.Contains(script, "systemctl enable --now cloudvm-otel.service") {
			t.Errorf("cloud-init missing systemd unit activation for otel forwarder")
		}
		if !strings.Contains(script, "/v1/metrics") {
			t.Errorf("cloud-init missing /v1/metrics target path")
		}
	})

	t.Run("omitted when empty", func(t *testing.T) {
		cfg := &CloudInitConfig{
			PodName:         "no-otel-pod",
			PodNamespace:    "default",
			RunnerAuthToken: "token-4",
		}

		script, err := GenerateCloudInit(cfg)
		if err != nil {
			t.Fatalf("GenerateCloudInit failed: %v", err)
		}

		if strings.Contains(script, "cloudvm-otel.service") {
			t.Errorf("cloud-init should not configure otel service when OtelEndpoint is empty")
		}
	})
}

func TestCloudInit_NodeProblemDetector(t *testing.T) {
	t.Run("enabled node-problem-detector", func(t *testing.T) {
		cfg := &CloudInitConfig{
			PodName:             "npd-pod",
			PodNamespace:        "default",
			RunnerAuthToken:     "token-5",
			NodeProblemDetector: true,
		}

		script, err := GenerateCloudInit(cfg)
		if err != nil {
			t.Fatalf("GenerateCloudInit failed: %v", err)
		}

		if !strings.Contains(script, "registry.k8s.io/node-problem-detector/node-problem-detector:v0.8.20") {
			t.Errorf("cloud-init missing upstream node-problem-detector image")
		}
		if !strings.Contains(script, "--enable-k8s-exporter=false") {
			t.Errorf("cloud-init missing standalone mode flag --enable-k8s-exporter=false")
		}
		if !strings.Contains(script, "--port=20256") {
			t.Errorf("cloud-init missing NPD standalone port 20256")
		}
		if !strings.Contains(script, "/dev/kmsg") {
			t.Errorf("cloud-init missing /dev/kmsg mount or check")
		}
		if !strings.Contains(script, "/etc/node-problem-detector/gpu-monitor.json") {
			t.Errorf("cloud-init missing GPU problem detector config")
		}
		if !strings.Contains(script, "GPUProblem") {
			t.Errorf("cloud-init missing GPUProblem condition in NPD config")
		}
		if !strings.Contains(script, "GPUFallenOffBus") {
			t.Errorf("cloud-init missing GPUFallenOffBus rule in NPD config")
		}
	})

	t.Run("disabled node-problem-detector", func(t *testing.T) {
		cfg := &CloudInitConfig{
			PodName:             "no-npd-pod",
			PodNamespace:        "default",
			RunnerAuthToken:     "token-6",
			NodeProblemDetector: false,
		}

		script, err := GenerateCloudInit(cfg)
		if err != nil {
			t.Fatalf("GenerateCloudInit failed: %v", err)
		}

		if strings.Contains(script, "node-problem-detector") {
			t.Errorf("cloud-init should not configure NPD when NodeProblemDetector is false")
		}
	})
}

func TestCloudInit_CustomBootstrapScript(t *testing.T) {
	t.Run("appends custom script at end of user-data", func(t *testing.T) {
		customCode := "echo 'custom bootstrap running' >> /var/log/custom.log\ninstall -d /opt/custom\n"
		cfg := &CloudInitConfig{
			PodName:               "custom-script-pod",
			PodNamespace:          "default",
			RunnerAuthToken:       "token-7",
			CustomBootstrapScript: customCode,
		}

		script, err := GenerateCloudInit(cfg)
		if err != nil {
			t.Fatalf("GenerateCloudInit failed: %v", err)
		}

		if !strings.Contains(script, customCode) {
			t.Errorf("cloud-init missing custom bootstrap script contents")
		}

		idxCustom := strings.Index(script, customCode)
		idxRunner := strings.Index(script, "cloudvm-runner.py")
		if idxCustom < idxRunner {
			t.Errorf("custom bootstrap script should appear after cloudvm-runner setup, got idxCustom=%d, idxRunner=%d", idxCustom, idxRunner)
		}
	})

	t.Run("omitted when empty", func(t *testing.T) {
		cfg := &CloudInitConfig{
			PodName:         "no-custom-pod",
			PodNamespace:    "default",
			RunnerAuthToken: "token-8",
		}

		script, err := GenerateCloudInit(cfg)
		if err != nil {
			t.Fatalf("GenerateCloudInit failed: %v", err)
		}

		if strings.Contains(script, "Executing custom bootstrap script") {
			t.Errorf("cloud-init should not contain custom bootstrap header when empty")
		}
	})
}

func TestCloudInit_GenericRuntimeAndDaemonOptionsCombined(t *testing.T) {
	customCode := "echo 'all features combined' > /etc/combined-test.txt"
	cfg := &CloudInitConfig{
		PodName:              "combined-pod",
		PodNamespace:         "prod-ml",
		PodUID:               "uid-combined-999",
		RunnerAuthToken:      "secret-token-xyz",
		TargetContainerImage: "nvcr.io/nvidia/pytorch:23.10-py3",
		RegistryMirrors: []string{
			"https://mirror.gcr.io",
			"https://dockerhub.internal.mirror",
		},
		OtelEndpoint:          "otel.telemetry.svc.cluster.local:4317",
		NodeProblemDetector:   true,
		CustomBootstrapScript: customCode,
	}

	script, err := GenerateCloudInit(cfg)
	if err != nil {
		t.Fatalf("GenerateCloudInit failed: %v", err)
	}

	// Verify all features are present
	if !strings.Contains(script, "registry-mirrors") {
		t.Errorf("combined cloud-init missing registry-mirrors")
	}
	if !strings.Contains(script, "https://dockerhub.internal.mirror") {
		t.Errorf("combined cloud-init missing dockerhub.internal.mirror")
	}
	if !strings.Contains(script, "cloudvm-otel.service") {
		t.Errorf("combined cloud-init missing cloudvm-otel.service")
	}
	if !strings.Contains(script, "otel.telemetry.svc.cluster.local:4317") {
		t.Errorf("combined cloud-init missing OtelEndpoint")
	}
	if !strings.Contains(script, "node-problem-detector") {
		t.Errorf("combined cloud-init missing node-problem-detector")
	}
	if !strings.Contains(script, "GPUProblem") {
		t.Errorf("combined cloud-init missing GPUProblem check")
	}
	if !strings.Contains(script, customCode) {
		t.Errorf("combined cloud-init missing custom bootstrap script")
	}

	// Verify sequential ordering of sections
	idxMirrors := strings.Index(script, "registry-mirrors")
	idxOtel := strings.Index(script, "cloudvm-otel.service")
	idxNPD := strings.Index(script, "node-problem-detector")
	idxRunner := strings.Index(script, "cloudvm-runner.py")
	idxCustom := strings.Index(script, customCode)

	if idxMirrors >= idxOtel || idxOtel >= idxNPD || idxNPD >= idxRunner || idxRunner >= idxCustom {
		t.Errorf("unexpected section ordering: mirrors=%d, otel=%d, npd=%d, runner=%d, custom=%d",
			idxMirrors, idxOtel, idxNPD, idxRunner, idxCustom)
	}
}

func TestCloudInit_DockerInstallAndHeartbeatBootstrap(t *testing.T) {
	cfg := &CloudInitConfig{
		PodName:         "boot-test-pod",
		PodNamespace:    "default",
		RunnerAuthToken: "token-boot-123",
	}

	script, err := GenerateCloudInit(cfg)
	if err != nil {
		t.Fatalf("GenerateCloudInit failed: %v", err)
	}

	if !strings.Contains(script, "touch /tmp/cloudvm-heartbeat") {
		t.Errorf("cloud-init missing boot heartbeat initialization touch")
	}
	if !strings.Contains(script, "Installing Docker and Python 3") {
		t.Errorf("cloud-init missing Docker and Python 3 automated installation block")
	}
	if !strings.Contains(script, "systemctl enable --now docker") {
		t.Errorf("cloud-init missing docker enable/start command")
	}
	if !strings.Contains(script, "HB_AGE=0") {
		t.Errorf("cloud-init watchdog missing safe HB_AGE default 0")
	}
}

func TestCloudInit_IdleGracePeriodSeconds(t *testing.T) {
	t.Run("default_when_zero", func(t *testing.T) {
		cfg := &CloudInitConfig{
			PodName:         "idle-default-pod",
			PodNamespace:    "default",
			RunnerAuthToken: "token-idle-1",
		}
		script, err := GenerateCloudInit(cfg)
		if err != nil {
			t.Fatalf("GenerateCloudInit failed: %v", err)
		}
		if cfg.IdleGracePeriodSeconds != 600 {
			t.Errorf("expected IdleGracePeriodSeconds to default to 600, got %d", cfg.IdleGracePeriodSeconds)
		}
		if !strings.Contains(script, "no provider heartbeat for >600s") {
			t.Errorf("expected watchdog comment to reflect 600s grace period")
		}
		if !strings.Contains(script, `[ "$HB_AGE" -gt 600 ]`) {
			t.Errorf("expected watchdog age check to reflect 600s grace period")
		}
		if !strings.Contains(script, "no provider heartbeat for >600s. Terminating VM...") {
			t.Errorf("expected watchdog log message to reflect 600s grace period")
		}
	})

	t.Run("custom_grace_period", func(t *testing.T) {
		cfg := &CloudInitConfig{
			PodName:                "idle-custom-pod",
			PodNamespace:           "default",
			RunnerAuthToken:        "token-idle-2",
			IdleGracePeriodSeconds: 1200,
		}
		script, err := GenerateCloudInit(cfg)
		if err != nil {
			t.Fatalf("GenerateCloudInit failed: %v", err)
		}
		if cfg.IdleGracePeriodSeconds != 1200 {
			t.Errorf("expected IdleGracePeriodSeconds to remain 1200, got %d", cfg.IdleGracePeriodSeconds)
		}
		if !strings.Contains(script, "no provider heartbeat for >1200s") {
			t.Errorf("expected watchdog comment to reflect 1200s grace period")
		}
		if !strings.Contains(script, `[ "$HB_AGE" -gt 1200 ]`) {
			t.Errorf("expected watchdog age check to reflect 1200s grace period")
		}
		if !strings.Contains(script, "no provider heartbeat for >1200s. Terminating VM...") {
			t.Errorf("expected watchdog log message to reflect 1200s grace period")
		}
	})

	t.Run("default_when_negative", func(t *testing.T) {
		cfg := &CloudInitConfig{
			PodName:                "idle-neg-pod",
			PodNamespace:           "default",
			RunnerAuthToken:        "token-idle-3",
			IdleGracePeriodSeconds: -50,
		}
		script, err := GenerateCloudInit(cfg)
		if err != nil {
			t.Fatalf("GenerateCloudInit failed: %v", err)
		}
		if cfg.IdleGracePeriodSeconds != 600 {
			t.Errorf("expected IdleGracePeriodSeconds to default to 600 on negative input, got %d", cfg.IdleGracePeriodSeconds)
		}
		if !strings.Contains(script, `[ "$HB_AGE" -gt 600 ]`) {
			t.Errorf("expected watchdog age check to reflect 600s grace period")
		}
	})
}

func TestCloudInit_RunnerConcurrencyAndShutdown(t *testing.T) {
	cfg := &CloudInitConfig{
		PodName:         "concurrency-pod",
		PodNamespace:    "default",
		RunnerAuthToken: "token-conc-1",
	}
	script, err := GenerateCloudInit(cfg)
	if err != nil {
		t.Fatalf("GenerateCloudInit failed: %v", err)
	}

	// Verify semaphore definition and usage
	if !strings.Contains(script, "DOCKER_SEMAPHORE = threading.Semaphore(8)") {
		t.Errorf("cloud-init runner missing DOCKER_SEMAPHORE definition")
	}
	if !strings.Contains(script, "with DOCKER_SEMAPHORE:") {
		t.Errorf("cloud-init runner missing with DOCKER_SEMAPHORE acquisition around container launch")
	}

	// Verify shutdown fail-fast check
	if !strings.Contains(script, "if os.path.exists('/tmp/cloudvm-shutting-down'):") {
		t.Errorf("cloud-init runner missing /tmp/cloudvm-shutting-down check")
	}
	if !strings.Contains(script, "self.send_response(503)") {
		t.Errorf("cloud-init runner missing 503 response on shutting-down check")
	}
	if !strings.Contains(script, `{"error": "node is shutting down"}`) {
		t.Errorf("cloud-init runner missing shutdown error payload")
	}
}
