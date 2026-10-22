package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServerCreateAndDelete(t *testing.T) {
	mock := NewMockProvider()
	server := NewServer(mock, "test-cluster")
	handler := server.Routes()

	pod := Pod{
		Metadata: PodMeta{
			Name:      "test-worker-1",
			Namespace: "default",
			Annotations: map[string]string{
				"cloudvm-job.vk.io/capacity-type": "spot",
				"cloudvm-job.vk.io/region":        "us-east-1",
				"cloudvm-job.vk.io/instance-type": "g5.xlarge",
			},
		},
		Spec: PodSpecWrapper{
			Containers: []ContainerSpec{
				{
					Name:  "worker",
					Image: "ubuntu:24.04",
					Resources: ContainerResourceRequirements{
						Limits: map[string]string{"nvidia.com/gpu": "1"},
					},
				},
			},
		},
	}

	body, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("failed to marshal pod: %v", err)
	}

	// 1. Test /create
	req := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /create, got %d: %s", rec.Code, rec.Body.String())
	}

	// 2. Test /status
	reqStatus := httptest.NewRequest(http.MethodPost, "/status", bytes.NewReader(body))
	recStatus := httptest.NewRecorder()
	handler.ServeHTTP(recStatus, reqStatus)

	if recStatus.Code != http.StatusOK {
		t.Fatalf("expected 200 from /status, got %d", recStatus.Code)
	}

	var statusResp []PodStatusResponse
	if err := json.Unmarshal(recStatus.Body.Bytes(), &statusResp); err != nil {
		t.Fatalf("failed to parse status response: %v", err)
	}
	if len(statusResp) != 1 || len(statusResp[0].Containers) != 1 {
		t.Fatalf("unexpected status response shape: %+v", statusResp)
	}
	if !statusResp[0].Containers[0].Ready {
		t.Fatalf("expected container to be ready")
	}

	// 3. Test /delete
	reqDel := httptest.NewRequest(http.MethodPost, "/delete", bytes.NewReader(body))
	recDel := httptest.NewRecorder()
	handler.ServeHTTP(recDel, reqDel)

	if recDel.Code != http.StatusOK {
		t.Fatalf("expected 200 from /delete, got %d", recDel.Code)
	}
}

func TestFleetOverridesWeightedCapacity(t *testing.T) {
	instanceTypes := []string{"g5.xlarge", "g5.12xlarge", "p4d.24xlarge"}
	subnets := []string{"subnet-1"}

	overrides := BuildFleetOverrides(instanceTypes, subnets)
	if len(overrides) != 3 {
		t.Fatalf("expected 3 overrides, got %d", len(overrides))
	}

	expectedWeights := map[string]float64{
		"g5.xlarge":    1.0,
		"g5.12xlarge":  4.0,
		"p4d.24xlarge": 8.0,
	}

	for _, ov := range overrides {
		itype := string(ov.InstanceType)
		expected := expectedWeights[itype]
		if ov.WeightedCapacity == nil || *ov.WeightedCapacity != expected {
			t.Errorf("expected weight %.1f for %s, got %v", expected, itype, ov.WeightedCapacity)
		}
	}
}

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
