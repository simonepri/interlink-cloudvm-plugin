// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	compute "google.golang.org/api/compute/v1"
)

type mockGCPComputeService struct {
	mu          sync.Mutex
	instances   map[string]*compute.Instance
	insertCalls []*compute.Instance
	deleteCalls []string
	getCalls    []string
	listCalls   []string
	insertErr   error
	deleteErr   error
	getErr      error
	listErr     error
	opErr       error
}

func newMockGCPComputeService() *mockGCPComputeService {
	return &mockGCPComputeService{
		instances: make(map[string]*compute.Instance),
	}
}

func (m *mockGCPComputeService) instanceKey(zone, name string) string {
	return fmt.Sprintf("%s/%s", zone, name)
}

func (m *mockGCPComputeService) Insert(_ context.Context, _ string, zone string, instance *compute.Instance) (*compute.Operation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.insertErr != nil {
		return nil, m.insertErr
	}
	m.insertCalls = append(m.insertCalls, instance)
	key := m.instanceKey(zone, instance.Name)
	m.instances[key] = instance
	return &compute.Operation{Status: "DONE"}, nil
}

func (m *mockGCPComputeService) Delete(_ context.Context, _ string, zone string, instance string) (*compute.Operation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deleteErr != nil {
		return nil, m.deleteErr
	}
	m.deleteCalls = append(m.deleteCalls, instance)
	key := m.instanceKey(zone, instance)
	delete(m.instances, key)
	return &compute.Operation{Status: "DONE"}, nil
}

func (m *mockGCPComputeService) Get(_ context.Context, _ string, zone string, instance string) (*compute.Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getErr != nil {
		return nil, m.getErr
	}
	m.getCalls = append(m.getCalls, instance)
	key := m.instanceKey(zone, instance)
	if inst, ok := m.instances[key]; ok {
		return inst, nil
	}
	return nil, fmt.Errorf("instance %s not found in zone %s", instance, zone)
}

func (m *mockGCPComputeService) WaitZoneOperation(_ context.Context, _, _, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.opErr
}

func (m *mockGCPComputeService) List(_ context.Context, _ string, zone string, filter string) (*compute.InstanceList, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listErr != nil {
		return nil, m.listErr
	}
	m.listCalls = append(m.listCalls, filter)

	var items []*compute.Instance
	for key, inst := range m.instances {
		if strings.HasPrefix(key, zone+"/") {
			items = append(items, inst)
		}
	}
	return &compute.InstanceList{Items: items}, nil
}

func TestGCPProvider_Launch_Spot(t *testing.T) {
	mockClient := newMockGCPComputeService()
	p := NewGCPProvider("test-project", "us-central1-a")
	p.SetClient(mockClient)

	spec := &InstanceSpec{
		PodName:         "test-pod",
		PodNamespace:    "default",
		PodUID:          "uid-12345-67890",
		ClusterTag:      "my-cluster",
		MarketType:      "spot",
		RunnerAuthToken: "token-abc",
		Region:          "us-central1",
	}

	inst, err := p.Launch(context.Background(), spec)
	if err != nil {
		t.Fatalf("Launch failed: %v", err)
	}

	if inst == nil {
		t.Fatal("expected non-nil instance")
	}
	if inst.Provider != ProviderGCP {
		t.Errorf("expected provider %s, got %s", ProviderGCP, inst.Provider)
	}
	if inst.Region != "us-central1" {
		t.Errorf("expected region us-central1, got %s", inst.Region)
	}
	if inst.Metadata["marketType"] != "spot" {
		t.Errorf("expected marketType spot, got %s", inst.Metadata["marketType"])
	}
	if inst.Metadata["podName"] != "test-pod" {
		t.Errorf("expected podName test-pod, got %s", inst.Metadata["podName"])
	}

	if len(mockClient.insertCalls) != 1 {
		t.Fatalf("expected 1 insert call, got %d", len(mockClient.insertCalls))
	}

	call := mockClient.insertCalls[0]
	if call.Scheduling == nil {
		t.Fatal("expected scheduling config")
	}
	if !call.Scheduling.Preemptible {
		t.Error("expected Preemptible = true for spot")
	}
	if call.Scheduling.ProvisioningModel != "SPOT" {
		t.Errorf("expected ProvisioningModel SPOT, got %s", call.Scheduling.ProvisioningModel)
	}
	if call.Scheduling.OnHostMaintenance != "TERMINATE" {
		t.Errorf("expected OnHostMaintenance TERMINATE, got %s", call.Scheduling.OnHostMaintenance)
	}
	if call.Scheduling.AutomaticRestart == nil || *call.Scheduling.AutomaticRestart {
		t.Error("expected AutomaticRestart = false for spot")
	}

	// Verify labels
	expectedLabels := map[string]string{
		"cluster_tag": "my-cluster",
		"pod_name":    "test-pod",
		"namespace":   "default",
		"pod_uid":     "uid-12345-67890",
		"managed_by":  "interlink-provider-cloudvm",
	}
	for k, v := range expectedLabels {
		if call.Labels[k] != v {
			t.Errorf("expected label %s=%s, got %s", k, v, call.Labels[k])
		}
	}

	// Verify startup-script metadata
	if call.Metadata == nil || len(call.Metadata.Items) == 0 {
		t.Fatal("expected metadata items")
	}
	foundStartupScript := false
	for _, item := range call.Metadata.Items {
		if item.Key == "startup-script" {
			foundStartupScript = true
			if item.Value == nil || !strings.Contains(*item.Value, "bash") {
				t.Errorf("startup-script missing bash header: %v", item.Value)
			}
		}
	}
	if !foundStartupScript {
		t.Error("startup-script metadata item not found")
	}

	// Verify network interface
	if len(call.NetworkInterfaces) != 1 {
		t.Fatalf("expected 1 network interface, got %d", len(call.NetworkInterfaces))
	}
	if call.NetworkInterfaces[0].Network != "global/networks/default" {
		t.Errorf("expected default VPC network, got %s", call.NetworkInterfaces[0].Network)
	}

	// Verify service account
	if len(call.ServiceAccounts) != 1 {
		t.Fatalf("expected 1 service account, got %d", len(call.ServiceAccounts))
	}
	if call.ServiceAccounts[0].Email != "default" {
		t.Errorf("expected default service account, got %s", call.ServiceAccounts[0].Email)
	}
}

func TestGCPProvider_Launch_OnDemand(t *testing.T) {
	mockClient := newMockGCPComputeService()
	p := NewGCPProvider("test-project", "us-central1-a")
	p.SetClient(mockClient)

	spec := &InstanceSpec{
		PodName:         "ondemand-pod",
		PodNamespace:    "prod",
		PodUID:          "uid-ondemand",
		ClusterTag:      "prod-cluster",
		MarketType:      "on-demand",
		RunnerAuthToken: "token-xyz",
		Region:          "us-central1-b",
	}

	inst, err := p.Launch(context.Background(), spec)
	if err != nil {
		t.Fatalf("Launch failed: %v", err)
	}

	if inst.Metadata["marketType"] != "on-demand" {
		t.Errorf("expected marketType on-demand, got %s", inst.Metadata["marketType"])
	}

	if len(mockClient.insertCalls) != 1 {
		t.Fatalf("expected 1 insert call, got %d", len(mockClient.insertCalls))
	}

	call := mockClient.insertCalls[0]
	if call.Scheduling == nil {
		t.Fatal("expected scheduling config")
	}
	if call.Scheduling.Preemptible {
		t.Error("expected Preemptible = false for on-demand")
	}
	if call.Scheduling.ProvisioningModel != "STANDARD" {
		t.Errorf("expected ProvisioningModel STANDARD, got %s", call.Scheduling.ProvisioningModel)
	}
	if call.Scheduling.AutomaticRestart == nil || !*call.Scheduling.AutomaticRestart {
		t.Error("expected AutomaticRestart = true for on-demand")
	}
	if call.Scheduling.OnHostMaintenance != "MIGRATE" {
		t.Errorf("expected OnHostMaintenance MIGRATE, got %s", call.Scheduling.OnHostMaintenance)
	}
}

func TestGCPProvider_Launch_CustomSubnetAndServiceAccount(t *testing.T) {
	mockClient := newMockGCPComputeService()
	p := NewGCPProvider("test-project", "us-central1-a")
	p.SetClient(mockClient)

	spec := &InstanceSpec{
		PodName:             "subnet-pod",
		PodNamespace:        "default",
		PodUID:              "uid-sub",
		SubnetIDs:           []string{"custom-subnetwork"},
		ServiceAccountEmail: "custom-sa@test-project.iam.gserviceaccount.com",
		CostCenter:          "ml-research",
		JobID:               "train-job-42",
		Region:              "us-central1",
	}

	_, err := p.Launch(context.Background(), spec)
	if err != nil {
		t.Fatalf("Launch failed: %v", err)
	}

	call := mockClient.insertCalls[0]
	nic := call.NetworkInterfaces[0]
	expectedSubnet := "regions/us-central1/subnetworks/custom-subnetwork"
	if nic.Subnetwork != expectedSubnet {
		t.Errorf("expected subnetwork %s, got %s", expectedSubnet, nic.Subnetwork)
	}
	if nic.Network != "" {
		t.Errorf("expected empty Network when Subnetwork is configured, got %s", nic.Network)
	}

	if call.ServiceAccounts[0].Email != "custom-sa@test-project.iam.gserviceaccount.com" {
		t.Errorf("expected custom service account, got %s", call.ServiceAccounts[0].Email)
	}

	if call.Labels["cost_center"] != "ml-research" {
		t.Errorf("expected cost_center label ml-research, got %s", call.Labels["cost_center"])
	}
	if call.Labels["job_id"] != "train-job-42" {
		t.Errorf("expected job_id label train-job-42, got %s", call.Labels["job_id"])
	}
}

func TestGCPProvider_Launch_Accelerators_NVIDIA(t *testing.T) {
	mockClient := newMockGCPComputeService()
	p := NewGCPProvider("test-project", "us-central1-a")
	p.SetClient(mockClient)

	tests := []struct {
		name              string
		spec              *InstanceSpec
		expectedAccelType string
		expectedCount     int64
	}{
		{
			name: "NVIDIA T4 via GPUType and NumGPUs",
			spec: &InstanceSpec{
				PodName:      "gpu-pod-t4",
				PodNamespace: "default",
				PodUID:       "uid-t4",
				InstanceType: "n1-standard-4",
				GPUType:      "nvidia-tesla-t4",
				NumGPUs:      2,
				MarketType:   "on-demand",
			},
			expectedAccelType: "zones/us-central1-a/acceleratorTypes/nvidia-tesla-t4",
			expectedCount:     2,
		},
		{
			name: "NVIDIA A100 via alias on custom shape",
			spec: &InstanceSpec{
				PodName:      "gpu-pod-a100",
				PodNamespace: "default",
				PodUID:       "uid-a100",
				InstanceType: "n1-standard-8",
				GPUType:      "a100",
				NumGPUs:      1,
			},
			expectedAccelType: "zones/us-central1-a/acceleratorTypes/nvidia-tesla-a100",
			expectedCount:     1,
		},
		{
			name: "NVIDIA V100 via alias on custom shape",
			spec: &InstanceSpec{
				PodName:      "gpu-pod-v100",
				PodNamespace: "default",
				PodUID:       "uid-v100",
				InstanceType: "n1-standard-16",
				GPUType:      "v100",
				NumGPUs:      2,
			},
			expectedAccelType: "zones/us-central1-a/acceleratorTypes/nvidia-tesla-v100",
			expectedCount:     2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockClient.insertCalls = nil
			_, err := p.Launch(context.Background(), tc.spec)
			if err != nil {
				t.Fatalf("Launch failed: %v", err)
			}

			call := mockClient.insertCalls[0]
			if len(call.GuestAccelerators) != 1 {
				t.Fatalf("expected 1 accelerator, got %d", len(call.GuestAccelerators))
			}
			accel := call.GuestAccelerators[0]
			if accel.AcceleratorType != tc.expectedAccelType {
				t.Errorf("expected accelerator %s, got %s", tc.expectedAccelType, accel.AcceleratorType)
			}
			if accel.AcceleratorCount != tc.expectedCount {
				t.Errorf("expected accelerator count %d, got %d", tc.expectedCount, accel.AcceleratorCount)
			}
			if call.Scheduling.OnHostMaintenance != "TERMINATE" {
				t.Errorf("accelerator VM must have OnHostMaintenance TERMINATE, got %s", call.Scheduling.OnHostMaintenance)
			}
		})
	}
}

func TestGCPProvider_Launch_Accelerators_TPU(t *testing.T) {
	mockClient := newMockGCPComputeService()
	p := NewGCPProvider("test-project", "us-central1-a")
	p.SetClient(mockClient)

	spec := &InstanceSpec{
		PodName:      "tpu-worker",
		PodNamespace: "default",
		PodUID:       "uid-tpu",
		InstanceType: "v4-8",
	}

	_, err := p.Launch(context.Background(), spec)
	if err == nil {
		t.Fatalf("expected error launching TPU via GCE compute instances API, got nil")
	}
	if !strings.Contains(err.Error(), "cloud tpu api") {
		t.Errorf("expected error mentioning cloud tpu api, got %v", err)
	}
}

func TestGCPProvider_Launch_CustomBootImage(t *testing.T) {
	mockClient := newMockGCPComputeService()
	p := NewGCPProvider("test-project", "us-central1-a")
	p.SetClient(mockClient)

	customImg := "projects/my-ml-images/global/images/cuda-v1"
	spec := &InstanceSpec{
		PodName:      "custom-image-pod",
		PodNamespace: "default",
		PodUID:       "uid-img",
		RegionalAMI: map[string]string{
			"us-central1": customImg,
		},
	}

	_, err := p.Launch(context.Background(), spec)
	if err != nil {
		t.Fatalf("Launch failed: %v", err)
	}

	call := mockClient.insertCalls[0]
	if len(call.Disks) != 1 {
		t.Fatalf("expected 1 disk, got %d", len(call.Disks))
	}
	disk := call.Disks[0]
	if disk.InitializeParams == nil || disk.InitializeParams.SourceImage != customImg {
		t.Errorf("expected SourceImage %s, got %v", customImg, disk.InitializeParams)
	}
}

func TestGCPProvider_Terminate(t *testing.T) {
	mockClient := newMockGCPComputeService()
	p := NewGCPProvider("test-project", "us-central1-a")
	p.SetClient(mockClient)

	spec := &InstanceSpec{
		PodName:      "term-pod",
		PodNamespace: "default",
		PodUID:       "uid-term",
		Region:       "us-central1-b",
	}

	inst, err := p.Launch(context.Background(), spec)
	if err != nil {
		t.Fatalf("Launch failed: %v", err)
	}

	err = p.Terminate(context.Background(), inst.ID)
	if err != nil {
		t.Fatalf("Terminate failed: %v", err)
	}

	if len(mockClient.deleteCalls) != 1 {
		t.Fatalf("expected 1 delete call, got %d", len(mockClient.deleteCalls))
	}
	if mockClient.deleteCalls[0] != inst.ID {
		t.Errorf("expected delete call for %s, got %s", inst.ID, mockClient.deleteCalls[0])
	}

	// Verify error handling
	mockClient.deleteErr = errors.New("gcp delete failed")
	err = p.Terminate(context.Background(), "unknown-instance")
	if err == nil {
		t.Error("expected error when delete fails")
	}
}

func TestGCPProvider_Status(t *testing.T) {
	mockClient := newMockGCPComputeService()
	p := NewGCPProvider("test-project", "us-central1-a")
	p.SetClient(mockClient)

	createdAt := time.Now().UTC().Truncate(time.Second)
	testInst := &compute.Instance{
		Name:              "cvm-status-test",
		MachineType:       "zones/us-central1-a/machineTypes/n1-standard-4",
		Status:            "RUNNING",
		CreationTimestamp: createdAt.Format(time.RFC3339),
		Zone:              "projects/test-project/zones/us-central1-a",
		NetworkInterfaces: []*compute.NetworkInterface{
			{
				NetworkIP: "10.128.0.5",
				AccessConfigs: []*compute.AccessConfig{
					{NatIP: "34.120.10.20"},
				},
			},
		},
		Scheduling: &compute.Scheduling{
			ProvisioningModel: "SPOT",
			Preemptible:       true,
		},
		Labels: map[string]string{
			"cluster_tag": "my-cluster",
			"pod_name":    "status-pod",
			"namespace":   "default",
			"pod_uid":     "uid-status",
		},
	}

	key := mockClient.instanceKey("us-central1-a", "cvm-status-test")
	mockClient.instances[key] = testInst

	inst, err := p.Status(context.Background(), "cvm-status-test")
	if err != nil {
		t.Fatalf("Status failed: %v", err)
	}

	if inst.ID != "cvm-status-test" {
		t.Errorf("expected ID cvm-status-test, got %s", inst.ID)
	}
	if inst.Provider != ProviderGCP {
		t.Errorf("expected ProviderGCP, got %s", inst.Provider)
	}
	if inst.Region != "us-central1" {
		t.Errorf("expected Region us-central1, got %s", inst.Region)
	}
	if inst.InstanceType != "n1-standard-4" {
		t.Errorf("expected InstanceType n1-standard-4, got %s", inst.InstanceType)
	}
	if inst.IP != "10.128.0.5" {
		t.Errorf("expected IP 10.128.0.5, got %s", inst.IP)
	}
	if inst.State != StateRunning {
		t.Errorf("expected StateRunning, got %s", inst.State)
	}
	if inst.Metadata["marketType"] != "spot" {
		t.Errorf("expected marketType spot, got %s", inst.Metadata["marketType"])
	}
	if inst.Metadata["podName"] != "status-pod" {
		t.Errorf("expected podName status-pod, got %s", inst.Metadata["podName"])
	}

	// Status not found
	_, err = p.Status(context.Background(), "non-existent")
	if err == nil {
		t.Error("expected error for non-existent instance")
	}
}

func TestGCPProvider_ListActiveInstances(t *testing.T) {
	mockClient := newMockGCPComputeService()
	p := NewGCPProvider("test-project", "us-central1-a", "us-central1-b")
	p.SetClient(mockClient)

	// Add an active matching instance in zone a
	instA := &compute.Instance{
		Name:        "inst-active-a",
		MachineType: "zones/us-central1-a/machineTypes/n1-standard-4",
		Status:      "RUNNING",
		Labels: map[string]string{
			"cluster_tag": "my-cluster",
			"pod_name":    "pod-a",
		},
	}
	mockClient.instances[mockClient.instanceKey("us-central1-a", "inst-active-a")] = instA

	// Add an active matching instance in zone b
	instB := &compute.Instance{
		Name:        "inst-active-b",
		MachineType: "zones/us-central1-b/machineTypes/n1-standard-4",
		Status:      "RUNNING",
		Labels: map[string]string{
			"cluster_tag": "my-cluster",
			"pod_name":    "pod-b",
		},
	}
	mockClient.instances[mockClient.instanceKey("us-central1-b", "inst-active-b")] = instB

	// Add a terminated instance in zone a that should be filtered out
	instTerm := &compute.Instance{
		Name:        "inst-terminated",
		MachineType: "zones/us-central1-a/machineTypes/n1-standard-4",
		Status:      "TERMINATED",
		Labels: map[string]string{
			"cluster_tag": "my-cluster",
		},
	}
	mockClient.instances[mockClient.instanceKey("us-central1-a", "inst-terminated")] = instTerm

	activeList, err := p.ListActiveInstances(context.Background(), "my-cluster")
	if err != nil {
		t.Fatalf("ListActiveInstances failed: %v", err)
	}

	if len(activeList) != 2 {
		t.Fatalf("expected 2 active instances, got %d", len(activeList))
	}

	foundA := false
	foundB := false
	for _, inst := range activeList {
		if inst.ID == "inst-active-a" {
			foundA = true
		}
		if inst.ID == "inst-active-b" {
			foundB = true
		}
		if inst.ID == "inst-terminated" {
			t.Error("terminated instance should not be included in active list")
		}
	}
	if !foundA || !foundB {
		t.Errorf("missing active instances: foundA=%v, foundB=%v", foundA, foundB)
	}

	// Verify filter was applied
	expectedFilter := `labels.cluster_tag = "my-cluster"`
	filterMatched := false
	for _, f := range mockClient.listCalls {
		if f == expectedFilter {
			filterMatched = true
			break
		}
	}
	if !filterMatched {
		t.Errorf("expected filter %s in list calls: %v", expectedFilter, mockClient.listCalls)
	}
}

func TestGCPProvider_SanitizeHelpers(t *testing.T) {
	if sanitizeGCPName("My_Pod.Worker-0") != "my-pod-worker-0" {
		t.Errorf("unexpected sanitized name: %s", sanitizeGCPName("My_Pod.Worker-0"))
	}
	if sanitizeGCPLabel("My_Cluster:Test-1") != "my_cluster-test-1" {
		t.Errorf("unexpected sanitized label: %s", sanitizeGCPLabel("My_Cluster:Test-1"))
	}
	if zoneToRegion("us-central1-a") != "us-central1" {
		t.Errorf("unexpected region: %s", zoneToRegion("us-central1-a"))
	}
	if zoneToRegion("europe-west4-b") != "europe-west4" {
		t.Errorf("unexpected region: %s", zoneToRegion("europe-west4-b"))
	}
}
