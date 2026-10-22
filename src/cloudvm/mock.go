// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MockProvider provides an in-memory simulation for local testing and CI.
type MockProvider struct {
	mu          sync.RWMutex
	instances   map[string]*Instance
	counter     int
	terminateFn func(ctx context.Context, instanceID string) error
}

// NewMockProvider creates an initialized MockProvider.
func NewMockProvider() *MockProvider {
	return &MockProvider{
		instances: make(map[string]*Instance),
	}
}

func (m *MockProvider) Launch(_ context.Context, spec *InstanceSpec) (*Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.counter++
	id := fmt.Sprintf("i-mock-%04d", m.counter)
	inst := &Instance{
		ID:           id,
		Provider:     ProviderMock,
		Region:       spec.Region,
		InstanceType: spec.InstanceType,
		IP:           fmt.Sprintf("100.64.0.%d", m.counter),
		State:        StateRunning,
		CreatedAt:    time.Now(),
		Metadata: map[string]string{
			"podName":     spec.PodName,
			"namespace":   spec.PodNamespace,
			"podUID":      spec.PodUID,
			"clusterTag":  spec.ClusterTag,
			"runnerToken": spec.RunnerAuthToken,
			"marketType":  spec.MarketType,
		},
	}
	m.instances[id] = inst
	return cloneInstance(inst), nil
}

func (m *MockProvider) Terminate(ctx context.Context, instanceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.terminateFn != nil {
		if err := m.terminateFn(ctx, instanceID); err != nil {
			return err
		}
	}

	inst, exists := m.instances[instanceID]
	if !exists {
		return fmt.Errorf("instance not found: %s", instanceID)
	}
	now := time.Now()
	inst.State = StateTerminated
	inst.TerminatedAt = &now
	return nil
}

func (m *MockProvider) Status(_ context.Context, instanceID string) (*Instance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	inst, exists := m.instances[instanceID]
	if !exists {
		return nil, fmt.Errorf("instance not found: %s", instanceID)
	}
	return cloneInstance(inst), nil
}

func (m *MockProvider) ListActiveInstances(_ context.Context, clusterTag string) ([]*Instance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var active []*Instance
	for _, inst := range m.instances {
		if inst.State == StateRunning || inst.State == StateProvisioning {
			if clusterTag == "" || inst.Metadata["clusterTag"] == clusterTag {
				active = append(active, inst)
			}
		}
	}
	return active, nil
}

// SetTerminateFn overrides the termination logic (used for testing failure scenarios).
func (m *MockProvider) SetTerminateFn(fn func(ctx context.Context, instanceID string) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.terminateFn = fn
}

// LaunchCount returns the total number of instance launches performed by MockProvider.
func (m *MockProvider) LaunchCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.counter
}

func cloneInstance(inst *Instance) *Instance {
	if inst == nil {
		return nil
	}
	cp := *inst
	if inst.Metadata != nil {
		cp.Metadata = make(map[string]string, len(inst.Metadata))
		for k, v := range inst.Metadata {
			cp.Metadata[k] = v
		}
	}
	return &cp
}
