// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// RegionStore safely persists instance-to-region mappings locally on PVC storage
// so multi-region instance inventories survive provider restarts.
type RegionStore struct {
	mu       sync.RWMutex
	filePath string
	regions  map[string]string // instanceID -> AWS region
}

// NewRegionStore initializes a RegionStore with the given file path.
// If filePath is empty, a default path in os.TempDir is used.
func NewRegionStore(filePath string) (*RegionStore, error) {
	if filePath == "" {
		filePath = filepath.Join(os.TempDir(), "cloudvm-instance-regions.json")
	}
	filePath = filepath.Clean(filePath)
	rs := &RegionStore{
		filePath: filePath,
		regions:  make(map[string]string),
	}
	err := rs.load()
	return rs, err
}

func (rs *RegionStore) load() error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	cleanPath := filepath.Clean(rs.filePath)
	data, err := os.ReadFile(cleanPath) // #nosec G304,G703
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var loaded map[string]string
	if err := json.Unmarshal(data, &loaded); err != nil {
		corruptPath := fmt.Sprintf("%s.corrupt.%d", cleanPath, time.Now().UnixNano())
		_ = os.Rename(cleanPath, corruptPath)
		return fmt.Errorf("corrupt region store data moved to %s: %w", corruptPath, err)
	}
	if loaded == nil {
		loaded = make(map[string]string)
	}
	rs.regions = loaded
	return nil
}

func (rs *RegionStore) saveLocked() error {
	data, err := json.Marshal(rs.regions)
	if err != nil {
		return err
	}
	return atomicSaveWithFsync(rs.filePath, data, 0600)
}

// Get returns the AWS region for the specified instance ID, or empty string if not found.
func (rs *RegionStore) Get(instanceID string) string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.regions[instanceID]
}

// GetAll returns a thread-safe copy of all tracked instance-to-region mappings.
func (rs *RegionStore) GetAll() map[string]string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	cp := make(map[string]string, len(rs.regions))
	for k, v := range rs.regions {
		cp[k] = v
	}
	return cp
}

// Set stores and persists the AWS region for the specified instance ID.
func (rs *RegionStore) Set(instanceID, region string) error {
	if instanceID == "" || region == "" {
		return nil
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.regions[instanceID] = region
	return rs.saveLocked()
}

// Delete removes and persists the deletion of the specified instance ID.
func (rs *RegionStore) Delete(instanceID string) error {
	if instanceID == "" {
		return nil
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	delete(rs.regions, instanceID)
	return rs.saveLocked()
}
