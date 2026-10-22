package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// TombstoneStore persists deleted pod UIDs and instance IDs locally
// so that failed or lagging cloud terminations are not accidentally re-adopted across restarts.
type TombstoneStore struct {
	mu         sync.RWMutex
	filePath   string
	tombstones map[string]time.Time // podUID or instanceID -> deletedAt
}

// NewTombstoneStore initializes a TombstoneStore with the given file path.
func NewTombstoneStore(filePath string) (*TombstoneStore, error) {
	if filePath == "" {
		filePath = filepath.Join(os.TempDir(), "cloudvm-tombstones.json")
	}
	filePath = filepath.Clean(filePath)
	ts := &TombstoneStore{
		filePath:   filePath,
		tombstones: make(map[string]time.Time),
	}
	err := ts.load()
	return ts, err
}

func (ts *TombstoneStore) load() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	cleanPath := filepath.Clean(ts.filePath)
	data, err := os.ReadFile(cleanPath) // #nosec G304,G703
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var loaded map[string]time.Time
	if err := json.Unmarshal(data, &loaded); err != nil {
		corruptPath := fmt.Sprintf("%s.corrupt.%d", cleanPath, time.Now().UnixNano())
		_ = os.Rename(cleanPath, corruptPath)
		return fmt.Errorf("corrupt tombstone store data moved to %s: %w", corruptPath, err)
	}
	if loaded == nil {
		loaded = make(map[string]time.Time)
	}
	ts.tombstones = loaded
	return nil
}

func (ts *TombstoneStore) pruneLocked() {
	if len(ts.tombstones) <= 10000 {
		return
	}
	now := time.Now()
	for k, t := range ts.tombstones {
		if now.Sub(t) > 24*time.Hour {
			delete(ts.tombstones, k)
		}
	}
}

func (ts *TombstoneStore) saveLocked() error {
	ts.pruneLocked()
	data, err := json.Marshal(ts.tombstones)
	if err != nil {
		return err
	}
	return atomicSaveWithFsync(ts.filePath, data)
}

// Has returns true if the specified ID is recorded in the tombstone store.
func (ts *TombstoneStore) Has(id string) bool {
	if id == "" {
		return false
	}
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	_, exists := ts.tombstones[id]
	return exists
}

// Set commits a tombstone to memory and disk.
func (ts *TombstoneStore) Set(id string) error {
	if id == "" {
		return nil
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.tombstones[id] = time.Now()
	return ts.saveLocked()
}

// Delete removes a tombstone from memory and disk.
func (ts *TombstoneStore) Delete(id string) error {
	if id == "" {
		return nil
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	delete(ts.tombstones, id)
	return ts.saveLocked()
}
