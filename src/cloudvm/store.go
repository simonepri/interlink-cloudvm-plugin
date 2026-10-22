// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"
)

// MaxStoreRecords prevents unbounded store growth across millions of pods.
const MaxStoreRecords = 10000

// CompletedPodRecord stores the terminal execution telemetry of a finished pod.
type CompletedPodRecord struct {
	PodUID     string    `json:"podUID"`
	PodJID     string    `json:"podJID,omitempty"`
	ExitCode   int32     `json:"exitCode"`
	Reason     string    `json:"reason"`
	Message    string    `json:"message"`
	FinishedAt time.Time `json:"finishedAt"`
}

// diskMap is a generic, thread-safe, fsync-backed persistent key-value map.
type diskMap[V any] struct {
	mu       sync.RWMutex
	name     string
	filePath string
	records  map[string]V
	pruneFn  func(map[string]V)
}

func newDiskMap[V any](name, filePath, defaultFilename string, mkdirParent bool, pruneFn func(map[string]V)) (*diskMap[V], error) {
	if filePath == "" {
		filePath = filepath.Join(os.TempDir(), defaultFilename)
	}
	filePath = filepath.Clean(filePath)
	if mkdirParent {
		if err := os.MkdirAll(filepath.Dir(filePath), 0750); err != nil { //nolint:gosec // directory creation for local store
			filePath = filepath.Join(os.TempDir(), defaultFilename)
		}
	}
	dm := &diskMap[V]{
		name:     name,
		filePath: filePath,
		records:  make(map[string]V),
		pruneFn:  pruneFn,
	}
	err := dm.load()
	return dm, err
}

func (dm *diskMap[V]) load() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	cleanPath := filepath.Clean(dm.filePath)
	data, err := os.ReadFile(cleanPath) // #nosec G304,G703
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var loaded map[string]V
	if err := json.Unmarshal(data, &loaded); err != nil {
		corruptPath := fmt.Sprintf("%s.corrupt.%d", cleanPath, time.Now().UnixNano())
		_ = os.Rename(cleanPath, corruptPath)
		return fmt.Errorf("corrupt %s data moved to %s: %w", dm.name, corruptPath, err)
	}
	if loaded == nil {
		loaded = make(map[string]V)
	}
	dm.records = loaded
	return nil
}

func (dm *diskMap[V]) saveLocked() error {
	if dm.pruneFn != nil {
		dm.pruneFn(dm.records)
	}
	data, err := json.Marshal(dm.records)
	if err != nil {
		return err
	}
	return atomicSaveWithFsync(dm.filePath, data)
}

func (dm *diskMap[V]) get(key string) (V, bool) {
	var zero V
	if key == "" {
		return zero, false
	}
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	v, ok := dm.records[key]
	return v, ok
}

func (dm *diskMap[V]) getAll() map[string]V {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	cp := make(map[string]V, len(dm.records))
	for k, v := range dm.records {
		cp[k] = v
	}
	return cp
}

func (dm *diskMap[V]) set(key string, val V, skipIfEqual bool) error {
	if key == "" {
		return nil
	}
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if skipIfEqual {
		if existing, exists := dm.records[key]; exists && reflect.DeepEqual(existing, val) {
			return nil
		}
	}
	dm.records[key] = val
	return dm.saveLocked()
}

func (dm *diskMap[V]) delete(key string, skipIfMissing bool) error {
	if key == "" {
		return nil
	}
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if skipIfMissing {
		if _, exists := dm.records[key]; !exists {
			return nil
		}
	}
	delete(dm.records, key)
	return dm.saveLocked()
}

// atomicSaveWithFsync safely persists data to disk using a tempfile, fsync, and atomic rename.
func atomicSaveWithFsync(path string, data []byte) error {
	cleanPath := filepath.Clean(path)
	dir := filepath.Dir(cleanPath)
	if err := os.MkdirAll(dir, 0700); err != nil { // #nosec G301,G703
		return err
	}

	tmpFile := cleanPath + ".tmp"
	f, err := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600) // #nosec G304,G703
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpFile) // #nosec G703
		return err
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpFile) // #nosec G703
		return err
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmpFile) // #nosec G703
		return err
	}

	if err := os.Rename(tmpFile, cleanPath); err != nil { // #nosec G703
		_ = os.Remove(tmpFile) // #nosec G703
		return err
	}

	// Fsync parent directory to ensure metadata is flushed
	if df, err := os.Open(dir); err == nil { // #nosec G304,G703
		_ = df.Sync()
		_ = df.Close()
	}

	return nil
}

// CompletedStore persists terminal pod status records locally on PVC storage.
type CompletedStore struct {
	*diskMap[*CompletedPodRecord]
}

// NewCompletedStore initializes a CompletedStore with the given file path.
func NewCompletedStore(filePath string) (*CompletedStore, error) {
	dm, err := newDiskMap("completed store", filePath, "cloudvm-completed-pods.json", false, func(records map[string]*CompletedPodRecord) {
		if len(records) <= MaxStoreRecords {
			return
		}
		var oldestUID string
		var oldestTime time.Time
		for uid, rec := range records {
			if rec == nil {
				delete(records, uid)
				continue
			}
			if oldestTime.IsZero() || rec.FinishedAt.Before(oldestTime) {
				oldestTime = rec.FinishedAt
				oldestUID = uid
			}
		}
		if oldestUID != "" {
			delete(records, oldestUID)
		}
	})
	return &CompletedStore{diskMap: dm}, err
}

// Get returns the terminal record for a given podUID, or nil if not found.
func (cs *CompletedStore) Get(podUID string) *CompletedPodRecord {
	rec, _ := cs.get(podUID)
	return rec
}

// Set commits a completed pod record to memory and disk.
func (cs *CompletedStore) Set(podUID string, rec *CompletedPodRecord) error {
	if rec == nil {
		return nil
	}
	return cs.set(podUID, rec, false)
}

// Delete removes a completed pod record upon explicit /delete.
func (cs *CompletedStore) Delete(podUID string) error {
	return cs.delete(podUID, false)
}

// RegionStore safely persists instance-to-region mappings locally on PVC storage.
type RegionStore struct {
	*diskMap[string]
}

// NewRegionStore initializes a RegionStore with the given file path.
func NewRegionStore(filePath string) (*RegionStore, error) {
	dm, err := newDiskMap[string]("region store", filePath, "cloudvm-instance-regions.json", true, nil)
	return &RegionStore{diskMap: dm}, err
}

// Get returns the AWS region for the specified instance ID, or empty string if not found.
func (rs *RegionStore) Get(instanceID string) string {
	reg, _ := rs.get(instanceID)
	return reg
}

// GetAll returns a thread-safe copy of all tracked instance-to-region mappings.
func (rs *RegionStore) GetAll() map[string]string {
	return rs.getAll()
}

// Set stores and persists the AWS region for the specified instance ID.
func (rs *RegionStore) Set(instanceID, region string) error {
	if region == "" {
		return nil
	}
	return rs.set(instanceID, region, true)
}

// Delete removes and persists the deletion of the specified instance ID.
func (rs *RegionStore) Delete(instanceID string) error {
	return rs.delete(instanceID, true)
}

// TokenStore safely persists in-VM runner authorization tokens locally.
type TokenStore struct {
	*diskMap[string]
}

// NewTokenStore initializes a TokenStore with the given file path.
func NewTokenStore(filePath string) (*TokenStore, error) {
	dm, err := newDiskMap[string]("token store", filePath, "cloudvm-tokens.json", false, nil)
	return &TokenStore{diskMap: dm}, err
}

// Get returns the runner token for the specified instance ID, or empty string if not found.
func (ts *TokenStore) Get(instanceID string) string {
	tok, _ := ts.get(instanceID)
	return tok
}

// Set stores and persists the runner token for the specified instance ID.
func (ts *TokenStore) Set(instanceID, token string) error {
	if token == "" {
		return nil
	}
	return ts.set(instanceID, token, false)
}

// Delete removes and persists the token deletion for the specified instance ID.
func (ts *TokenStore) Delete(instanceID string) error {
	return ts.delete(instanceID, false)
}

// TombstoneStore safely persists explicitly deleted pod UIDs and instance IDs locally.
type TombstoneStore struct {
	*diskMap[time.Time]
}

// NewTombstoneStore initializes a TombstoneStore with the given file path.
func NewTombstoneStore(filePath string) (*TombstoneStore, error) {
	dm, err := newDiskMap("tombstone store", filePath, "cloudvm-tombstones.json", false, func(tombstones map[string]time.Time) {
		if len(tombstones) <= MaxStoreRecords {
			return
		}
		now := time.Now()
		for k, t := range tombstones {
			if now.Sub(t) > 24*time.Hour {
				delete(tombstones, k)
			}
		}
	})
	return &TombstoneStore{diskMap: dm}, err
}

// Has returns true if the specified ID is recorded in the tombstone store.
func (ts *TombstoneStore) Has(id string) bool {
	_, exists := ts.get(id)
	return exists
}

// Set commits a tombstone to memory and disk.
func (ts *TombstoneStore) Set(id string) error {
	return ts.set(id, time.Now(), false)
}

// Delete removes a tombstone from memory and disk.
func (ts *TombstoneStore) Delete(id string) error {
	return ts.delete(id, false)
}

// GetAll returns a thread-safe copy of all tombstones.
func (ts *TombstoneStore) GetAll() map[string]time.Time {
	return ts.getAll()
}
