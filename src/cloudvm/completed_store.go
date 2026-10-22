package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// CompletedRecordTTL defines the duration to retain finished pod telemetry before garbage collection.
const CompletedRecordTTL = 24 * time.Hour

// CompletedPodRecord stores the terminal execution telemetry of a finished pod.
type CompletedPodRecord struct {
	PodUID     string    `json:"podUID"`
	PodJID     string    `json:"podJID,omitempty"`
	ExitCode   int32     `json:"exitCode"`
	Reason     string    `json:"reason"`
	Message    string    `json:"message"`
	FinishedAt time.Time `json:"finishedAt"`
}

// CompletedStore persists terminal pod status records locally on PVC storage
// so completed pod status survives provider pod restarts and upgrades until explicit /delete.
type CompletedStore struct {
	mu       sync.RWMutex
	filePath string
	records  map[string]*CompletedPodRecord // podUID -> terminal record
}

// NewCompletedStore initializes a CompletedStore with the given file path.
func NewCompletedStore(filePath string) *CompletedStore {
	if filePath == "" {
		filePath = filepath.Join(os.TempDir(), "cloudvm-completed-pods.json")
	}
	filePath = filepath.Clean(filePath)
	cs := &CompletedStore{
		filePath: filePath,
		records:  make(map[string]*CompletedPodRecord),
	}
	_ = cs.load()
	return cs
}

func (cs *CompletedStore) pruneLocked() {
	now := time.Now()
	for uid, rec := range cs.records {
		if rec != nil && !rec.FinishedAt.IsZero() && now.Sub(rec.FinishedAt) > CompletedRecordTTL {
			delete(cs.records, uid)
		}
	}
}

func (cs *CompletedStore) load() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cleanPath := filepath.Clean(cs.filePath)
	data, err := os.ReadFile(cleanPath) // #nosec G304,G703
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var loaded map[string]*CompletedPodRecord
	if err := json.Unmarshal(data, &loaded); err != nil {
		return fmt.Errorf("corrupt completed store data: %w", err)
	}

	cs.records = loaded
	cs.pruneLocked()
	return nil
}

func (cs *CompletedStore) saveLocked() error {
	cs.pruneLocked()
	data, err := json.Marshal(cs.records)
	if err != nil {
		return err
	}

	cleanPath := filepath.Clean(cs.filePath)
	dir := filepath.Dir(cleanPath)
	if err := os.MkdirAll(dir, 0700); err != nil { // #nosec G301,G703
		return err
	}

	tmpFile := cleanPath + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0600); err != nil { // #nosec G304,G703
		return err
	}
	return os.Rename(tmpFile, cleanPath) // #nosec G703
}

// Get returns the terminal record for a given podUID, or nil if not found.
func (cs *CompletedStore) Get(podUID string) *CompletedPodRecord {
	if podUID == "" {
		return nil
	}
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.records[podUID]
}

// Set commits a completed pod record to memory and disk.
func (cs *CompletedStore) Set(podUID string, rec *CompletedPodRecord) error {
	if podUID == "" || rec == nil {
		return nil
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.records[podUID] = rec
	return cs.saveLocked()
}

// Delete removes a completed pod record upon explicit /delete.
func (cs *CompletedStore) Delete(podUID string) error {
	if podUID == "" {
		return nil
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	delete(cs.records, podUID)
	return cs.saveLocked()
}
