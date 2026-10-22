package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// MaxCompletedRecords prevents unbounded store growth across millions of pods.
const MaxCompletedRecords = 10000

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
func NewCompletedStore(filePath string) (*CompletedStore, error) {
	if filePath == "" {
		filePath = filepath.Join(os.TempDir(), "cloudvm-completed-pods.json")
	}
	filePath = filepath.Clean(filePath)
	cs := &CompletedStore{
		filePath: filePath,
		records:  make(map[string]*CompletedPodRecord),
	}
	err := cs.load()
	return cs, err
}

func (cs *CompletedStore) pruneLocked() {
	if len(cs.records) <= MaxCompletedRecords {
		return
	}
	var oldestUID string
	var oldestTime time.Time
	for uid, rec := range cs.records {
		if rec == nil {
			delete(cs.records, uid)
			continue
		}
		if oldestTime.IsZero() || rec.FinishedAt.Before(oldestTime) {
			oldestTime = rec.FinishedAt
			oldestUID = uid
		}
	}
	if oldestUID != "" {
		delete(cs.records, oldestUID)
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
		corruptPath := fmt.Sprintf("%s.corrupt.%d", cleanPath, time.Now().UnixNano())
		_ = os.Rename(cleanPath, corruptPath)
		return fmt.Errorf("corrupt completed store data moved to %s: %w", corruptPath, err)
	}
	if loaded == nil {
		loaded = make(map[string]*CompletedPodRecord)
	}

	cs.records = loaded
	return nil
}

func (cs *CompletedStore) saveLocked() error {
	cs.pruneLocked()
	data, err := json.Marshal(cs.records)
	if err != nil {
		return err
	}
	return atomicSaveWithFsync(cs.filePath, data)
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
