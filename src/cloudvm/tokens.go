package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// TokenStore safely persists in-VM runner authorization tokens locally
// without leaking secrets into cloud provider tags or external metadata.
type TokenStore struct {
	mu       sync.RWMutex
	filePath string
	tokens   map[string]string // instanceID or podUID -> runnerAuthToken
}

// NewTokenStore initializes a TokenStore with the given file path.
// If filePath is empty, a default path in os.TempDir is used.
func NewTokenStore(filePath string) (*TokenStore, error) {
	if filePath == "" {
		filePath = filepath.Join(os.TempDir(), "cloudvm-tokens.json")
	}
	filePath = filepath.Clean(filePath)
	ts := &TokenStore{
		filePath: filePath,
		tokens:   make(map[string]string),
	}
	err := ts.load()
	return ts, err
}

func (ts *TokenStore) load() error {
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
	var loaded map[string]string
	if err := json.Unmarshal(data, &loaded); err != nil {
		corruptPath := fmt.Sprintf("%s.corrupt.%d", cleanPath, time.Now().UnixNano())
		_ = os.Rename(cleanPath, corruptPath)
		return fmt.Errorf("corrupt token store data moved to %s: %w", corruptPath, err)
	}
	if loaded == nil {
		loaded = make(map[string]string)
	}
	ts.tokens = loaded
	return nil
}

func (ts *TokenStore) saveLocked() error {
	data, err := json.Marshal(ts.tokens)
	if err != nil {
		return err
	}
	return atomicSaveWithFsync(ts.filePath, data)
}

// Get returns the runner token for the specified instance ID, or empty string if not found.
func (ts *TokenStore) Get(instanceID string) string {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.tokens[instanceID]
}

// Set stores and persists the runner token for the specified instance ID.
func (ts *TokenStore) Set(instanceID, token string) error {
	if instanceID == "" || token == "" {
		return nil
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.tokens[instanceID] = token
	return ts.saveLocked()
}

// Delete removes and persists the token deletion for the specified instance ID.
func (ts *TokenStore) Delete(instanceID string) error {
	if instanceID == "" {
		return nil
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	delete(ts.tokens, instanceID)
	return ts.saveLocked()
}
