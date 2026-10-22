package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// TokenStore safely persists in-VM runner authorization tokens locally
// without leaking secrets into cloud provider tags or external metadata.
type TokenStore struct {
	mu       sync.RWMutex
	filePath string
	tokens   map[string]string // instanceID -> runnerAuthToken
}

// NewTokenStore initializes a TokenStore with the given file path.
// If filePath is empty, a default path in os.TempDir is used.
func NewTokenStore(filePath string) *TokenStore {
	if filePath == "" {
		filePath = filepath.Join(os.TempDir(), "cloudvm-tokens.json")
	}
	filePath = filepath.Clean(filePath)
	ts := &TokenStore{
		filePath: filePath,
		tokens:   make(map[string]string),
	}
	_ = ts.load()
	return ts
}

func (ts *TokenStore) load() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	cleanPath := filepath.Clean(ts.filePath)
	data, err := os.ReadFile(cleanPath) // #nosec G304,G703
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &ts.tokens)
}

func (ts *TokenStore) saveLocked() error {
	data, err := json.Marshal(ts.tokens)
	if err != nil {
		return err
	}

	cleanPath := filepath.Clean(ts.filePath)
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
