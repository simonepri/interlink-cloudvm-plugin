package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTombstoneStore_BasicOperations(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "tombstones.json")

	ts, err := NewTombstoneStore(storePath)
	if err != nil {
		t.Fatalf("failed to create tombstone store: %v", err)
	}

	if ts.Has("u1") {
		t.Fatalf("expected u1 not to be in store initially")
	}

	if err := ts.Set("u1"); err != nil {
		t.Fatalf("failed to set tombstone: %v", err)
	}
	if !ts.Has("u1") {
		t.Fatalf("expected u1 to be in store")
	}

	// Reload from disk
	ts2, err := NewTombstoneStore(storePath)
	if err != nil {
		t.Fatalf("failed to reload tombstone store: %v", err)
	}
	if !ts2.Has("u1") {
		t.Fatalf("expected u1 to persist across reloads")
	}

	// Delete
	if err := ts2.Delete("u1"); err != nil {
		t.Fatalf("failed to delete tombstone: %v", err)
	}
	if ts2.Has("u1") {
		t.Fatalf("expected u1 to be deleted")
	}
}

func TestTombstoneStore_CorruptRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "tombstones.json")

	if err := os.WriteFile(storePath, []byte("not-valid-json"), 0600); err != nil {
		t.Fatalf("failed to write corrupt file: %v", err)
	}

	_, err := NewTombstoneStore(storePath)
	if err == nil {
		t.Fatalf("expected error on corrupt tombstone store file")
	}

	// Should be able to initialize fresh store afterwards
	ts, err := NewTombstoneStore(storePath)
	if err != nil {
		t.Fatalf("failed to initialize fresh store: %v", err)
	}
	if err := ts.Set("u2"); err != nil {
		t.Fatalf("failed to set tombstone: %v", err)
	}
	if !ts.Has("u2") {
		t.Fatalf("expected u2 to be present")
	}
}

func TestTombstoneStore_PruneOldRecords(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "tombstones.json")

	ts, err := NewTombstoneStore(storePath)
	if err != nil {
		t.Fatalf("failed to create tombstone store: %v", err)
	}

	ts.mu.Lock()
	// Fill with 10001 old records
	for i := 0; i <= 10001; i++ {
		ts.tombstones["old-"+string(rune(i))] = time.Now().Add(-25 * time.Hour)
	}
	ts.mu.Unlock()

	if err := ts.Set("fresh"); err != nil {
		t.Fatalf("failed to set fresh record: %v", err)
	}

	if !ts.Has("fresh") {
		t.Fatalf("expected fresh record to exist")
	}
	ts.mu.RLock()
	count := len(ts.tombstones)
	ts.mu.RUnlock()
	if count > 10000 {
		t.Fatalf("expected old records pruned, got count %d", count)
	}
}
