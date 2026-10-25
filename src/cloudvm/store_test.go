package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRegionStore_SetGetDelete(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "instance-regions.json")

	rs, err := NewRegionStore(storePath)
	if err != nil {
		t.Fatalf("failed to create RegionStore: %v", err)
	}

	if reg := rs.Get("i-123"); reg != "" {
		t.Fatalf("expected empty region for missing instance, got %s", reg)
	}

	if err := rs.Set("i-123", "us-west-2"); err != nil {
		t.Fatalf("failed to set region: %v", err)
	}
	if reg := rs.Get("i-123"); reg != "us-west-2" {
		t.Fatalf("expected us-west-2, got %s", reg)
	}

	all := rs.GetAll()
	if len(all) != 1 || all["i-123"] != "us-west-2" {
		t.Fatalf("expected 1 mapping in GetAll, got %+v", all)
	}

	// Reload from disk
	rs2, err := NewRegionStore(storePath)
	if err != nil {
		t.Fatalf("failed to reload RegionStore: %v", err)
	}
	if reg := rs2.Get("i-123"); reg != "us-west-2" {
		t.Fatalf("expected us-west-2 after reload, got %s", reg)
	}

	if err := rs2.Delete("i-123"); err != nil {
		t.Fatalf("failed to delete mapping: %v", err)
	}
	if reg := rs2.Get("i-123"); reg != "" {
		t.Fatalf("expected empty region after delete, got %s", reg)
	}

	rs3, err := NewRegionStore(storePath)
	if err != nil {
		t.Fatalf("failed to reload RegionStore after delete: %v", err)
	}
	if reg := rs3.Get("i-123"); reg != "" {
		t.Fatalf("expected empty region after reload, got %s", reg)
	}
}

func TestRegionStore_CorruptFilePreserved(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "corrupt-regions.json")

	if err := os.WriteFile(storePath, []byte("NOT_VALID_JSON{{{"), 0600); err != nil {
		t.Fatalf("failed to write corrupt file: %v", err)
	}

	store, err := NewRegionStore(storePath)
	if err == nil {
		t.Fatal("expected error on corrupt file, got nil")
	}
	if store == nil {
		t.Fatal("expected non-nil store instance even on corrupt file")
	}

	// Verify corrupt file was backed up and original is replaced/ready
	matches, _ := filepath.Glob(storePath + ".corrupt.*")
	if len(matches) == 0 {
		t.Fatalf("expected corrupt file backup, found none")
	}

	// Should be usable immediately after
	if err := store.Set("i-999", "eu-central-1"); err != nil {
		t.Fatalf("failed to set after corrupt recovery: %v", err)
	}
	if reg := store.Get("i-999"); reg != "eu-central-1" {
		t.Fatalf("expected eu-central-1, got %s", reg)
	}
}

func TestRegionStore_ConcurrentAccess(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "concurrent-regions.json")

	rs, err := NewRegionStore(storePath)
	if err != nil {
		t.Fatalf("failed to create RegionStore: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		instID := "inst-" + string(rune(i+65))
		go func(id string) {
			defer wg.Done()
			_ = rs.Set(id, "us-east-1")
			_ = rs.Get(id)
			_ = rs.GetAll()
		}(instID)
	}
	wg.Wait()

	if len(rs.GetAll()) != 20 {
		t.Fatalf("expected 20 mappings, got %d", len(rs.GetAll()))
	}
}

func TestRegionStore_NoOpWhenUnchanged(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "noop-regions.json")

	rs, err := NewRegionStore(storePath)
	if err != nil {
		t.Fatalf("failed to create RegionStore: %v", err)
	}

	if err := rs.Set("i-unchanged", "us-east-1"); err != nil {
		t.Fatalf("failed to set: %v", err)
	}

	info1, err := os.Stat(storePath)
	if err != nil {
		t.Fatalf("failed to stat: %v", err)
	}

	// Calling Set with same region should be a no-op without rewriting disk
	if err := rs.Set("i-unchanged", "us-east-1"); err != nil {
		t.Fatalf("failed second set: %v", err)
	}

	info2, err := os.Stat(storePath)
	if err != nil {
		t.Fatalf("failed to stat: %v", err)
	}

	if info1.ModTime() != info2.ModTime() {
		t.Fatalf("expected unchanged modtime on identical Set, but file was rewritten")
	}

	// Delete for missing ID should also be a no-op
	if err := rs.Delete("non-existent-id"); err != nil {
		t.Fatalf("failed delete of missing ID: %v", err)
	}
}

func TestTokenStore_SetGetDelete(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "test-tokens.json")

	ts, _ := NewTokenStore(storePath)

	// Initial get
	if tok := ts.Get("i-12345"); tok != "" {
		t.Fatalf("expected empty token, got %s", tok)
	}

	// Set and get
	if err := ts.Set("i-12345", "secret-tok-abc"); err != nil {
		t.Fatalf("failed to set token: %v", err)
	}
	if tok := ts.Get("i-12345"); tok != "secret-tok-abc" {
		t.Fatalf("expected secret-tok-abc, got %s", tok)
	}

	// Reload from disk to verify persistence
	ts2, _ := NewTokenStore(storePath)
	if tok := ts2.Get("i-12345"); tok != "secret-tok-abc" {
		t.Fatalf("expected persisted secret-tok-abc, got %s", tok)
	}

	// Delete
	if err := ts2.Delete("i-12345"); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	if tok := ts2.Get("i-12345"); tok != "" {
		t.Fatalf("expected empty token after delete, got %s", tok)
	}

	// Verify file updated
	ts3, _ := NewTokenStore(storePath)
	if tok := ts3.Get("i-12345"); tok != "" {
		t.Fatalf("expected empty token in reloaded store, got %s", tok)
	}
}

func TestTokenStore_ConcurrentAccess(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "concurrent-tokens.json")
	ts, _ := NewTokenStore(storePath)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			instID := fmt.Sprintf("inst-%d", id)
			_ = ts.Set(instID, "token-"+instID)
			_ = ts.Get(instID)
		}(i)
	}
	wg.Wait()

	for i := 0; i < 20; i++ {
		instID := fmt.Sprintf("inst-%d", i)
		if tok := ts.Get(instID); tok != "token-"+instID {
			t.Fatalf("expected token-%s, got %s", instID, tok)
		}
	}
}

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
		ts.records["old-"+string(rune(i))] = time.Now().Add(-25 * time.Hour)
	}
	ts.mu.Unlock()

	if err := ts.Set("fresh"); err != nil {
		t.Fatalf("failed to set fresh record: %v", err)
	}

	if !ts.Has("fresh") {
		t.Fatalf("expected fresh record to exist")
	}
	ts.mu.RLock()
	count := len(ts.records)
	ts.mu.RUnlock()
	if count > 10000 {
		t.Fatalf("expected old records pruned, got count %d", count)
	}
}

func TestCompletedStore_PersistenceAcrossReload(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "completed-test.json")
	store1, _ := NewCompletedStore(tmpFile)

	rec := &CompletedPodRecord{
		PodUID:     "uid-persist-1",
		ExitCode:   0,
		Reason:     "Completed",
		Message:    "job succeeded",
		FinishedAt: time.Now().Round(time.Millisecond),
	}

	if err := store1.Set("uid-persist-1", rec); err != nil {
		t.Fatalf("failed to set completed record: %v", err)
	}

	// Verify second store reloads the record from disk
	store2, _ := NewCompletedStore(tmpFile)
	loaded := store2.Get("uid-persist-1")
	if loaded == nil {
		t.Fatalf("expected record to survive reload from disk")
	}
	if loaded.PodUID != rec.PodUID || loaded.ExitCode != rec.ExitCode || loaded.Reason != rec.Reason {
		t.Fatalf("mismatched record after reload: got %+v, expected %+v", loaded, rec)
	}

	// Test deletion persistence
	if err := store2.Delete("uid-persist-1"); err != nil {
		t.Fatalf("failed to delete record: %v", err)
	}

	store3, _ := NewCompletedStore(tmpFile)
	if store3.Get("uid-persist-1") != nil {
		t.Fatalf("expected record to remain deleted after reload")
	}
}

func TestCompletedStore_CorruptFilePreserved(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "completed.json")

	// Write corrupt JSON
	if err := os.WriteFile(storePath, []byte(`{"corrupt": [truncated...`), 0600); err != nil {
		t.Fatalf("failed to write corrupt file: %v", err)
	}

	store, err := NewCompletedStore(storePath)
	if err == nil {
		t.Fatal("expected error loading corrupt store, got nil")
	}

	// Verify a .corrupt file was created
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}
	var corruptFileFound bool
	for _, f := range files {
		if strings.Contains(f.Name(), ".corrupt.") {
			corruptFileFound = true
			data, _ := os.ReadFile(filepath.Join(tmpDir, f.Name()))
			if !strings.Contains(string(data), "truncated") {
				t.Fatalf("corrupt file preserved incorrect data: %s", string(data))
			}
		}
	}
	if !corruptFileFound {
		t.Fatal("expected .corrupt file backup to be created")
	}

	// Write a new record to verify store can continue functioning
	if err := store.Set("uid-new", &CompletedPodRecord{PodUID: "uid-new", ExitCode: 0}); err != nil {
		t.Fatalf("failed to set record in new store: %v", err)
	}

	// Verify the original corrupt backup is still present and was not clobbered
	filesAfter, _ := os.ReadDir(tmpDir)
	var backupStillPresent bool
	for _, f := range filesAfter {
		if strings.Contains(f.Name(), ".corrupt.") {
			backupStillPresent = true
		}
	}
	if !backupStillPresent {
		t.Fatal("corrupt backup was clobbered by subsequent Set")
	}
}

func TestTokenStore_CorruptFilePreserved(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "tokens.json")

	if err := os.WriteFile(storePath, []byte(`{"corrupt": [truncated...`), 0600); err != nil {
		t.Fatalf("failed to write corrupt file: %v", err)
	}

	store, err := NewTokenStore(storePath)
	if err == nil {
		t.Fatal("expected error loading corrupt token store, got nil")
	}

	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}
	var corruptFileFound bool
	for _, f := range files {
		if strings.Contains(f.Name(), ".corrupt.") {
			corruptFileFound = true
			data, _ := os.ReadFile(filepath.Join(tmpDir, f.Name()))
			if !strings.Contains(string(data), "truncated") {
				t.Fatalf("corrupt file preserved incorrect data: %s", string(data))
			}
		}
	}
	if !corruptFileFound {
		t.Fatal("expected .corrupt file backup to be created")
	}

	if err := store.Set("inst-1", "tok-1"); err != nil {
		t.Fatalf("failed to set token in new store: %v", err)
	}

	// Verify backup was not clobbered
	filesAfter, _ := os.ReadDir(tmpDir)
	var backupStillPresent bool
	for _, f := range filesAfter {
		if strings.Contains(f.Name(), ".corrupt.") {
			backupStillPresent = true
		}
	}
	if !backupStillPresent {
		t.Fatal("corrupt backup was clobbered by subsequent Set")
	}
}

func TestTokenStore_NullFileDoesNotPanic(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "tokens-null.json")

	if err := os.WriteFile(storePath, []byte("null"), 0600); err != nil {
		t.Fatalf("failed to write null file: %v", err)
	}

	ts, err := NewTokenStore(storePath)
	if err != nil {
		t.Fatalf("unexpected error loading null token store: %v", err)
	}

	// Must not panic with assignment to entry in nil map
	if err := ts.Set("inst-null", "tok-null"); err != nil {
		t.Fatalf("failed to set token on previously null store: %v", err)
	}
	if tok := ts.Get("inst-null"); tok != "tok-null" {
		t.Fatalf("expected tok-null, got %s", tok)
	}
}

func TestCompletedStore_NullFileDoesNotPanic(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "completed-null.json")

	if err := os.WriteFile(storePath, []byte("null"), 0600); err != nil {
		t.Fatalf("failed to write null file: %v", err)
	}

	cs, err := NewCompletedStore(storePath)
	if err != nil {
		t.Fatalf("unexpected error loading null completed store: %v", err)
	}

	// Must not panic with assignment to entry in nil map
	rec := &CompletedPodRecord{PodUID: "uid-null", ExitCode: 0}
	if err := cs.Set("uid-null", rec); err != nil {
		t.Fatalf("failed to set completed record on previously null store: %v", err)
	}
	if got := cs.Get("uid-null"); got == nil || got.PodUID != "uid-null" {
		t.Fatalf("expected record to be retrievable from previously null store")
	}
}

func TestNodeStore_SetGetDelete(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "nodes.json")

	ns, err := NewNodeStore(storePath)
	if err != nil {
		t.Fatalf("failed to create NodeStore: %v", err)
	}

	if node := ns.Get("i-999"); node != nil {
		t.Fatalf("expected nil for non-existent node, got %+v", node)
	}

	now := time.Now().UTC()
	rec := &NodeRecord{
		InstanceID:   "i-999",
		Region:       "us-east-1",
		Provider:     "aws",
		IP:           "10.0.0.99",
		InstanceType: "g5.2xlarge",
		Status:       "Running",
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	if err := ns.Set("i-999", rec); err != nil {
		t.Fatalf("failed to set node record: %v", err)
	}

	got := ns.Get("i-999")
	if got == nil || got.Region != "us-east-1" || got.InstanceID != "i-999" {
		t.Fatalf("unexpected node retrieved: %+v", got)
	}

	all := ns.GetAll()
	if len(all) != 1 || all["i-999"].Region != "us-east-1" {
		t.Fatalf("unexpected GetAll result: %+v", all)
	}

	// Reload from disk
	ns2, err := NewNodeStore(storePath)
	if err != nil {
		t.Fatalf("failed to reload NodeStore: %v", err)
	}
	if got2 := ns2.Get("i-999"); got2 == nil || got2.Region != "us-east-1" {
		t.Fatalf("expected node to persist across reload: %+v", got2)
	}

	// Delete
	if err := ns2.Delete("i-999"); err != nil {
		t.Fatalf("failed to delete node: %v", err)
	}
	if got3 := ns2.Get("i-999"); got3 != nil {
		t.Fatalf("expected node to be deleted, got %+v", got3)
	}
}
