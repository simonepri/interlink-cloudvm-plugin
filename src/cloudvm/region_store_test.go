// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
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
