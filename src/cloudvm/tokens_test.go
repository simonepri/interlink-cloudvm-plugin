package main

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestTokenStore_SetGetDelete(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "test-tokens.json")

	ts := NewTokenStore(storePath)

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
	ts2 := NewTokenStore(storePath)
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
	ts3 := NewTokenStore(storePath)
	if tok := ts3.Get("i-12345"); tok != "" {
		t.Fatalf("expected empty token in reloaded store, got %s", tok)
	}
}

func TestTokenStore_ConcurrentAccess(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "concurrent-tokens.json")
	ts := NewTokenStore(storePath)

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
