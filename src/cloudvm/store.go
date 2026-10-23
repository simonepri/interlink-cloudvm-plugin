// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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

// ----------------------------------------------------------------------------
// Backend Interfaces
// ----------------------------------------------------------------------------

// CompletedStoreBackend defines the storage operations for completed pod records.
type CompletedStoreBackend interface {
	Get(podUID string) *CompletedPodRecord
	Set(podUID string, rec *CompletedPodRecord) error
	Delete(podUID string) error
	GetAll() map[string]*CompletedPodRecord
}

// RegionStoreBackend defines the storage operations for instance-to-region mappings.
type RegionStoreBackend interface {
	Get(instanceID string) string
	Set(instanceID, region string) error
	Delete(instanceID string) error
	GetAll() map[string]string
}

// TokenStoreBackend defines the storage operations for runner tokens.
type TokenStoreBackend interface {
	Get(instanceID string) string
	Set(instanceID, token string) error
	Delete(instanceID string) error
	GetAll() map[string]string
}

// TombstoneStoreBackend defines the storage operations for tombstones.
type TombstoneStoreBackend interface {
	Has(id string) bool
	Get(id string) (time.Time, bool)
	Set(id string) error
	Delete(id string) error
	GetAll() map[string]time.Time
}

// ----------------------------------------------------------------------------
// Connection Pooling Cache and Connection String Resolver
// ----------------------------------------------------------------------------

var (
	globalPGStoresMu sync.Mutex
	globalPGStores   = make(map[string]*PGStore)
)

func resolveConnStr(connStr ...string) string {
	if len(connStr) > 0 {
		return connStr[0]
	}
	return os.Getenv("DATABASE_URL")
}

func getOrCreatePGStore(connStr string) (*PGStore, error) {
	globalPGStoresMu.Lock()
	defer globalPGStoresMu.Unlock()

	if store, ok := globalPGStores[connStr]; ok {
		return store, nil
	}

	store, err := NewPGStore(connStr)
	if err != nil {
		return nil, err
	}
	globalPGStores[connStr] = store
	return store, nil
}

// ResetGlobalPGStores closes all active cached PGStore connections and resets the cache.
func ResetGlobalPGStores() {
	globalPGStoresMu.Lock()
	defer globalPGStoresMu.Unlock()
	for _, s := range globalPGStores {
		_ = s.Close()
	}
	globalPGStores = make(map[string]*PGStore)
}

// ----------------------------------------------------------------------------
// diskMap Backend Implementations
// ----------------------------------------------------------------------------

type diskMapCompletedBackend struct {
	dm *diskMap[*CompletedPodRecord]
}

func (b *diskMapCompletedBackend) Get(podUID string) *CompletedPodRecord {
	rec, _ := b.dm.get(podUID)
	return rec
}

func (b *diskMapCompletedBackend) Set(podUID string, rec *CompletedPodRecord) error {
	if rec == nil {
		return nil
	}
	return b.dm.set(podUID, rec, false)
}

func (b *diskMapCompletedBackend) Delete(podUID string) error {
	return b.dm.delete(podUID, false)
}

func (b *diskMapCompletedBackend) GetAll() map[string]*CompletedPodRecord {
	return b.dm.getAll()
}

type diskMapRegionBackend struct {
	dm *diskMap[string]
}

func (b *diskMapRegionBackend) Get(instanceID string) string {
	reg, _ := b.dm.get(instanceID)
	return reg
}

func (b *diskMapRegionBackend) Set(instanceID, region string) error {
	if region == "" {
		return nil
	}
	return b.dm.set(instanceID, region, true)
}

func (b *diskMapRegionBackend) Delete(instanceID string) error {
	return b.dm.delete(instanceID, true)
}

func (b *diskMapRegionBackend) GetAll() map[string]string {
	return b.dm.getAll()
}

type diskMapTokenBackend struct {
	dm *diskMap[string]
}

func (b *diskMapTokenBackend) Get(instanceID string) string {
	tok, _ := b.dm.get(instanceID)
	return tok
}

func (b *diskMapTokenBackend) Set(instanceID, token string) error {
	if token == "" {
		return nil
	}
	return b.dm.set(instanceID, token, false)
}

func (b *diskMapTokenBackend) Delete(instanceID string) error {
	return b.dm.delete(instanceID, false)
}

func (b *diskMapTokenBackend) GetAll() map[string]string {
	return b.dm.getAll()
}

type diskMapTombstoneBackend struct {
	dm *diskMap[time.Time]
}

func (b *diskMapTombstoneBackend) Has(id string) bool {
	_, exists := b.dm.get(id)
	return exists
}

func (b *diskMapTombstoneBackend) Get(id string) (time.Time, bool) {
	return b.dm.get(id)
}

func (b *diskMapTombstoneBackend) Set(id string) error {
	return b.dm.set(id, time.Now(), false)
}

func (b *diskMapTombstoneBackend) Delete(id string) error {
	return b.dm.delete(id, false)
}

func (b *diskMapTombstoneBackend) GetAll() map[string]time.Time {
	return b.dm.getAll()
}

// ----------------------------------------------------------------------------
// DBStore Generic Backend Implementations
// ----------------------------------------------------------------------------

type dbStoreCompletedBackend struct {
	db DBStore
}

func (b *dbStoreCompletedBackend) Get(podUID string) *CompletedPodRecord {
	rec, err := b.db.GetCompletedPod(context.Background(), podUID)
	if err != nil {
		log.Printf("[store] GetCompletedPod failed for %s: %v", podUID, err)
	}
	return rec
}

func (b *dbStoreCompletedBackend) Set(podUID string, rec *CompletedPodRecord) error {
	return b.db.SetCompletedPod(context.Background(), podUID, rec)
}

func (b *dbStoreCompletedBackend) Delete(podUID string) error {
	return b.db.DeleteCompletedPod(context.Background(), podUID)
}

func (b *dbStoreCompletedBackend) GetAll() map[string]*CompletedPodRecord {
	all, err := b.db.GetAllCompletedPods(context.Background())
	if err != nil {
		log.Printf("[store] GetAllCompletedPods failed: %v", err)
		return make(map[string]*CompletedPodRecord)
	}
	return all
}

type dbStoreRegionBackend struct {
	db DBStore
}

func (b *dbStoreRegionBackend) Get(instanceID string) string {
	reg, err := b.db.GetRegion(context.Background(), instanceID)
	if err != nil {
		log.Printf("[store] GetRegion failed for %s: %v", instanceID, err)
	}
	return reg
}

func (b *dbStoreRegionBackend) Set(instanceID, region string) error {
	return b.db.SetRegion(context.Background(), instanceID, region)
}

func (b *dbStoreRegionBackend) Delete(instanceID string) error {
	return b.db.DeleteRegion(context.Background(), instanceID)
}

func (b *dbStoreRegionBackend) GetAll() map[string]string {
	all, err := b.db.GetAllRegions(context.Background())
	if err != nil {
		log.Printf("[store] GetAllRegions failed: %v", err)
		return make(map[string]string)
	}
	return all
}

type dbStoreTokenBackend struct {
	db DBStore
}

func (b *dbStoreTokenBackend) Get(instanceID string) string {
	tok, err := b.db.GetToken(context.Background(), instanceID)
	if err != nil {
		log.Printf("[store] GetToken failed for %s: %v", instanceID, err)
	}
	return tok
}

func (b *dbStoreTokenBackend) Set(instanceID, token string) error {
	return b.db.SetToken(context.Background(), instanceID, token)
}

func (b *dbStoreTokenBackend) Delete(instanceID string) error {
	return b.db.DeleteToken(context.Background(), instanceID)
}

func (b *dbStoreTokenBackend) GetAll() map[string]string {
	all, err := b.db.GetAllTokens(context.Background())
	if err != nil {
		log.Printf("[store] GetAllTokens failed: %v", err)
		return make(map[string]string)
	}
	return all
}

type dbStoreTombstoneBackend struct {
	db DBStore
}

func (b *dbStoreTombstoneBackend) Has(id string) bool {
	exists, err := b.db.HasTombstone(context.Background(), id)
	if err != nil {
		log.Printf("[store] HasTombstone failed for %s: %v", id, err)
	}
	return exists
}

func (b *dbStoreTombstoneBackend) Get(id string) (time.Time, bool) {
	t, exists, err := b.db.GetTombstone(context.Background(), id)
	if err != nil {
		log.Printf("[store] GetTombstone failed for %s: %v", id, err)
	}
	return t, exists
}

func (b *dbStoreTombstoneBackend) Set(id string) error {
	return b.db.SetTombstone(context.Background(), id)
}

func (b *dbStoreTombstoneBackend) Delete(id string) error {
	return b.db.DeleteTombstone(context.Background(), id)
}

func (b *dbStoreTombstoneBackend) GetAll() map[string]time.Time {
	all, err := b.db.GetAllTombstones(context.Background())
	if err != nil {
		log.Printf("[store] GetAllTombstones failed: %v", err)
		return make(map[string]time.Time)
	}
	return all
}

// ----------------------------------------------------------------------------
// CompletedStore
// ----------------------------------------------------------------------------

// CompletedStore persists terminal pod status records locally on PVC storage or remotely in PostgreSQL.
type CompletedStore struct {
	*diskMap[*CompletedPodRecord]
	backend CompletedStoreBackend
}

// NewCompletedStore initializes a CompletedStore with the given file path or connection string.
// If connStr is provided and non-empty (or DATABASE_URL is set), it uses PostgreSQL;
// otherwise it falls back to diskMap persistence.
func NewCompletedStore(filePath string, connStr ...string) (*CompletedStore, error) {
	targetConn := resolveConnStr(connStr...)
	if targetConn != "" {
		pg, err := getOrCreatePGStore(targetConn)
		if err != nil {
			return nil, err
		}
		return NewCompletedStoreFromDB(pg), nil
	}
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
	return &CompletedStore{
		backend: &diskMapCompletedBackend{dm: dm},
		diskMap: dm,
	}, err
}

// NewCompletedStoreFromDB initializes a CompletedStore backed by a DBStore.
func NewCompletedStoreFromDB(db DBStore) *CompletedStore {
	if pg, ok := db.(*PGStore); ok && pg != nil {
		return &CompletedStore{backend: pg.Completed}
	}
	return &CompletedStore{backend: &dbStoreCompletedBackend{db: db}}
}

// Get returns the terminal record for a given podUID, or nil if not found.
func (cs *CompletedStore) Get(podUID string) *CompletedPodRecord {
	if cs == nil || cs.backend == nil {
		return nil
	}
	return cs.backend.Get(podUID)
}

// Set commits a completed pod record to memory and disk or database.
func (cs *CompletedStore) Set(podUID string, rec *CompletedPodRecord) error {
	if cs == nil || cs.backend == nil || rec == nil {
		return nil
	}
	return cs.backend.Set(podUID, rec)
}

// Delete removes a completed pod record upon explicit /delete.
func (cs *CompletedStore) Delete(podUID string) error {
	if cs == nil || cs.backend == nil {
		return nil
	}
	return cs.backend.Delete(podUID)
}

// GetAll returns a copy of all completed pod records.
func (cs *CompletedStore) GetAll() map[string]*CompletedPodRecord {
	if cs == nil || cs.backend == nil {
		return nil
	}
	return cs.backend.GetAll()
}

// ----------------------------------------------------------------------------
// RegionStore
// ----------------------------------------------------------------------------

// RegionStore safely persists instance-to-region mappings locally on PVC storage or remotely in PostgreSQL.
type RegionStore struct {
	*diskMap[string]
	backend RegionStoreBackend
}

// NewRegionStore initializes a RegionStore with the given file path or connection string.
// If connStr is provided and non-empty (or DATABASE_URL is set), it uses PostgreSQL;
// otherwise it falls back to diskMap persistence.
func NewRegionStore(filePath string, connStr ...string) (*RegionStore, error) {
	targetConn := resolveConnStr(connStr...)
	if targetConn != "" {
		pg, err := getOrCreatePGStore(targetConn)
		if err != nil {
			return nil, err
		}
		return NewRegionStoreFromDB(pg), nil
	}
	dm, err := newDiskMap[string]("region store", filePath, "cloudvm-instance-regions.json", true, nil)
	return &RegionStore{
		backend: &diskMapRegionBackend{dm: dm},
		diskMap: dm,
	}, err
}

// NewRegionStoreFromDB initializes a RegionStore backed by a DBStore.
func NewRegionStoreFromDB(db DBStore) *RegionStore {
	if pg, ok := db.(*PGStore); ok && pg != nil {
		return &RegionStore{backend: pg.Regions}
	}
	return &RegionStore{backend: &dbStoreRegionBackend{db: db}}
}

// Get returns the cloud region for the specified instance ID, or empty string if not found.
func (rs *RegionStore) Get(instanceID string) string {
	if rs == nil || rs.backend == nil {
		return ""
	}
	return rs.backend.Get(instanceID)
}

// GetAll returns a thread-safe copy of all tracked instance-to-region mappings.
func (rs *RegionStore) GetAll() map[string]string {
	if rs == nil || rs.backend == nil {
		return nil
	}
	return rs.backend.GetAll()
}

// Set stores and persists the cloud region for the specified instance ID.
func (rs *RegionStore) Set(instanceID, region string) error {
	if rs == nil || rs.backend == nil || region == "" {
		return nil
	}
	return rs.backend.Set(instanceID, region)
}

// Delete removes and persists the deletion of the specified instance ID.
func (rs *RegionStore) Delete(instanceID string) error {
	if rs == nil || rs.backend == nil {
		return nil
	}
	return rs.backend.Delete(instanceID)
}

// ----------------------------------------------------------------------------
// TokenStore
// ----------------------------------------------------------------------------

// TokenStore safely persists in-VM runner authorization tokens locally on PVC storage or remotely in PostgreSQL.
type TokenStore struct {
	*diskMap[string]
	backend TokenStoreBackend
}

// NewTokenStore initializes a TokenStore with the given file path or connection string.
// If connStr is provided and non-empty (or DATABASE_URL is set), it uses PostgreSQL;
// otherwise it falls back to diskMap persistence.
func NewTokenStore(filePath string, connStr ...string) (*TokenStore, error) {
	targetConn := resolveConnStr(connStr...)
	if targetConn != "" {
		pg, err := getOrCreatePGStore(targetConn)
		if err != nil {
			return nil, err
		}
		return NewTokenStoreFromDB(pg), nil
	}
	dm, err := newDiskMap[string]("token store", filePath, "cloudvm-tokens.json", false, nil)
	return &TokenStore{
		backend: &diskMapTokenBackend{dm: dm},
		diskMap: dm,
	}, err
}

// NewTokenStoreFromDB initializes a TokenStore backed by a DBStore.
func NewTokenStoreFromDB(db DBStore) *TokenStore {
	if pg, ok := db.(*PGStore); ok && pg != nil {
		return &TokenStore{backend: pg.Tokens}
	}
	return &TokenStore{backend: &dbStoreTokenBackend{db: db}}
}

// Get returns the runner token for the specified instance ID, or empty string if not found.
func (ts *TokenStore) Get(instanceID string) string {
	if ts == nil || ts.backend == nil {
		return ""
	}
	return ts.backend.Get(instanceID)
}

// Set stores and persists the runner token for the specified instance ID.
func (ts *TokenStore) Set(instanceID, token string) error {
	if ts == nil || ts.backend == nil || token == "" {
		return nil
	}
	return ts.backend.Set(instanceID, token)
}

// Delete removes and persists the token deletion for the specified instance ID.
func (ts *TokenStore) Delete(instanceID string) error {
	if ts == nil || ts.backend == nil {
		return nil
	}
	return ts.backend.Delete(instanceID)
}

// GetAll returns all stored runner tokens mapped by instance ID.
func (ts *TokenStore) GetAll() map[string]string {
	if ts == nil || ts.backend == nil {
		return nil
	}
	return ts.backend.GetAll()
}

// ----------------------------------------------------------------------------
// TombstoneStore
// ----------------------------------------------------------------------------

// TombstoneStore safely persists explicitly deleted pod UIDs and instance IDs locally or remotely in PostgreSQL.
type TombstoneStore struct {
	*diskMap[time.Time]
	backend TombstoneStoreBackend
}

// NewTombstoneStore initializes a TombstoneStore with the given file path or connection string.
// If connStr is provided and non-empty (or DATABASE_URL is set), it uses PostgreSQL;
// otherwise it falls back to diskMap persistence.
func NewTombstoneStore(filePath string, connStr ...string) (*TombstoneStore, error) {
	targetConn := resolveConnStr(connStr...)
	if targetConn != "" {
		pg, err := getOrCreatePGStore(targetConn)
		if err != nil {
			return nil, err
		}
		return NewTombstoneStoreFromDB(pg), nil
	}
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
	return &TombstoneStore{
		backend: &diskMapTombstoneBackend{dm: dm},
		diskMap: dm,
	}, err
}

// NewTombstoneStoreFromDB initializes a TombstoneStore backed by a DBStore.
func NewTombstoneStoreFromDB(db DBStore) *TombstoneStore {
	if pg, ok := db.(*PGStore); ok && pg != nil {
		return &TombstoneStore{backend: pg.Tombstones}
	}
	return &TombstoneStore{backend: &dbStoreTombstoneBackend{db: db}}
}

// Has returns true if the specified ID is recorded in the tombstone store.
func (ts *TombstoneStore) Has(id string) bool {
	if ts == nil || ts.backend == nil {
		return false
	}
	return ts.backend.Has(id)
}

// Get returns the creation timestamp and presence flag for the given ID.
func (ts *TombstoneStore) Get(id string) (time.Time, bool) {
	if ts == nil || ts.backend == nil {
		return time.Time{}, false
	}
	return ts.backend.Get(id)
}

// Set commits a tombstone to memory and disk or database.
func (ts *TombstoneStore) Set(id string) error {
	if ts == nil || ts.backend == nil {
		return nil
	}
	return ts.backend.Set(id)
}

// Delete removes a tombstone from memory and disk or database.
func (ts *TombstoneStore) Delete(id string) error {
	if ts == nil || ts.backend == nil {
		return nil
	}
	return ts.backend.Delete(id)
}

// GetAll returns a thread-safe copy of all tombstones.
func (ts *TombstoneStore) GetAll() map[string]time.Time {
	if ts == nil || ts.backend == nil {
		return nil
	}
	return ts.backend.GetAll()
}
