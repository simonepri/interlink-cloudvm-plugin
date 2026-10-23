// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// DBStore defines the unified database store interface for all cloudvm state.
type DBStore interface {
	// Tokens
	GetToken(ctx context.Context, instanceID string) (string, error)
	SetToken(ctx context.Context, instanceID, token string) error
	DeleteToken(ctx context.Context, instanceID string) error
	GetAllTokens(ctx context.Context) (map[string]string, error)

	// Completed Pods
	GetCompletedPod(ctx context.Context, podUID string) (*CompletedPodRecord, error)
	SetCompletedPod(ctx context.Context, podUID string, rec *CompletedPodRecord) error
	DeleteCompletedPod(ctx context.Context, podUID string) error
	GetAllCompletedPods(ctx context.Context) (map[string]*CompletedPodRecord, error)

	// Instance Regions
	GetRegion(ctx context.Context, instanceID string) (string, error)
	SetRegion(ctx context.Context, instanceID, region string) error
	DeleteRegion(ctx context.Context, instanceID string) error
	GetAllRegions(ctx context.Context) (map[string]string, error)

	// Tombstones
	HasTombstone(ctx context.Context, id string) (bool, error)
	GetTombstone(ctx context.Context, id string) (time.Time, bool, error)
	SetTombstone(ctx context.Context, id string) error
	DeleteTombstone(ctx context.Context, id string) error
	GetAllTombstones(ctx context.Context) (map[string]time.Time, error)

	// Lifecycle
	Ping(ctx context.Context) error
	Close() error
}

var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS cloudvm_tokens (
		instance_id TEXT PRIMARY KEY,
		token TEXT,
		updated_at TIMESTAMPTZ
	)`,
	`CREATE TABLE IF NOT EXISTS cloudvm_completed_pods (
		pod_uid TEXT PRIMARY KEY,
		pod_jid TEXT,
		exit_code INT,
		reason TEXT,
		message TEXT,
		finished_at TIMESTAMPTZ
	)`,
	`CREATE TABLE IF NOT EXISTS cloudvm_instance_regions (
		instance_id TEXT PRIMARY KEY,
		region TEXT,
		updated_at TIMESTAMPTZ
	)`,
	`CREATE TABLE IF NOT EXISTS cloudvm_tombstones (
		id TEXT PRIMARY KEY,
		created_at TIMESTAMPTZ
	)`,
}

// PGStore implements DBStore using PostgreSQL via database/sql and pgx.
type PGStore struct {
	db *sql.DB

	Tokens     *PGTokenStore
	Completed  *PGCompletedStore
	Regions    *PGRegionStore
	Tombstones *PGTombstoneStore
}

// NewPGStore connects to PostgreSQL using the provided connection string,
// configures connection pooling, and initializes schema tables.
func NewPGStore(connStr string) (*PGStore, error) {
	return NewPGStoreWithContext(context.Background(), connStr)
}

// NewPGStoreWithContext connects to PostgreSQL using the provided context and connection string.
func NewPGStoreWithContext(ctx context.Context, connStr string) (*PGStore, error) {
	if connStr == "" {
		return nil, errors.New("empty connection string for postgres store")
	}

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres database: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(2 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	store := NewPGStoreWithDB(db)
	if err := store.InitSchema(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to initialize postgres schema: %w", err)
	}

	return store, nil
}

// NewPGStoreWithDB wraps an existing sql.DB into a PGStore without auto-migration.
func NewPGStoreWithDB(db *sql.DB) *PGStore {
	store := &PGStore{
		db: db,
	}
	store.Tokens = &PGTokenStore{pg: store}
	store.Completed = &PGCompletedStore{pg: store}
	store.Regions = &PGRegionStore{pg: store}
	store.Tombstones = &PGTombstoneStore{pg: store}
	return store
}

// InitSchema creates tables if they do not exist.
func (s *PGStore) InitSchema(ctx context.Context) error {
	for _, stmt := range schemaStatements {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("failed to execute schema initialization statement: %w", err)
		}
	}
	return nil
}

// DB returns the underlying sql.DB instance.
func (s *PGStore) DB() *sql.DB {
	return s.db
}

// Ping checks database connectivity.
func (s *PGStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close terminates the database connection pool.
func (s *PGStore) Close() error {
	return s.db.Close()
}

// TokenStore returns a TokenStore adapter backed by this PGStore.
func (s *PGStore) TokenStore() *TokenStore {
	return NewTokenStoreFromDB(s)
}

// CompletedStore returns a CompletedStore adapter backed by this PGStore.
func (s *PGStore) CompletedStore() *CompletedStore {
	return NewCompletedStoreFromDB(s)
}

// RegionStore returns a RegionStore adapter backed by this PGStore.
func (s *PGStore) RegionStore() *RegionStore {
	return NewRegionStoreFromDB(s)
}

// TombstoneStore returns a TombstoneStore adapter backed by this PGStore.
func (s *PGStore) TombstoneStore() *TombstoneStore {
	return NewTombstoneStoreFromDB(s)
}

// ----------------------------------------------------------------------------
// Tokens: GetToken, SetToken, DeleteToken, GetAllTokens
// ----------------------------------------------------------------------------

// GetToken retrieves the runner auth token for the given instance ID.
func (s *PGStore) GetToken(ctx context.Context, instanceID string) (string, error) {
	if instanceID == "" {
		return "", nil
	}
	query := "SELECT token FROM cloudvm_tokens WHERE instance_id = $1"
	var token sql.NullString
	err := s.db.QueryRowContext(ctx, query, instanceID).Scan(&token)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("failed to get token for instance %s: %w", instanceID, err)
	}
	return token.String, nil
}

// SetToken persists or updates the runner auth token for the given instance ID.
func (s *PGStore) SetToken(ctx context.Context, instanceID, token string) error {
	if instanceID == "" || token == "" {
		return nil
	}
	query := `
		INSERT INTO cloudvm_tokens (instance_id, token, updated_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (instance_id) DO UPDATE SET
			token = EXCLUDED.token,
			updated_at = EXCLUDED.updated_at
	`
	_, err := s.db.ExecContext(ctx, query, instanceID, token, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("failed to set token for instance %s: %w", instanceID, err)
	}
	return nil
}

// DeleteToken removes the runner auth token for the given instance ID.
func (s *PGStore) DeleteToken(ctx context.Context, instanceID string) error {
	if instanceID == "" {
		return nil
	}
	query := "DELETE FROM cloudvm_tokens WHERE instance_id = $1"
	_, err := s.db.ExecContext(ctx, query, instanceID)
	if err != nil {
		return fmt.Errorf("failed to delete token for instance %s: %w", instanceID, err)
	}
	return nil
}

// GetAllTokens returns all active runner auth tokens mapped by instance ID.
func (s *PGStore) GetAllTokens(ctx context.Context) (map[string]string, error) {
	query := "SELECT instance_id, COALESCE(token, '') FROM cloudvm_tokens"
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query all tokens: %w", err)
	}
	defer rows.Close()

	tokens := make(map[string]string)
	for rows.Next() {
		var instanceID, token string
		if err := rows.Scan(&instanceID, &token); err != nil {
			return nil, fmt.Errorf("failed to scan token row: %w", err)
		}
		tokens[instanceID] = token
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during tokens iteration: %w", err)
	}
	return tokens, nil
}

// ----------------------------------------------------------------------------
// Completed Pods: GetCompletedPod, SetCompletedPod, DeleteCompletedPod, GetAllCompletedPods
// ----------------------------------------------------------------------------

// GetCompletedPod retrieves terminal execution state for a completed pod.
func (s *PGStore) GetCompletedPod(ctx context.Context, podUID string) (*CompletedPodRecord, error) {
	if podUID == "" {
		return nil, nil
	}
	query := `
		SELECT pod_uid, COALESCE(pod_jid, ''), exit_code, COALESCE(reason, ''), COALESCE(message, ''), finished_at
		FROM cloudvm_completed_pods
		WHERE pod_uid = $1
	`
	var rec CompletedPodRecord
	var podJID, reason, message string
	var exitCode sql.NullInt32
	var finishedAt time.Time
	err := s.db.QueryRowContext(ctx, query, podUID).Scan(
		&rec.PodUID,
		&podJID,
		&exitCode,
		&reason,
		&message,
		&finishedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get completed pod for %s: %w", podUID, err)
	}
	rec.PodJID = podJID
	rec.ExitCode = exitCode.Int32
	rec.Reason = reason
	rec.Message = message
	rec.FinishedAt = finishedAt
	return &rec, nil
}

// SetCompletedPod saves or updates terminal execution state for a completed pod.
func (s *PGStore) SetCompletedPod(ctx context.Context, podUID string, rec *CompletedPodRecord) error {
	if podUID == "" || rec == nil {
		return nil
	}
	finishedAt := rec.FinishedAt
	if finishedAt.IsZero() {
		finishedAt = time.Now().UTC()
	}
	query := `
		INSERT INTO cloudvm_completed_pods (pod_uid, pod_jid, exit_code, reason, message, finished_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (pod_uid) DO UPDATE SET
			pod_jid = EXCLUDED.pod_jid,
			exit_code = EXCLUDED.exit_code,
			reason = EXCLUDED.reason,
			message = EXCLUDED.message,
			finished_at = EXCLUDED.finished_at
	`
	_, err := s.db.ExecContext(ctx, query, podUID, rec.PodJID, rec.ExitCode, rec.Reason, rec.Message, finishedAt)
	if err != nil {
		return fmt.Errorf("failed to set completed pod for %s: %w", podUID, err)
	}
	return nil
}

// DeleteCompletedPod removes terminal execution state for a pod.
func (s *PGStore) DeleteCompletedPod(ctx context.Context, podUID string) error {
	if podUID == "" {
		return nil
	}
	query := "DELETE FROM cloudvm_completed_pods WHERE pod_uid = $1"
	_, err := s.db.ExecContext(ctx, query, podUID)
	if err != nil {
		return fmt.Errorf("failed to delete completed pod for %s: %w", podUID, err)
	}
	return nil
}

// GetAllCompletedPods returns all completed pod records mapped by pod UID.
func (s *PGStore) GetAllCompletedPods(ctx context.Context) (map[string]*CompletedPodRecord, error) {
	query := `
		SELECT pod_uid, COALESCE(pod_jid, ''), exit_code, COALESCE(reason, ''), COALESCE(message, ''), finished_at
		FROM cloudvm_completed_pods
	`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query all completed pods: %w", err)
	}
	defer rows.Close()

	records := make(map[string]*CompletedPodRecord)
	for rows.Next() {
		var rec CompletedPodRecord
		var podJID, reason, message string
		var exitCode sql.NullInt32
		var finishedAt time.Time
		if err := rows.Scan(&rec.PodUID, &podJID, &exitCode, &reason, &message, &finishedAt); err != nil {
			return nil, fmt.Errorf("failed to scan completed pod row: %w", err)
		}
		rec.PodJID = podJID
		rec.ExitCode = exitCode.Int32
		rec.Reason = reason
		rec.Message = message
		rec.FinishedAt = finishedAt
		records[rec.PodUID] = &rec
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during completed pods iteration: %w", err)
	}
	return records, nil
}

// ----------------------------------------------------------------------------
// Instance Regions: GetRegion, SetRegion, DeleteRegion, GetAllRegions
// ----------------------------------------------------------------------------

// GetRegion returns the cloud region for the given instance ID.
func (s *PGStore) GetRegion(ctx context.Context, instanceID string) (string, error) {
	if instanceID == "" {
		return "", nil
	}
	query := "SELECT region FROM cloudvm_instance_regions WHERE instance_id = $1"
	var region sql.NullString
	err := s.db.QueryRowContext(ctx, query, instanceID).Scan(&region)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("failed to get region for instance %s: %w", instanceID, err)
	}
	return region.String, nil
}

// SetRegion persists or updates the cloud region for the given instance ID.
func (s *PGStore) SetRegion(ctx context.Context, instanceID, region string) error {
	if instanceID == "" || region == "" {
		return nil
	}
	query := `
		INSERT INTO cloudvm_instance_regions (instance_id, region, updated_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (instance_id) DO UPDATE SET
			region = EXCLUDED.region,
			updated_at = EXCLUDED.updated_at
	`
	_, err := s.db.ExecContext(ctx, query, instanceID, region, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("failed to set region for instance %s: %w", instanceID, err)
	}
	return nil
}

// DeleteRegion removes the cloud region record for the given instance ID.
func (s *PGStore) DeleteRegion(ctx context.Context, instanceID string) error {
	if instanceID == "" {
		return nil
	}
	query := "DELETE FROM cloudvm_instance_regions WHERE instance_id = $1"
	_, err := s.db.ExecContext(ctx, query, instanceID)
	if err != nil {
		return fmt.Errorf("failed to delete region for instance %s: %w", instanceID, err)
	}
	return nil
}

// GetAllRegions returns all instance-to-region mappings.
func (s *PGStore) GetAllRegions(ctx context.Context) (map[string]string, error) {
	query := "SELECT instance_id, COALESCE(region, '') FROM cloudvm_instance_regions"
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query all regions: %w", err)
	}
	defer rows.Close()

	regions := make(map[string]string)
	for rows.Next() {
		var instanceID, region string
		if err := rows.Scan(&instanceID, &region); err != nil {
			return nil, fmt.Errorf("failed to scan region row: %w", err)
		}
		regions[instanceID] = region
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during regions iteration: %w", err)
	}
	return regions, nil
}

// ----------------------------------------------------------------------------
// Tombstones: HasTombstone, GetTombstone, SetTombstone, DeleteTombstone, GetAllTombstones
// ----------------------------------------------------------------------------

// HasTombstone checks whether the given ID exists in the tombstone store.
func (s *PGStore) HasTombstone(ctx context.Context, id string) (bool, error) {
	if id == "" {
		return false, nil
	}
	query := "SELECT 1 FROM cloudvm_tombstones WHERE id = $1"
	var dummy int
	err := s.db.QueryRowContext(ctx, query, id).Scan(&dummy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check tombstone for id %s: %w", id, err)
	}
	return true, nil
}

// GetTombstone returns the creation timestamp and presence flag for the given ID.
func (s *PGStore) GetTombstone(ctx context.Context, id string) (time.Time, bool, error) {
	if id == "" {
		return time.Time{}, false, nil
	}
	query := "SELECT created_at FROM cloudvm_tombstones WHERE id = $1"
	var createdAt time.Time
	err := s.db.QueryRowContext(ctx, query, id).Scan(&createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, fmt.Errorf("failed to get tombstone for id %s: %w", id, err)
	}
	return createdAt, true, nil
}

// SetTombstone commits a tombstone record for the given ID.
func (s *PGStore) SetTombstone(ctx context.Context, id string) error {
	if id == "" {
		return nil
	}
	query := `
		INSERT INTO cloudvm_tombstones (id, created_at)
		VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE SET
			created_at = EXCLUDED.created_at
	`
	_, err := s.db.ExecContext(ctx, query, id, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("failed to set tombstone for id %s: %w", id, err)
	}
	return nil
}

// DeleteTombstone removes a tombstone record for the given ID.
func (s *PGStore) DeleteTombstone(ctx context.Context, id string) error {
	if id == "" {
		return nil
	}
	query := "DELETE FROM cloudvm_tombstones WHERE id = $1"
	_, err := s.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete tombstone for id %s: %w", id, err)
	}
	return nil
}

// GetAllTombstones returns all tombstones mapped by ID to creation timestamp.
func (s *PGStore) GetAllTombstones(ctx context.Context) (map[string]time.Time, error) {
	query := "SELECT id, created_at FROM cloudvm_tombstones"
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query all tombstones: %w", err)
	}
	defer rows.Close()

	tombstones := make(map[string]time.Time)
	for rows.Next() {
		var id string
		var createdAt time.Time
		if err := rows.Scan(&id, &createdAt); err != nil {
			return nil, fmt.Errorf("failed to scan tombstone row: %w", err)
		}
		tombstones[id] = createdAt
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during tombstones iteration: %w", err)
	}
	return tombstones, nil
}

// ----------------------------------------------------------------------------
// Sub-Store Adapters: PGTokenStore, PGCompletedStore, PGRegionStore, PGTombstoneStore
// ----------------------------------------------------------------------------

// PGTokenStore provides token store operations backed by PGStore.
type PGTokenStore struct {
	pg *PGStore
}

// Get returns the runner auth token for the given instance ID.
func (ts *PGTokenStore) Get(instanceID string) string {
	tok, err := ts.pg.GetToken(context.Background(), instanceID)
	if err != nil {
		log.Printf("[pg_store] GetToken failed for %s: %v", instanceID, err)
	}
	return tok
}

// Set persists the runner auth token for the given instance ID.
func (ts *PGTokenStore) Set(instanceID, token string) error {
	return ts.pg.SetToken(context.Background(), instanceID, token)
}

// Delete removes the runner auth token for the given instance ID.
func (ts *PGTokenStore) Delete(instanceID string) error {
	return ts.pg.DeleteToken(context.Background(), instanceID)
}

// GetAll returns all runner auth tokens.
func (ts *PGTokenStore) GetAll() map[string]string {
	tokens, err := ts.pg.GetAllTokens(context.Background())
	if err != nil {
		log.Printf("[pg_store] GetAllTokens failed: %v", err)
		return make(map[string]string)
	}
	return tokens
}

// PGCompletedStore provides completed pod store operations backed by PGStore.
type PGCompletedStore struct {
	pg *PGStore
}

// Get returns terminal pod execution record for the given pod UID.
func (cs *PGCompletedStore) Get(podUID string) *CompletedPodRecord {
	rec, err := cs.pg.GetCompletedPod(context.Background(), podUID)
	if err != nil {
		log.Printf("[pg_store] GetCompletedPod failed for %s: %v", podUID, err)
	}
	return rec
}

// Set commits a completed pod record.
func (cs *PGCompletedStore) Set(podUID string, rec *CompletedPodRecord) error {
	return cs.pg.SetCompletedPod(context.Background(), podUID, rec)
}

// Delete removes a completed pod record.
func (cs *PGCompletedStore) Delete(podUID string) error {
	return cs.pg.DeleteCompletedPod(context.Background(), podUID)
}

// GetAll returns all completed pod records.
func (cs *PGCompletedStore) GetAll() map[string]*CompletedPodRecord {
	records, err := cs.pg.GetAllCompletedPods(context.Background())
	if err != nil {
		log.Printf("[pg_store] GetAllCompletedPods failed: %v", err)
		return make(map[string]*CompletedPodRecord)
	}
	return records
}

// PGRegionStore provides instance-to-region mapping operations backed by PGStore.
type PGRegionStore struct {
	pg *PGStore
}

// Get returns the cloud region for the given instance ID.
func (rs *PGRegionStore) Get(instanceID string) string {
	region, err := rs.pg.GetRegion(context.Background(), instanceID)
	if err != nil {
		log.Printf("[pg_store] GetRegion failed for %s: %v", instanceID, err)
	}
	return region
}

// Set persists the cloud region for the given instance ID.
func (rs *PGRegionStore) Set(instanceID, region string) error {
	return rs.pg.SetRegion(context.Background(), instanceID, region)
}

// Delete removes the instance-to-region mapping for the given instance ID.
func (rs *PGRegionStore) Delete(instanceID string) error {
	return rs.pg.DeleteRegion(context.Background(), instanceID)
}

// GetAll returns all instance-to-region mappings.
func (rs *PGRegionStore) GetAll() map[string]string {
	regions, err := rs.pg.GetAllRegions(context.Background())
	if err != nil {
		log.Printf("[pg_store] GetAllRegions failed: %v", err)
		return make(map[string]string)
	}
	return regions
}

// PGTombstoneStore provides tombstone operations backed by PGStore.
type PGTombstoneStore struct {
	pg *PGStore
}

// Has checks whether the given ID is tombstoned.
func (ts *PGTombstoneStore) Has(id string) bool {
	exists, err := ts.pg.HasTombstone(context.Background(), id)
	if err != nil {
		log.Printf("[pg_store] HasTombstone failed for %s: %v", id, err)
	}
	return exists
}

// Get returns the creation timestamp and presence flag for the given ID.
func (ts *PGTombstoneStore) Get(id string) (time.Time, bool) {
	t, exists, err := ts.pg.GetTombstone(context.Background(), id)
	if err != nil {
		log.Printf("[pg_store] GetTombstone failed for %s: %v", id, err)
	}
	return t, exists
}

// Set records a tombstone for the given ID.
func (ts *PGTombstoneStore) Set(id string) error {
	return ts.pg.SetTombstone(context.Background(), id)
}

// Delete removes a tombstone record for the given ID.
func (ts *PGTombstoneStore) Delete(id string) error {
	return ts.pg.DeleteTombstone(context.Background(), id)
}

// GetAll returns all tombstones mapped by ID to creation timestamp.
func (ts *PGTombstoneStore) GetAll() map[string]time.Time {
	tombstones, err := ts.pg.GetAllTombstones(context.Background())
	if err != nil {
		log.Printf("[pg_store] GetAllTombstones failed: %v", err)
		return make(map[string]time.Time)
	}
	return tombstones
}
