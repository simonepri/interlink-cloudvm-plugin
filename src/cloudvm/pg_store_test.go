// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func setupMockPGStore(t *testing.T) (*PGStore, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	store := NewPGStoreWithDB(db)
	return store, mock
}

// ----------------------------------------------------------------------------
// Schema Initialization Tests
// ----------------------------------------------------------------------------

func TestPGStore_InitSchema_Success(t *testing.T) {
	store, mock := setupMockPGStore(t)
	ctx := context.Background()

	for _, stmt := range schemaStatements {
		mock.ExpectExec(regexp.QuoteMeta(stmt)).WillReturnResult(sqlmock.NewResult(0, 0))
	}

	if err := store.InitSchema(ctx); err != nil {
		t.Fatalf("expected InitSchema to succeed, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

func TestPGStore_InitSchema_Error(t *testing.T) {
	store, mock := setupMockPGStore(t)
	ctx := context.Background()

	mock.ExpectExec(regexp.QuoteMeta(schemaStatements[0])).
		WillReturnError(errors.New("db permission denied"))

	if err := store.InitSchema(ctx); err == nil {
		t.Fatal("expected InitSchema to fail on db error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

// ----------------------------------------------------------------------------
// Token Store Tests
// ----------------------------------------------------------------------------

func TestPGStore_Tokens(t *testing.T) {
	store, mock := setupMockPGStore(t)
	ctx := context.Background()

	// 1. GetToken with empty ID
	tok, err := store.GetToken(ctx, "")
	if err != nil || tok != "" {
		t.Fatalf("expected empty token without error for empty instanceID, got %q, %v", tok, err)
	}

	// 2. GetToken found
	queryGet := "SELECT token FROM cloudvm_tokens WHERE instance_id = $1"
	mock.ExpectQuery(regexp.QuoteMeta(queryGet)).
		WithArgs("inst-1").
		WillReturnRows(sqlmock.NewRows([]string{"token"}).AddRow("tok-abc"))

	tok, err = store.GetToken(ctx, "inst-1")
	if err != nil || tok != "tok-abc" {
		t.Fatalf("expected tok-abc, got %q, err: %v", tok, err)
	}

	// 3. GetToken not found (sql.ErrNoRows)
	mock.ExpectQuery(regexp.QuoteMeta(queryGet)).
		WithArgs("inst-missing").
		WillReturnError(sql.ErrNoRows)

	tok, err = store.GetToken(ctx, "inst-missing")
	if err != nil || tok != "" {
		t.Fatalf("expected empty string without error for not found, got %q, %v", tok, err)
	}

	// 4. GetToken error
	mock.ExpectQuery(regexp.QuoteMeta(queryGet)).
		WithArgs("inst-err").
		WillReturnError(errors.New("connection reset"))

	_, err = store.GetToken(ctx, "inst-err")
	if err == nil {
		t.Fatal("expected error from GetToken, got nil")
	}

	// 5. SetToken empty arguments
	if err := store.SetToken(ctx, "", "tok"); err != nil {
		t.Fatalf("expected nil for empty instanceID: %v", err)
	}
	if err := store.SetToken(ctx, "inst-1", ""); err != nil {
		t.Fatalf("expected nil for empty token: %v", err)
	}

	// 6. SetToken success
	querySet := `
		INSERT INTO cloudvm_tokens (instance_id, token, updated_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (instance_id) DO UPDATE SET
			token = EXCLUDED.token,
			updated_at = EXCLUDED.updated_at
	`
	mock.ExpectExec(regexp.QuoteMeta(querySet)).
		WithArgs("inst-1", "tok-xyz", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := store.SetToken(ctx, "inst-1", "tok-xyz"); err != nil {
		t.Fatalf("expected SetToken success, got: %v", err)
	}

	// 7. SetToken error
	mock.ExpectExec(regexp.QuoteMeta(querySet)).
		WithArgs("inst-fail", "tok-xyz", sqlmock.AnyArg()).
		WillReturnError(errors.New("disk full"))

	if err := store.SetToken(ctx, "inst-fail", "tok-xyz"); err == nil {
		t.Fatal("expected SetToken error, got nil")
	}

	// 8. DeleteToken empty ID
	if err := store.DeleteToken(ctx, ""); err != nil {
		t.Fatalf("expected nil for empty instanceID: %v", err)
	}

	// 9. DeleteToken success
	queryDel := "DELETE FROM cloudvm_tokens WHERE instance_id = $1"
	mock.ExpectExec(regexp.QuoteMeta(queryDel)).
		WithArgs("inst-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := store.DeleteToken(ctx, "inst-1"); err != nil {
		t.Fatalf("expected DeleteToken success, got: %v", err)
	}

	// 10. DeleteToken error
	mock.ExpectExec(regexp.QuoteMeta(queryDel)).
		WithArgs("inst-err").
		WillReturnError(errors.New("db error"))

	if err := store.DeleteToken(ctx, "inst-err"); err == nil {
		t.Fatal("expected DeleteToken error, got nil")
	}

	// 11. GetAllTokens success
	queryAll := "SELECT instance_id, COALESCE(token, '') FROM cloudvm_tokens"
	mock.ExpectQuery(regexp.QuoteMeta(queryAll)).
		WillReturnRows(sqlmock.NewRows([]string{"instance_id", "token"}).
			AddRow("inst-1", "tok-1").
			AddRow("inst-2", "tok-2"))

	all, err := store.GetAllTokens(ctx)
	if err != nil {
		t.Fatalf("expected GetAllTokens success, got: %v", err)
	}
	if len(all) != 2 || all["inst-1"] != "tok-1" || all["inst-2"] != "tok-2" {
		t.Fatalf("unexpected GetAllTokens result: %+v", all)
	}

	// 12. GetAllTokens error
	mock.ExpectQuery(regexp.QuoteMeta(queryAll)).
		WillReturnError(errors.New("table locked"))

	if _, err := store.GetAllTokens(ctx); err == nil {
		t.Fatal("expected GetAllTokens error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

// ----------------------------------------------------------------------------
// Completed Pods Store Tests
// ----------------------------------------------------------------------------

func TestPGStore_CompletedPods(t *testing.T) {
	store, mock := setupMockPGStore(t)
	ctx := context.Background()

	// 1. GetCompletedPod empty UID
	rec, err := store.GetCompletedPod(ctx, "")
	if err != nil || rec != nil {
		t.Fatalf("expected nil for empty podUID, got %v, %v", rec, err)
	}

	// 2. GetCompletedPod found
	queryGet := `
		SELECT pod_uid, COALESCE(pod_jid, ''), exit_code, COALESCE(reason, ''), COALESCE(message, ''), finished_at
		FROM cloudvm_completed_pods
		WHERE pod_uid = $1
	`
	now := time.Now().UTC().Truncate(time.Second)
	mock.ExpectQuery(regexp.QuoteMeta(queryGet)).
		WithArgs("pod-1").
		WillReturnRows(sqlmock.NewRows([]string{"pod_uid", "pod_jid", "exit_code", "reason", "message", "finished_at"}).
			AddRow("pod-1", "jid-1", 0, "Completed", "Success", now))

	rec, err = store.GetCompletedPod(ctx, "pod-1")
	if err != nil || rec == nil {
		t.Fatalf("expected completed pod record, got %v, err: %v", rec, err)
	}
	if rec.PodUID != "pod-1" || rec.PodJID != "jid-1" || rec.ExitCode != 0 || rec.Reason != "Completed" || rec.Message != "Success" || !rec.FinishedAt.Equal(now) {
		t.Fatalf("unexpected record contents: %+v", rec)
	}

	// 3. GetCompletedPod not found (sql.ErrNoRows)
	mock.ExpectQuery(regexp.QuoteMeta(queryGet)).
		WithArgs("pod-missing").
		WillReturnError(sql.ErrNoRows)

	rec, err = store.GetCompletedPod(ctx, "pod-missing")
	if err != nil || rec != nil {
		t.Fatalf("expected nil without error for missing pod, got %v, %v", rec, err)
	}

	// 4. GetCompletedPod error
	mock.ExpectQuery(regexp.QuoteMeta(queryGet)).
		WithArgs("pod-err").
		WillReturnError(errors.New("db failure"))

	if _, err := store.GetCompletedPod(ctx, "pod-err"); err == nil {
		t.Fatal("expected GetCompletedPod error, got nil")
	}

	// 5. SetCompletedPod empty arguments
	if err := store.SetCompletedPod(ctx, "", &CompletedPodRecord{}); err != nil {
		t.Fatalf("expected nil for empty podUID: %v", err)
	}
	if err := store.SetCompletedPod(ctx, "pod-1", nil); err != nil {
		t.Fatalf("expected nil for nil record: %v", err)
	}

	// 6. SetCompletedPod success with zero FinishedAt
	querySet := `
		INSERT INTO cloudvm_completed_pods (pod_uid, pod_jid, exit_code, reason, message, finished_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (pod_uid) DO UPDATE SET
			pod_jid = EXCLUDED.pod_jid,
			exit_code = EXCLUDED.exit_code,
			reason = EXCLUDED.reason,
			message = EXCLUDED.message,
			finished_at = EXCLUDED.finished_at
	`
	mock.ExpectExec(regexp.QuoteMeta(querySet)).
		WithArgs("pod-1", "jid-1", int32(1), "Error", "Failed", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = store.SetCompletedPod(ctx, "pod-1", &CompletedPodRecord{
		PodUID:   "pod-1",
		PodJID:   "jid-1",
		ExitCode: 1,
		Reason:   "Error",
		Message:  "Failed",
	})
	if err != nil {
		t.Fatalf("expected SetCompletedPod success, got: %v", err)
	}

	// 7. SetCompletedPod error
	mock.ExpectExec(regexp.QuoteMeta(querySet)).
		WithArgs("pod-1", "jid-1", int32(0), "", "", sqlmock.AnyArg()).
		WillReturnError(errors.New("constraint violation"))

	err = store.SetCompletedPod(ctx, "pod-1", &CompletedPodRecord{
		PodUID: "pod-1",
		PodJID: "jid-1",
	})
	if err == nil {
		t.Fatal("expected SetCompletedPod error, got nil")
	}

	// 8. DeleteCompletedPod empty UID
	if err := store.DeleteCompletedPod(ctx, ""); err != nil {
		t.Fatalf("expected nil for empty podUID: %v", err)
	}

	// 9. DeleteCompletedPod success
	queryDel := "DELETE FROM cloudvm_completed_pods WHERE pod_uid = $1"
	mock.ExpectExec(regexp.QuoteMeta(queryDel)).
		WithArgs("pod-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := store.DeleteCompletedPod(ctx, "pod-1"); err != nil {
		t.Fatalf("expected DeleteCompletedPod success, got: %v", err)
	}

	// 10. DeleteCompletedPod error
	mock.ExpectExec(regexp.QuoteMeta(queryDel)).
		WithArgs("pod-err").
		WillReturnError(errors.New("db error"))

	if err := store.DeleteCompletedPod(ctx, "pod-err"); err == nil {
		t.Fatal("expected DeleteCompletedPod error, got nil")
	}

	// 11. GetAllCompletedPods success
	queryAll := `
		SELECT pod_uid, COALESCE(pod_jid, ''), exit_code, COALESCE(reason, ''), COALESCE(message, ''), finished_at
		FROM cloudvm_completed_pods
	`
	mock.ExpectQuery(regexp.QuoteMeta(queryAll)).
		WillReturnRows(sqlmock.NewRows([]string{"pod_uid", "pod_jid", "exit_code", "reason", "message", "finished_at"}).
			AddRow("pod-1", "jid-1", 0, "Completed", "ok", now).
			AddRow("pod-2", "jid-2", 137, "OOMKilled", "killed", now))

	all, err := store.GetAllCompletedPods(ctx)
	if err != nil {
		t.Fatalf("expected GetAllCompletedPods success, got: %v", err)
	}
	if len(all) != 2 || all["pod-1"].ExitCode != 0 || all["pod-2"].ExitCode != 137 {
		t.Fatalf("unexpected GetAllCompletedPods result: %+v", all)
	}

	// 12. GetAllCompletedPods error
	mock.ExpectQuery(regexp.QuoteMeta(queryAll)).
		WillReturnError(errors.New("query failed"))

	if _, err := store.GetAllCompletedPods(ctx); err == nil {
		t.Fatal("expected GetAllCompletedPods error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

// ----------------------------------------------------------------------------
// Region Store Tests
// ----------------------------------------------------------------------------

func TestPGStore_Regions(t *testing.T) {
	store, mock := setupMockPGStore(t)
	ctx := context.Background()

	// 1. GetRegion empty instanceID
	reg, err := store.GetRegion(ctx, "")
	if err != nil || reg != "" {
		t.Fatalf("expected empty string for empty instanceID, got %q, %v", reg, err)
	}

	// 2. GetRegion found
	queryGet := "SELECT region FROM cloudvm_instance_regions WHERE instance_id = $1"
	mock.ExpectQuery(regexp.QuoteMeta(queryGet)).
		WithArgs("i-aws-1").
		WillReturnRows(sqlmock.NewRows([]string{"region"}).AddRow("eu-west-1"))

	reg, err = store.GetRegion(ctx, "i-aws-1")
	if err != nil || reg != "eu-west-1" {
		t.Fatalf("expected eu-west-1, got %q, err: %v", reg, err)
	}

	// 3. GetRegion not found (sql.ErrNoRows)
	mock.ExpectQuery(regexp.QuoteMeta(queryGet)).
		WithArgs("i-missing").
		WillReturnError(sql.ErrNoRows)

	reg, err = store.GetRegion(ctx, "i-missing")
	if err != nil || reg != "" {
		t.Fatalf("expected empty string without error for missing region, got %q, %v", reg, err)
	}

	// 4. GetRegion error
	mock.ExpectQuery(regexp.QuoteMeta(queryGet)).
		WithArgs("i-err").
		WillReturnError(errors.New("db error"))

	if _, err := store.GetRegion(ctx, "i-err"); err == nil {
		t.Fatal("expected GetRegion error, got nil")
	}

	// 5. SetRegion empty arguments
	if err := store.SetRegion(ctx, "", "us-east-1"); err != nil {
		t.Fatalf("expected nil for empty instanceID: %v", err)
	}
	if err := store.SetRegion(ctx, "i-1", ""); err != nil {
		t.Fatalf("expected nil for empty region: %v", err)
	}

	// 6. SetRegion success
	querySet := `
		INSERT INTO cloudvm_instance_regions (instance_id, region, updated_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (instance_id) DO UPDATE SET
			region = EXCLUDED.region,
			updated_at = EXCLUDED.updated_at
	`
	mock.ExpectExec(regexp.QuoteMeta(querySet)).
		WithArgs("i-1", "us-east-1", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := store.SetRegion(ctx, "i-1", "us-east-1"); err != nil {
		t.Fatalf("expected SetRegion success, got: %v", err)
	}

	// 7. SetRegion error
	mock.ExpectExec(regexp.QuoteMeta(querySet)).
		WithArgs("i-fail", "us-east-1", sqlmock.AnyArg()).
		WillReturnError(errors.New("write timeout"))

	if err := store.SetRegion(ctx, "i-fail", "us-east-1"); err == nil {
		t.Fatal("expected SetRegion error, got nil")
	}

	// 8. DeleteRegion empty instanceID
	if err := store.DeleteRegion(ctx, ""); err != nil {
		t.Fatalf("expected nil for empty instanceID: %v", err)
	}

	// 9. DeleteRegion success
	queryDel := "DELETE FROM cloudvm_instance_regions WHERE instance_id = $1"
	mock.ExpectExec(regexp.QuoteMeta(queryDel)).
		WithArgs("i-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := store.DeleteRegion(ctx, "i-1"); err != nil {
		t.Fatalf("expected DeleteRegion success, got: %v", err)
	}

	// 10. DeleteRegion error
	mock.ExpectExec(regexp.QuoteMeta(queryDel)).
		WithArgs("i-err").
		WillReturnError(errors.New("db error"))

	if err := store.DeleteRegion(ctx, "i-err"); err == nil {
		t.Fatal("expected DeleteRegion error, got nil")
	}

	// 11. GetAllRegions success
	queryAll := "SELECT instance_id, COALESCE(region, '') FROM cloudvm_instance_regions"
	mock.ExpectQuery(regexp.QuoteMeta(queryAll)).
		WillReturnRows(sqlmock.NewRows([]string{"instance_id", "region"}).
			AddRow("i-1", "us-east-1").
			AddRow("i-2", "us-west-2"))

	all, err := store.GetAllRegions(ctx)
	if err != nil {
		t.Fatalf("expected GetAllRegions success, got: %v", err)
	}
	if len(all) != 2 || all["i-1"] != "us-east-1" || all["i-2"] != "us-west-2" {
		t.Fatalf("unexpected GetAllRegions result: %+v", all)
	}

	// 12. GetAllRegions error
	mock.ExpectQuery(regexp.QuoteMeta(queryAll)).
		WillReturnError(errors.New("db error"))

	if _, err := store.GetAllRegions(ctx); err == nil {
		t.Fatal("expected GetAllRegions error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

// ----------------------------------------------------------------------------
// Tombstones Store Tests
// ----------------------------------------------------------------------------

func TestPGStore_Tombstones(t *testing.T) {
	store, mock := setupMockPGStore(t)
	ctx := context.Background()

	// 1. HasTombstone empty ID
	has, err := store.HasTombstone(ctx, "")
	if err != nil || has {
		t.Fatalf("expected false without error for empty id, got %v, %v", has, err)
	}

	// 2. HasTombstone found
	queryHas := "SELECT 1 FROM cloudvm_tombstones WHERE id = $1"
	mock.ExpectQuery(regexp.QuoteMeta(queryHas)).
		WithArgs("tomb-1").
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))

	has, err = store.HasTombstone(ctx, "tomb-1")
	if err != nil || !has {
		t.Fatalf("expected true for tomb-1, got %v, err: %v", has, err)
	}

	// 3. HasTombstone not found (sql.ErrNoRows)
	mock.ExpectQuery(regexp.QuoteMeta(queryHas)).
		WithArgs("tomb-missing").
		WillReturnError(sql.ErrNoRows)

	has, err = store.HasTombstone(ctx, "tomb-missing")
	if err != nil || has {
		t.Fatalf("expected false without error for missing tombstone, got %v, %v", has, err)
	}

	// 4. HasTombstone error
	mock.ExpectQuery(regexp.QuoteMeta(queryHas)).
		WithArgs("tomb-err").
		WillReturnError(errors.New("db failure"))

	if _, err := store.HasTombstone(ctx, "tomb-err"); err == nil {
		t.Fatal("expected HasTombstone error, got nil")
	}

	// 5. GetTombstone empty ID
	tsTime, found, err := store.GetTombstone(ctx, "")
	if err != nil || found || !tsTime.IsZero() {
		t.Fatalf("expected zero time and false for empty id: %v, %v, %v", tsTime, found, err)
	}

	// 6. GetTombstone found
	now := time.Now().UTC().Truncate(time.Second)
	queryGet := "SELECT created_at FROM cloudvm_tombstones WHERE id = $1"
	mock.ExpectQuery(regexp.QuoteMeta(queryGet)).
		WithArgs("tomb-1").
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(now))

	tsTime, found, err = store.GetTombstone(ctx, "tomb-1")
	if err != nil || !found || !tsTime.Equal(now) {
		t.Fatalf("expected found %v, got %v, %v, %v", now, tsTime, found, err)
	}

	// 7. GetTombstone not found
	mock.ExpectQuery(regexp.QuoteMeta(queryGet)).
		WithArgs("tomb-missing").
		WillReturnError(sql.ErrNoRows)

	_, found, err = store.GetTombstone(ctx, "tomb-missing")
	if err != nil || found {
		t.Fatalf("expected not found without error, got found=%v, err=%v", found, err)
	}

	// 8. GetTombstone error
	mock.ExpectQuery(regexp.QuoteMeta(queryGet)).
		WithArgs("tomb-err").
		WillReturnError(errors.New("db error"))

	if _, _, err := store.GetTombstone(ctx, "tomb-err"); err == nil {
		t.Fatal("expected GetTombstone error, got nil")
	}

	// 9. SetTombstone empty ID
	if err := store.SetTombstone(ctx, ""); err != nil {
		t.Fatalf("expected nil for empty id: %v", err)
	}

	// 10. SetTombstone success
	querySet := `
		INSERT INTO cloudvm_tombstones (id, created_at)
		VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE SET
			created_at = EXCLUDED.created_at
	`
	mock.ExpectExec(regexp.QuoteMeta(querySet)).
		WithArgs("tomb-1", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := store.SetTombstone(ctx, "tomb-1"); err != nil {
		t.Fatalf("expected SetTombstone success, got: %v", err)
	}

	// 11. SetTombstone error
	mock.ExpectExec(regexp.QuoteMeta(querySet)).
		WithArgs("tomb-fail", sqlmock.AnyArg()).
		WillReturnError(errors.New("db full"))

	if err := store.SetTombstone(ctx, "tomb-fail"); err == nil {
		t.Fatal("expected SetTombstone error, got nil")
	}

	// 12. DeleteTombstone empty ID
	if err := store.DeleteTombstone(ctx, ""); err != nil {
		t.Fatalf("expected nil for empty id: %v", err)
	}

	// 13. DeleteTombstone success
	queryDel := "DELETE FROM cloudvm_tombstones WHERE id = $1"
	mock.ExpectExec(regexp.QuoteMeta(queryDel)).
		WithArgs("tomb-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := store.DeleteTombstone(ctx, "tomb-1"); err != nil {
		t.Fatalf("expected DeleteTombstone success, got: %v", err)
	}

	// 14. DeleteTombstone error
	mock.ExpectExec(regexp.QuoteMeta(queryDel)).
		WithArgs("tomb-err").
		WillReturnError(errors.New("db error"))

	if err := store.DeleteTombstone(ctx, "tomb-err"); err == nil {
		t.Fatal("expected DeleteTombstone error, got nil")
	}

	// 15. GetAllTombstones success
	queryAll := "SELECT id, created_at FROM cloudvm_tombstones"
	mock.ExpectQuery(regexp.QuoteMeta(queryAll)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "created_at"}).
			AddRow("tomb-1", now).
			AddRow("tomb-2", now))

	all, err := store.GetAllTombstones(ctx)
	if err != nil {
		t.Fatalf("expected GetAllTombstones success, got: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 tombstones, got %d", len(all))
	}

	// 16. GetAllTombstones error
	mock.ExpectQuery(regexp.QuoteMeta(queryAll)).
		WillReturnError(errors.New("db error"))

	if _, err := store.GetAllTombstones(ctx); err == nil {
		t.Fatal("expected GetAllTombstones error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

// ----------------------------------------------------------------------------
// SubStore and HighLevel Wrapper Tests
// ----------------------------------------------------------------------------

func TestPGStore_SubStoresAndAdapters(t *testing.T) {
	store, mock := setupMockPGStore(t)
	now := time.Now().UTC().Truncate(time.Second)

	// Tokens SubStore
	mock.ExpectQuery(regexp.QuoteMeta("SELECT token FROM cloudvm_tokens WHERE instance_id = $1")).
		WithArgs("inst-1").
		WillReturnRows(sqlmock.NewRows([]string{"token"}).AddRow("tok-sub"))
	if tok := store.Tokens.Get("inst-1"); tok != "tok-sub" {
		t.Fatalf("expected tok-sub, got %q", tok)
	}

	mock.ExpectExec(regexp.QuoteMeta(`
		INSERT INTO cloudvm_tokens (instance_id, token, updated_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (instance_id) DO UPDATE SET
			token = EXCLUDED.token,
			updated_at = EXCLUDED.updated_at
	`)).WithArgs("inst-1", "tok-sub", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := store.Tokens.Set("inst-1", "tok-sub"); err != nil {
		t.Fatalf("unexpected set error: %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM cloudvm_tokens WHERE instance_id = $1")).
		WithArgs("inst-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.Tokens.Delete("inst-1"); err != nil {
		t.Fatalf("unexpected delete error: %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT instance_id, COALESCE(token, '') FROM cloudvm_tokens")).
		WillReturnRows(sqlmock.NewRows([]string{"instance_id", "token"}).AddRow("inst-1", "tok-sub"))
	if all := store.Tokens.GetAll(); len(all) != 1 || all["inst-1"] != "tok-sub" {
		t.Fatalf("unexpected GetAll tokens: %+v", all)
	}

	// Completed Pods SubStore
	queryGetComp := `
		SELECT pod_uid, COALESCE(pod_jid, ''), exit_code, COALESCE(reason, ''), COALESCE(message, ''), finished_at
		FROM cloudvm_completed_pods
		WHERE pod_uid = $1
	`
	mock.ExpectQuery(regexp.QuoteMeta(queryGetComp)).
		WithArgs("pod-sub").
		WillReturnRows(sqlmock.NewRows([]string{"pod_uid", "pod_jid", "exit_code", "reason", "message", "finished_at"}).
			AddRow("pod-sub", "jid-sub", 0, "Completed", "ok", now))
	if rec := store.Completed.Get("pod-sub"); rec == nil || rec.PodUID != "pod-sub" {
		t.Fatalf("unexpected completed pod: %+v", rec)
	}

	querySetComp := `
		INSERT INTO cloudvm_completed_pods (pod_uid, pod_jid, exit_code, reason, message, finished_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (pod_uid) DO UPDATE SET
			pod_jid = EXCLUDED.pod_jid,
			exit_code = EXCLUDED.exit_code,
			reason = EXCLUDED.reason,
			message = EXCLUDED.message,
			finished_at = EXCLUDED.finished_at
	`
	mock.ExpectExec(regexp.QuoteMeta(querySetComp)).
		WithArgs("pod-sub", "jid-sub", int32(0), "Completed", "ok", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := store.Completed.Set("pod-sub", &CompletedPodRecord{PodUID: "pod-sub", PodJID: "jid-sub", Reason: "Completed", Message: "ok"}); err != nil {
		t.Fatalf("unexpected set completed error: %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM cloudvm_completed_pods WHERE pod_uid = $1")).
		WithArgs("pod-sub").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.Completed.Delete("pod-sub"); err != nil {
		t.Fatalf("unexpected delete completed error: %v", err)
	}

	queryAllComp := `
		SELECT pod_uid, COALESCE(pod_jid, ''), exit_code, COALESCE(reason, ''), COALESCE(message, ''), finished_at
		FROM cloudvm_completed_pods
	`
	mock.ExpectQuery(regexp.QuoteMeta(queryAllComp)).
		WillReturnRows(sqlmock.NewRows([]string{"pod_uid", "pod_jid", "exit_code", "reason", "message", "finished_at"}).
			AddRow("pod-sub", "jid-sub", 0, "Completed", "ok", now))
	if all := store.Completed.GetAll(); len(all) != 1 || all["pod-sub"].PodUID != "pod-sub" {
		t.Fatalf("unexpected GetAll completed pods: %+v", all)
	}

	// Regions SubStore
	mock.ExpectQuery(regexp.QuoteMeta("SELECT region FROM cloudvm_instance_regions WHERE instance_id = $1")).
		WithArgs("i-sub").
		WillReturnRows(sqlmock.NewRows([]string{"region"}).AddRow("ap-southeast-1"))
	if reg := store.Regions.Get("i-sub"); reg != "ap-southeast-1" {
		t.Fatalf("expected ap-southeast-1, got %q", reg)
	}

	querySetReg := `
		INSERT INTO cloudvm_instance_regions (instance_id, region, updated_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (instance_id) DO UPDATE SET
			region = EXCLUDED.region,
			updated_at = EXCLUDED.updated_at
	`
	mock.ExpectExec(regexp.QuoteMeta(querySetReg)).
		WithArgs("i-sub", "ap-southeast-1", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := store.Regions.Set("i-sub", "ap-southeast-1"); err != nil {
		t.Fatalf("unexpected set region error: %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM cloudvm_instance_regions WHERE instance_id = $1")).
		WithArgs("i-sub").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.Regions.Delete("i-sub"); err != nil {
		t.Fatalf("unexpected delete region error: %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT instance_id, COALESCE(region, '') FROM cloudvm_instance_regions")).
		WillReturnRows(sqlmock.NewRows([]string{"instance_id", "region"}).AddRow("i-sub", "ap-southeast-1"))
	if all := store.Regions.GetAll(); len(all) != 1 || all["i-sub"] != "ap-southeast-1" {
		t.Fatalf("unexpected GetAll regions: %+v", all)
	}

	// Tombstones SubStore
	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM cloudvm_tombstones WHERE id = $1")).
		WithArgs("tomb-sub").
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	if !store.Tombstones.Has("tomb-sub") {
		t.Fatal("expected Has true")
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT created_at FROM cloudvm_tombstones WHERE id = $1")).
		WithArgs("tomb-sub").
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(now))
	if gotT, ok := store.Tombstones.Get("tomb-sub"); !ok || !gotT.Equal(now) {
		t.Fatalf("unexpected tombstone: %v, %v", gotT, ok)
	}

	querySetTomb := `
		INSERT INTO cloudvm_tombstones (id, created_at)
		VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE SET
			created_at = EXCLUDED.created_at
	`
	mock.ExpectExec(regexp.QuoteMeta(querySetTomb)).
		WithArgs("tomb-sub", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := store.Tombstones.Set("tomb-sub"); err != nil {
		t.Fatalf("unexpected set tombstone error: %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM cloudvm_tombstones WHERE id = $1")).
		WithArgs("tomb-sub").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.Tombstones.Delete("tomb-sub"); err != nil {
		t.Fatalf("unexpected delete tombstone error: %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, created_at FROM cloudvm_tombstones")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "created_at"}).AddRow("tomb-sub", now))
	if all := store.Tombstones.GetAll(); len(all) != 1 || !all["tomb-sub"].Equal(now) {
		t.Fatalf("unexpected GetAll tombstones: %+v", all)
	}

	// High Level Store factory methods
	tokStore := store.TokenStore()
	if tokStore == nil {
		t.Fatal("expected TokenStore adapter")
	}
	compStore := store.CompletedStore()
	if compStore == nil {
		t.Fatal("expected CompletedStore adapter")
	}
	regStore := store.RegionStore()
	if regStore == nil {
		t.Fatal("expected RegionStore adapter")
	}
	tombStore := store.TombstoneStore()
	if tombStore == nil {
		t.Fatal("expected TombstoneStore adapter")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

// ----------------------------------------------------------------------------
// Lifecycle & Constructor Tests
// ----------------------------------------------------------------------------

func TestPGStore_LifecycleAndErrors(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	store := NewPGStoreWithDB(db)
	ctx := context.Background()

	// Ping success
	mock.ExpectPing()
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("expected ping success: %v", err)
	}

	// Ping error
	mock.ExpectPing().WillReturnError(errors.New("db unreachable"))
	if err := store.Ping(ctx); err == nil {
		t.Fatal("expected ping error, got nil")
	}

	// DB getter
	if store.DB() == nil {
		t.Fatal("expected non-nil sql.DB")
	}

	// Close
	mock.ExpectClose()
	if err := store.Close(); err != nil {
		t.Fatalf("expected close success: %v", err)
	}

	// Empty connection string error
	if _, err := NewPGStore(""); err == nil {
		t.Fatal("expected error with empty connection string, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

// ----------------------------------------------------------------------------
// Store Fallback to diskMap When Empty Connection String
// ----------------------------------------------------------------------------

func TestStore_DiskMapFallbackWhenEmptyConn(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. CompletedStore fallback
	csPath := filepath.Join(tmpDir, "comp.json")
	cs, err := NewCompletedStore(csPath, "")
	if err != nil {
		t.Fatalf("failed to create completed store: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := cs.Set("pod-local", &CompletedPodRecord{PodUID: "pod-local", ExitCode: 0, FinishedAt: now}); err != nil {
		t.Fatalf("failed to set completed pod locally: %v", err)
	}
	if rec := cs.Get("pod-local"); rec == nil || rec.PodUID != "pod-local" {
		t.Fatalf("unexpected record: %+v", rec)
	}
	if all := cs.GetAll(); len(all) != 1 {
		t.Fatalf("expected 1 record in GetAll, got %d", len(all))
	}
	if err := cs.Delete("pod-local"); err != nil {
		t.Fatalf("failed to delete completed record locally: %v", err)
	}
	if rec := cs.Get("pod-local"); rec != nil {
		t.Fatalf("expected nil after delete, got %+v", rec)
	}

	// 2. RegionStore fallback
	rsPath := filepath.Join(tmpDir, "region.json")
	rs, err := NewRegionStore(rsPath, "")
	if err != nil {
		t.Fatalf("failed to create region store: %v", err)
	}
	if err := rs.Set("inst-local", "us-west-1"); err != nil {
		t.Fatalf("failed to set region locally: %v", err)
	}
	if reg := rs.Get("inst-local"); reg != "us-west-1" {
		t.Fatalf("expected us-west-1, got %s", reg)
	}
	if all := rs.GetAll(); len(all) != 1 || all["inst-local"] != "us-west-1" {
		t.Fatalf("unexpected GetAll regions: %+v", all)
	}
	if err := rs.Delete("inst-local"); err != nil {
		t.Fatalf("failed to delete region locally: %v", err)
	}
	if reg := rs.Get("inst-local"); reg != "" {
		t.Fatalf("expected empty after delete, got %s", reg)
	}

	// 3. TokenStore fallback
	tsPath := filepath.Join(tmpDir, "tokens.json")
	ts, err := NewTokenStore(tsPath, "")
	if err != nil {
		t.Fatalf("failed to create token store: %v", err)
	}
	if err := ts.Set("inst-local", "tok-local-secret"); err != nil {
		t.Fatalf("failed to set token locally: %v", err)
	}
	if tok := ts.Get("inst-local"); tok != "tok-local-secret" {
		t.Fatalf("expected tok-local-secret, got %s", tok)
	}
	if all := ts.GetAll(); len(all) != 1 || all["inst-local"] != "tok-local-secret" {
		t.Fatalf("unexpected GetAll tokens: %+v", all)
	}
	if err := ts.Delete("inst-local"); err != nil {
		t.Fatalf("failed to delete token locally: %v", err)
	}
	if tok := ts.Get("inst-local"); tok != "" {
		t.Fatalf("expected empty after delete, got %s", tok)
	}

	// 4. TombstoneStore fallback
	tombPath := filepath.Join(tmpDir, "tombstones.json")
	tomb, err := NewTombstoneStore(tombPath, "")
	if err != nil {
		t.Fatalf("failed to create tombstone store: %v", err)
	}
	if err := tomb.Set("id-local"); err != nil {
		t.Fatalf("failed to set tombstone locally: %v", err)
	}
	if !tomb.Has("id-local") {
		t.Fatal("expected Has id-local to be true")
	}
	if _, ok := tomb.Get("id-local"); !ok {
		t.Fatal("expected Get id-local to be ok")
	}
	if all := tomb.GetAll(); len(all) != 1 {
		t.Fatalf("expected 1 tombstone, got %d", len(all))
	}
	if err := tomb.Delete("id-local"); err != nil {
		t.Fatalf("failed to delete tombstone locally: %v", err)
	}
	if tomb.Has("id-local") {
		t.Fatal("expected Has id-local to be false after delete")
	}

	// ResetGlobalPGStores
	ResetGlobalPGStores()
}
