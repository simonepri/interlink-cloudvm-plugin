// Tests for the pod watch and the reconciler's termination of VMs whose pod no longer exists.
package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func kubePod(ns, name, uid string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(uid)}}
}

type orphanFixture struct {
	srv    *Server
	rec    *Reconciler
	instID string
}

func newOrphanFixture(t *testing.T, uid string) *orphanFixture {
	t.Helper()
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))
	t.Setenv("TOMBSTONE_STORE_PATH", filepath.Join(t.TempDir(), "tombstones.json"))
	mock := NewMockProvider()
	srv := NewServer(mock, "test-cluster")
	podJSON := `{"metadata":{"name":"worker","namespace":"jobs","uid":"` + uid + `"},"spec":{"containers":[{"name":"c1","resources":{"limits":{"cpu":"2","memory":"4Gi"}}}]}}`
	rec := httptest.NewRecorder()
	srv.handleCreate(rec, httptest.NewRequest(http.MethodPost, "/create", bytes.NewBufferString(podJSON)))
	if rec.Code != http.StatusOK {
		t.Fatalf("create failed: %d %s", rec.Code, rec.Body.String())
	}
	active, _ := srv.GetActivePods(context.Background())
	instID := active[uid]
	if instID == "" {
		t.Fatalf("pod %s not active: %v", uid, active)
	}
	r := NewReconciler(mock, srv, mock, "test-cluster", time.Hour)
	r.SetLaunchGracePeriod(0)
	return &orphanFixture{srv: srv, rec: r, instID: instID}
}

func startPodWatch(t *testing.T, pods ...*corev1.Pod) *PodWatch {
	t.Helper()
	cs := fake.NewSimpleClientset()
	for _, p := range pods {
		if _, err := cs.CoreV1().Pods(p.Namespace).Create(context.Background(), p, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	pw := NewPodWatch(cs)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	pw.Start(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for !pw.HasSynced() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !pw.HasSynced() {
		t.Fatal("pod watch did not sync")
	}
	return pw
}

func instanceAlive(t *testing.T, f *orphanFixture) bool {
	t.Helper()
	insts, err := f.rec.inventory.ListActiveInstances(context.Background(), "test-cluster")
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range insts {
		if i.ID == f.instID {
			return true
		}
	}
	return false
}

func TestReconciler_LostDeleteTerminatesVM(t *testing.T) {
	f := newOrphanFixture(t, "uid-lost")
	f.rec.SetPodWatch(startPodWatch(t))
	var terminated []string
	f.rec.SetOnTerminated(func(id string) { terminated = append(terminated, id) })

	res, err := f.rec.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if instanceAlive(t, f) {
		t.Fatalf("VM %s still running; result %+v", f.instID, res)
	}
	if active, _ := f.srv.GetActivePods(context.Background()); active["uid-lost"] != "" {
		t.Fatalf("pod still tracked: %v", active)
	}
	f.srv.mu.RLock()
	tombstoned := f.srv.isTombstonedLocked("uid-lost")
	f.srv.mu.RUnlock()
	if !tombstoned {
		t.Fatal("pod was not tombstoned")
	}
}

func TestReconciler_ExistingPodKeepsVM(t *testing.T) {
	f := newOrphanFixture(t, "uid-present")
	f.rec.SetPodWatch(startPodWatch(t, kubePod("jobs", "worker", "uid-present")))
	if _, err := f.rec.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !instanceAlive(t, f) {
		t.Fatal("VM of an existing pod was terminated")
	}
}

type unsyncedWatch struct{}

func (unsyncedWatch) HasSynced() bool       { return false }
func (unsyncedWatch) PodExists(string) bool { return false }

func TestReconciler_UnsyncedWatchKeepsVM(t *testing.T) {
	f := newOrphanFixture(t, "uid-unsynced")
	f.rec.SetPodWatch(unsyncedWatch{})
	if _, err := f.rec.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !instanceAlive(t, f) {
		t.Fatal("VM terminated before the pod watch synced")
	}
}

func TestReconciler_RecreatedPodTerminatesOldVM(t *testing.T) {
	f := newOrphanFixture(t, "uid-old")
	f.rec.SetPodWatch(startPodWatch(t, kubePod("jobs", "worker", "uid-new")))
	if _, err := f.rec.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if instanceAlive(t, f) {
		t.Fatal("VM of the replaced pod incarnation is still running")
	}
}

func TestReconciler_LaunchGraceProtectsOrphanCandidate(t *testing.T) {
	f := newOrphanFixture(t, "uid-fresh")
	f.rec.SetLaunchGracePeriod(time.Hour)
	f.rec.SetPodWatch(startPodWatch(t))
	if _, err := f.rec.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !instanceAlive(t, f) {
		t.Fatal("recently launched VM was terminated")
	}
}
