// Tests that a pod deleted while its VM boots is released when its container never reached the VM, waits for the VM's
// tunnel when it may have, and in neither case marks the VM as failing to clean up.
package main

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

func TestPodStartsTellWhetherAStartReachedTheVM(t *testing.T) {
	notDeleted := func() bool { return false }
	starts := newPodStarts()
	if starts.delete("unknown") {
		t.Fatal("a pod placed before a restart counted as never started")
	}

	starts.begin("u", notDeleted)
	if starts.delete("u") {
		t.Fatal("a pod whose start is in flight counted as never started")
	}
	starts.end("u", false)
	if !starts.delete("u") {
		t.Fatal("a pod whose only start never left the plugin counted as started")
	}
	if starts.begin("u", notDeleted) {
		t.Fatal("a deleted pod's start was sent")
	}

	starts.begin("v", notDeleted)
	starts.end("v", true)
	if starts.delete("v") {
		t.Fatal("a pod whose start reached the runner counted as never started")
	}

	starts.forget("u")
	if starts.begin("u", func() bool { return true }) {
		t.Fatal("a tombstoned pod's start was sent after its record was dropped")
	}
}

func TestPodDeletedBeforeItsVMTunnelRegisteredIsReleased(t *testing.T) {
	s, client, url := newClaimServer(t, NewMockProvider())
	s.EnableTunnel(TunnelSettings{Endpoint: "tunnel.example.internal:443", MeshAuthKey: "tskey-auth-test"})
	if _, err := client.Create(context.Background(), claimRequest("nc-boot", "g6.48xlarge")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if code := createClaimPod(t, url, "nc-boot", "team-a", "gfd", 0); code != http.StatusOK {
		t.Fatalf("create pod: HTTP %d", code)
	}
	begun := time.Now()
	deleteClaimPod(t, url, "gfd")
	if took := time.Since(begun); took > 10*time.Second {
		t.Fatalf("the delete waited %v for a VM that never received the pod", took)
	}
	if got := mustGet(t, client, "nc-boot"); got.State == nodeclaim.StateUnhealthy || len(got.Pods) != 0 {
		t.Fatalf("claim = %s with pods %v, want the pod released and the VM healthy", got.State, got.Pods)
	}
}

// bootingRunner answers like the mock runner once the VM's worker registered a tunnel session, and without one fails
// every stop the way the tunnel runner does.
type bootingRunner struct {
	*MockContainerRunner
	registry *SessionRegistry
	stops    atomic.Int32
}

func (r *bootingRunner) StopContainer(ctx context.Context, node *CloudVMNode, uid string) error {
	r.stops.Add(1)
	if !r.registry.Has(node.InstanceID) {
		return errNoTunnelSession
	}
	return r.MockContainerRunner.StopContainer(ctx, node, uid)
}

func TestPodDeletedBeforeItsVMTunnelReconnectedWaitsForTheTunnel(t *testing.T) {
	s, client, url := newClaimServer(t, NewMockProvider())
	registry := NewSessionRegistry()
	s.EnableTunnel(TunnelSettings{Registry: registry, Endpoint: "tunnel.example.internal:443", MeshAuthKey: "tskey-auth-test"})
	runner := &bootingRunner{MockContainerRunner: NewMockContainerRunner(), registry: registry}
	s.SetContainerRunner(runner)
	s.stopRetryDelay = time.Millisecond
	claim, err := client.Create(context.Background(), claimRequest("nc-wait", "g6.48xlarge"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _, instanceID, _ := nodeclaim.ParseProviderID(claim.ProviderID)
	if code := createClaimPod(t, url, "nc-wait", "team-a", "gfd", 0); code != http.StatusOK {
		t.Fatalf("create pod: HTTP %d", code)
	}

	const registerAfter = 300 * time.Millisecond
	session := pipeSession(t)
	time.AfterFunc(registerAfter, func() { registry.Register(instanceID, session) })
	begun := time.Now()
	deleteClaimPod(t, url, "gfd")
	if took := time.Since(begun); took < registerAfter {
		t.Fatalf("the delete returned after %v, before the tunnel registered", took)
	}
	if got := mustGet(t, client, "nc-wait"); got.State == nodeclaim.StateUnhealthy || len(got.Pods) != 0 {
		t.Fatalf("claim = %s with pods %v, want the container removed once the tunnel registered", got.State, got.Pods)
	}
	if status, _ := runner.GetStatus(context.Background(), nil, "uid-gfd"); status == nil || status.Running {
		t.Fatalf("container status = %+v, want stopped", status)
	}
}
