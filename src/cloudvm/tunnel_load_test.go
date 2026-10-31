// Package main implements the interlink-cloudvm-plugin daemon.
// This file load-tests the tunnel gateway: hundreds of worker sessions keep their tunnels through a link that stalls.
package main

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
)

// stallingRelay forwards TCP connections to a target and, while stalled, holds every byte in both directions, as a
// congested cross-region link does when frames queue behind megabytes of stream data.
type stallingRelay struct {
	listener net.Listener
	target   string
	stalled  atomic.Bool
}

func newStallingRelay(t *testing.T, target string) *stallingRelay {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("relay listen: %v", err)
	}
	r := &stallingRelay{listener: listener, target: target}
	t.Cleanup(func() { _ = listener.Close() })
	go r.serve()
	return r
}

func (r *stallingRelay) serve() {
	for {
		down, err := r.listener.Accept()
		if err != nil {
			return
		}
		up, err := net.Dial("tcp", r.target)
		if err != nil {
			_ = down.Close()
			continue
		}
		go r.pipe(down, up)
		go r.pipe(up, down)
	}
}

func (r *stallingRelay) pipe(dst, src net.Conn) {
	defer func() { _ = dst.Close(); _ = src.Close() }()
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		for r.stalled.Load() {
			time.Sleep(10 * time.Millisecond)
		}
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (r *stallingRelay) Addr() string { return r.listener.Addr().String() }

// TestTunnelGateway_SessionsSurviveALinkStallUnderLoad connects 200 workers, 150 of them through a link that then stalls
// for 12 seconds, longer than the 10 seconds Yamux allows a ping by default, and requires every session to survive.
func TestTunnelGateway_SessionsSurviveALinkStallUnderLoad(t *testing.T) {
	const remoteWorkers, localWorkers = 150, 50
	const stall = 12 * time.Second

	f := newTunnelFixture(t, time.Hour)
	// The gateway pings every second instead of every 30 so the stall spans several pings; the ping timeout stays the
	// production one.
	cfg := DefaultYamuxConfig()
	cfg.KeepAliveInterval = time.Second
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gateway, err := NewTunnelGateway(TunnelGatewayConfig{
		Listener:       listener,
		Registry:       f.registry,
		TokenValidator: f.server.ValidateTunnelToken,
		YamuxConfig:    cfg,
	})
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	gateway.Start(ctx)
	t.Cleanup(func() { cancel(); _ = gateway.Close() })
	relay := newStallingRelay(t, gateway.Addr().String())

	nodes := make(map[string]*CloudVMNode, remoteWorkers+localWorkers)
	for i := range remoteWorkers + localWorkers {
		id, token := fmt.Sprintf("i-load-%03d", i), fmt.Sprintf("token-%03d", i)
		nodes[id] = f.addNode(id, "ap-northeast-2", token)
		endpoint := gateway.Addr().String()
		if i < remoteWorkers {
			endpoint = relay.Addr()
		}
		d, err := NewReverseTunnelDialer(ReverseTunnelDialerConfig{
			Endpoint: endpoint, InstanceID: id, AuthToken: token,
			InitialBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("dialer: %v", err)
		}
		go func() {
			_ = d.Run(ctx, NewRunnerHTTPMux(&RunnerHandlerAdapter{Runner: NewMockContainerRunner()}, token))
		}()
	}
	waitForWithin(t, 30*time.Second, "every session", func() bool { return f.registry.Count() == len(nodes) })
	before := make(map[string]*yamux.Session, len(nodes))
	for id := range nodes {
		before[id], _ = f.registry.Get(id)
	}

	relay.stalled.Store(true)
	time.Sleep(stall)
	relay.stalled.Store(false)

	runner := gateway.Runner()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []string
	for id, node := range nodes {
		wg.Go(func() {
			callCtx, callCancel := context.WithTimeout(ctx, 20*time.Second)
			defer callCancel()
			_, err := runner.GetHostStats(callCtx, node)
			current, _ := f.registry.Get(id)
			if err != nil || current != before[id] {
				mu.Lock()
				failures = append(failures, fmt.Sprintf("%s: err=%v reconnected=%t", id, err, current != before[id]))
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if len(failures) > 0 {
		t.Fatalf("%d of %d sessions dropped across a %v stall, first: %s", len(failures), len(nodes), stall, failures[0])
	}
}
