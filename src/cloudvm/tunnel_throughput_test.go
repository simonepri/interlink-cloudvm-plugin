// Package main implements the interlink-cloudvm-plugin daemon.
// This file measures how fast one TCP connection moves data through the Python agent and the gateway over a 120 ms link.
package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// delayedLink relays TCP connections to a target and delivers every chunk one way after a fixed delay, as a long
// link with ample bandwidth does; it never slows a sender, so only the tunnel's own flow control limits throughput.
func delayedLink(t *testing.T, target string, oneWay time.Duration) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("link listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			down, err := listener.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", target)
			if err != nil {
				_ = down.Close()
				continue
			}
			go delayCopy(up, down, oneWay)
			go delayCopy(down, up, oneWay)
		}
	}()
	return listener.Addr().String()
}

func delayCopy(dst, src net.Conn, delay time.Duration) {
	type chunk struct {
		data []byte
		due  time.Time
	}
	queue := make(chan chunk, 1<<16)
	go func() {
		defer close(queue)
		for {
			buf := make([]byte, 64*1024)
			n, err := src.Read(buf)
			if n > 0 {
				queue <- chunk{buf[:n], time.Now().Add(delay)}
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() { _ = dst.Close(); _ = src.Close() }()
	for c := range queue {
		time.Sleep(time.Until(c.due))
		if _, err := dst.Write(c.data); err != nil {
			return
		}
	}
}

// startBlobServer answers "get <n>" with n bytes and "put <n>" followed by n bytes with "ok", one request per connection.
func startBlobServer(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("blob listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				line, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				verb, size, _ := strings.Cut(strings.TrimSpace(line), " ")
				n, _ := strconv.ParseInt(size, 10, 64)
				switch verb {
				case "get":
					_, _ = io.CopyN(conn, zeros{}, n)
				case "put":
					if _, err := io.CopyN(io.Discard, reader, n); err == nil {
						_, _ = conn.Write([]byte("ok\n"))
					}
				}
			}()
		}
	}()
	return l.Addr().String()
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// transfer opens a CONNECT tunnel through proxyAddr to the blob server, moves size bytes in direction verb, and
// returns the time from the request to the last byte.
func transfer(t *testing.T, proxyAddr, blob, verb string, size int64) time.Duration {
	t.Helper()
	conn, err := net.DialTimeout("tcp", proxyAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
	_, _ = fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", blob, blob)
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT %s: %v %v", blob, resp, err)
	}
	_ = resp.Body.Close()
	start := time.Now()
	_, _ = fmt.Fprintf(conn, "%s %d\n", verb, size)
	switch verb {
	case "get":
		if _, err := io.CopyN(io.Discard, reader, size); err != nil {
			t.Fatalf("get %d bytes: %v", size, err)
		}
	case "put":
		if _, err := io.CopyN(conn, zeros{}, size); err != nil {
			t.Fatalf("put %d bytes: %v", size, err)
		}
		if line, err := reader.ReadString('\n'); err != nil || line != "ok\n" {
			t.Fatalf("put reply %q: %v", line, err)
		}
	}
	return time.Since(start)
}

// TestTunnelThroughput_OneConnectionFillsALongLink moves 32 MiB each way through one connection over a 120 ms link. The
// floors sit above what the former 2 MiB stream window allowed, about 16 MiB/s down and 8 MiB/s up, since the gateway
// returns window only after half of it is consumed.
func TestTunnelThroughput_OneConnectionFillsALongLink(t *testing.T) {
	const rtt = 120 * time.Millisecond
	const size = 32 << 20
	f := newTunnelFixture(t, time.Minute)
	const instanceID, token = "i-python-throughput", "agent-token"
	f.addNode(instanceID, "ap-northeast-2", token)
	f.gateway.directTCPNetworks, _ = ParseDirectTCPNetworks("127.0.0.0/8")
	isolateNamespaces(f, instanceID, "team-throughput")
	agentProxyPort := freePort(t)
	startPythonAgentBehind(t, f, delayedLink(t, f.gateway.Addr().String(), rtt/2), instanceID, token,
		fmt.Sprintf("CLOUDVM_TCP_PROXY_PORT=%d", agentProxyPort))
	agentProxy := fmt.Sprintf("127.0.0.1:%d", agentProxyPort)
	blob := startBlobServer(t)

	for _, tc := range []struct {
		verb  string
		floor float64 // MiB/s
	}{{"get", 18}, {"put", 9}} {
		t.Run(tc.verb, func(t *testing.T) {
			elapsed := transfer(t, agentProxy, blob, tc.verb, size)
			rate := float64(size) / elapsed.Seconds() / (1 << 20)
			t.Logf("%s %d MiB over a %s link: %s, %.1f MiB/s", tc.verb, size>>20, rtt, elapsed.Round(time.Millisecond), rate)
			if rate < tc.floor {
				t.Errorf("%s moved %.1f MiB/s, want at least %.0f MiB/s", tc.verb, rate, tc.floor)
			}
		})
	}
}
