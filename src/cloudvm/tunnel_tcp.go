// Bridges plain TCP connections over worker tunnel sessions, so processes on worker VMs and in the cluster can reach each other.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
)

// WorkerTCPStreamKind names a stream that carries one plain TCP connection to the target in its header.
const WorkerTCPStreamKind = "tcp"

const tcpDialTimeout = 10 * time.Second

// minDirectTCPPort keeps direct forwarding off well-known ports, such as the API servers the gateway itself reaches.
const minDirectTCPPort = 1024

// InstanceAddressResolver maps a worker VM's private IP to the instance ID of its tunnel session.
type InstanceAddressResolver func(ip string) (string, bool)

// serveWorkerTCP bridges a worker-opened tcp stream to its target: another worker VM when the target is one of their
// addresses, otherwise a direct connection from the gateway when allowDirect permits it.
func (g *TunnelGateway) serveWorkerTCP(instanceID string, reader *bufio.Reader, stream *yamux.Stream, target string) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return
	}
	var upstream net.Conn
	if peer, ok := g.resolveInstance(host); ok {
		upstream, err = g.openWorkerTCP(peer, target)
	} else if err = g.allowDirect(host, port); err == nil {
		upstream, err = (&net.Dialer{Timeout: tcpDialTimeout}).Dial("tcp", target)
	}
	if err != nil {
		log.Printf("[cloudvm-tunnel] Refused tcp stream from %s to %s: %v", sanitizeLog(instanceID), sanitizeLog(target), err) //nolint:gosec // sanitized
		return
	}
	bridge(reader, stream, upstream)
}

// ServeConnectProxy accepts HTTP CONNECT requests from the cluster and bridges each to the worker VM that owns the
// requested address, until the listener closes.
func (g *TunnelGateway) ServeConnectProxy(ctx context.Context, listener net.Listener) {
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go g.serveConnect(conn)
	}
}

func (g *TunnelGateway) serveConnect(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(g.handshakeTimeout))
	reader := bufio.NewReader(conn)
	req, err := http.ReadRequest(reader)
	if err != nil {
		return
	}
	if req.Method != http.MethodConnect {
		_, _ = io.WriteString(conn, "HTTP/1.1 405 Method Not Allowed\r\n\r\n")
		return
	}
	host, _, err := net.SplitHostPort(req.Host)
	if err != nil {
		_, _ = io.WriteString(conn, "HTTP/1.1 400 Bad Request\r\n\r\n")
		return
	}
	peer, ok := g.resolveInstance(host)
	if !ok {
		log.Printf("[tunnel] CONNECT %s: no registered instance has this address", req.Host)
		_, _ = io.WriteString(conn, "HTTP/1.1 502 Bad Gateway\r\n\r\n")
		return
	}
	upstream, err := g.openWorkerTCP(peer, req.Host)
	if err != nil {
		log.Printf("[tunnel] CONNECT %s: %v", req.Host, err)
		_, _ = io.WriteString(conn, "HTTP/1.1 502 Bad Gateway\r\n\r\n")
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	if _, err := io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		_ = upstream.Close()
		return
	}
	bridge(reader, conn, upstream)
}

// openWorkerTCP opens a stream on the worker's session that its agent connects to target, an address of the worker
// itself, so a process listening only on the worker's private IP is reachable too.
func (g *TunnelGateway) openWorkerTCP(instanceID, target string) (net.Conn, error) {
	session, ok := g.registry.Get(instanceID)
	if !ok {
		return nil, fmt.Errorf("no active tunnel session for instance %s", instanceID)
	}
	stream, err := session.Open()
	if err != nil {
		return nil, err
	}
	header, err := json.Marshal(struct {
		Stream  string `json:"stream"`
		Version int    `json:"version"`
		Target  string `json:"target"`
	}{WorkerTCPStreamKind, 1, target})
	if err != nil {
		_ = stream.Close()
		return nil, err
	}
	if _, err := stream.Write(append(header, '\n')); err != nil {
		_ = stream.Close()
		return nil, err
	}
	return stream, nil
}

// allowDirect reports why the gateway may not dial target itself: direct forwarding reaches only addresses inside the
// configured networks, on ports from minDirectTCPPort up.
func (g *TunnelGateway) allowDirect(host, port string) error {
	if len(g.directTCPNetworks) == 0 {
		return errors.New("direct forwarding is disabled")
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("target %q is not an IP address", host)
	}
	if p, err := strconv.Atoi(port); err != nil || p < minDirectTCPPort || p > 65535 {
		return fmt.Errorf("port %q is outside %d-65535", port, minDirectTCPPort)
	}
	for _, network := range g.directTCPNetworks {
		if network.Contains(ip) {
			return nil
		}
	}
	return fmt.Errorf("target %s is outside the direct forwarding networks", host)
}

// ParseDirectTCPNetworks parses the comma-separated CIDRs direct forwarding may reach.
func ParseDirectTCPNetworks(raw string) ([]*net.IPNet, error) {
	var networks []*net.IPNet
	for _, cidr := range strings.Split(raw, ",") {
		cidr = strings.TrimSpace(cidr)
		if cidr == "" {
			continue
		}
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, fmt.Errorf("invalid direct forwarding network %q: %w", cidr, err)
		}
		networks = append(networks, network)
	}
	return networks, nil
}

func (g *TunnelGateway) resolveInstance(host string) (string, bool) {
	if g.resolver == nil {
		return "", false
	}
	return g.resolver(host)
}

// bridge copies between a connection, read through reader, and upstream until either side ends, then closes both.
func bridge(reader io.Reader, conn io.WriteCloser, upstream io.ReadWriteCloser) {
	var once sync.Once
	done := make(chan struct{})
	closeBoth := func() {
		once.Do(func() {
			_ = conn.Close()
			_ = upstream.Close()
			close(done)
		})
	}
	go func() {
		_, _ = io.Copy(upstream, reader)
		closeBoth()
	}()
	go func() {
		_, _ = io.Copy(conn, upstream)
		closeBoth()
	}()
	<-done
}

// ResolveInstanceAddress maps a worker VM's private IP to its instance ID.
func (s *Server) ResolveInstanceAddress(ip string) (string, bool) {
	np := s.NodePool()
	if np == nil {
		return "", false
	}
	return np.InstanceByIP(ip)
}
