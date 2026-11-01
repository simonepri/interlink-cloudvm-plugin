// Bridges plain TCP connections over worker tunnel sessions, so processes on worker VMs and in the cluster can reach each
// other, and refuses every connection whose two ends belong to different Kubernetes namespaces unless configuration allows it.
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

// PodNamespaces maps a cluster pod IP to the namespace of the pods that hold it.
type PodNamespaces interface {
	// HasSynced reports whether the mapping covers every pod. The gateway refuses lookups before it does.
	HasSynced() bool
	// PodNamespace returns the namespace of the pods holding ip; false when none or several namespaces hold it.
	PodNamespace(ip string) (string, bool)
}

// WorkerNamespaceResolver maps a worker's instance ID to the one namespace whose pods it runs.
type WorkerNamespaceResolver func(instanceID string) (string, bool)

// PodNamespaceOnWorkerResolver maps a pod UID to the pod's namespace when the plugin placed that pod on the worker.
type PodNamespaceOnWorkerResolver func(instanceID, podUID string) (string, bool)

// tcpRefusalLogInterval bounds how often the gateway logs refusals that share a reason.
const tcpRefusalLogInterval = 10 * time.Second

// tcpRefusal is why the gateway refuses a stream; reason is the metric label.
type tcpRefusal struct {
	reason string
	detail string
}

func (r *tcpRefusal) Error() string { return r.detail }

func refuse(reason, format string, args ...any) error {
	return &tcpRefusal{reason: reason, detail: fmt.Sprintf(format, args...)}
}

// serveWorkerTCP bridges a worker-opened tcp stream to its target: another worker VM when the target is one of their
// addresses, otherwise a direct connection from the gateway to a Service ClusterIP that allowService permits, to an
// external host the source namespace's destinations list, or to a pod address that allowDirect permits. Both ends must
// share a namespace, unless the target is a shared Service or one the source namespace's destinations list.
func (g *TunnelGateway) serveWorkerTCP(instanceID, pod string, reader *bufio.Reader, stream *yamux.Stream, target string) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return
	}
	upstream, err := g.dialFromWorker(instanceID, pod, host, port, target)
	if err != nil {
		source := instanceID
		if pod != "" {
			source += " pod " + pod
		}
		g.logTCPError(source, target, err)
		return
	}
	bridge(reader, stream, upstream)
}

func (g *TunnelGateway) dialFromWorker(instanceID, pod, host, port, target string) (net.Conn, error) {
	source, err := g.streamNamespaceOf(instanceID, pod)
	if err != nil {
		return nil, err
	}
	if peer, ok := g.resolveInstance(host); ok {
		dest, err := g.workerNamespaceOf(peer, "unknown_target")
		if err != nil {
			return nil, err
		}
		if err := sameNamespace(source, dest); err != nil {
			return nil, err
		}
		return g.openWorkerTCP(peer, target)
	}
	if g.inServiceNetworks(host) {
		if err := g.allowService(source, host, port); err != nil {
			return nil, err
		}
		return (&net.Dialer{Timeout: tcpDialTimeout}).Dial("tcp", target)
	}
	if g.destinations.AllowsHost(source, host, port) {
		return g.dialExternal(target)
	}
	if err := g.allowDirect(host, port); err != nil {
		return nil, refuse("direct_forbidden", "%v", err)
	}
	dest, err := g.podNamespaceOf(host, "unknown_target")
	if err != nil {
		return nil, err
	}
	if err := sameNamespace(source, dest); err != nil {
		return nil, err
	}
	return (&net.Dialer{Timeout: tcpDialTimeout}).Dial("tcp", target)
}

// dialExternal connects the gateway to an allow-listed external host:port, resolving a hostname with the plugin's own
// resolver.
func (g *TunnelGateway) dialExternal(target string) (net.Conn, error) {
	if g.externalDialer != nil {
		return g.externalDialer(target)
	}
	return (&net.Dialer{Timeout: tcpDialTimeout}).Dial("tcp", target)
}

// streamNamespaceOf returns the namespace a worker's stream acts for: that of the pod the stream names, which must run
// on the worker, or the worker's namespace for a stream that names none, such as one from a process of the VM itself.
func (g *TunnelGateway) streamNamespaceOf(instanceID, pod string) (string, error) {
	if pod == "" || g.podOnWorker == nil {
		return g.workerNamespaceOf(instanceID, "unknown_source")
	}
	if namespace, ok := g.podOnWorker(instanceID, pod); ok {
		return namespace, nil
	}
	return "", refuse("foreign_pod", "pod %s does not run on worker %s", pod, instanceID)
}

// workerNamespaceOf returns the namespace of a worker's pods, or a refusal with unknownReason when it has none.
func (g *TunnelGateway) workerNamespaceOf(instanceID, unknownReason string) (string, error) {
	if g.workerNamespace != nil {
		if namespace, ok := g.workerNamespace(instanceID); ok {
			return namespace, nil
		}
	}
	return "", refuse(unknownReason, "worker %s runs no pods of a single namespace", instanceID)
}

// podNamespaceOf returns the namespace of the cluster pod holding ip, or a refusal with unknownReason when no pod of a
// single namespace does; before the pod mapping syncs, every lookup is refused.
func (g *TunnelGateway) podNamespaceOf(ip, unknownReason string) (string, error) {
	if g.podNamespaces == nil || !g.podNamespaces.HasSynced() {
		return "", refuse("unsynced", "the pod watch has not synced")
	}
	if parsed := net.ParseIP(ip); parsed != nil {
		if namespace, ok := g.podNamespaces.PodNamespace(parsed.String()); ok {
			return namespace, nil
		}
	}
	return "", refuse(unknownReason, "no pod of a single namespace holds %s", ip)
}

// inServiceNetworks reports whether host is an IP inside the configured Service networks.
func (g *TunnelGateway) inServiceNetworks(host string) bool {
	ip := net.ParseIP(host)
	for _, network := range g.serviceNetworks {
		if ip != nil && network.Contains(ip) {
			return true
		}
	}
	return false
}

// allowService refuses a stream from a pod of namespace source to a Service ClusterIP unless the Service lies in source,
// among the shared Services or the destinations of source, or is one that a Service of source stands in for, and exposes
// port. Unlike pod addresses, Services may expose ports below 1024: a ClusterIP reaches only the Service's own endpoints.
func (g *TunnelGateway) allowService(source, host, port string) error {
	if g.services == nil || !g.services.HasSynced() {
		return refuse("unsynced", "the service watch has not synced")
	}
	svc, ok := g.services.ServiceByIP(net.ParseIP(host).String())
	if !ok {
		return refuse("unknown_target", "no service holds %s", host)
	}
	if svc.Namespace != source && !g.sharedServices[svc.Namespace+"/"+svc.Name] &&
		!g.destinations.AllowsService(source, svc.Namespace, svc.Name) &&
		(g.regionalServices == nil || !g.regionalServices.StandsInFor(source, svc.Namespace, svc.Name)) {
		return refuse("namespace_mismatch", "namespace %s may not reach service %s/%s", source, svc.Namespace, svc.Name)
	}
	if p, err := strconv.Atoi(port); err != nil || !svc.exposes(p) {
		return refuse("direct_forbidden", "service %s/%s does not expose tcp port %s", svc.Namespace, svc.Name, port)
	}
	return nil
}

func sameNamespace(source, dest string) error {
	if source != dest {
		return refuse("namespace_mismatch", "namespace %s may not reach namespace %s", source, dest)
	}
	return nil
}

// logTCPError records a refusal in the metric and the rate-limited log, and logs any other failure as it happens.
func (g *TunnelGateway) logTCPError(source, target string, err error) {
	var refusal *tcpRefusal
	if !errors.As(err, &refusal) {
		log.Printf("[cloudvm-tunnel] tcp stream from %s to %s failed: %v", sanitizeLog(source), sanitizeLog(target), err) //nolint:gosec // sanitized
		return
	}
	if g.onTCPRefusal != nil {
		g.onTCPRefusal(refusal.reason)
	}
	if suppressed, ok := g.tcpRefusalLog.allow(refusal.reason, time.Now()); ok {
		log.Printf("[cloudvm-tunnel] Refused tcp stream from %s to %s (%s): %s; %d more refusals with this reason were not logged", //nolint:gosec // sanitized
			sanitizeLog(source), sanitizeLog(target), refusal.reason, sanitizeLog(refusal.detail), suppressed)
	}
}

// refusalLog lets one log line per reason through every tcpRefusalLogInterval and counts the refusals it holds back.
type refusalLog struct {
	mu         sync.Mutex
	next       map[string]time.Time
	suppressed map[string]int
}

// allow reports whether a refusal with reason may be logged at now, and how many were held back since the last one.
func (l *refusalLog) allow(reason string, now time.Time) (int, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.next == nil {
		l.next = make(map[string]time.Time)
		l.suppressed = make(map[string]int)
	}
	if now.Before(l.next[reason]) {
		l.suppressed[reason]++
		return 0, false
	}
	suppressed := l.suppressed[reason]
	l.suppressed[reason] = 0
	l.next[reason] = now.Add(tcpRefusalLogInterval)
	return suppressed, true
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
	client, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
	upstream, err := g.dialFromCluster(client, host, req.Host)
	if err != nil {
		g.logTCPError(client, req.Host, err)
		var refusal *tcpRefusal
		if errors.As(err, &refusal) && refusal.reason != "unknown_target" {
			_, _ = io.WriteString(conn, "HTTP/1.1 403 Forbidden\r\n\r\n")
		} else {
			_, _ = io.WriteString(conn, "HTTP/1.1 502 Bad Gateway\r\n\r\n")
		}
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	if _, err := io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		_ = upstream.Close()
		return
	}
	bridge(reader, conn, upstream)
}

// dialFromCluster opens a stream to the worker that owns host for the cluster pod at client, when both share a
// namespace.
func (g *TunnelGateway) dialFromCluster(client, host, target string) (net.Conn, error) {
	peer, ok := g.resolveInstance(host)
	if !ok {
		return nil, refuse("unknown_target", "no registered instance has address %s", host)
	}
	source, err := g.podNamespaceOf(client, "unknown_source")
	if err != nil {
		return nil, err
	}
	dest, err := g.workerNamespaceOf(peer, "unknown_target")
	if err != nil {
		return nil, err
	}
	if err := sameNamespace(source, dest); err != nil {
		return nil, err
	}
	return g.openWorkerTCP(peer, target)
}

// A worker whose session closed less than workerReconnectWindow ago is reconnecting, as every worker behind a gateway
// replica does when that replica stops; a stream to it waits up to workerReconnectWait for the new session instead of
// failing, since a refused connection makes clients such as Ray back off for far longer than the agent takes to return.
const (
	workerReconnectWindow = time.Minute
	workerReconnectWait   = 10 * time.Second
)

// openWorkerTCP opens a stream on the worker's session that its agent connects to target, an address of the worker
// itself, so a process listening only on the worker's private IP is reachable too.
func (g *TunnelGateway) openWorkerTCP(instanceID, target string) (net.Conn, error) {
	session, ok := g.registry.Await(instanceID, workerReconnectWindow, workerReconnectWait)
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

// ParseDirectTCPNetworks parses the comma-separated CIDRs that direct or Service forwarding may reach.
func ParseDirectTCPNetworks(raw string) ([]*net.IPNet, error) {
	var networks []*net.IPNet
	for _, cidr := range strings.Split(raw, ",") {
		cidr = strings.TrimSpace(cidr)
		if cidr == "" {
			continue
		}
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, fmt.Errorf("invalid forwarding network %q: %w", cidr, err)
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

// WorkerRegion returns the region a worker VM runs in.
func (s *Server) WorkerRegion(instanceID string) (string, bool) {
	np := s.NodePool()
	if np == nil {
		return "", false
	}
	node, ok := np.Node(instanceID)
	if !ok || node.Region == "" {
		return "", false
	}
	return node.Region, true
}

// WorkerNamespace returns the namespace of the pods a worker VM runs.
func (s *Server) WorkerNamespace(instanceID string) (string, bool) {
	np := s.NodePool()
	if np == nil {
		return "", false
	}
	return np.InstanceNamespace(instanceID)
}

// ResolveInstanceAddress maps a worker VM's private IP to its instance ID.
func (s *Server) ResolveInstanceAddress(ip string) (string, bool) {
	np := s.NodePool()
	if np == nil {
		return "", false
	}
	return np.InstanceByIP(ip)
}
