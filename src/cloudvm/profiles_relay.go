// Profiles relay: serves the gRPC uploads of worker parca-agents arriving on tunnel streams and forwards them to the Parca
// server with the plugin's bearer token, so workers never hold the token.
package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"strings"
	"sync"
	"time"
)

// WorkerProfilesStreamKind names the worker-opened stream that carries one HTTP/2 connection of the worker's
// parca-agent.
const WorkerProfilesStreamKind = "profiles"

// relayedProfileServices are the Parca gRPC services an agent writes to; the relay refuses every other call, such as
// queries, so a worker cannot read profiles with the plugin's token.
var relayedProfileServices = []string{
	"/parca.debuginfo.v1alpha1.DebuginfoService/",
	"/parca.profilestore.v1alpha1.ProfileStoreService/",
	"/parca.telemetry.v1alpha1.TelemetryService/",
}

// profilesRelayIdleTimeout closes a worker connection that carried no request for this long.
const profilesRelayIdleTimeout = 5 * time.Minute

// ProfilesRelay forwards worker profile uploads to one Parca server.
type ProfilesRelay struct {
	tokenFile string
	proxy     *httputil.ReverseProxy
	conns     *connListener
}

// NewProfilesRelay returns a relay to the Parca gRPC server at address (host:port), over TLS unless insecure, that
// authenticates every call with the bearer token in tokenFile when it is set. The file is read on every call, so a
// rotated token takes effect without a restart.
func NewProfilesRelay(address string, insecure bool, tokenFile string) (*ProfilesRelay, error) {
	if _, _, err := net.SplitHostPort(address); err != nil {
		return nil, fmt.Errorf("invalid profiles endpoint %q: want host:port", address)
	}
	// gRPC needs HTTP/2 end to end: over TLS upstream, or with prior knowledge in plaintext.
	scheme := "https"
	var protocols http.Protocols
	protocols.SetHTTP2(true)
	if insecure {
		scheme = "http"
		protocols = http.Protocols{}
		protocols.SetUnencryptedHTTP2(true)
	}
	transport := &http.Transport{
		Protocols:   &protocols,
		DialContext: (&net.Dialer{Timeout: tcpDialTimeout}).DialContext,
		HTTP2:       &http.HTTP2Config{PingTimeout: 30 * time.Second, SendPingTimeout: 30 * time.Second},
	}
	r := &ProfilesRelay{tokenFile: tokenFile, conns: newConnListener()}
	r.proxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = scheme
			pr.Out.URL.Host = address
			pr.Out.Host = address
		},
		Transport:     transport,
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, req *http.Request, err error) {
			log.Printf("[cloudvm-profiles] Cannot relay %s to %s: %v", req.URL.Path, address, err) //nolint:gosec // request path and configuration value
			writeGRPCStatus(w, 14, "profile store unavailable")
		},
	}
	var workerProtocols http.Protocols
	workerProtocols.SetUnencryptedHTTP2(true)
	server := &http.Server{Handler: r, Protocols: &workerProtocols, IdleTimeout: profilesRelayIdleTimeout, ReadHeaderTimeout: 30 * time.Second}
	go func() { _ = server.Serve(r.conns) }()
	return r, nil
}

// ServeHTTP relays one gRPC call of a worker's agent.
func (r *ProfilesRelay) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !relayedProfileService(req.URL.Path) {
		writeGRPCStatus(w, 12, "only profile uploads are relayed")
		return
	}
	req.Header.Del("Authorization")
	if r.tokenFile != "" {
		token, err := os.ReadFile(r.tokenFile)
		if err != nil {
			log.Printf("[cloudvm-profiles] Cannot read the profile store token: %v", err)
			writeGRPCStatus(w, 14, "profile store token unavailable")
			return
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	}
	r.proxy.ServeHTTP(w, req)
}

// Serve serves the HTTP/2 connection a worker's profiles stream carries until it ends; reader holds the bytes the
// gateway read past the stream header.
func (r *ProfilesRelay) Serve(conn net.Conn, reader *bufio.Reader) {
	c := &relayConn{Conn: conn, reader: reader, done: make(chan struct{})}
	r.conns.conns <- c
	<-c.done
}

func relayedProfileService(path string) bool {
	for _, prefix := range relayedProfileServices {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

// writeGRPCStatus ends a gRPC call with status code and message and no response message.
func writeGRPCStatus(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/grpc")
	w.Header().Set("Grpc-Status", fmt.Sprint(code))
	w.Header().Set("Grpc-Message", message)
	w.WriteHeader(http.StatusOK)
}

// relayConn reads through the reader that already consumed the start of the connection, and tells Serve when the
// relay's server closed it.
type relayConn struct {
	net.Conn
	reader *bufio.Reader
	once   sync.Once
	done   chan struct{}
}

func (c *relayConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

func (c *relayConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return c.Conn.Close()
}

// connListener hands the relay's HTTP server the connections Serve receives; it never closes, like the relay.
type connListener struct {
	conns chan net.Conn
}

func newConnListener() *connListener {
	return &connListener{conns: make(chan net.Conn)}
}

func (l *connListener) Accept() (net.Conn, error) {
	c, ok := <-l.conns
	if !ok {
		return nil, errors.New("profiles relay closed")
	}
	return c, nil
}

func (l *connListener) Close() error { return nil }

func (l *connListener) Addr() net.Addr { return relayAddr{} }

// relayAddr names the relay's connections, which arrive on tunnel streams rather than a socket.
type relayAddr struct{}

func (relayAddr) Network() string { return "tunnel" }
func (relayAddr) String() string  { return "profiles-relay" }
