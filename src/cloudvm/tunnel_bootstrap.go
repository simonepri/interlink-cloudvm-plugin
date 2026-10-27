// Serves the in-VM Python scripts to booting workers over the tunnel endpoint, addressed by the sha256 the user data pins.
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
)

// bootstrapPathPrefix starts the path of every script request; the hex sha256 of the script follows it.
const bootstrapPathPrefix = "/bootstrap/"

// bootstrapScripts maps the hex sha256 of each in-VM script to its source. The scripts are public, so the hash the
// user data pins is the only check a worker needs.
var bootstrapScripts = func() map[string]string {
	scripts := make(map[string]string)
	for _, src := range []string{runnerScript, tunnelAgentScript, spotWatcherScript} {
		scripts[scriptSHA256(src)] = src
	}
	return scripts
}()

// scriptSHA256 returns the hex sha256 a worker requests and checks a script by.
func scriptSHA256(src string) string {
	sum := sha256.Sum256([]byte(src))
	return hex.EncodeToString(sum[:])
}

// isBootstrapRequest reports whether a connection opens with an HTTP GET rather than a tunnel handshake line.
func isBootstrapRequest(reader *bufio.Reader) bool {
	prefix, err := reader.Peek(4)
	return err == nil && string(prefix) == "GET "
}

// serveBootstrapScript answers one GET /bootstrap/<sha256> with the script of that hash and closes the connection;
// the caller's deadline and read limit bound the request.
func (g *TunnelGateway) serveBootstrapScript(conn net.Conn, reader *bufio.Reader) {
	defer conn.Close()
	req, err := http.ReadRequest(reader)
	if err != nil {
		g.recordBootstrap("bad_request")
		_, _ = io.WriteString(conn, "HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
		return
	}
	_ = req.Body.Close()
	src, ok := bootstrapScripts[strings.TrimPrefix(req.URL.Path, bootstrapPathPrefix)]
	if !ok || !strings.HasPrefix(req.URL.Path, bootstrapPathPrefix) {
		g.recordBootstrap("not_found")
		_, _ = io.WriteString(conn, "HTTP/1.1 404 Not Found\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
		return
	}
	g.recordBootstrap("served")
	header := "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: text/x-python\r\nContent-Length: " + strconv.Itoa(len(src)) + "\r\n\r\n"
	if _, err := io.WriteString(conn, header+src); err != nil {
		log.Printf("[cloudvm-tunnel] Cannot send bootstrap script to %s: %v", conn.RemoteAddr(), err)
	}
}

func (g *TunnelGateway) recordBootstrap(result string) {
	if g.onBootstrap != nil {
		g.onBootstrap(result)
	}
}
