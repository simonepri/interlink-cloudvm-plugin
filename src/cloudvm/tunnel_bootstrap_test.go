// Tests that booting workers fetch their scripts from the gateway: the bootstrap's own fetch function through TLS.
package main

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// bootstrapFetchFunction returns the cloudvm_fetch_script shell function the bootstrap of a worker dialing endpoint runs.
func bootstrapFetchFunction(t *testing.T, endpoint string) string {
	t.Helper()
	script, err := RenderCloudInit(&CloudInitConfig{
		PodName: "fetch-pod", RunnerAuthToken: "tok", TunnelEndpoint: endpoint, TargetContainerImage: "alpine",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	start := strings.Index(script, "cloudvm_fetch_script() {\n")
	end := strings.Index(script[start:], "\n}\n")
	if start < 0 || end < 0 {
		t.Fatal("cloudvm_fetch_script not found in the bootstrap script")
	}
	return script[start : start+end+3]
}

// runBootstrapFetch runs cloudvm_fetch_script for hash into a new file, trusting caFile, and returns the file and the output.
func runBootstrapFetch(t *testing.T, function, caFile, hash string) (string, string, error) {
	t.Helper()
	for _, tool := range []string{"bash", "curl", "sha256sum"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not available", tool)
		}
	}
	dest := filepath.Join(t.TempDir(), "script.py")
	cmd := exec.Command("bash", "-c", function+"cloudvm_fetch_script \"$0\" \"$1\"", hash, dest) //nolint:gosec // test-controlled script
	cmd.Env = append(os.Environ(), "CURL_CA_BUNDLE="+caFile)
	out, err := cmd.CombinedOutput()
	return dest, string(out), err
}

func TestTunnelBootstrap_WorkersFetchVerifiedScriptsFromTheGateway(t *testing.T) {
	f := newTunnelFixture(t, time.Minute)
	endpoint, caFile := startTLSTerminator(t, f.gateway.Addr().String())
	function := bootstrapFetchFunction(t, endpoint)

	for name, src := range map[string]string{
		"runner.py": runnerScript, "tunnel_agent.py": tunnelAgentScript, "spot_watcher.py": spotWatcherScript,
	} {
		t.Run(name, func(t *testing.T) {
			dest, out, err := runBootstrapFetch(t, function, caFile, scriptSHA256(src))
			if err != nil {
				t.Fatalf("fetch: %v\n%s", err, out)
			}
			if got, _ := os.ReadFile(dest); string(got) != src { //nolint:gosec // test path
				t.Fatalf("fetched %d bytes that differ from the %d-byte source", len(got), len(src))
			}
		})
	}
	t.Run("an unknown hash is not found", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", f.gateway.Addr().String(), 5*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()
		_, _ = io.WriteString(conn, "GET /bootstrap/"+strings.Repeat("0", 64)+" HTTP/1.1\r\nHost: gateway\r\n\r\n")
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status %d, want 404", resp.StatusCode)
		}
	})
	t.Run("requests are counted", func(t *testing.T) {
		body := scrapeMetrics(t, f.server)
		for _, want := range []string{
			`cloudvm_tunnel_bootstrap_requests_total{result="served"} 3`,
			`cloudvm_tunnel_bootstrap_requests_total{result="not_found"} 1`,
		} {
			if !strings.Contains(body, want) {
				t.Errorf("metrics missing %s", want)
			}
		}
	})
}

func TestTunnelBootstrap_FetchRejectsATamperedScript(t *testing.T) {
	// The tampering server answers every request with a body other than the one the hash names.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			_, _ = http.ReadRequest(bufio.NewReader(conn))
			_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 9\r\n\r\ntampered\n")
			_ = conn.Close()
		}
	}()
	endpoint, caFile := startTLSTerminator(t, l.Addr().String())
	dest, out, err := runBootstrapFetch(t, bootstrapFetchFunction(t, endpoint), caFile, scriptSHA256(runnerScript))
	if err == nil {
		t.Fatalf("fetch accepted a tampered script:\n%s", out)
	}
	if !strings.Contains(out, "FATAL: cannot fetch") {
		t.Errorf("fetch failed without saying so:\n%s", out)
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Errorf("the tampered script was left at %s", dest)
	}
}
