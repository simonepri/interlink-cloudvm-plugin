// Tests that the plugin client authenticates every request with the token file and picks up a rotated token.

package pluginclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

func writeToken(t *testing.T, path, token string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// tokenServer accepts only the bearer token in valid and records every Authorization header it saw.
type tokenServer struct {
	mu    sync.Mutex
	valid string
	seen  []string
}

func (s *tokenServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	got := r.Header.Get("Authorization")
	s.seen = append(s.seen, got)
	w.Header().Set("Content-Type", "application/json")
	if got != "Bearer "+s.valid {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(nodeclaim.ErrorResponse{Reason: nodeclaim.ReasonUnauthorized})
		return
	}
	_ = json.NewEncoder(w).Encode(nodeclaim.List{Items: []nodeclaim.NodeClaim{}})
}

func TestNewRequiresToken(t *testing.T) {
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	writeToken(t, empty, "  ")
	for name, path := range map[string]string{"unset": "", "missing": filepath.Join(dir, "missing"), "empty": empty} {
		t.Run(name, func(t *testing.T) {
			if _, err := New("http://plugin.example", path); err == nil {
				t.Fatal("expected an error without a usable token")
			}
		})
	}
}

func TestClientSendsBearerTokenAndPicksUpRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	writeToken(t, path, "token-1")
	srv := &tokenServer{valid: "token-1"}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	c, err := New(ts.URL, path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := c.List(ctx); err != nil {
		t.Fatalf("first request: %v", err)
	}

	// The kubelet rotates the projected token and the plugin stops accepting the old one.
	writeToken(t, path, "token-2")
	srv.mu.Lock()
	srv.valid = "token-2"
	srv.mu.Unlock()
	if _, err := c.List(ctx); err != nil {
		t.Fatalf("request after rotation did not send the new token: %v", err)
	}

	// A token the plugin rejects surfaces as an authorization error, not as a missing claim.
	srv.mu.Lock()
	srv.valid = "token-3"
	srv.mu.Unlock()
	if _, err := c.List(ctx); !nodeclaim.IsUnauthorized(err) {
		t.Fatalf("err = %v, want an unauthorized error", err)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	want := []string{"Bearer token-1", "Bearer token-2", "Bearer token-2"}
	if len(srv.seen) != len(want) {
		t.Fatalf("saw %v, want %v", srv.seen, want)
	}
	for i := range want {
		if srv.seen[i] != want[i] {
			t.Fatalf("request %d sent %q, want %q", i, srv.seen[i], want[i])
		}
	}
}

func TestChartDefaultAudienceMatchesPlugin(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "charts", "cloudvm-karpenter", "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var values struct {
		Plugin struct {
			TokenAudience string `json:"tokenAudience"`
		} `json:"plugin"`
	}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatal(err)
	}
	if values.Plugin.TokenAudience != nodeclaim.DefaultTokenAudience {
		t.Fatalf("chart tokenAudience = %q, want the plugin default %q", values.Plugin.TokenAudience, nodeclaim.DefaultTokenAudience)
	}
}
