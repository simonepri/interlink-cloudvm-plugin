// TokenFileTransport authenticates API calls with a projected service account token that it re-reads from its file
// on every request, so kubelet rotations of the token take effect without a restart.

package nodeclaim

import (
	"fmt"
	"net/http"
	"os"
	"strings"
)

// TokenFileTransport adds "Authorization: Bearer <token>" read from Path to every request it sends through Base.
type TokenFileTransport struct {
	Path string
	// Base sends the requests; nil uses http.DefaultTransport.
	Base http.RoundTripper
}

// RoundTrip implements http.RoundTripper.
func (t *TokenFileTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	raw, err := os.ReadFile(t.Path)
	if err != nil {
		return nil, fmt.Errorf("read API token: %w", err)
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return nil, fmt.Errorf("API token file %s is empty", t.Path)
	}
	out := req.Clone(req.Context())
	out.Header.Set("Authorization", "Bearer "+token)
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(out)
}

// IsUnauthorized reports whether err says the caller presented no valid token, or one the plugin does not allow.
func IsUnauthorized(err error) bool {
	return hasReason(err, ReasonUnauthorized) || hasReason(err, ReasonForbidden)
}
