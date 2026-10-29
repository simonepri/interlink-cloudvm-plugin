// Package pluginclient builds the plugin NodeClaim API client that authenticates with a projected service account
// token, re-read from its file on every request so kubelet rotations take effect without a restart.
package pluginclient

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

// Timeout bounds one plugin call; a launch blocks until the cloud returns the instance ID.
const Timeout = 2 * time.Minute

// New returns a client that sends the token in tokenFile as a bearer token on every request. It fails when the file
// is missing or empty, so the controller never starts without a credential for the plugin.
func New(baseURL, tokenFile string) (*nodeclaim.Client, error) {
	if tokenFile == "" {
		return nil, fmt.Errorf("a plugin token file is required")
	}
	raw, err := os.ReadFile(tokenFile) //nolint:gosec // the path is the operator-supplied token mount
	if err != nil {
		return nil, fmt.Errorf("reading plugin token: %w", err)
	}
	if strings.TrimSpace(string(raw)) == "" {
		return nil, fmt.Errorf("plugin token file %s is empty", tokenFile)
	}
	return nodeclaim.NewClient(baseURL, &http.Client{Timeout: Timeout, Transport: &nodeclaim.TokenFileTransport{Path: tokenFile}}), nil
}
