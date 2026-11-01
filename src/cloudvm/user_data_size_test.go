// Tests that the largest boot script a production worker gets fits EC2's user data limit with room to spare, and
// that a launch whose boot script would not fit is refused with a clear error before it reaches EC2.
package main

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

// productionStargzConfig is the snapshotter tuning a production plugin sets in STARGZ_CONFIG.
const productionStargzConfig = `max_concurrency = 16
prefetch_timeout_sec = 90

[blob]
chunk_size = 67108864
prefetch_chunk_size = 134217728
force_single_range_mode = true
`

// userDataMargin is the room the largest production boot script must leave under the limit, so settings an operator
// adds later, such as namespace tunnel redirects, do not break launches.
const userDataMargin = 6 * 1024

// worstCaseProductionSpec is a GPU NodeClaim worker as production launches it: router tunnel transport, lazy pulls with
// tuned stargz settings, a regional Docker Hub mirror, a KMS-enveloped runner token, and tunnelled Service and
// namespace destinations.
func worstCaseProductionSpec(t *testing.T) *InstanceSpec {
	t.Helper()
	ciphertext := make([]byte, 512)
	if _, err := rand.Read(ciphertext); err != nil {
		t.Fatal(err)
	}
	var redirects []TunnelRedirect
	for _, network := range []string{"198.18.0.0/15", "203.0.113.10/32", "203.0.113.11/32", "203.0.113.12/32"} {
		redirects = append(redirects, TunnelRedirect{Network: network, Port: 443})
	}
	return &InstanceSpec{ //nolint:gosec // test fixture with a placeholder token
		PodName: "cloudvm-gpu-on-demand-8xh100-abcde", PodNamespace: "karpenter", PodUID: "0f9c3f1e-6a4b-4f7e-9a71-2c8d5e4b3a10",
		NodeClaim: true, Provider: "aws", InstanceType: "p5.48xlarge", NumGPUs: 8, AssignedGPUs: []int{0, 1, 2, 3, 4, 5, 6, 7},
		RunnerAuthToken: "unused-when-kms-wraps-it", KMSEncryptedToken: true,
		KMSKeyID:          "arn:aws:kms:us-east-1:123456789012:key/0f9c3f1e-6a4b-4f7e-9a71-2c8d5e4b3a10",
		KMSCiphertext:     base64.StdEncoding.EncodeToString(ciphertext),
		TunnelEndpoint:    "clvm-tunnel.cell-production-us-east-1.example.com:443",
		TunnelTransport:   "router",
		TunnelGatewayIP:   "172.17.11.46",
		TunnelDirectCIDRs: []string{"10.112.0.0/12"}, TunnelServiceCIDRs: []string{"172.20.0.0/16"}, TunnelRedirects: redirects,
		ImageMirrors:      map[string]string{"docker.io": "123456789012.dkr.ecr.us-east-1.amazonaws.com/docker-hub"},
		StargzConfig:      productionStargzConfig,
		RunnerConcurrency: 16,
	}
}

func compressedUserData(t *testing.T, spec *InstanceSpec) int {
	t.Helper()
	encoded, err := renderFleetUserData(spec, "us-east-1")
	if err != nil {
		t.Fatalf("render user data: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return len(raw)
}

func TestWorstCaseProductionUserDataFitsWithMargin(t *testing.T) {
	spec := worstCaseProductionSpec(t)
	for _, transport := range []TunnelTransport{TunnelTransportRouter, TunnelTransportTailnet, TunnelTransportPublic} {
		variant := *spec
		variant.TunnelTransport = string(transport)
		if transport == TunnelTransportTailnet {
			// Tailnet workers also carry the KMS-enveloped mesh key and install Tailscale.
			variant.TunnelGatewayIP, variant.MeshAuthKeyCiphertext, variant.MeshTags = "", spec.KMSCiphertext, "tag:clvm-worker"
		}
		size := compressedUserData(t, &variant)
		t.Logf("worst-case production user data over the %s transport: %d of %d bytes", transport, size, maxUserDataBytes)
		if size > maxUserDataBytes-userDataMargin {
			t.Errorf("worst-case production user data over the %s transport is %d bytes, leaving less than %d of the %d byte limit", transport, size, userDataMargin, maxUserDataBytes)
		}
	}
	script, err := RenderCloudInit(&CloudInitConfig{TunnelEndpoint: spec.TunnelEndpoint, StargzConfig: spec.StargzConfig})
	if err != nil {
		t.Fatal(err)
	}
	for _, sum := range []string{scriptSHA256(runnerScript), scriptSHA256(trafficMeterScript)} {
		if !strings.Contains(script, "cloudvm_fetch_script "+sum) {
			t.Fatalf("workers must fetch the script with sha256 %s", sum)
		}
	}
	if strings.Contains(script, "def ") {
		t.Fatal("workers must fetch the runner and traffic meter instead of carrying them in user data")
	}
}

func TestContainerModeBootScriptWithoutTunnelEndpointIsRefused(t *testing.T) {
	if _, err := RenderCloudInit(&CloudInitConfig{PodName: "p"}); err == nil || !strings.Contains(err.Error(), "no tunnel endpoint") {
		t.Fatalf("err = %v, want a refusal naming the missing tunnel endpoint", err)
	}
	if _, err := RenderCloudInit(&CloudInitConfig{PodName: "p", UseNativeDLAMI: true}); err != nil {
		t.Fatalf("native mode runs no runner and needs no tunnel endpoint: %v", err)
	}
}

func TestUserDataOverTheLimitIsRefused(t *testing.T) {
	spec := worstCaseProductionSpec(t)
	noise := make([]byte, maxUserDataBytes)
	if _, err := rand.Read(noise); err != nil {
		t.Fatal(err)
	}
	spec.CustomBootstrapScript = "# " + base64.StdEncoding.EncodeToString(noise)
	if _, err := renderFleetUserData(spec, "us-east-1"); !errors.Is(err, errUserDataTooLarge) || !strings.Contains(err.Error(), spec.PodName) {
		t.Fatalf("err = %v, want errUserDataTooLarge naming the VM", err)
	}
}
