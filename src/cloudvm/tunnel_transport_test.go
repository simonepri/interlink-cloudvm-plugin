// Package main implements the interlink-cloudvm-plugin daemon.
// This file tests the per-region tunnel transports: facts parsing, required settings, launch specs, bootstrap scripts, and identity checks.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTunnelTransportsFromClusterFacts(t *testing.T) {
	transports, err := TunnelTransportsFromClusterFacts(`{"regions":{
		"us-east-1":{"launchTemplateId":"lt-1"},
		"us-west-2":{"tunnelTransport":"router"},
		"eu-west-1":{"tunnelTransport":"public"}}}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	for region, want := range map[string]TunnelTransport{
		"us-east-1":     TunnelTransportTailnet,
		"us-west-2":     TunnelTransportRouter,
		"eu-west-1":     TunnelTransportPublic,
		"ap-unlisted-1": TunnelTransportTailnet,
	} {
		if got := transports.For(region); got != want {
			t.Errorf("%s: got %q, want %q", region, got, want)
		}
	}
	if transports.meshKeyRequired() != true {
		t.Error("a tailnet region must require the mesh key")
	}

	if empty, err := TunnelTransportsFromClusterFacts(""); err != nil || empty.For("us-east-1") != TunnelTransportTailnet || !empty.meshKeyRequired() {
		t.Fatalf("absent facts must mean tailnet everywhere: %v %v", empty, err)
	}
	if onlyRouter, _ := TunnelTransportsFromClusterFacts(`{"regions":{"us-west-2":{"tunnelTransport":"router"}}}`); onlyRouter.meshKeyRequired() {
		t.Error("router-only facts must not require the mesh key")
	}
	if _, err := TunnelTransportsFromClusterFacts(`{"regions":{"us-west-2":{"tunnelTransport":"vpn"}}}`); err == nil || !strings.Contains(err.Error(), "us-west-2") {
		t.Fatalf("unknown transport must fail naming the region, got %v", err)
	}
	if _, err := TunnelTransportsFromClusterFacts(`{not json`); err == nil {
		t.Fatal("malformed CLUSTER_FACTS must fail")
	}
}

func TestTunnelSettings_ValidateTransportsFailsFastOnMissingEnv(t *testing.T) {
	cases := map[string]struct {
		settings TunnelSettings
		wantErr  string
	}{
		"tailnet only":         {TunnelSettings{Transports: TunnelTransports{"us-east-1": TunnelTransportTailnet}}, ""},
		"router without ip":    {TunnelSettings{Transports: TunnelTransports{"us-west-2": TunnelTransportRouter}}, "TUNNEL_GATEWAY_IP is not set"},
		"router with bad ip":   {TunnelSettings{Transports: TunnelTransports{"us-west-2": TunnelTransportRouter}, GatewayIP: "gateway"}, "not an IP address"},
		"router with ip":       {TunnelSettings{Transports: TunnelTransports{"us-west-2": TunnelTransportRouter}, GatewayIP: "172.17.11.46"}, ""},
		"public without host":  {TunnelSettings{Transports: TunnelTransports{"eu-west-1": TunnelTransportPublic}}, "TUNNEL_PUBLIC_ENDPOINT is not set"},
		"public without port":  {TunnelSettings{Transports: TunnelTransports{"eu-west-1": TunnelTransportPublic}, PublicEndpoint: "tunnel.example.com"}, "host:port"},
		"public with endpoint": {TunnelSettings{Transports: TunnelTransports{"eu-west-1": TunnelTransportPublic}, PublicEndpoint: "tunnel.example.com:443"}, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := tc.settings.validateTransports()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func transportSettings() TunnelSettings {
	return TunnelSettings{
		ConnectTimeout: time.Minute,
		Transports: TunnelTransports{
			"us-west-2": TunnelTransportRouter,
			"eu-west-1": TunnelTransportPublic,
		},
		GatewayIP:      "172.17.11.46",
		PublicEndpoint: "clvm-tunnel-public.cell-test.boltz.com:443",
	}
}

func TestApplyTunnelSpec_PerTransport(t *testing.T) {
	f := newTunnelFixtureWith(t, transportSettings())
	privateEndpoint := "clvm-tunnel.cell-test.c.corp.boltz.com:443"

	tailnet := &InstanceSpec{Region: "us-east-1"}
	router := &InstanceSpec{Region: "us-west-2", MeshAuthKey: "pod-supplied"}
	public := &InstanceSpec{Region: "eu-west-1", MeshAuthKey: "pod-supplied"}
	for _, spec := range []*InstanceSpec{tailnet, router, public} {
		if err := f.server.applyTunnelSpec(spec); err != nil {
			t.Fatalf("%s: %v", spec.Region, err)
		}
	}

	if tailnet.TunnelTransport != "tailnet" || tailnet.TunnelEndpoint != privateEndpoint || tailnet.MeshAuthKey != "tskey-auth-test" || tailnet.MeshTags != DefaultMeshTags {
		t.Errorf("tailnet spec: %+v", tailnet)
	}
	if router.TunnelTransport != "router" || router.TunnelEndpoint != privateEndpoint || router.TunnelGatewayIP != "172.17.11.46" {
		t.Errorf("router spec must dial the private hostname through the gateway IP: %+v", router)
	}
	if public.TunnelTransport != "public" || public.TunnelEndpoint != "clvm-tunnel-public.cell-test.boltz.com:443" || public.TunnelGatewayIP != "" {
		t.Errorf("public spec must dial the public endpoint: %+v", public)
	}
	for _, spec := range []*InstanceSpec{router, public} {
		if spec.MeshAuthKey != "" || spec.MeshTags != "" {
			t.Errorf("%s workers must not receive tailnet credentials: %+v", spec.TunnelTransport, spec)
		}
	}
}

func TestApplyTunnelSpec_TailnetWithoutMeshKeyFails(t *testing.T) {
	server := NewServer(NewMockProvider(), "test-cluster")
	server.EnableTunnel(TunnelSettings{
		Endpoint:   "clvm-tunnel.cell-test.c.corp.boltz.com:443",
		Transports: TunnelTransports{"us-west-2": TunnelTransportRouter},
		GatewayIP:  "172.17.11.46",
	})
	if err := server.applyTunnelSpec(&InstanceSpec{Region: "us-west-2"}); err != nil {
		t.Fatalf("router launch needs no mesh key: %v", err)
	}
	err := server.applyTunnelSpec(&InstanceSpec{Region: "us-east-1"})
	if err == nil || !strings.Contains(err.Error(), "TAILSCALE_AUTHKEY") {
		t.Fatalf("tailnet launch without a mesh key must fail clearly, got %v", err)
	}
}

func TestRenderCloudInit_PerTransport(t *testing.T) {
	base := CloudInitConfig{
		PodName:              "transport-pod",
		Region:               "us-west-2",
		RunnerAuthToken:      "runner-secret",
		TunnelEndpoint:       FormatTunnelEndpoint("cell-aws-usw2"),
		TargetContainerImage: "alpine",
	}
	hostsLine := `"172.17.11.46" "clvm-tunnel.cell-aws-usw2.c.corp.boltz.com" > /etc/hosts`

	t.Run("tailnet joins the mesh", func(t *testing.T) {
		cfg := base
		cfg.TunnelTransport = "tailnet"
		cfg.MeshAuthKey = "tskey-auth-plaintext"
		script := renderTransportScript(t, &cfg)
		if !strings.Contains(script, "tailscale up --auth-key=file:/etc/cloudvm/mesh-authkey") || strings.Contains(script, hostsLine) {
			t.Error("tailnet workers must join the mesh and leave /etc/hosts alone")
		}
	})

	t.Run("router pins the gateway host", func(t *testing.T) {
		cfg := base
		cfg.TunnelTransport = "router"
		cfg.TunnelGatewayIP = "172.17.11.46"
		script := renderTransportScript(t, &cfg)
		if !strings.Contains(script, hostsLine) {
			t.Errorf("router script must map the tunnel host to the gateway IP")
		}
		if strings.Contains(script, "tailscale up") {
			t.Error("router workers must not join the tailnet")
		}
		if !strings.Contains(script, `CLOUDVM_TUNNEL_ENDPOINT="clvm-tunnel.cell-aws-usw2.c.corp.boltz.com:443"`) {
			t.Error("router workers must dial the private hostname so TLS verifies the gateway certificate")
		}
		if !strings.Contains(script, "instance-identity/pkcs7") || strings.Contains(script, "instance-identity/rsa2048") {
			t.Error("router workers keep sending the default identity envelope")
		}
	})

	t.Run("router without gateway ip is rejected", func(t *testing.T) {
		cfg := base
		cfg.TunnelTransport = "router"
		if _, err := RenderCloudInit(&cfg); err == nil {
			t.Fatal("router script without a gateway IP must fail to render")
		}
	})

	t.Run("public sends the identity document", func(t *testing.T) {
		cfg := base
		cfg.TunnelTransport = "public"
		cfg.TunnelEndpoint = "clvm-tunnel-public.cell-test.boltz.com:443"
		script := renderTransportScript(t, &cfg)
		if strings.Contains(script, "tailscale up") || strings.Contains(script, "/etc/hosts") {
			t.Error("public workers must neither join the tailnet nor pin a host")
		}
		for _, want := range []string{
			`CLOUDVM_TUNNEL_ENDPOINT="clvm-tunnel-public.cell-test.boltz.com:443"`,
			"instance-identity/rsa2048 > /etc/cloudvm/attestation",
		} {
			if !strings.Contains(script, want) {
				t.Errorf("public script missing %q", want)
			}
		}
	})
}

func renderTransportScript(t *testing.T, cfg *CloudInitConfig) string {
	t.Helper()
	script, err := RenderCloudInit(cfg)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	assertBashSyntax(t, script)
	assertUserDataFits(t, script)
	return script
}

// recordingVerifier remembers every AWS document and answers with the identity it names, or with err.
type recordingVerifier struct {
	DisabledAttestationVerifier
	mu        sync.Mutex
	documents [][]byte
	err       error
}

func (v *recordingVerifier) VerifyAWS(_ context.Context, data []byte) (*AWSInstanceIdentity, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.documents = append(v.documents, data)
	if v.err != nil {
		return nil, v.err
	}
	var identity AWSInstanceIdentity
	if err := json.Unmarshal(data, &identity); err != nil {
		return nil, err
	}
	return &identity, nil
}

func (v *recordingVerifier) fail(err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.err = err
}

func (v *recordingVerifier) last() ([]byte, int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.documents) == 0 {
		return nil, 0
	}
	return v.documents[len(v.documents)-1], len(v.documents)
}

func TestTunnelGateway_PublicTransportRequiresVerifiedIdentity(t *testing.T) {
	verifier := &recordingVerifier{}
	settings := transportSettings()
	settings.IdentityVerifier = verifier
	f := newTunnelFixtureWith(t, settings)
	f.addNode("i-public", "eu-west-1", "public-token")
	f.addNode("i-tailnet", "us-east-1", "tailnet-token")
	document := []byte(`{"instanceId":"i-public","region":"eu-west-1"}`)
	attested := func(id, token string, doc []byte) *ReverseTunnelDialer {
		d := f.dialer(t, id, token)
		d.cfg.Platform, d.cfg.Attestation = "aws", doc
		return d
	}

	t.Run("missing document is rejected", func(t *testing.T) {
		err := f.dialer(t, "i-public", "public-token").DialAndServeOnce(context.Background(), http.NewServeMux())
		if err == nil || !strings.Contains(err.Error(), "instance identity") {
			t.Fatalf("expected identity rejection, got %v", err)
		}
		if _, n := verifier.last(); n != 0 {
			t.Fatal("verifier must not be called without a document")
		}
	})

	t.Run("failed verification is rejected", func(t *testing.T) {
		verifier.fail(errors.New("bad signature"))
		defer verifier.fail(nil)
		err := attested("i-public", "public-token", document).DialAndServeOnce(context.Background(), http.NewServeMux())
		if err == nil || !strings.Contains(err.Error(), "bad signature") {
			t.Fatalf("expected verifier error, got %v", err)
		}
		if f.registry.Has("i-public") {
			t.Fatal("unverified worker must not be registered")
		}
	})

	t.Run("another instance's document is rejected", func(t *testing.T) {
		err := attested("i-public", "public-token", []byte(`{"instanceId":"i-other"}`)).DialAndServeOnce(context.Background(), http.NewServeMux())
		if err == nil || !strings.Contains(err.Error(), "i-other") {
			t.Fatalf("expected instance mismatch, got %v", err)
		}
	})

	t.Run("verified document is accepted", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = attested("i-public", "public-token", document).Run(ctx, http.NewServeMux()) }()
		waitFor(t, "public session", func() bool { return f.registry.Has("i-public") })
		if got, _ := verifier.last(); string(got) != string(document) {
			t.Fatalf("verifier got %q", got)
		}
	})

	t.Run("tailnet worker needs no document", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, before := verifier.last()
		go func() { _ = f.dialer(t, "i-tailnet", "tailnet-token").Run(ctx, http.NewServeMux()) }()
		waitFor(t, "tailnet session", func() bool { return f.registry.Has("i-tailnet") })
		if _, after := verifier.last(); after != before {
			t.Fatal("tailnet workers must not be identity-checked")
		}
	})
}

func TestPublicIdentityVerifier(t *testing.T) {
	server := NewServer(NewMockProvider(), "test-cluster")
	if _, err := publicIdentityVerifier(server, "", nil); err == nil || !strings.Contains(err.Error(), TunnelAllowedAccountsEnv) {
		t.Fatalf("public transport without an account allowlist must fail fast, got %v", err)
	}
	if v, err := publicIdentityVerifier(server, "", []string{"111122223333"}); err != nil {
		t.Fatalf("enabled: %v", err)
	} else if _, ok := v.(*AWSIdentityVerifier); !ok {
		t.Fatalf("enabled verification must use the AWS verifier, got %T", v)
	}
	if v, err := publicIdentityVerifier(server, "disabled", nil); err != nil {
		t.Fatalf("disabled: %v", err)
	} else if _, ok := v.(DisabledAttestationVerifier); !ok {
		t.Fatalf("disabled verification must use the accepting stub, got %T", v)
	}
	if _, err := publicIdentityVerifier(server, "off", nil); err == nil {
		t.Fatal("unknown verification mode must fail")
	}
}

func TestTunnelAgent_PythonAgentSendsIdentityDocument(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	verifier := &recordingVerifier{}
	settings := transportSettings()
	settings.IdentityVerifier = verifier
	f := newTunnelFixtureWith(t, settings)
	const instanceID, token = "i-python-public", "agent-token"
	f.addNode(instanceID, "eu-west-1", token)

	endpoint, caFile := startTLSTerminator(t, f.gateway.Addr().String())
	dir := t.TempDir()
	document := `{"instanceId":"i-python-public","region":"eu-west-1"}`
	files := map[string]string{
		"runner-token": token,
		"attestation":  document,
		"agent.py":     tunnelAgentScript,
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var agentLog syncBuffer
	cmd := exec.CommandContext(ctx, python, "-I", filepath.Join(dir, "agent.py")) //nolint:gosec // test-controlled script path
	cmd.Env = append(os.Environ(),
		"CLOUDVM_TUNNEL_CA_FILE="+caFile,
		"CLOUDVM_TUNNEL_ENDPOINT="+endpoint,
		"CLOUDVM_INSTANCE_ID="+instanceID,
		"CLOUDVM_PLATFORM=aws",
		"CLOUDVM_RUNNER_TOKEN_FILE="+filepath.Join(dir, "runner-token"),
		"CLOUDVM_ATTESTATION_FILE="+filepath.Join(dir, "attestation"),
		"CLOUDVM_TUNNEL_MAX_BACKOFF=0.2",
	)
	cmd.Stderr = &agentLog
	if err := cmd.Start(); err != nil {
		t.Fatalf("start agent: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("agent log:\n%s", agentLog.String())
		}
	})

	waitForWithin(t, 60*time.Second, "python agent public session", func() bool { return f.registry.Has(instanceID) })
	if got, _ := verifier.last(); string(got) != document {
		t.Fatalf("agent sent document %q, want %q", got, document)
	}
}

// scriptSection returns the rendered script from the first line starting with from up to the next line starting with to.
func scriptSection(t *testing.T, script, from, to string) string {
	t.Helper()
	start := strings.Index(script, "\n"+from)
	if start < 0 {
		t.Fatalf("script has no section %q", from)
	}
	end := strings.Index(script[start+1:], "\n"+to)
	if end < 0 {
		t.Fatalf("script has no section %q after %q", to, from)
	}
	return script[start+1 : start+1+end]
}

// runBash runs script with a stub ip command that reports a default route via ens5 and records every call.
func runBash(t *testing.T, script string) (output, ipCalls string) {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available")
	}
	dir := t.TempDir()
	stub := `#!/bin/sh
echo "$*" >> "` + filepath.Join(dir, "ip-calls") + `"
case "$*" in
  "-4 route show default") echo "default via 10.20.0.1 dev ens5 proto dhcp src 10.20.3.4 metric 100" ;;
  "-4 route get "*) echo "172.17.11.46 via 10.20.0.1 dev ens5 src 10.20.3.4" ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "ip"), []byte(stub), 0o700); err != nil { //nolint:gosec // test stub must be executable
		t.Fatalf("write stub: %v", err)
	}
	cmd := exec.Command(bash, "-c", script) //nolint:gosec // test-rendered script
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("script failed: %v\n%s", err, out)
	}
	calls, _ := os.ReadFile(filepath.Join(dir, "ip-calls"))
	return string(out), string(calls)
}

// Docker's default bridge owns 172.17.0.0/16, so without a host route a router worker sends its dial to docker0.
func TestRenderCloudInit_RouterRoutesGatewayAroundDockerBridge(t *testing.T) {
	cfg := CloudInitConfig{
		PodName:              "router-gpu",
		Region:               "us-west-2",
		NumGPUs:              1,
		AssignedGPUs:         []int{0},
		KMSEncryptedToken:    true,
		KMSKeyID:             "arn:aws:kms:us-west-2:111122223333:key/k",
		KMSCiphertext:        "Y2lwaGVy",
		TunnelEndpoint:       FormatTunnelEndpoint("cell-aws-usw2"),
		TunnelTransport:      "router",
		TunnelGatewayIP:      "172.17.11.46",
		TargetContainerImage: "ghcr.io/example/gpu:1",
	}
	script := renderTransportScript(t, &cfg)
	section := scriptSection(t, script, "# 2. Reach the tunnel gateway", "# 3. ")
	hosts := filepath.Join(t.TempDir(), "hosts")
	if err := os.WriteFile(hosts, []byte("127.0.0.1 localhost\n10.9.9.9 clvm-tunnel.cell-aws-usw2.c.corp.boltz.com\n"), 0o600); err != nil {
		t.Fatalf("write hosts: %v", err)
	}
	out, calls := runBash(t, strings.ReplaceAll(section, "/etc/hosts", hosts))

	if !strings.Contains(calls, "route replace 172.17.11.46/32 via 10.20.0.1 dev ens5") {
		t.Fatalf("router worker must pin a host route for the gateway via the VPC uplink, ip calls:\n%s", calls)
	}
	got, _ := os.ReadFile(hosts)
	if want := "127.0.0.1 localhost\n172.17.11.46 clvm-tunnel.cell-aws-usw2.c.corp.boltz.com\n"; string(got) != want {
		t.Fatalf("hosts file:\n%s\nwant:\n%s", got, want)
	}
	if !strings.Contains(out, "Gateway route: 172.17.11.46 via 10.20.0.1 dev ens5") {
		t.Errorf("bootstrap must log the gateway route, got:\n%s", out)
	}

	route := strings.Index(script, "ip route replace")
	agent := strings.Index(script, "nohup python3 /usr/local/bin/cloudvm-tunnel-agent.py")
	if route < 0 || agent < 0 || route > agent {
		t.Fatal("the gateway route must be in place before the tunnel agent starts")
	}
	if !strings.Contains(script, "cloudvm-tunnel-agent.py 2>&1 < /dev/null | cloudvm_mirror /var/log/cloudvm-tunnel-agent.log &") {
		t.Error("tunnel agent output must be mirrored to the serial console")
	}
}

// The serial getty hangs up ttyS0 during boot; the mirror must keep writing to the console after that.
func TestRenderCloudInit_ConsoleMirrorSurvivesHangup(t *testing.T) {
	cfg := CloudInitConfig{PodName: "mirror", Region: "us-west-2", TargetContainerImage: "alpine"}
	script, err := RenderCloudInit(&cfg)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	dir := t.TempDir()
	console, logFile := filepath.Join(dir, "console"), filepath.Join(dir, "bootstrap.log")
	prelude := scriptSection(t, script, "cloudvm_mirror() {", "exec > ")
	harness := prelude + `
CLOUDVM_SERIAL="` + console + `"
{
  echo first
  rm -f "$CLOUDVM_SERIAL"
  echo second
} | cloudvm_mirror "` + logFile + `"
`
	runBash(t, harness)
	if got, _ := os.ReadFile(console); string(got) != "second\n" {
		t.Fatalf("console must be reopened per line, got %q", got)
	}
	if got, _ := os.ReadFile(logFile); string(got) != "first\nsecond\n" {
		t.Fatalf("log file got %q", got)
	}
}
