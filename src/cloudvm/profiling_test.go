// Tests worker profiling: the parca-agent config and boot steps, the runner's pod variables and ParcaGPU mount, and the
// gateway relay that authenticates profile uploads.
package main

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"
)

const operatorAgentConfig = `relabel_configs:
  - source_labels: [__meta_kubernetes_namespace]
    regex: team-.+
    action: keep
  - source_labels: [__meta_kubernetes_pod_label_profiling]
    regex: disabled
    action: drop
  - source_labels: [__meta_kubernetes_pod_label_ray_io_cluster]
    target_label: ray_cluster
  - source_labels: [__meta_kubernetes_pod_name]
    target_label: pod
`

func TestNewWorkerProfiling_MapsRunnerVariablesBeforeOperatorRules(t *testing.T) {
	p, err := NewWorkerProfiling([]byte(operatorAgentConfig), []string{"--off-cpu-threshold=0.001"})
	if err != nil {
		t.Fatal(err)
	}
	if got := p.PodLabelList(); got != "profiling,ray_io_cluster" {
		t.Errorf("pod labels = %q, want the labels the operator rules read", got)
	}
	var cfg struct {
		RelabelConfigs []map[string]any `json:"relabel_configs"`
	}
	if err := yaml.Unmarshal([]byte(p.Config), &cfg); err != nil {
		t.Fatal(err)
	}
	first := cfg.RelabelConfigs[0]
	if first["target_label"] != "__meta_kubernetes_namespace" || first["source_labels"].([]any)[0] != "__meta_env_var_CLOUDVM_POD_NAMESPACE" {
		t.Errorf("first rule must map the namespace variable, got %v", first)
	}
	if last := cfg.RelabelConfigs[len(cfg.RelabelConfigs)-1]; last["target_label"] != "pod" {
		t.Errorf("operator rules must follow the mapping rules unchanged, got last %v", last)
	}
	vars := strings.Join(p.EnvVars(), ",")
	for _, want := range []string{"CLOUDVM_POD_NAME", "CLOUDVM_POD_LABEL_ray_io_cluster"} {
		if !strings.Contains(vars, want) {
			t.Errorf("env vars %q lack %s", vars, want)
		}
	}
}

func TestNewWorkerProfiling_RejectsUnknownConfigKeys(t *testing.T) {
	if _, err := NewWorkerProfiling([]byte("relabel_config: []\n"), nil); err == nil {
		t.Fatal("a misspelled key must fail instead of dropping the operator's rules")
	}
}

func TestRenderCloudInit_ProfiledWorkerRunsParcaAgent(t *testing.T) {
	p, err := NewWorkerProfiling([]byte(operatorAgentConfig), []string{"--metadata-external-labels=cluster=c1", "--x=50%"})
	if err != nil {
		t.Fatal(err)
	}
	script, err := RenderCloudInit(&CloudInitConfig{PodName: "p", PodUID: "uid", TunnelEndpoint: "gw.example.com:7443", Profiling: p})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`CLOUDVM_PARCA_AGENT_SHA256="b27f1e844fb7e27c918021e0f8118381f3a95e13deddfadd8f9f56b36c103ad1"`,
		"--remote-store-address=unix:///run/cloudvm-profiles/profiles.sock --remote-store-insecure",
		"--include-env-var=CLOUDVM_POD_LABEL_profiling",
		"--metadata-external-labels=cluster=c1 --x=50%%",
		`CLOUDVM_PROFILES_SOCKET="/run/cloudvm-profiles/profiles.sock"`,
		`CLOUDVM_PROFILING=1 CLOUDVM_PROFILING_POD_LABELS="profiling,ray_io_cluster"`,
		"__meta_env_var_CLOUDVM_POD_NAMESPACE",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("profiled worker bootstrap lacks %q", want)
		}
	}
	checkBashSyntax(t, script)

	plain, err := RenderCloudInit(&CloudInitConfig{PodName: "p", PodUID: "uid", TunnelEndpoint: "gw.example.com:7443"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plain, "parca-agent") || strings.Contains(plain, "CLOUDVM_PROFILING") {
		t.Error("a worker without profiling must not install parca-agent")
	}
	checkBashSyntax(t, plain)
}

func checkBashSyntax(t *testing.T, script string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "user-data.sh")
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("bash", "-n", path).CombinedOutput(); err != nil { //nolint:gosec // test-rendered script
		t.Fatalf("bootstrap is not valid bash: %v\n%s", err, out)
	}
}

func TestRunnerScript_ProfiledWorkerNamesPodAndMountsParcaGPU(t *testing.T) {
	h, state := newPullingRunnerHarness(t)
	lib := filepath.Join(filepath.Dir(h.script), "libparcagpucupti.so")
	if err := os.WriteFile(lib, []byte("lib"), 0o600); err != nil {
		t.Fatal(err)
	}
	h.env = append(h.env, "CLOUDVM_PROFILING=1", "CLOUDVM_PROFILING_POD_LABELS=ray_io_cluster", "CLOUDVM_PARCAGPU_LIBRARY="+lib)
	defer h.start()()

	resp := h.do(http.MethodPost, "/containers/start", `{"podUID":"u1","namespace":"team-a","podName":"w-0","containerName":"ray-worker",
		"image":"busybox","nodeName":"vn-1","podLabels":{"ray.io/cluster":"rc","other":"x"},
		"env":{"CUDA_INJECTION64_PATH":"/opt/parcagpu/libparcagpucupti.so","PARCAGPU_PC_SAMPLING_RATE":"100"}}`)
	_ = resp.Body.Close()
	waitStatus(t, h, "u1", func(r ContainerStatusResult) bool { return r.Running })

	raw, _ := os.ReadFile(filepath.Join(state, "run"))
	run := string(raw)
	for _, want := range []string{
		"-e CLOUDVM_POD_NAMESPACE=team-a", "-e CLOUDVM_POD_NAME=w-0", "-e CLOUDVM_CONTAINER_NAME=ray-worker",
		"-e CLOUDVM_NODE_NAME=vn-1", "-e CLOUDVM_POD_LABEL_ray_io_cluster=rc",
		"-v " + lib + ":/opt/parcagpu/libparcagpucupti.so:ro", "-e CUDA_INJECTION64_PATH=/opt/parcagpu/libparcagpucupti.so",
	} {
		if !strings.Contains(run, want) {
			t.Errorf("run %q lacks %q", run, want)
		}
	}
	if strings.Contains(run, "CLOUDVM_POD_LABEL_other") {
		t.Errorf("run %q exposes a label the profiler does not read", run)
	}
}

func TestRunnerScript_DropsParcaGPUInjectionWithoutLibrary(t *testing.T) {
	h, state := newPullingRunnerHarness(t)
	h.env = append(h.env, "CLOUDVM_PARCAGPU_LIBRARY="+filepath.Join(filepath.Dir(h.script), "missing.so"))
	defer h.start()()

	resp := h.do(http.MethodPost, "/containers/start", `{"podUID":"u2","image":"busybox",
		"env":{"CUDA_INJECTION64_PATH":"/opt/parcagpu/libparcagpucupti.so","PARCAGPU_PC_SAMPLING_RATE":"100","KEEP":"1"}}`)
	_ = resp.Body.Close()
	waitStatus(t, h, "u2", func(r ContainerStatusResult) bool { return r.Running })

	raw, _ := os.ReadFile(filepath.Join(state, "run"))
	run := string(raw)
	if strings.Contains(run, "CUDA_INJECTION64_PATH") || strings.Contains(run, "PARCAGPU_PC_SAMPLING_RATE") || strings.Contains(run, "CLOUDVM_POD_") {
		t.Errorf("an unprofiled worker without the library must drop the injection, got %q", run)
	}
	if !strings.Contains(run, "-e KEEP=1") {
		t.Errorf("other variables must stay, got %q", run)
	}
}

// startH2CUpstream serves plaintext HTTP/2 and records the Authorization header of each call.
func h2cProtocols() *http.Protocols {
	var p http.Protocols
	p.SetUnencryptedHTTP2(true)
	return &p
}

func startH2CUpstream(t *testing.T, seen chan<- string) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.URL.Path + " " + r.Header.Get("Authorization")
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("Trailer", "Grpc-Status")
		_, _ = w.Write([]byte("reply"))
		w.Header().Set("Grpc-Status", "0")
	})
	server := &http.Server{Handler: handler, Protocols: h2cProtocols(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = server.Serve(l) }()
	t.Cleanup(func() { _ = server.Close() })
	return l.Addr().String()
}

func TestProfilesRelay_AuthenticatesUploadsAndRefusesOtherCalls(t *testing.T) {
	seen := make(chan string, 4)
	addr := startH2CUpstream(t, seen)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	relay, err := NewProfilesRelay(addr, true, tokenFile)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{
		Protocols: h2cProtocols(),
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			worker, gateway := net.Pipe()
			go relay.Serve(gateway, bufio.NewReader(gateway))
			return worker, nil
		},
	}}
	type result struct {
		header, trailer http.Header
		body            string
	}
	call := func(path string) result {
		req, _ := http.NewRequest(http.MethodPost, "http://relay"+path, strings.NewReader("msg"))
		req.Header.Set("Content-Type", "application/grpc")
		req.Header.Set("Authorization", "Bearer forged")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return result{header: resp.Header, trailer: resp.Trailer, body: string(body)}
	}

	resp := call("/parca.profilestore.v1alpha1.ProfileStoreService/Write")
	if got := <-seen; got != "/parca.profilestore.v1alpha1.ProfileStoreService/Write Bearer secret" {
		t.Errorf("upstream saw %q, want the plugin's token in place of the worker's", got)
	}
	if resp.body != "reply" || resp.trailer.Get("Grpc-Status") != "0" {
		t.Errorf("relay must pass the reply and its trailers, got body %q trailers %v", resp.body, resp.trailer)
	}

	resp = call("/parca.query.v1alpha1.QueryService/Query")
	if resp.header.Get("Grpc-Status") != "12" {
		t.Errorf("a query must be refused with Unimplemented, got headers %v", resp.header)
	}
	select {
	case got := <-seen:
		t.Errorf("a refused call reached the upstream: %q", got)
	default:
	}
}

func TestTunnelAgent_RelaysProfilerConnectionsThroughGateway(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	seen := make(chan string, 1)
	relay, err := NewProfilesRelay(startH2CUpstream(t, seen), true, "")
	if err != nil {
		t.Fatal(err)
	}
	f := newTunnelFixtureWith(t, TunnelSettings{ConnectTimeout: time.Minute}, func(c *TunnelGatewayConfig) { c.ProfilesRelay = relay })
	const instanceID, token = "i-profiled", "agent-token"
	f.addNode(instanceID, "eu-west-1", token)
	endpoint, caFile := startTLSTerminator(t, f.gateway.Addr().String())

	// Unix socket paths are limited to about 100 bytes, which a macOS test temp dir can exceed.
	dir, err := os.MkdirTemp("/tmp", "clvm-prof")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	tokenFile := filepath.Join(dir, "runner-token")
	agentFile := filepath.Join(dir, "agent.py")
	if err := os.WriteFile(tokenFile, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(agentFile, []byte(tunnelAgentScript), 0o600); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "profiles", "p.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var agentLog syncBuffer
	cmd := exec.CommandContext(ctx, python, "-I", agentFile) //nolint:gosec // test-controlled script path
	cmd.Env = append(os.Environ(),
		"CLOUDVM_TUNNEL_CA_FILE="+caFile, "CLOUDVM_TUNNEL_ENDPOINT="+endpoint, "CLOUDVM_INSTANCE_ID="+instanceID,
		"CLOUDVM_RUNNER_TOKEN_FILE="+tokenFile, "CLOUDVM_ATTESTATION_FILE="+filepath.Join(dir, "absent"),
		"CLOUDVM_TUNNEL_MAX_BACKOFF=0.2", "CLOUDVM_TCP_PROXY_PORT=0", "CLOUDVM_CONTAINER_LOGS=0",
		"CLOUDVM_PROFILES_SOCKET="+socket)
	cmd.Stderr = &agentLog
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("agent log:\n%s", agentLog.String())
		}
	})
	waitForWithin(t, 60*time.Second, "python agent session", func() bool { return f.registry.Has(instanceID) })
	if info, err := os.Stat(filepath.Dir(socket)); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("socket directory must be private to root, got %v %v", info, err)
	}

	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
		Protocols: h2cProtocols(),
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}}
	resp, err := client.Post("http://localhost/parca.profilestore.v1alpha1.ProfileStoreService/Write", "application/grpc", strings.NewReader("msg"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if got := <-seen; got != "/parca.profilestore.v1alpha1.ProfileStoreService/Write " {
		t.Errorf("upstream saw %q", got)
	}
	if resp.Trailer.Get("Grpc-Status") != "0" {
		t.Errorf("trailers %v, want the upstream's grpc status", resp.Trailer)
	}
}
