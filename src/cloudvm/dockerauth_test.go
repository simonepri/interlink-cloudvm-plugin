// Package main implements unit tests for Docker authentication handling.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestParseDockerConfigJSON_Standard(t *testing.T) {
	authVal := base64.StdEncoding.EncodeToString([]byte("dockeruser:dockerpassword"))
	raw := `{
		"auths": {
			"https://index.docker.io/v1/": {
				"username": "dockeruser",
				"password": "dockerpassword",
				"email": "docker@example.com",
				"auth": "` + authVal + `"
			}
		}
	}`

	cfg, err := ParseDockerConfigJSON(raw)
	if err != nil {
		t.Fatalf("unexpected error parsing standard docker config: %v", err)
	}
	if cfg == nil || len(cfg.Auths) != 1 {
		t.Fatalf("expected 1 auth entry, got %+v", cfg)
	}

	entry, ok := cfg.Auths["https://index.docker.io/v1/"]
	if !ok {
		t.Fatalf("missing expected registry entry")
	}
	if entry.Username != "dockeruser" || entry.Password != "dockerpassword" {
		t.Errorf("unexpected credentials: username=%s password=%s", entry.Username, entry.Password)
	}
	expectedAuth := base64.StdEncoding.EncodeToString([]byte("dockeruser:dockerpassword"))
	if entry.Auth != expectedAuth {
		t.Errorf("expected auth %s, got %s", expectedAuth, entry.Auth)
	}
}

func TestParseDockerConfigJSON_Base64(t *testing.T) {
	rawJSON := `{"auths":{"ghcr.io":{"auth":"dXNlcjpwYXNz"}}}`
	encoded := base64.StdEncoding.EncodeToString([]byte(rawJSON))

	cfg, err := ParseDockerConfigJSON(encoded)
	if err != nil {
		t.Fatalf("unexpected error parsing base64 docker config: %v", err)
	}
	if cfg == nil || len(cfg.Auths) != 1 {
		t.Fatalf("expected 1 auth entry, got %+v", cfg)
	}
	entry := cfg.Auths["ghcr.io"]
	if entry.Username != "user" || entry.Password != "pass" {
		t.Errorf("expected user:pass decoded from auth, got %s:%s", entry.Username, entry.Password)
	}
}

func TestParseDockerConfigJSON_LegacyDockercfg(t *testing.T) {
	raw := `{
		"quay.io": {
			"auth": "cXVheXVzZXI6cXVheXBhc3M=",
			"email": "quay@example.com"
		}
	}`

	cfg, err := ParseDockerConfigJSON(raw)
	if err != nil {
		t.Fatalf("unexpected error parsing legacy dockercfg: %v", err)
	}
	if cfg == nil || len(cfg.Auths) != 1 {
		t.Fatalf("expected 1 auth entry from legacy dockercfg, got %+v", cfg)
	}
	entry := cfg.Auths["quay.io"]
	if entry.Username != "quayuser" || entry.Password != "quaypass" {
		t.Errorf("unexpected decoded credentials: %s:%s", entry.Username, entry.Password)
	}
}

func TestParseDockerConfigJSON_KubernetesSecret(t *testing.T) {
	innerJSON := `{"auths":{"registry.gitlab.com":{"username":"gitlabuser","password":"gitlabtoken"}}}`
	b64Inner := base64.StdEncoding.EncodeToString([]byte(innerJSON))

	secretJSON := `{
		"apiVersion": "v1",
		"kind": "Secret",
		"type": "kubernetes.io/dockerconfigjson",
		"data": {
			".dockerconfigjson": "` + b64Inner + `"
		}
	}`

	cfg, err := ParseDockerConfigJSON(secretJSON)
	if err != nil {
		t.Fatalf("unexpected error parsing k8s secret JSON: %v", err)
	}
	entry, ok := cfg.Auths["registry.gitlab.com"]
	if !ok {
		t.Fatalf("missing registry.gitlab.com entry in %+v", cfg)
	}
	if entry.Username != "gitlabuser" || entry.Password != "gitlabtoken" {
		t.Errorf("unexpected credentials: %s:%s", entry.Username, entry.Password)
	}
	if entry.Auth == "" {
		t.Errorf("expected synthesized auth string, got empty")
	}

	// Also test stringData variant
	secretStringData := `{
		"apiVersion": "v1",
		"kind": "Secret",
		"stringData": {
			".dockerconfigjson": ` + strconvQuote(innerJSON) + `
		}
	}`
	cfg2, err := ParseDockerConfigJSON(secretStringData)
	if err != nil {
		t.Fatalf("unexpected error parsing k8s secret stringData: %v", err)
	}
	if len(cfg2.Auths) != 1 {
		t.Fatalf("expected 1 auth entry from stringData, got %+v", cfg2)
	}
}

func strconvQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func TestParseDockerConfigJSON_SynthesizeAndExtract(t *testing.T) {
	// 1. Username/Password with empty Auth
	raw1 := `{"auths":{"reg1.io":{"username":"admin","password":"secretpassword"}}}`
	cfg1, err := ParseDockerConfigJSON(raw1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expectedAuth := base64.StdEncoding.EncodeToString([]byte("admin:secretpassword"))
	if cfg1.Auths["reg1.io"].Auth != expectedAuth {
		t.Errorf("expected auth %s, got %s", expectedAuth, cfg1.Auths["reg1.io"].Auth)
	}

	// 2. Auth with empty Username/Password
	raw2 := `{"auths":{"reg2.io":{"auth":"` + expectedAuth + `"}}}`
	cfg2, err := ParseDockerConfigJSON(raw2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg2.Auths["reg2.io"].Username != "admin" || cfg2.Auths["reg2.io"].Password != "secretpassword" {
		t.Errorf("expected admin/secretpassword, got %s/%s", cfg2.Auths["reg2.io"].Username, cfg2.Auths["reg2.io"].Password)
	}
}

func TestParseDockerConfigJSON_Errors(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"empty string", ""},
		{"whitespace only", "   \n\t  "},
		{"invalid json", "{not-valid-json}"},
		{"empty json object", "{}"},
		{"invalid structure", `{"other":"field"}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseDockerConfigJSON(tc.in)
			if err == nil {
				t.Errorf("expected error for input %q, got nil", tc.in)
			}
		})
	}
}

func TestSerializeDockerConfig(t *testing.T) {
	cfg := &DockerConfigFile{
		Auths: map[string]DockerAuthEntry{
			"quay.io": {
				Username: "quayuser",
				Password: "quaypassword",
			},
		},
	}

	serialized, err := SerializeDockerConfig(cfg)
	if err != nil {
		t.Fatalf("SerializeDockerConfig failed: %v", err)
	}
	if !strings.Contains(serialized, "quay.io") || !strings.Contains(serialized, "quayuser") {
		t.Fatalf("serialized output missing expected fields: %s", serialized)
	}

	// Round-trip parse
	cfg2, err := ParseDockerConfigJSON(serialized)
	if err != nil {
		t.Fatalf("failed to round-trip parse serialized config: %v", err)
	}
	if cfg2.Auths["quay.io"].Username != "quayuser" {
		t.Errorf("expected quayuser after roundtrip, got %s", cfg2.Auths["quay.io"].Username)
	}

	// Empty config
	emptySerialized, err := SerializeDockerConfig(nil)
	if err != nil || emptySerialized != "" {
		t.Errorf("expected empty string for nil config, got %q (err: %v)", emptySerialized, err)
	}
}

func TestMergeDockerConfigs(t *testing.T) {
	cfg1 := &DockerConfigFile{
		Auths: map[string]DockerAuthEntry{
			"registry-a.io": {Username: "userA", Password: "pwdA"},
		},
	}
	cfg2 := &DockerConfigFile{
		Auths: map[string]DockerAuthEntry{
			"registry-b.io": {Username: "userB", Password: "pwdB"},
		},
	}

	merged := MergeDockerConfigs(cfg1, cfg2)
	if merged == nil || len(merged.Auths) != 2 {
		t.Fatalf("expected 2 merged entries, got %+v", merged)
	}
	if _, ok := merged.Auths["registry-a.io"]; !ok {
		t.Errorf("missing registry-a.io in merged config")
	}
	if _, ok := merged.Auths["registry-b.io"]; !ok {
		t.Errorf("missing registry-b.io in merged config")
	}
}

type mockSecretResolver struct {
	secrets map[string]map[string][]byte
	calls   int
}

func (m *mockSecretResolver) GetSecret(_ context.Context, namespace, name string) (map[string][]byte, error) {
	m.calls++
	key := fmt.Sprintf("%s/%s", namespace, name)
	if data, ok := m.secrets[key]; ok {
		return data, nil
	}
	return nil, fmt.Errorf("secret %s not found", key)
}

func TestResolveImagePullSecrets(t *testing.T) {
	ctx := context.Background()
	resolver := &mockSecretResolver{
		secrets: map[string]map[string][]byte{
			"default/regcred1": {
				corev1.DockerConfigJsonKey: []byte(`{"auths":{"ghcr.io":{"username":"u1","password":"p1"}}}`),
			},
			"default/regcred2": {
				corev1.DockerConfigJsonKey: []byte(`{"auths":{"docker.io":{"username":"u2","password":"p2"}}}`),
			},
			"default/invalid": {
				corev1.DockerConfigJsonKey: []byte(`{invalid-json}`),
			},
		},
	}

	// 1. Single secret
	res1 := ResolveImagePullSecrets(ctx, nil, resolver, "default", []corev1.LocalObjectReference{{Name: "regcred1"}})
	if !strings.Contains(res1, "ghcr.io") {
		t.Errorf("expected ghcr.io in resolved config, got: %s", res1)
	}

	// 2. Multiple secrets merged
	res2 := ResolveImagePullSecrets(ctx, nil, resolver, "default", []corev1.LocalObjectReference{
		{Name: "regcred1"},
		{Name: "regcred2"},
	})
	if !strings.Contains(res2, "ghcr.io") || !strings.Contains(res2, "docker.io") {
		t.Errorf("expected ghcr.io and docker.io in merged config, got: %s", res2)
	}

	// 3. Missing and malformed secrets are skipped, the readable one is kept
	res3 := ResolveImagePullSecrets(ctx, nil, resolver, "default", []corev1.LocalObjectReference{
		{Name: "missing"}, {Name: "invalid"}, {Name: "regcred2"},
	})
	if strings.Contains(res3, "ghcr.io") || !strings.Contains(res3, "docker.io") {
		t.Errorf("expected only docker.io after skipping unreadable secrets, got: %s", res3)
	}

	// 4. Only unreadable secrets yield no config
	if res := ResolveImagePullSecrets(ctx, nil, resolver, "default", []corev1.LocalObjectReference{{Name: "missing"}}); res != "" {
		t.Fatalf("expected empty config for missing secret, got %q", res)
	}

	// 5. Empty secretRefs returns empty string
	if res := ResolveImagePullSecrets(ctx, nil, resolver, "default", nil); res != "" {
		t.Fatalf("expected empty string, got %q", res)
	}

	// 6. Nil resolver and no request secrets returns empty string
	if res := ResolveImagePullSecrets(ctx, nil, nil, "default", []corev1.LocalObjectReference{{Name: "regcred1"}}); res != "" {
		t.Fatalf("expected empty string for nil resolver, got %q", res)
	}

	// 7. Request secrets take precedence over the resolver, which serves only the ones the request lacks
	provided := []corev1.Secret{{
		ObjectMeta: metav1.ObjectMeta{Name: "regcred1", Namespace: "default"},
		Data:       map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths":{"quay.io":{"username":"u3","password":"p3"}}}`)},
	}}
	res7 := ResolveImagePullSecrets(ctx, provided, resolver, "default", []corev1.LocalObjectReference{{Name: "regcred1"}, {Name: "regcred2"}})
	if !strings.Contains(res7, "quay.io") || strings.Contains(res7, "ghcr.io") || !strings.Contains(res7, "docker.io") {
		t.Errorf("expected quay.io from the request and docker.io from the resolver, got: %s", res7)
	}

	// 8. Request secrets work without a resolver
	if res := ResolveImagePullSecrets(ctx, provided, nil, "default", []corev1.LocalObjectReference{{Name: "regcred1"}}); !strings.Contains(res, "quay.io") {
		t.Errorf("expected quay.io from the request without a resolver, got: %s", res)
	}
}

func TestExtractDockerConfigFromSecretData(t *testing.T) {
	payload := []byte(`{"auths":{"ecr.aws":{"username":"aws","password":"token"}}}`)
	secretData := map[string][]byte{
		corev1.DockerConfigJsonKey: payload,
	}

	cfg, err := ExtractDockerConfigFromSecretData(secretData)
	if err != nil {
		t.Fatalf("ExtractDockerConfigFromSecretData failed: %v", err)
	}
	if cfg.Auths["ecr.aws"].Username != "aws" {
		t.Errorf("expected aws username, got: %s", cfg.Auths["ecr.aws"].Username)
	}

	// String data
	stringData := map[string]string{
		corev1.DockerConfigJsonKey: string(payload),
	}
	cfg2, err := ExtractDockerConfigFromSecretStringData(stringData)
	if err != nil {
		t.Fatalf("ExtractDockerConfigFromSecretStringData failed: %v", err)
	}
	if cfg2.Auths["ecr.aws"].Username != "aws" {
		t.Errorf("expected aws username, got: %s", cfg2.Auths["ecr.aws"].Username)
	}

	// Empty data returns error
	if _, err := ExtractDockerConfigFromSecretData(nil); err == nil {
		t.Errorf("expected error for nil secret data")
	}
}

func TestPodToInstanceSpec_ZeroAnnotationLeakage(t *testing.T) {
	pod := &Pod{
		Metadata: PodMeta{
			Name:      "ai-worker",
			Namespace: "production",
			Annotations: map[string]string{
				AnnotationDockerConfig: `{"auths":{"custom.registry.io":{"username":"svc-user","password":"svc-password"}}}`,
			},
		},
		Spec: PodSpecWrapper{
			Containers: []ContainerSpec{
				{
					Name:  "worker",
					Image: "custom.registry.io/org/model:v1",
				},
			},
		},
	}

	spec, err := PodToInstanceSpec(pod, "test-cluster", "us-west-2")
	if err != nil {
		t.Fatalf("PodToInstanceSpec failed: %v", err)
	}

	// Verify annotations are ignored to prevent credential leakage in pod annotations
	if spec.DockerConfigJSON != "" {
		t.Fatalf("expected empty DockerConfigJSON from PodToInstanceSpec (credentials must come from SecretResolver), got: %s", spec.DockerConfigJSON)
	}
}

func TestCloudInit_DockerAuth(t *testing.T) {
	cfg := &CloudInitConfig{
		PodName:              "auth-pod",
		PodNamespace:         "default",
		PodUID:               "pod-uid-101",
		RunnerAuthToken:      "token-xyz",
		TargetContainerImage: "private.registry.io/image:latest",
	}

	// Generate cloud-init script and verify zero credential leakage
	script, err := GenerateCloudInit(cfg)
	if err != nil {
		t.Fatalf("GenerateCloudInit failed: %v", err)
	}

	// Verify Python runner isolates credentials in ephemeral directory with 0600 mode
	if !strings.Contains(script, "dockerConfigJSON") {
		t.Errorf("cloud-init runner missing dockerConfigJSON parameter handling")
	}
	if !strings.Contains(script, "tempfile.mkdtemp") {
		t.Errorf("cloud-init runner missing isolated ephemeral directory creation")
	}
	if !strings.Contains(script, "0o600") {
		t.Errorf("cloud-init runner missing 0600 file permission enforcement")
	}
	// 3. Verify Python script syntax
	tmpDir := t.TempDir()
	runnerPyPath := filepath.Join(tmpDir, "runner.py")

	// Extract the python runner script from the template
	startMarker := "cat << 'RUNNER_EOF' > /usr/local/bin/cloudvm-runner.py\n"
	startIdx := strings.Index(script, "cat << 'RUNNER_EOF' > /usr/local/bin/cloudvm-runner.py")
	if startIdx == -1 {
		t.Fatalf("runner script heredoc start not found in generated cloud-init")
	}
	startIdx += len(startMarker)
	endIdx := strings.Index(script[startIdx:], "RUNNER_EOF")
	if endIdx == -1 {
		t.Fatalf("runner script heredoc end not found in generated cloud-init")
	}
	pyCode := script[startIdx : startIdx+endIdx]

	if err := os.WriteFile(runnerPyPath, []byte(pyCode), 0o600); err != nil {
		t.Fatalf("failed to write runner.py to temp dir: %v", err)
	}

	// Compile python script using python3 -m py_compile
	pyCmd := exec.Command("python3", "-m", "py_compile", runnerPyPath)
	if out, err := pyCmd.CombinedOutput(); err != nil {
		t.Fatalf("python syntax error in generated runner.py: %v\nOutput: %s", err, string(out))
	}
}

func TestHTTPContainerRunner_StartContainer_DockerConfigJSON(t *testing.T) {
	var receivedReq StartContainerRequest
	var authHeader string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		if r.URL.Path == "/containers/start" {
			if err := json.NewDecoder(r.Body).Decode(&receivedReq); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"starting"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	hostPort := strings.TrimPrefix(ts.URL, "http://")
	parts := strings.Split(hostPort, ":")
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		t.Fatalf("failed to parse port from test server URL: %v", err)
	}

	runner := NewHTTPContainerRunner(port)
	runner.client = ts.Client()

	node := &CloudVMNode{
		MeshIP:          parts[0],
		RunnerAuthToken: "secret-bearer-token",
	}

	dockerAuthData := `{"auths":{"ghcr.io":{"username":"ci-bot","password":"ghp_secret"}}}`
	req := &StartContainerRequest{
		PodUID:           "test-pod-uid-200",
		Namespace:        "default",
		PodName:          "runner-test-pod",
		ContainerName:    "worker",
		Image:            "ghcr.io/private/app:latest",
		DockerConfigJSON: dockerAuthData,
	}

	if err := runner.StartContainer(t.Context(), node, req); err != nil {
		t.Fatalf("StartContainer failed: %v", err)
	}

	if authHeader != "Bearer secret-bearer-token" {
		t.Errorf("expected Bearer secret-bearer-token, got: %s", authHeader)
	}
	if receivedReq.DockerConfigJSON != dockerAuthData {
		t.Errorf("expected DockerConfigJSON %s, got: %s", dockerAuthData, receivedReq.DockerConfigJSON)
	}
	if receivedReq.PodUID != "test-pod-uid-200" || receivedReq.Image != "ghcr.io/private/app:latest" {
		t.Errorf("unexpected request fields in received runner request: %+v", receivedReq)
	}
}

func TestServer_SecretResolver_EndToEnd(t *testing.T) {
	mockProv := NewMockProvider()
	server := NewServer(mockProv, "test-cluster")
	mockRunner := NewMockContainerRunner()
	server.SetContainerRunner(mockRunner)

	resolver := &mockSecretResolver{
		secrets: map[string]map[string][]byte{
			"production/my-registry-secret": {
				corev1.DockerConfigJsonKey: []byte(`{"auths":{"private.repo.org":{"username":"deployer","password":"sec-token"}}}`),
			},
		},
	}
	server.SetSecretResolver(resolver)

	pod := Pod{
		Metadata: PodMeta{
			Name:      "secure-workload",
			Namespace: "production",
			UID:       "pod-regcred-999",
		},
		Spec: PodSpecWrapper{
			ImagePullSecrets: []corev1.LocalObjectReference{
				{Name: "my-registry-secret"},
			},
			Containers: []ContainerSpec{
				{
					Name:  "worker",
					Image: "private.repo.org/models/llm:v2",
				},
			},
		},
	}

	body, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("marshal pod failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// 1. Verify SecretResolver was called for the pod namespace and secret name
	if resolver.calls != 1 {
		t.Fatalf("expected 1 call to SecretResolver.GetSecret, got %d", resolver.calls)
	}

	// 2. Verify StartContainer on runner received DockerConfigJSON
	startReq := mockRunner.GetStartRequest("pod-regcred-999")
	if startReq == nil {
		t.Fatal("expected recorded StartContainerRequest on runner")
	}
	if !strings.Contains(startReq.DockerConfigJSON, "private.repo.org") {
		t.Errorf("expected DockerConfigJSON in StartContainerRequest to contain private.repo.org, got: %s", startReq.DockerConfigJSON)
	}
	if !strings.Contains(startReq.DockerConfigJSON, "deployer") {
		t.Errorf("expected DockerConfigJSON in StartContainerRequest to contain username deployer, got: %s", startReq.DockerConfigJSON)
	}
}

func TestServer_RequestPullSecrets_EndToEnd(t *testing.T) {
	server := NewServer(NewMockProvider(), "test-cluster")
	mockRunner := NewMockContainerRunner()
	server.SetContainerRunner(mockRunner)
	// The resolver knows no secrets, as when the API denies access; the launch must not fail.
	resolver := &mockSecretResolver{secrets: map[string]map[string][]byte{}}
	server.SetSecretResolver(resolver)

	payload := map[string]any{
		"pod": Pod{
			Metadata: PodMeta{Name: "vk-workload", Namespace: "production", UID: "pod-request-secret-1"},
			Spec: PodSpecWrapper{
				ImagePullSecrets: []corev1.LocalObjectReference{{Name: "docker-hub-credentials"}, {Name: "unreadable"}},
				Containers:       []ContainerSpec{{Name: "worker", Image: "docker.io/private/app:v1"}},
			},
		},
		"secrets": []corev1.Secret{{
			ObjectMeta: metav1.ObjectMeta{Name: "docker-hub-credentials", Namespace: "production"},
			Type:       corev1.SecretTypeDockerConfigJson,
			Data:       map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths":{"docker.io":{"username":"hub","password":"hub-token"}}}`)},
		}},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload failed: %v", err)
	}

	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if resolver.calls != 1 {
		t.Fatalf("expected the resolver to be asked only for the secret the request lacks, got %d calls", resolver.calls)
	}
	startReq := mockRunner.GetStartRequest("pod-request-secret-1")
	if startReq == nil {
		t.Fatal("expected recorded StartContainerRequest on runner")
	}
	if !strings.Contains(startReq.DockerConfigJSON, "hub-token") {
		t.Errorf("expected DockerConfigJSON from the request secret, got: %s", startReq.DockerConfigJSON)
	}
}
