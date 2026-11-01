// Tests that the boot script runs the stargz snapshotter as containerd's lazy pull proxy with the operator's config, the
// Docker Hub mirrors and host credentials normal pulls use, and that LAZY_IMAGE_PULLS=false leaves it off.
package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pelletier/go-toml"
)

func TestStargzConfigTOML_MirrorsDockerHubPulls(t *testing.T) {
	cfg := CloudInitConfig{RegistryMirrors: []string{
		"https://mirror.gcr.io",
		"http://cache.example.internal:5000/",
		"registry.example.com/v2-proxy",
		"://bad",
	}}
	want := `[[resolver.host."docker.io".mirrors]]
host = "mirror.gcr.io"
[[resolver.host."docker.io".mirrors]]
host = "cache.example.internal:5000"
insecure = true
[[resolver.host."docker.io".mirrors]]
host = "registry.example.com/v2-proxy"
`
	if got := cfg.StargzConfigTOML(); got != want {
		t.Errorf("StargzConfigTOML:\n%s\nwant:\n%s", got, want)
	}
	if got := (&CloudInitConfig{}).StargzConfigTOML(); got != "" {
		t.Errorf("without mirrors the config should be empty, got %q", got)
	}
}

func TestStargzConfigTOML_OpensWithOperatorConfig(t *testing.T) {
	cfg := CloudInitConfig{
		StargzConfig:    "\nmax_concurrency = 16\n\n[blob]\nchunk_size = 67108864\n\n",
		RegistryMirrors: []string{"https://mirror.gcr.io"},
	}
	want := `max_concurrency = 16

[blob]
chunk_size = 67108864
[[resolver.host."docker.io".mirrors]]
host = "mirror.gcr.io"
`
	if got := cfg.StargzConfigTOML(); got != want {
		t.Errorf("StargzConfigTOML:\n%s\nwant:\n%s", got, want)
	}
	if got := (&CloudInitConfig{StargzConfig: "max_concurrency = 16"}).StargzConfigTOML(); got != "max_concurrency = 16\n" {
		t.Errorf("without mirrors the config should be the operator's alone, got %q", got)
	}
}

func TestServerDefaults_StargzConfigEnv(t *testing.T) {
	t.Setenv("STARGZ_CONFIG", "max_concurrency = 16")
	s := NewServer(NewMockProvider(), "test-cluster")
	spec := &InstanceSpec{}
	s.applyServerDefaults(spec)
	if spec.StargzConfig != "max_concurrency = 16" {
		t.Errorf("applyServerDefaults StargzConfig=%q, want the STARGZ_CONFIG value", spec.StargzConfig)
	}
	if got := s.BuildCloudInitConfig(&InstanceSpec{}).StargzConfig; got != "max_concurrency = 16" {
		t.Errorf("BuildCloudInitConfig StargzConfig=%q, want the STARGZ_CONFIG value", got)
	}
	if got := s.BuildCloudInitConfig(&InstanceSpec{StargzConfig: "max_concurrency = 4"}).StargzConfig; got != "max_concurrency = 4" {
		t.Errorf("BuildCloudInitConfig StargzConfig=%q, want the spec's own value", got)
	}
}

func TestRenderCloudInit_RunsStargzSnapshotterForLazyPulls(t *testing.T) {
	script, err := RenderCloudInit(&CloudInitConfig{PodName: "p", PodUID: "uid", RunnerAuthToken: "tok", RegistryMirrors: []string{"https://mirror.gcr.io"}, StargzConfig: "max_concurrency = 16"})
	if err != nil {
		t.Fatalf("render cloud-init: %v", err)
	}
	for _, want := range []string{
		`CLOUDVM_STARGZ_VERSION="0.18.2"`,
		`CLOUDVM_STARGZ_SHA256="515a3c3af0012f192ace31fb79e910597977c77227e976680aeaaef6e9ae50a9"`,
		"cat << 'STARGZ_EOF' > /etc/containerd-stargz-grpc/config.toml\nmax_concurrency = 16\n[[resolver.host.\"docker.io\".mirrors]]\nhost = \"mirror.gcr.io\"\nSTARGZ_EOF\n",
		"[proxy_plugins.stargz]\n  type = \"snapshot\"\n  address = \"/run/containerd-stargz-grpc/containerd-stargz-grpc.sock\"\n  [proxy_plugins.stargz.exports]\n    root = \"/var/lib/containerd-stargz-grpc\"\n",
		"    enable_remote_snapshot_annotations = \"true\"\nCONTAINERD_EOF",
		"Wants=stargz-snapshotter.service\\nAfter=stargz-snapshotter.service",
		"Type=notify",
		"Environment=HOME=/root DOCKER_CONFIG=/root/.docker",
		"StateDirectory=containerd-stargz-grpc",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("rendered script missing %q", want)
		}
	}
	assertBashSyntax(t, script)
	assertUserDataFits(t, script)
}

func TestRenderCloudInit_DisableLazyPullsLeavesStargzOff(t *testing.T) {
	script, err := RenderCloudInit(&CloudInitConfig{PodName: "p", PodUID: "uid", RunnerAuthToken: "tok", DisableLazyPulls: true})
	if err != nil {
		t.Fatalf("render cloud-init: %v", err)
	}
	for _, unwanted := range []string{"stargz-snapshotter.service", "proxy_plugins.stargz", "/usr/local/bin/containerd-stargz-grpc"} {
		if strings.Contains(script, unwanted) {
			t.Errorf("script with lazy pulls disabled contains %q", unwanted)
		}
	}
	if !strings.Contains(script, "# cloudvm-unpack-overlayfs") || !strings.Contains(script, "systemctl restart containerd") {
		t.Error("script with lazy pulls disabled must still configure and start containerd")
	}
	assertBashSyntax(t, script)
}

func TestServerDefaults_LazyImagePullsEnv(t *testing.T) {
	for env, disabled := range map[string]bool{"": false, "true": false, "false": true, "0": true} {
		t.Setenv("LAZY_IMAGE_PULLS", env)
		s := NewServer(NewMockProvider(), "test-cluster")
		spec := &InstanceSpec{}
		s.applyServerDefaults(spec)
		if spec.DisableLazyPulls != disabled {
			t.Errorf("LAZY_IMAGE_PULLS=%q: DisableLazyPulls=%v, want %v", env, spec.DisableLazyPulls, disabled)
		}
		if got := s.BuildCloudInitConfig(&InstanceSpec{}).DisableLazyPulls; got != disabled {
			t.Errorf("LAZY_IMAGE_PULLS=%q: BuildCloudInitConfig DisableLazyPulls=%v, want %v", env, got, disabled)
		}
	}
}

// stargzSnapshotterConfig mirrors the fields of the stargz snapshotter's config that operators tune and the plugin
// writes, with the snapshotter's own TOML tags.
type stargzSnapshotterConfig struct {
	MaxConcurrency     int64 `toml:"max_concurrency"`
	PrefetchTimeoutSec int64 `toml:"prefetch_timeout_sec"`
	PrefetchAsyncSize  int64 `toml:"prefetch_async_size"`
	Blob               struct {
		ChunkSize            int64 `toml:"chunk_size"`
		PrefetchChunkSize    int64 `toml:"prefetch_chunk_size"`
		ForceSingleRangeMode bool  `toml:"force_single_range_mode"`
	} `toml:"blob"`
	Resolver struct {
		Host map[string]struct {
			Mirrors []struct {
				Host     string `toml:"host"`
				Insecure bool   `toml:"insecure"`
			} `toml:"mirrors"`
		} `toml:"host"`
	} `toml:"resolver"`
}

// TestStargzConfigTOML_ParsesWithOperatorTables loads the rendered config with go-toml v1, the parser the pinned
// snapshotter loads its config with, so a table the operator and the plugin both declared would fail here as it
// would fail the snapshotter's start. The plugin writes only entries of the Docker Hub mirrors array, which join any
// mirrors the operator lists.
func TestStargzConfigTOML_ParsesWithOperatorTables(t *testing.T) {
	cfg := CloudInitConfig{
		StargzConfig: `max_concurrency = 16
prefetch_timeout_sec = 120
prefetch_async_size = 0

[blob]
chunk_size = 67108864
prefetch_chunk_size = 134217728
force_single_range_mode = true

[[resolver.host."docker.io".mirrors]]
host = "operator.example.com"
`,
		RegistryMirrors: []string{"https://mirror.gcr.io", "http://cache.example.internal:5000"},
	}
	tree, err := toml.Load(cfg.StargzConfigTOML())
	if err != nil {
		t.Fatalf("rendered config is not valid TOML: %v\n%s", err, cfg.StargzConfigTOML())
	}
	var got stargzSnapshotterConfig
	if err := tree.Unmarshal(&got); err != nil {
		t.Fatalf("unmarshal rendered config: %v", err)
	}
	if got.MaxConcurrency != 16 || got.PrefetchTimeoutSec != 120 || got.PrefetchAsyncSize != 0 {
		t.Errorf("top-level tuning = %+v", got)
	}
	if got.Blob.ChunkSize != 67108864 || got.Blob.PrefetchChunkSize != 134217728 || !got.Blob.ForceSingleRangeMode {
		t.Errorf("[blob] = %+v", got.Blob)
	}
	var hosts []string
	for _, m := range got.Resolver.Host["docker.io"].Mirrors {
		hosts = append(hosts, fmt.Sprintf("%s insecure=%v", m.Host, m.Insecure))
	}
	want := []string{"operator.example.com insecure=false", "mirror.gcr.io insecure=false", "cache.example.internal:5000 insecure=true"}
	if strings.Join(hosts, ",") != strings.Join(want, ",") {
		t.Errorf("docker.io mirrors = %v, want %v", hosts, want)
	}
}
