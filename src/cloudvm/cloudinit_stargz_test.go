// Tests that the boot script runs the stargz snapshotter as containerd's lazy pull proxy with the operator's config, the
// Docker Hub mirrors and host credentials normal pulls use, and that LAZY_IMAGE_PULLS=false leaves it off.
package main

import (
	"strings"
	"testing"
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
