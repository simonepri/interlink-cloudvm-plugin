// Tests that every Kubernetes client of the plugin gets its config through kubeConfig, which TestMain replaces so no
// test reaches a real cluster.
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestKubernetesClientsLoadTheirConfigThroughKubeConfig(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if f == "kube_config.go" || f == "kube_config_test.go" {
			continue
		}
		src, err := os.ReadFile(f) //nolint:gosec // package source file
		if err != nil {
			t.Fatal(err)
		}
		for _, call := range []string{"rest.InClusterConfig(", "clientcmd.", "rest.InClusterConfig)"} {
			if strings.Contains(string(src), call) {
				t.Errorf("%s calls %s directly; build Kubernetes clients from kubeConfig so tests cannot reach a real cluster", f, strings.TrimSuffix(call, "("))
			}
		}
	}
	if cfg, err := kubeConfig(true); err == nil || cfg != nil {
		t.Fatalf("tests must get no Kubernetes config, got %v, %v", cfg, err)
	}
}
