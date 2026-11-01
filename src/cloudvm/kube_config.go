// Loads the configuration every Kubernetes client of the plugin is built from, through one variable that tests replace
// so that no test reaches a real cluster.
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// kubeConfig returns the in-cluster config, or, when kubeconfigFallback is set and the plugin runs outside a cluster,
// the config KUBECONFIG or ~/.kube/config names.
var kubeConfig = func(kubeconfigFallback bool) (*rest.Config, error) {
	cfg, err := rest.InClusterConfig()
	if err == nil || !kubeconfigFallback {
		return cfg, err
	}
	path := os.Getenv("KUBECONFIG")
	if path == "" {
		if home, _ := os.UserHomeDir(); home != "" {
			path = filepath.Join(home, ".kube", "config")
		}
	}
	cfg, err = clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		return nil, fmt.Errorf("unable to load kubernetes config: %w", err)
	}
	return cfg, nil
}
