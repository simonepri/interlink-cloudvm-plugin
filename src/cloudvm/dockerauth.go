// Package main provides Docker authentication configuration handling for InterLink CloudVM Provider.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// SecretResolver abstracts retrieval of Kubernetes Secret data payloads.
type SecretResolver interface {
	GetSecret(ctx context.Context, namespace, name string) (map[string][]byte, error)
}

// KubeSecretResolver implements SecretResolver by querying Kubernetes secrets via client-go.
type KubeSecretResolver struct {
	client kubernetes.Interface
}

// NewKubeSecretResolver creates a new KubeSecretResolver with the given Kubernetes client.
func NewKubeSecretResolver(client kubernetes.Interface) *KubeSecretResolver {
	return &KubeSecretResolver{client: client}
}

// NewKubeSecretResolverFromConfig creates a SecretResolver from in-cluster config or KUBECONFIG.
func NewKubeSecretResolverFromConfig() (*KubeSecretResolver, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			if home, _ := os.UserHomeDir(); home != "" {
				kubeconfig = filepath.Join(home, ".kube", "config")
			}
		}
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("unable to load kubernetes config: %w", err)
		}
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create kubernetes client: %w", err)
	}
	return NewKubeSecretResolver(cs), nil
}

// GetSecret fetches secret data for the given namespace and secret name.
func (r *KubeSecretResolver) GetSecret(ctx context.Context, namespace, name string) (map[string][]byte, error) {
	if r.client == nil {
		return nil, fmt.Errorf("kubernetes client not initialized")
	}
	sec, err := r.client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get secret %s/%s: %w", namespace, name, err)
	}
	return sec.Data, nil
}

// ResolveImagePullSecrets retrieves and consolidates Docker configs from Kubernetes imagePullSecrets using SecretResolver.
func ResolveImagePullSecrets(ctx context.Context, resolver SecretResolver, namespace string, secretRefs []corev1.LocalObjectReference) (string, error) {
	if resolver == nil || len(secretRefs) == 0 {
		return "", nil
	}
	if namespace == "" {
		namespace = "default"
	}
	var configs []*DockerConfigFile
	for _, ref := range secretRefs {
		if ref.Name == "" {
			continue
		}
		data, err := resolver.GetSecret(ctx, namespace, ref.Name)
		if err != nil {
			return "", fmt.Errorf("failed to get imagePullSecret %s/%s: %w", namespace, ref.Name, err)
		}
		cfg, err := ExtractDockerConfigFromSecretData(data)
		if err != nil {
			return "", fmt.Errorf("failed to extract docker auth from secret %s/%s: %w", namespace, ref.Name, err)
		}
		configs = append(configs, cfg)
	}
	merged := MergeDockerConfigs(configs...)
	if merged == nil {
		return "", nil
	}
	b, err := json.Marshal(merged)
	if err != nil {
		return "", fmt.Errorf("failed to serialize resolved docker config: %w", err)
	}
	return string(b), nil
}

// Annotation keys for Docker registry credentials on Pods.
const (
	AnnotationDockerConfig               = "cloudvm.vk.io/docker-config"
	AnnotationImagePullSecret            = "cloudvm.vk.io/image-pull-secret"  //nolint:gosec // annotation key, not a hardcoded secret
	AnnotationImagePullSecrets           = "cloudvm.vk.io/image-pull-secrets" //nolint:gosec // annotation key, not a hardcoded secret
	AnnotationDockerConfigPrefix         = "cloudvm.vk.io/docker-config."
	AnnotationDockerConfigSlashPrefix    = "cloudvm.vk.io/docker-config/"
	AnnotationImagePullSecretPrefix      = "cloudvm.vk.io/image-pull-secret." //nolint:gosec // annotation key, not a hardcoded secret
	AnnotationImagePullSecretSlashPrefix = "cloudvm.vk.io/image-pull-secret/" //nolint:gosec // annotation key, not a hardcoded secret
)

// DockerConfigFile represents a standard Docker ~/.docker/config.json (.dockerconfigjson) file.
type DockerConfigFile struct {
	Auths       map[string]DockerAuthEntry `json:"auths"`
	HTTPHeaders map[string]string          `json:"HttpHeaders,omitempty"`
}

// DockerAuthEntry represents authentication credentials for a single container registry.
type DockerAuthEntry struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	Email    string `json:"email,omitempty"`
	Auth     string `json:"auth,omitempty"`
}

// Normalize ensures Auth is derived from Username/Password or vice-versa.
func (e *DockerAuthEntry) Normalize() {
	if e.Auth == "" && (e.Username != "" || e.Password != "") {
		cred := e.Username + ":" + e.Password
		e.Auth = base64.StdEncoding.EncodeToString([]byte(cred))
	} else if e.Auth != "" && e.Username == "" && e.Password == "" {
		if dec, err := base64.StdEncoding.DecodeString(e.Auth); err == nil {
			parts := strings.SplitN(string(dec), ":", 2)
			e.Username = parts[0]
			if len(parts) > 1 {
				e.Password = parts[1]
			}
		}
	}
}

// normalizeConfig ensures all auth entries in cfg are normalized.
func normalizeConfig(cfg *DockerConfigFile) *DockerConfigFile {
	if cfg == nil {
		return nil
	}
	if cfg.Auths == nil {
		cfg.Auths = make(map[string]DockerAuthEntry)
	}
	for k, v := range cfg.Auths {
		v.Normalize()
		cfg.Auths[k] = v
	}
	return cfg
}

// ParseDockerConfigJSON parses raw or base64-encoded Docker auth configurations.
// It supports standard {"auths": {...}}, legacy {"registry": {"auth": ...}},
// and Kubernetes Secret wrappers {"data": {".dockerconfigjson": "..."}}.
func ParseDockerConfigJSON(raw string) (*DockerConfigFile, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, fmt.Errorf("docker auth configuration is empty")
	}

	// Try base64 decoding if not starting with "{"
	if !strings.HasPrefix(trimmed, "{") {
		decoded, err := base64.StdEncoding.DecodeString(trimmed)
		if err != nil {
			var urlErr error
			decoded, urlErr = base64.URLEncoding.DecodeString(trimmed)
			if urlErr != nil {
				return nil, fmt.Errorf("invalid base64 docker auth data: %w", err)
			}
		}
		trimmed = strings.TrimSpace(string(decoded))
		if trimmed == "" {
			return nil, fmt.Errorf("decoded docker auth data is empty")
		}
	}

	// 1. Try Kubernetes Secret wrapper: {"data": {".dockerconfigjson": "..."}}
	var secretWrapper struct {
		Data       map[string]string `json:"data"`
		StringData map[string]string `json:"stringData"`
	}
	if err := json.Unmarshal([]byte(trimmed), &secretWrapper); err == nil && (len(secretWrapper.Data) > 0 || len(secretWrapper.StringData) > 0) {
		if val, ok := secretWrapper.Data[corev1.DockerConfigJsonKey]; ok && val != "" {
			return ParseDockerConfigJSON(val)
		}
		if val, ok := secretWrapper.Data[corev1.DockerConfigKey]; ok && val != "" {
			return ParseDockerConfigJSON(val)
		}
		if val, ok := secretWrapper.StringData[corev1.DockerConfigJsonKey]; ok && val != "" {
			return ParseDockerConfigJSON(val)
		}
		if val, ok := secretWrapper.StringData[corev1.DockerConfigKey]; ok && val != "" {
			return ParseDockerConfigJSON(val)
		}
	}

	// 2. Try standard DockerConfigFile: {"auths": {...}}
	var standard DockerConfigFile
	if err := json.Unmarshal([]byte(trimmed), &standard); err == nil && standard.Auths != nil {
		return normalizeConfig(&standard), nil
	}

	// 3. Try legacy dockercfg: {"registry": {"auth": "..."}}
	var legacy map[string]DockerAuthEntry
	if err := json.Unmarshal([]byte(trimmed), &legacy); err == nil && len(legacy) > 0 {
		hasAuth := false
		for _, v := range legacy {
			if v.Auth != "" || v.Username != "" || v.Password != "" {
				hasAuth = true
				break
			}
		}
		if hasAuth {
			cfg := &DockerConfigFile{Auths: legacy}
			return normalizeConfig(cfg), nil
		}
	}

	return nil, fmt.Errorf("unrecognized docker auth configuration format")
}

// SerializeDockerConfig serializes DockerConfigFile to canonical JSON.
func SerializeDockerConfig(cfg *DockerConfigFile) (string, error) {
	if cfg == nil || len(cfg.Auths) == 0 {
		return "", nil
	}
	cfg = normalizeConfig(cfg)
	b, err := json.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("failed to serialize docker config: %w", err)
	}
	return string(b), nil
}

// Serialize returns the JSON representation of DockerConfigFile.
func (c *DockerConfigFile) Serialize() (string, error) {
	return SerializeDockerConfig(c)
}

// MergeDockerConfigs merges multiple DockerConfigFiles into one.
func MergeDockerConfigs(configs ...*DockerConfigFile) *DockerConfigFile {
	merged := &DockerConfigFile{
		Auths:       make(map[string]DockerAuthEntry),
		HTTPHeaders: make(map[string]string),
	}
	for _, cfg := range configs {
		if cfg == nil {
			continue
		}
		for reg, entry := range cfg.Auths {
			merged.Auths[reg] = entry
		}
		for k, v := range cfg.HTTPHeaders {
			merged.HTTPHeaders[k] = v
		}
	}
	if len(merged.Auths) == 0 && len(merged.HTTPHeaders) == 0 {
		return nil
	}
	return normalizeConfig(merged)
}

// ExtractDockerConfigFromSecretData extracts DockerConfigFile from k8s Secret Data map.
func ExtractDockerConfigFromSecretData(data map[string][]byte) (*DockerConfigFile, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("secret data is empty")
	}
	if b, ok := data[corev1.DockerConfigJsonKey]; ok && len(b) > 0 {
		return ParseDockerConfigJSON(string(b))
	}
	if b, ok := data[corev1.DockerConfigKey]; ok && len(b) > 0 {
		return ParseDockerConfigJSON(string(b))
	}
	return nil, fmt.Errorf("secret data does not contain %s or %s", corev1.DockerConfigJsonKey, corev1.DockerConfigKey)
}

// ExtractDockerConfigFromSecretStringData extracts DockerConfigFile from k8s Secret StringData map.
func ExtractDockerConfigFromSecretStringData(data map[string]string) (*DockerConfigFile, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("secret stringData is empty")
	}
	if s, ok := data[corev1.DockerConfigJsonKey]; ok && s != "" {
		return ParseDockerConfigJSON(s)
	}
	if s, ok := data[corev1.DockerConfigKey]; ok && s != "" {
		return ParseDockerConfigJSON(s)
	}
	return nil, fmt.Errorf("secret stringData does not contain %s or %s", corev1.DockerConfigJsonKey, corev1.DockerConfigKey)
}

// ExtractDockerConfig is deprecated; credentials must be resolved from Kubernetes imagePullSecrets via SecretResolver.
// This function always returns ("", nil) to prevent credential leakage through pod annotations.
func ExtractDockerConfig(_ *Pod) (string, error) {
	return "", nil
}
