// Resolves the volumes a pod's containers mount into what the in-VM runner materializes: emptyDir and hostPath
// directories, and the files of Secret, ConfigMap, downward API and projected volumes, service account tokens included.
package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// The kinds of PodVolume.
const (
	VolumeKindEmptyDir = "emptyDir"
	VolumeKindHostPath = "hostPath"
	// VolumeKindFiles is a read-only directory of files the plugin resolved, kept on the worker's tmpfs.
	VolumeKindFiles = "files"
)

// serviceAccountMountPath is where the kubelet mounts the API credentials of the pod's service account.
const serviceAccountMountPath = "/var/run/secrets/kubernetes.io/serviceaccount"

// serviceAccountVolumePrefix starts the name of the API credentials volume the ServiceAccount admission plugin adds.
const serviceAccountVolumePrefix = "kube-api-access-"

// defaultTokenExpirationSeconds is the lifetime of a projected service account token that does not set one.
const defaultTokenExpirationSeconds = 3600

// PodVolume is a pod volume as the runner materializes it.
type PodVolume struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
	// Medium Memory backs an emptyDir with tmpfs, of SizeLimitBytes when set.
	Medium         string `json:"medium,omitempty"`
	SizeLimitBytes int64  `json:"sizeLimitBytes,omitempty"`
	// Path and Type are a hostPath's host path and type check.
	Path  string       `json:"path,omitempty"`
	Type  string       `json:"type,omitempty"`
	Files []SecretFile `json:"files,omitempty"`
}

// String keeps file contents, which hold secrets and tokens, out of logs.
func (v PodVolume) String() string {
	return fmt.Sprintf("PodVolume{Name:%s, Kind:%s, Files:%d}", v.Name, v.Kind, len(v.Files))
}

// ContainerMount is one volume mount of a container.
type ContainerMount struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
	SubPath   string `json:"subPath,omitempty"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
	// Propagation is rslave for HostToContainer and rshared for Bidirectional.
	Propagation string `json:"propagation,omitempty"`
}

// PodToken is a projected service account token file of a pod volume, which the plugin refreshes.
type PodToken struct {
	Volume            string
	Path              string
	Mode              int32
	Audience          string
	ExpirationSeconds int64
	IssuedAt          time.Time
	ExpiresAt         time.Time
}

// runContainers are the containers the runner runs: every init container and the first regular one.
func runContainers(pod *Pod) []*ContainerSpec {
	out := make([]*ContainerSpec, 0, len(pod.Spec.InitContainers)+1)
	for i := range pod.Spec.InitContainers {
		out = append(out, &pod.Spec.InitContainers[i])
	}
	if len(pod.Spec.Containers) > 0 {
		out = append(out, &pod.Spec.Containers[0])
	}
	return out
}

// AddServiceAccountVolume gives the pod the API credentials volume the ServiceAccount admission plugin adds, when the
// pod lacks it and neither the pod nor its ServiceAccount opts out of automounting, and mounts it in every container
// without a mount at serviceAccountMountPath.
func AddServiceAccountVolume(ctx context.Context, pod *Pod, automount ServiceAccountAutomount) error {
	for _, v := range pod.Spec.Volumes {
		if strings.HasPrefix(v.Name, serviceAccountVolumePrefix) {
			return nil
		}
	}
	var missing []*ContainerSpec
	for _, c := range runContainers(pod) {
		mounted := false
		for _, m := range c.VolumeMounts {
			mounted = mounted || strings.TrimSuffix(m.MountPath, "/") == serviceAccountMountPath
		}
		if !mounted {
			missing = append(missing, c)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	enabled := pod.Spec.AutomountServiceAccountToken
	if enabled == nil {
		if automount == nil {
			return nil
		}
		sa := pod.Spec.ServiceAccountName
		if sa == "" {
			sa = "default"
		}
		var err error
		if enabled, err = automount.ServiceAccountAutomount(ctx, podNamespace(pod), sa); err != nil {
			return err
		}
	}
	if enabled != nil && !*enabled {
		return nil
	}
	name := serviceAccountVolumePrefix + "cloudvm"
	expiration := int64(defaultTokenExpirationSeconds)
	mode := int32(0o644)
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
		DefaultMode: &mode,
		Sources: []corev1.VolumeProjection{
			{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: &expiration}},
			{ConfigMap: &corev1.ConfigMapProjection{
				LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"},
				Items:                []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}},
			}},
			{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{
				{Path: "namespace", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}},
			}}},
		},
	}}})
	for _, c := range missing {
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: name, MountPath: serviceAccountMountPath, ReadOnly: true})
	}
	return nil
}

// ResolvePodVolumes returns every volume a run container mounts, with the files of file volumes, and the service
// account tokens it minted for them. A mount of a volume type a worker cannot provide is an error.
func ResolvePodVolumes(ctx context.Context, pod *Pod, src *PodSources) ([]PodVolume, []PodToken, error) {
	mounted := map[string]bool{}
	for _, c := range runContainers(pod) {
		for _, m := range c.VolumeMounts {
			mounted[m.Name] = true
		}
	}
	var volumes []PodVolume
	var tokens []PodToken
	for i := range pod.Spec.Volumes {
		v := &pod.Spec.Volumes[i]
		if !mounted[v.Name] {
			continue
		}
		pv, toks, err := src.resolveVolume(ctx, pod, v)
		if err != nil {
			return nil, nil, fmt.Errorf("volume %q: %w", v.Name, err)
		}
		volumes = append(volumes, pv)
		tokens = append(tokens, toks...)
		delete(mounted, v.Name)
	}
	if len(mounted) > 0 {
		return nil, nil, fmt.Errorf("volume mount %q names no volume of the pod", sortedKeys(mounted)[0])
	}
	return volumes, tokens, nil
}

func (src *PodSources) resolveVolume(ctx context.Context, pod *Pod, v *corev1.Volume) (PodVolume, []PodToken, error) {
	ns := podNamespace(pod)
	out := PodVolume{Name: v.Name, Kind: VolumeKindFiles}
	switch {
	case v.EmptyDir != nil:
		out.Kind, out.Medium = VolumeKindEmptyDir, string(v.EmptyDir.Medium)
		if v.EmptyDir.SizeLimit != nil {
			out.SizeLimitBytes = v.EmptyDir.SizeLimit.Value()
		}
		if out.Medium != "" && v.EmptyDir.Medium != corev1.StorageMediumMemory {
			return out, nil, fmt.Errorf("emptyDir medium %q is not supported", out.Medium)
		}
		return out, nil, nil
	case v.HostPath != nil:
		out.Kind, out.Path = VolumeKindHostPath, v.HostPath.Path
		if v.HostPath.Type != nil {
			out.Type = string(*v.HostPath.Type)
		}
		if !strings.HasPrefix(out.Path, "/") {
			return out, nil, fmt.Errorf("hostPath %q is not absolute", out.Path)
		}
		return out, nil, nil
	case v.Secret != nil:
		files, err := secretVolumeFiles(ctx, src.Provided, src.Secrets, ns, v.Secret)
		out.Files = files
		return out, nil, err
	case v.ConfigMap != nil:
		data, err := src.configMap(ctx, ns, v.ConfigMap.Name)
		out.Files, err = keyFiles(data, err, v.ConfigMap.Items, fileMode(v.ConfigMap.DefaultMode), v.ConfigMap.Optional, "configmap "+ns+"/"+v.ConfigMap.Name)
		return out, nil, err
	case v.DownwardAPI != nil:
		files, err := downwardFiles(pod, v.DownwardAPI.Items, fileMode(v.DownwardAPI.DefaultMode))
		out.Files = files
		return out, nil, err
	case v.Projected != nil:
		return src.projectedVolume(ctx, pod, v.Name, v.Projected)
	}
	return out, nil, fmt.Errorf("this volume type is not supported on CloudVM workers; use emptyDir, hostPath, secret, configMap, downwardAPI or projected")
}

func (src *PodSources) projectedVolume(ctx context.Context, pod *Pod, name string, p *corev1.ProjectedVolumeSource) (PodVolume, []PodToken, error) {
	ns := podNamespace(pod)
	out := PodVolume{Name: name, Kind: VolumeKindFiles}
	mode := fileMode(p.DefaultMode)
	var tokens []PodToken
	for _, s := range p.Sources {
		var files []SecretFile
		var err error
		switch {
		case s.Secret != nil:
			data, lookupErr := src.secret(ctx, ns, s.Secret.Name)
			files, err = keyFiles(data, lookupErr, s.Secret.Items, mode, s.Secret.Optional, "secret "+ns+"/"+s.Secret.Name)
		case s.ConfigMap != nil:
			data, lookupErr := src.configMap(ctx, ns, s.ConfigMap.Name)
			files, err = keyFiles(data, lookupErr, s.ConfigMap.Items, mode, s.ConfigMap.Optional, "configmap "+ns+"/"+s.ConfigMap.Name)
		case s.DownwardAPI != nil:
			files, err = downwardFiles(pod, s.DownwardAPI.Items, mode)
		case s.ServiceAccountToken != nil:
			tok, data, tokErr := src.mintToken(ctx, pod, name, s.ServiceAccountToken, mode)
			if tokErr != nil {
				return out, nil, tokErr
			}
			tokens = append(tokens, tok)
			files = []SecretFile{{Path: tok.Path, Data: data, Mode: mode}}
		default:
			return out, nil, fmt.Errorf("projected sources other than secret, configMap, downwardAPI and serviceAccountToken are not supported")
		}
		if err != nil {
			return out, nil, err
		}
		out.Files = append(out.Files, files...)
	}
	sort.Slice(out.Files, func(i, j int) bool { return out.Files[i].Path < out.Files[j].Path })
	return out, tokens, nil
}

// PodTokenProjections returns the service account token files of the pod's mounted volumes, without minting them.
func PodTokenProjections(pod *Pod) []PodToken {
	mounted := map[string]bool{}
	for _, c := range runContainers(pod) {
		for _, m := range c.VolumeMounts {
			mounted[m.Name] = true
		}
	}
	var out []PodToken
	for _, v := range pod.Spec.Volumes {
		if v.Projected == nil || !mounted[v.Name] {
			continue
		}
		for _, s := range v.Projected.Sources {
			if s.ServiceAccountToken != nil {
				out = append(out, tokenProjection(v.Name, s.ServiceAccountToken, fileMode(v.Projected.DefaultMode)))
			}
		}
	}
	return out
}

func tokenProjection(volume string, p *corev1.ServiceAccountTokenProjection, mode int32) PodToken {
	expiration := int64(defaultTokenExpirationSeconds)
	if p.ExpirationSeconds != nil {
		expiration = *p.ExpirationSeconds
	}
	return PodToken{Volume: volume, Path: p.Path, Mode: mode, Audience: p.Audience, ExpirationSeconds: expiration}
}

func (src *PodSources) mintToken(ctx context.Context, pod *Pod, volume string, p *corev1.ServiceAccountTokenProjection, mode int32) (PodToken, []byte, error) {
	tok := tokenProjection(volume, p, mode)
	if err := validateRelativePath(tok.Path, false); err != nil {
		return tok, nil, fmt.Errorf("serviceAccountToken path %q: %w", tok.Path, err)
	}
	if src.Tokens == nil {
		return tok, nil, fmt.Errorf("cannot mint a service account token: no Kubernetes API access")
	}
	token, expiresAt, err := src.Tokens.CreatePodToken(ctx, podRefOf(pod), tok.Audience, tok.ExpirationSeconds)
	if err != nil {
		return tok, nil, err
	}
	tok.IssuedAt, tok.ExpiresAt = time.Now(), expiresAt
	return tok, []byte(token), nil
}

func podRefOf(pod *Pod) PodRef {
	return PodRef{Namespace: podNamespace(pod), Name: pod.Metadata.Name, UID: pod.Metadata.UID, ServiceAccount: pod.Spec.ServiceAccountName}
}

// downwardFiles renders downward API volume items: pod metadata fields and container resources.
func downwardFiles(pod *Pod, items []corev1.DownwardAPIVolumeFile, mode int32) ([]SecretFile, error) {
	var files []SecretFile
	for _, item := range items {
		if err := validateRelativePath(item.Path, false); err != nil {
			return nil, fmt.Errorf("downwardAPI file %q: %w", item.Path, err)
		}
		var value string
		var err error
		switch {
		case item.FieldRef != nil:
			value, err = podFieldValue(pod, item.FieldRef.FieldPath)
		case item.ResourceFieldRef != nil:
			value, _, err = resourceFieldValue(pod, nil, item.ResourceFieldRef)
		}
		if err != nil {
			return nil, fmt.Errorf("downwardAPI file %q: %w", item.Path, err)
		}
		itemMode := mode
		if item.Mode != nil {
			itemMode = *item.Mode
		}
		files = append(files, SecretFile{Path: item.Path, Data: []byte(value), Mode: itemMode})
	}
	return files, nil
}

func fileMode(m *int32) int32 {
	if m != nil {
		return *m
	}
	return defaultSecretFileMode
}

// ContainerMounts returns the container's volume mounts as the runner applies them: file volumes always read-only,
// as the kubelet mounts them.
func ContainerMounts(c *ContainerSpec, volumes []PodVolume) ([]ContainerMount, error) {
	kinds := make(map[string]string, len(volumes))
	for _, v := range volumes {
		kinds[v.Name] = v.Kind
	}
	out := make([]ContainerMount, 0, len(c.VolumeMounts))
	for _, m := range c.VolumeMounts {
		if m.SubPathExpr != "" {
			return nil, fmt.Errorf("volume mount %q: subPathExpr is not supported", m.Name)
		}
		if err := validateRelativePath(m.SubPath, true); err != nil {
			return nil, fmt.Errorf("volume mount %q: subPath: %w", m.Name, err)
		}
		if !strings.HasPrefix(m.MountPath, "/") || strings.ContainsAny(m.MountPath, ":,") {
			return nil, fmt.Errorf("volume mount %q: invalid mountPath %q", m.Name, m.MountPath)
		}
		cm := ContainerMount{Name: m.Name, MountPath: m.MountPath, SubPath: m.SubPath, ReadOnly: m.ReadOnly || kinds[m.Name] == VolumeKindFiles}
		if m.MountPropagation != nil {
			switch *m.MountPropagation {
			case corev1.MountPropagationHostToContainer:
				cm.Propagation = "rslave"
			case corev1.MountPropagationBidirectional:
				cm.Propagation = "rshared"
			}
		}
		out = append(out, cm)
	}
	return out, nil
}
