// Resolves the Secret volumes the workload container mounts into the files the in-VM runner writes and mounts
// read-only, following the kubelet's rules for items, modes, optional Secrets, and subPath.
package main

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// defaultSecretFileMode is the kubelet's default mode for Secret volume files.
const defaultSecretFileMode = 0o644

// SecretFile is one file of a Secret volume, at a path relative to the volume root.
type SecretFile struct {
	Path string `json:"path"`
	Data []byte `json:"data"`
	Mode int32  `json:"mode"`
}

// SecretVolume is a Secret volume mount of the workload container.
type SecretVolume struct {
	MountPath string       `json:"mountPath"`
	SubPath   string       `json:"subPath,omitempty"`
	Files     []SecretFile `json:"files"`
}

// String keeps Secret data out of logs.
func (v SecretVolume) String() string {
	return fmt.Sprintf("SecretVolume{MountPath:%s, Files:%d}", v.MountPath, len(v.Files))
}

// ResolveSecretVolumes returns the Secret volumes the pod's first container mounts, with data from the create request
// or from resolver. Mounts of other volume types are left out. It fails when a required Secret or key is missing.
func ResolveSecretVolumes(ctx context.Context, pod *Pod, provided []corev1.Secret, resolver SecretResolver) ([]SecretVolume, error) {
	if len(pod.Spec.Containers) == 0 {
		return nil, nil
	}
	namespace := pod.Metadata.Namespace
	if namespace == "" {
		namespace = "default"
	}
	sources := make(map[string]*corev1.SecretVolumeSource, len(pod.Spec.Volumes))
	for i := range pod.Spec.Volumes {
		if v := pod.Spec.Volumes[i]; v.Secret != nil {
			sources[v.Name] = v.Secret
		}
	}
	var volumes []SecretVolume
	for _, mount := range pod.Spec.Containers[0].VolumeMounts {
		source, ok := sources[mount.Name]
		if !ok {
			continue
		}
		if err := validateRelativePath(mount.SubPath, true); err != nil {
			return nil, fmt.Errorf("volume mount %q: subPath: %w", mount.Name, err)
		}
		files, err := secretVolumeFiles(ctx, provided, resolver, namespace, source)
		if err != nil {
			return nil, fmt.Errorf("volume %q: %w", mount.Name, err)
		}
		volumes = append(volumes, SecretVolume{MountPath: mount.MountPath, SubPath: mount.SubPath, Files: files})
	}
	return volumes, nil
}

func secretVolumeFiles(ctx context.Context, provided []corev1.Secret, resolver SecretResolver, namespace string, source *corev1.SecretVolumeSource) ([]SecretFile, error) {
	data, err := pullSecretData(ctx, provided, resolver, namespace, source.SecretName)
	if err != nil {
		err = fmt.Errorf("secret %s/%s: %w", namespace, source.SecretName, err)
	}
	mode := int32(defaultSecretFileMode)
	if source.DefaultMode != nil {
		mode = *source.DefaultMode
	}
	return keyFiles(data, err, source.Items, mode, source.Optional, "secret "+namespace+"/"+source.SecretName)
}

// keyFiles returns the files a Secret or ConfigMap volume source projects from data, which lookupErr says could not
// be read: every key, or only items, at mode unless an item sets its own. An optional source tolerates missing data.
func keyFiles(data map[string][]byte, lookupErr error, items []corev1.KeyToPath, mode int32, optionalRef *bool, what string) ([]SecretFile, error) {
	optional := optionalRef != nil && *optionalRef
	if lookupErr != nil {
		if optional {
			return nil, nil
		}
		return nil, lookupErr
	}
	var files []SecretFile
	if len(items) == 0 {
		for key, value := range data {
			files = append(files, SecretFile{Path: key, Data: value, Mode: mode})
		}
	}
	for _, item := range items {
		value, ok := data[item.Key]
		if !ok {
			if optional {
				continue
			}
			return nil, fmt.Errorf("%s has no key %q", what, item.Key)
		}
		itemMode := mode
		if item.Mode != nil {
			itemMode = *item.Mode
		}
		files = append(files, SecretFile{Path: item.Path, Data: value, Mode: itemMode})
	}
	for _, f := range files {
		if err := validateRelativePath(f.Path, false); err != nil {
			return nil, fmt.Errorf("%s: file %q: %w", what, f.Path, err)
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

// validateRelativePath rejects a path that is absolute or leaves its root; an empty path is valid only when allowEmpty.
func validateRelativePath(p string, allowEmpty bool) error {
	if p == "" {
		if allowEmpty {
			return nil
		}
		return fmt.Errorf("path is empty")
	}
	if path.IsAbs(p) {
		return fmt.Errorf("path is absolute")
	}
	for _, part := range strings.Split(p, "/") {
		if part == ".." {
			return fmt.Errorf("path contains '..'")
		}
	}
	return nil
}
