// Resolves the workload container's environment variables that name a source instead of a value, as the kubelet
// does: Secret keys from the create request or the Kubernetes API, and the pod's own name, namespace and UID.
package main

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// ResolveEnvSources returns the value of every environment variable of the pod's first container that names a
// secretKeyRef or a pod identity fieldRef, keyed by variable name. A variable whose optional Secret or key is missing is
// left out and a required one that is missing fails the pod. Other sources, such as configMapKeyRef, resourceFieldRef
// or a fieldRef to the pod's status, are left out, so they keep the empty value they had before.
func ResolveEnvSources(ctx context.Context, pod *Pod, provided []corev1.Secret, resolver SecretResolver) (map[string]string, error) {
	if len(pod.Spec.Containers) == 0 {
		return nil, nil
	}
	namespace := pod.Metadata.Namespace
	if namespace == "" {
		namespace = "default"
	}
	values := map[string]string{}
	secrets := map[string]map[string][]byte{}
	for _, env := range pod.Spec.Containers[0].Env {
		source := env.ValueFrom
		if source == nil {
			continue
		}
		switch {
		case source.SecretKeyRef != nil:
			ref := source.SecretKeyRef
			optional := ref.Optional != nil && *ref.Optional
			data, ok := secrets[ref.Name]
			if !ok {
				var err error
				data, err = pullSecretData(ctx, provided, resolver, namespace, ref.Name)
				if err != nil {
					if optional {
						continue
					}
					return nil, fmt.Errorf("env %s: secret %s/%s: %w", env.Name, namespace, ref.Name, err)
				}
				secrets[ref.Name] = data
			}
			value, ok := data[ref.Key]
			if !ok {
				if optional {
					continue
				}
				return nil, fmt.Errorf("env %s: secret %s/%s has no key %q", env.Name, namespace, ref.Name, ref.Key)
			}
			values[env.Name] = string(value)
		case source.FieldRef != nil:
			switch source.FieldRef.FieldPath {
			case "metadata.name":
				values[env.Name] = pod.Metadata.Name
			case "metadata.namespace":
				values[env.Name] = namespace
			case "metadata.uid":
				values[env.Name] = pod.Metadata.UID
			}
		}
	}
	return values, nil
}
