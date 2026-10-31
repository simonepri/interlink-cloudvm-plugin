// Tests that environment variables naming a Secret key or the pod's identity resolve to values, like the kubelet's.
package main

import (
	"context"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func envPod(env ...EnvVar) *Pod {
	return &Pod{
		Metadata: PodMeta{Name: "p", Namespace: "team-a", UID: "u-1"},
		Spec:     PodSpecWrapper{Containers: []ContainerSpec{{Name: "main", Env: env}}},
	}
}

func secretEnv(name, secret, key string, optional bool) EnvVar {
	ref := &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: secret}, Key: key}
	if optional {
		ref.Optional = &optional
	}
	return EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: ref}}
}

func fieldEnv(name, path string) EnvVar {
	return EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: path}}}
}

func TestResolveEnvSources(t *testing.T) {
	resolver := mapSecretResolver{"team-a/cache": {"url": []byte("redis://cache:6379")}}

	t.Run("secret keys from the API, the request's copy first, and the pod's identity", func(t *testing.T) {
		provided := []corev1.Secret{{ObjectMeta: metav1.ObjectMeta{Name: "sent", Namespace: "team-a"}, Data: map[string][]byte{"k": []byte("v")}}}
		pod := envPod(EnvVar{Name: "LITERAL", Value: "x"}, secretEnv("URL", "cache", "url", false), secretEnv("SENT", "sent", "k", false),
			fieldEnv("NAME", "metadata.name"), fieldEnv("NS", "metadata.namespace"), fieldEnv("UID", "metadata.uid"))
		got, err := ResolveEnvSources(context.Background(), pod, provided, resolver)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]string{"URL": "redis://cache:6379", "SENT": "v", "NAME": "p", "NS": "team-a", "UID": "u-1"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
	t.Run("optional missing secrets and keys are left out", func(t *testing.T) {
		pod := envPod(secretEnv("A", "absent", "k", true), secretEnv("B", "cache", "absent", true))
		got, err := ResolveEnvSources(context.Background(), pod, nil, resolver)
		if err != nil || len(got) != 0 {
			t.Fatalf("got %v, %v; want nothing", got, err)
		}
	})
	t.Run("unsupported sources keep their empty value", func(t *testing.T) {
		pod := envPod(fieldEnv("IP", "status.podIP"), EnvVar{Name: "CM", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{Key: "k"}}})
		got, err := ResolveEnvSources(context.Background(), pod, nil, resolver)
		if err != nil || len(got) != 0 {
			t.Fatalf("got %v, %v; want nothing", got, err)
		}
	})
	for _, tc := range []struct{ name, want string }{{"absent", "secret team-a/absent"}, {"cache", `has no key "absent"`}} {
		t.Run("a required missing "+tc.name+" fails the pod", func(t *testing.T) {
			_, err := ResolveEnvSources(context.Background(), envPod(secretEnv("A", tc.name, "absent", false)), nil, resolver)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}
