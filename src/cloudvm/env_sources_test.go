// Tests that a container's environment resolves like the kubelet's: API server variables, envFrom, every valueFrom
// source, and $(VAR) references in values and arguments.
package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type mapConfigMapResolver map[string]map[string][]byte

func (m mapConfigMapResolver) GetConfigMap(_ context.Context, namespace, name string) (map[string][]byte, error) {
	if data, ok := m[namespace+"/"+name]; ok {
		return data, nil
	}
	return nil, errors.New("not found")
}

func envPod(env ...EnvVar) *Pod {
	return &Pod{
		Metadata: PodMeta{Name: "p", Namespace: "team-a", UID: "u-1", Labels: map[string]string{"app": "x"}, Annotations: map[string]string{"a": "b"}},
		Spec: PodSpecWrapper{NodeName: "node-1", ServiceAccountName: "runner", Containers: []ContainerSpec{{Name: "main", Env: env, Resources: ContainerResourceRequirements{
			Requests: map[string]string{"cpu": "1500m", "memory": "1Gi"}, Limits: map[string]string{"memory": "2Gi"},
		}}}},
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

func resolveMain(t *testing.T, pod *Pod, src *PodSources) *ContainerEnv {
	t.Helper()
	env, err := ResolveContainerEnv(context.Background(), pod, &pod.Spec.Containers[0], src)
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestResolveContainerEnv(t *testing.T) {
	secrets := mapSecretResolver{"team-a/cache": {"url": []byte("redis://cache:6379")}, "team-a/bulk": {"TOKEN": []byte("t")}}
	configMaps := mapConfigMapResolver{"team-a/settings": {"level": []byte("debug"), "MODE": []byte("fast")}}
	sources := func() *PodSources { return &PodSources{Secrets: secrets, ConfigMaps: configMaps} }

	t.Run("secret and configmap keys, the request's copy first, and every pod field", func(t *testing.T) {
		provided := []corev1.Secret{{ObjectMeta: metav1.ObjectMeta{Name: "sent", Namespace: "team-a"}, Data: map[string][]byte{"k": []byte("v")}}}
		pod := envPod(EnvVar{Name: "LITERAL", Value: "x"}, secretEnv("URL", "cache", "url", false), secretEnv("SENT", "sent", "k", false),
			EnvVar{Name: "LEVEL", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "settings"}, Key: "level"}}},
			fieldEnv("NAME", "metadata.name"), fieldEnv("NS", "metadata.namespace"), fieldEnv("UID", "metadata.uid"),
			fieldEnv("APP", "metadata.labels['app']"), fieldEnv("ANN", "metadata.annotations['a']"),
			fieldEnv("NODE", "spec.nodeName"), fieldEnv("SA", "spec.serviceAccountName"))
		src := sources()
		src.Provided = provided
		got := resolveMain(t, pod, src).Vars
		want := map[string]string{"LITERAL": "x", "URL": "redis://cache:6379", "SENT": "v", "LEVEL": "debug", "NAME": "p", "NS": "team-a",
			"UID": "u-1", "APP": "x", "ANN": "b", "NODE": "node-1", "SA": "runner"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
	t.Run("resource fields round up to the divisor and a missing limit falls back to the request", func(t *testing.T) {
		res := func(name, r, div string) EnvVar {
			sel := &corev1.ResourceFieldSelector{Resource: r}
			if div != "" {
				sel.Divisor = resource.MustParse(div)
			}
			return EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{ResourceFieldRef: sel}}
		}
		pod := envPod(res("CPU", "limits.cpu", ""), res("CPU_M", "requests.cpu", "1m"), res("MEM_MI", "limits.memory", "1Mi"), res("EPH", "limits.ephemeral-storage", ""))
		got := resolveMain(t, pod, sources()).Vars
		want := map[string]string{"CPU": "2", "CPU_M": "1500", "MEM_MI": "2048"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
	t.Run("API server variables come first, envFrom next with its prefix, then env, and references expand", func(t *testing.T) {
		pod := envPod(EnvVar{Name: "KUBERNETES_SERVICE_PORT", Value: "6443"}, EnvVar{Name: "URL", Value: "https://$(KUBERNETES_SERVICE_HOST):$(KUBERNETES_SERVICE_PORT)/$(cfg_MODE)/$$(KEEP)/$(UNSET)"})
		pod.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{
			{Prefix: "cfg_", ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "settings"}}},
			{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "bulk"}}},
		}
		pod.Spec.Containers[0].Args = []string{"--token=$(TOKEN)"}
		src := sources()
		src.Base = APIServerEnv("172.20.0.1", 443)
		env := resolveMain(t, pod, src)
		for k, want := range map[string]string{
			"KUBERNETES_SERVICE_HOST": "172.20.0.1", "KUBERNETES_SERVICE_PORT": "6443", "KUBERNETES_SERVICE_PORT_HTTPS": "443",
			"KUBERNETES_PORT_443_TCP_ADDR": "172.20.0.1", "cfg_level": "debug", "TOKEN": "t",
			"URL": "https://172.20.0.1:6443/fast/$(KEEP)/$(UNSET)",
		} {
			if env.Vars[k] != want {
				t.Errorf("%s = %q, want %q", k, env.Vars[k], want)
			}
		}
		if !reflect.DeepEqual(env.Args, []string{"--token=t"}) {
			t.Errorf("args = %v", env.Args)
		}
	})
	t.Run("pod and host IP variables and their references are left for the runner", func(t *testing.T) {
		pod := envPod(fieldEnv("POD_IP", "status.podIP"), fieldEnv("HOST_IP", "status.hostIP"), EnvVar{Name: "ADDR", Value: "$(POD_IP):8080"})
		env := resolveMain(t, pod, sources())
		if !reflect.DeepEqual(env.HostIPVars, []string{"HOST_IP", "POD_IP"}) || env.Vars["ADDR"] != "$(POD_IP):8080" {
			t.Fatalf("got %+v", env)
		}
	})
	t.Run("optional missing sources are left out", func(t *testing.T) {
		optional := true
		pod := envPod(secretEnv("A", "absent", "k", true), secretEnv("B", "cache", "absent", true))
		pod.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "absent"}, Optional: &optional}}}
		if got := resolveMain(t, pod, sources()).Vars; len(got) != 0 {
			t.Fatalf("got %v; want nothing", got)
		}
	})
	for _, tc := range []struct {
		name string
		env  EnvVar
		want string
	}{
		{"a required missing secret", secretEnv("A", "absent", "k", false), "secret team-a/absent"},
		{"a required missing key", secretEnv("A", "cache", "absent", false), `has no key "absent"`},
		{"an unknown field", fieldEnv("A", "spec.hostname"), `fieldRef "spec.hostname" is not supported`},
	} {
		t.Run(tc.name+" fails the pod", func(t *testing.T) {
			pod := envPod(tc.env)
			_, err := ResolveContainerEnv(context.Background(), pod, &pod.Spec.Containers[0], sources())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}
