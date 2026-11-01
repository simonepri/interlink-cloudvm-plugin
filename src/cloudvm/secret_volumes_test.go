// Tests how Secret volumes of the workload container resolve into the files older runners mount, and that the runner
// writes file volumes and mounts them read-only into the container.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type mapSecretResolver map[string]map[string][]byte

func (m mapSecretResolver) GetSecret(_ context.Context, namespace, name string) (map[string][]byte, error) {
	if data, ok := m[namespace+"/"+name]; ok {
		return data, nil
	}
	return nil, errors.New("not found")
}

func secretPod(volumes []corev1.Volume, mounts ...corev1.VolumeMount) *Pod {
	return &Pod{
		Metadata: PodMeta{Name: "p", Namespace: "team-a"},
		Spec:     PodSpecWrapper{Volumes: volumes, Containers: []ContainerSpec{{Name: "main", VolumeMounts: mounts}}},
	}
}

func secretVolume(secret string, edit func(*corev1.SecretVolumeSource)) corev1.Volume {
	src := &corev1.SecretVolumeSource{SecretName: secret}
	if edit != nil {
		edit(src)
	}
	return corev1.Volume{Name: "s3", VolumeSource: corev1.VolumeSource{Secret: src}}
}

func TestResolveSecretVolumes(t *testing.T) {
	resolver := mapSecretResolver{"team-a/team-s3": {"credentials": []byte("[default]\n"), "config": []byte("cfg")}}
	mode := int32(0o400)
	optional := true

	t.Run("every key at the default mode, from the API when the request lacks the secret", func(t *testing.T) {
		pod := secretPod([]corev1.Volume{secretVolume("team-s3", nil), {Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
			corev1.VolumeMount{Name: "s3", MountPath: "/var/run/team-s3"}, corev1.VolumeMount{Name: "scratch", MountPath: "/scratch"})
		got, err := ResolveSecretVolumes(context.Background(), pod, nil, resolver)
		if err != nil {
			t.Fatal(err)
		}
		want := []SecretVolume{{MountPath: "/var/run/team-s3", Files: []SecretFile{
			{Path: "config", Data: []byte("cfg"), Mode: 0o644}, {Path: "credentials", Data: []byte("[default]\n"), Mode: 0o644},
		}}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
	t.Run("items, item modes, and the request's copy of the secret", func(t *testing.T) {
		provided := []corev1.Secret{{ObjectMeta: metav1.ObjectMeta{Name: "team-s3", Namespace: "team-a"}, Data: map[string][]byte{"credentials": []byte("sent")}}}
		pod := secretPod([]corev1.Volume{secretVolume("team-s3", func(s *corev1.SecretVolumeSource) {
			s.Items = []corev1.KeyToPath{{Key: "credentials", Path: "aws/credentials", Mode: &mode}}
		})}, corev1.VolumeMount{Name: "s3", MountPath: "/home/app/.aws", SubPath: "aws"})
		got, err := ResolveSecretVolumes(context.Background(), pod, provided, resolver)
		if err != nil {
			t.Fatal(err)
		}
		want := []SecretVolume{{MountPath: "/home/app/.aws", SubPath: "aws", Files: []SecretFile{{Path: "aws/credentials", Data: []byte("sent"), Mode: 0o400}}}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
	t.Run("an optional missing secret mounts empty", func(t *testing.T) {
		pod := secretPod([]corev1.Volume{secretVolume("absent", func(s *corev1.SecretVolumeSource) { s.Optional = &optional })},
			corev1.VolumeMount{Name: "s3", MountPath: "/s3"})
		got, err := ResolveSecretVolumes(context.Background(), pod, nil, resolver)
		if err != nil || len(got) != 1 || len(got[0].Files) != 0 {
			t.Fatalf("got %v, %v; want one empty volume", got, err)
		}
	})
	for name, pod := range map[string]*Pod{
		"a missing secret": secretPod([]corev1.Volume{secretVolume("absent", nil)}, corev1.VolumeMount{Name: "s3", MountPath: "/s3"}),
		"a missing key": secretPod([]corev1.Volume{secretVolume("team-s3", func(s *corev1.SecretVolumeSource) {
			s.Items = []corev1.KeyToPath{{Key: "absent", Path: "absent"}}
		})}, corev1.VolumeMount{Name: "s3", MountPath: "/s3"}),
		"an item path that leaves the volume": secretPod([]corev1.Volume{secretVolume("team-s3", func(s *corev1.SecretVolumeSource) {
			s.Items = []corev1.KeyToPath{{Key: "config", Path: "../escape"}}
		})}, corev1.VolumeMount{Name: "s3", MountPath: "/s3"}),
		"a subPath that leaves the volume": secretPod([]corev1.Volume{secretVolume("team-s3", nil)}, corev1.VolumeMount{Name: "s3", MountPath: "/s3", SubPath: "../x"}),
	} {
		t.Run(name+" fails", func(t *testing.T) {
			if _, err := ResolveSecretVolumes(context.Background(), pod, nil, resolver); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestRunnerScript_MountsSecretVolumesReadOnly(t *testing.T) {
	h := newRunnerHarness(t)
	stateDir := strings.TrimPrefix(h.env[len(h.env)-1], "FAKE_NERDCTL_STATE=")
	if err := os.WriteFile(filepath.Join(filepath.Dir(stateDir), "bin", "nerdctl"), []byte(fakeRecordingNerdctl), 0o755); err != nil { //nolint:gosec // test fixture must be executable
		t.Fatal(err)
	}
	volumesDir := filepath.Join(t.TempDir(), "volumes")
	h.env = append(h.env, "CLOUDVM_SECRET_VOLUMES_DIR="+volumesDir)
	defer h.start()()

	start := func(volumes []PodVolume, mounts []ContainerMount) *http.Response {
		t.Helper()
		body, err := json.Marshal(StartContainerRequest{PodUID: "uid-1", Image: "busybox", Volumes: volumes, Mounts: mounts})
		if err != nil {
			t.Fatal(err)
		}
		return h.do(http.MethodPost, "/containers/start", string(body))
	}

	resp := start([]PodVolume{
		{Name: "s3", Kind: VolumeKindFiles, Files: []SecretFile{{Path: "aws/credentials", Data: []byte("key"), Mode: 0o400}}},
		{Name: "empty", Kind: VolumeKindFiles},
	}, []ContainerMount{{Name: "s3", MountPath: "/s3", SubPath: "aws", ReadOnly: true}, {Name: "empty", MountPath: "/empty", ReadOnly: true}})
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("start returned HTTP %d", resp.StatusCode)
	}
	argsFile := filepath.Join(stateDir, "run-cloudvm-uid-1")
	waitForWithin(t, 10*time.Second, "container run", func() bool { _, err := os.Stat(argsFile); return err == nil })
	args, err := os.ReadFile(argsFile) //nolint:gosec // test-controlled path
	if err != nil {
		t.Fatal(err)
	}
	podDir := filepath.Join(volumesDir, "uid-1")
	for _, want := range []string{"-v\n" + filepath.Join(podDir, "s3", "aws") + ":/s3:ro\n", "-v\n" + filepath.Join(podDir, "empty") + ":/empty:ro\n"} {
		if !strings.Contains(string(args), want) {
			t.Errorf("run args lack %q:\n%s", want, args)
		}
	}
	file := filepath.Join(podDir, "s3", "aws", "credentials")
	data, err := os.ReadFile(file) //nolint:gosec // test-controlled path
	if err != nil || string(data) != "key" {
		t.Fatalf("secret file = %q, %v", data, err)
	}
	if info, err := os.Stat(file); err != nil || info.Mode().Perm() != 0o400 {
		t.Fatalf("secret file mode = %v, %v; want 0400", info.Mode().Perm(), err)
	}

	resp = h.do(http.MethodPost, "/containers/stop?podUID=uid-1", "")
	_ = resp.Body.Close()
	if _, err := os.Stat(podDir); !os.IsNotExist(err) {
		t.Fatalf("stop must remove the pod's secret files, stat: %v", err)
	}

	resp = start([]PodVolume{{Name: "s3", Kind: VolumeKindFiles, Files: []SecretFile{{Path: "../escape", Data: []byte("x"), Mode: 0o644}}}}, nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("a file path that leaves its volume returned HTTP %d, want 400", resp.StatusCode)
	}
	if _, err := os.Stat(filepath.Join(volumesDir, "uid-1", "escape")); !os.IsNotExist(err) {
		t.Fatalf("the runner wrote outside the volume, stat: %v", err)
	}
}
