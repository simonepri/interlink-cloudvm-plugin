// Tests how a pod spec turns into the runner's view of it: QoS cgroup, effective security settings, volumes,
// mounts, the service account token volume, and refused settings.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func int64p(v int64) *int64 { return &v }
func boolp(v bool) *bool    { return &v }

func TestPodCgroupParent(t *testing.T) {
	for _, tc := range []struct {
		name string
		init []ContainerSpec
		main ContainerResourceRequirements
		want string
	}{
		{"no requests or limits is BestEffort", nil, ContainerResourceRequirements{}, "/kubepods/besteffort/podu-1"},
		{"GPU-only limits stay BestEffort, like the kubelet's QoS", nil, ContainerResourceRequirements{Limits: map[string]string{"nvidia.com/gpu": "1"}}, "/kubepods/besteffort/podu-1"},
		{"a CPU request is Burstable", nil, ContainerResourceRequirements{Requests: map[string]string{"cpu": "1"}}, "/kubepods/burstable/podu-1"},
		{"equal limits and requests still get a QoS segment", nil, ContainerResourceRequirements{
			Requests: map[string]string{"cpu": "1", "memory": "1Gi"}, Limits: map[string]string{"cpu": "1", "memory": "1Gi"},
		}, "/kubepods/burstable/podu-1"},
		{"an init container's memory limit counts", []ContainerSpec{{Resources: ContainerResourceRequirements{Limits: map[string]string{"memory": "1Gi"}}}},
			ContainerResourceRequirements{}, "/kubepods/burstable/podu-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := &Pod{Spec: PodSpecWrapper{InitContainers: tc.init, Containers: []ContainerSpec{{Resources: tc.main}}}}
			if got := PodCgroupParent(pod, "u-1"); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestEffectiveSecurity(t *testing.T) {
	pod := &Pod{Spec: PodSpecWrapper{SecurityContext: &PodSecurityContext{
		RunAsUser: int64p(0), RunAsGroup: int64p(10), RunAsNonRoot: boolp(true), SupplementalGroups: []int64{20}, FSGroup: int64p(30),
		SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeUnconfined},
	}}}
	c := &ContainerSpec{SecurityContext: &SecurityContext{
		RunAsUser: int64p(1000), RunAsNonRoot: boolp(false), Privileged: boolp(true), ReadOnlyRootFilesystem: boolp(true),
		Capabilities: &corev1.Capabilities{Add: []corev1.Capability{"SYS_ADMIN"}, Drop: []corev1.Capability{"all"}},
	}}
	got := EffectiveSecurity(pod, c)
	want := ContainerSecurity{
		RunAsUser: int64p(1000), RunAsGroup: int64p(10), SupplementalGroups: []int64{20, 30}, Privileged: true,
		CapAdd: []string{"SYS_ADMIN"}, CapDrop: []string{"ALL"}, ReadOnlyRootFilesystem: true, SeccompUnconfined: true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	for _, tc := range []struct {
		name string
		ape  *bool
		want bool
	}{{"unset leaves no_new_privs off, as the kubelet does", nil, false}, {"false sets it", boolp(false), true}, {"true leaves it off", boolp(true), false}} {
		t.Run(tc.name, func(t *testing.T) {
			sec := EffectiveSecurity(&Pod{}, &ContainerSpec{SecurityContext: &SecurityContext{AllowPrivilegeEscalation: tc.ape}})
			if sec.NoNewPrivileges != tc.want {
				t.Fatalf("NoNewPrivileges = %v, want %v", sec.NoNewPrivileges, tc.want)
			}
		})
	}
	if sec := EffectiveSecurity(&Pod{}, &ContainerSpec{}); sec.RunAsUser != nil || sec.RunAsGroup != nil {
		t.Fatalf("a pod without a user runs as the image's user, got %+v", sec)
	}
}

func TestValidatePodRuntime(t *testing.T) {
	always := corev1.ContainerRestartPolicyAlways
	for _, tc := range []struct {
		name string
		pod  Pod
		want string
	}{
		{"sidecar init containers", Pod{Spec: PodSpecWrapper{InitContainers: []ContainerSpec{{Name: "proxy", RestartPolicy: &always}}}}, "sidecar"},
		{"sysctls", Pod{Spec: PodSpecWrapper{SecurityContext: &PodSecurityContext{Sysctls: []corev1.Sysctl{{Name: "net.core.somaxconn", Value: "1024"}}}}}, "sysctls"},
		{"node-local seccomp profiles", Pod{Spec: PodSpecWrapper{Containers: []ContainerSpec{{Name: "m", SecurityContext: &SecurityContext{
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeLocalhost},
		}}}}}, "Localhost"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidatePodRuntime(&tc.pod); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

type fakeTokenIssuer struct {
	calls    []string
	lifetime time.Duration
}

func (f *fakeTokenIssuer) CreatePodToken(_ context.Context, pod PodRef, audience string, exp int64) (string, time.Time, error) {
	f.calls = append(f.calls, pod.ServiceAccount+"|"+audience)
	lifetime := f.lifetime
	if lifetime == 0 {
		lifetime = time.Duration(exp) * time.Second
	}
	return "token-" + strconv.Itoa(len(f.calls)), time.Now().Add(lifetime), nil
}

type fakeAutomount struct{ value *bool }

func (f fakeAutomount) ServiceAccountAutomount(context.Context, string, string) (*bool, error) {
	return f.value, nil
}

func volumePod(volumes []corev1.Volume, mounts ...corev1.VolumeMount) *Pod {
	pod := envPod()
	pod.Spec.Volumes = volumes
	pod.Spec.Containers[0].VolumeMounts = mounts
	return pod
}

func TestResolvePodVolumes(t *testing.T) {
	dirType := corev1.HostPathDirectoryOrCreate
	memory := resource.MustParse("64Mi")
	hostToContainer := corev1.MountPropagationHostToContainer
	pod := volumePod([]corev1.Volume{
		{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "shm", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: &memory}}},
		{Name: "logs", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/var/log", Type: &dirType}}},
		{Name: "cfg", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: "settings"}, Items: []corev1.KeyToPath{{Key: "level", Path: "conf/level"}},
		}}},
		{Name: "info", VolumeSource: corev1.VolumeSource{DownwardAPI: &corev1.DownwardAPIVolumeSource{Items: []corev1.DownwardAPIVolumeFile{
			{Path: "labels", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.labels"}},
			{Path: "mem", ResourceFieldRef: &corev1.ResourceFieldSelector{ContainerName: "main", Resource: "limits.memory", Divisor: resource.MustParse("1Mi")}},
		}}}},
		{Name: "unused", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "x"}}},
	},
		corev1.VolumeMount{Name: "scratch", MountPath: "/scratch"}, corev1.VolumeMount{Name: "shm", MountPath: "/dev/shm"},
		corev1.VolumeMount{Name: "logs", MountPath: "/host/logs", ReadOnly: true, MountPropagation: &hostToContainer},
		corev1.VolumeMount{Name: "cfg", MountPath: "/etc/app"}, corev1.VolumeMount{Name: "info", MountPath: "/etc/podinfo"})
	src := &PodSources{ConfigMaps: mapConfigMapResolver{"team-a/settings": {"level": []byte("debug")}}}
	volumes, tokens, err := ResolvePodVolumes(context.Background(), pod, src)
	if err != nil {
		t.Fatal(err)
	}
	want := []PodVolume{
		{Name: "scratch", Kind: VolumeKindEmptyDir},
		{Name: "shm", Kind: VolumeKindEmptyDir, Medium: "Memory", SizeLimitBytes: 64 << 20},
		{Name: "logs", Kind: VolumeKindHostPath, Path: "/var/log", Type: "DirectoryOrCreate"},
		{Name: "cfg", Kind: VolumeKindFiles, Files: []SecretFile{{Path: "conf/level", Data: []byte("debug"), Mode: 0o644}}},
		{Name: "info", Kind: VolumeKindFiles, Files: []SecretFile{{Path: "labels", Data: []byte(`app="x"`), Mode: 0o644}, {Path: "mem", Data: []byte("2048"), Mode: 0o644}}},
	}
	if !reflect.DeepEqual(volumes, want) || len(tokens) != 0 {
		t.Fatalf("volumes = %v, tokens = %v\nwant %v", volumes, tokens, want)
	}
	mounts, err := ContainerMounts(&pod.Spec.Containers[0], volumes)
	if err != nil {
		t.Fatal(err)
	}
	wantMounts := []ContainerMount{
		{Name: "scratch", MountPath: "/scratch"}, {Name: "shm", MountPath: "/dev/shm"},
		{Name: "logs", MountPath: "/host/logs", ReadOnly: true, Propagation: "rslave"},
		{Name: "cfg", MountPath: "/etc/app", ReadOnly: true}, {Name: "info", MountPath: "/etc/podinfo", ReadOnly: true},
	}
	if !reflect.DeepEqual(mounts, wantMounts) {
		t.Fatalf("mounts = %+v, want %+v", mounts, wantMounts)
	}

	t.Run("a mounted volume type a worker cannot provide refuses the pod", func(t *testing.T) {
		pod := volumePod([]corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "x"}}}},
			corev1.VolumeMount{Name: "data", MountPath: "/data"})
		if _, _, err := ResolvePodVolumes(context.Background(), pod, &PodSources{}); err == nil || !strings.Contains(err.Error(), "not supported") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestServiceAccountTokenVolume(t *testing.T) {
	t.Run("a pod without the admission volume gets the kubelet's token, CA and namespace", func(t *testing.T) {
		pod := envPod()
		if err := AddServiceAccountVolume(context.Background(), pod, fakeAutomount{}); err != nil {
			t.Fatal(err)
		}
		issuer := &fakeTokenIssuer{}
		src := &PodSources{Tokens: issuer, ConfigMaps: mapConfigMapResolver{"team-a/kube-root-ca.crt": {"ca.crt": []byte("CA")}}}
		volumes, tokens, err := ResolvePodVolumes(context.Background(), pod, src)
		if err != nil {
			t.Fatal(err)
		}
		want := []SecretFile{{Path: "ca.crt", Data: []byte("CA"), Mode: 0o644}, {Path: "namespace", Data: []byte("team-a"), Mode: 0o644}, {Path: "token", Data: []byte("token-1"), Mode: 0o644}}
		if len(volumes) != 1 || !reflect.DeepEqual(volumes[0].Files, want) {
			t.Fatalf("volumes = %+v", volumes)
		}
		if !reflect.DeepEqual(issuer.calls, []string{"runner|"}) {
			t.Fatalf("token requests = %v, want one for the pod's service account with the API server's audience", issuer.calls)
		}
		if len(tokens) != 1 || tokens[0].ExpirationSeconds != 3600 || tokens[0].Volume != volumes[0].Name || tokens[0].ExpiresAt.IsZero() {
			t.Fatalf("tokens = %+v", tokens)
		}
		mounts := pod.Spec.Containers[0].VolumeMounts
		if len(mounts) != 1 || mounts[0].MountPath != serviceAccountMountPath || !mounts[0].ReadOnly {
			t.Fatalf("mounts = %+v", mounts)
		}
	})
	for _, tc := range []struct {
		name      string
		pod       *bool
		sa        *bool
		admission bool
	}{
		{"the pod opts out", boolp(false), nil, false},
		{"the service account opts out", nil, boolp(false), false},
		{"admission already added the volume", nil, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := envPod()
			pod.Spec.AutomountServiceAccountToken = tc.pod
			if tc.admission {
				pod.Spec.Volumes = []corev1.Volume{{Name: "kube-api-access-abcde", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{}}}}
			}
			if err := AddServiceAccountVolume(context.Background(), pod, fakeAutomount{tc.sa}); err != nil {
				t.Fatal(err)
			}
			if n := len(pod.Spec.Containers[0].VolumeMounts); n != 0 {
				t.Fatalf("got %d mounts, want none", n)
			}
		})
	}
}

func TestPodTokensRefreshBeforeEightyPercentOfLifetime(t *testing.T) {
	issuer := &fakeTokenIssuer{lifetime: time.Hour}
	pt := NewPodTokens(issuer)
	start := time.Now()
	now := start
	pt.now = func() time.Time { return now }
	pt.Track(PodRef{Namespace: "team-a", Name: "p", UID: "u-1", ServiceAccount: "runner"}, []PodToken{{
		Volume: "kube-api-access-x", Path: "token", Mode: 0o644, ExpirationSeconds: 3600, IssuedAt: start, ExpiresAt: start.Add(time.Hour),
	}})
	var delivered [][]VolumeFile
	deliver := func(_ context.Context, uid string, files []VolumeFile) error {
		if uid != "u-1" {
			t.Errorf("delivered to %s", uid)
		}
		delivered = append(delivered, files)
		return nil
	}

	now = start.Add(47 * time.Minute)
	pt.RefreshOnce(context.Background(), deliver)
	if len(issuer.calls) != 0 || len(delivered) != 0 {
		t.Fatalf("refreshed before 80%% of the lifetime: %v, %v", issuer.calls, delivered)
	}
	now = start.Add(49 * time.Minute)
	pt.RefreshOnce(context.Background(), deliver)
	if len(delivered) != 1 || delivered[0][0].Volume != "kube-api-access-x" || string(delivered[0][0].Data) != "token-1" {
		t.Fatalf("delivered = %v", delivered)
	}
	pt.RefreshOnce(context.Background(), deliver)
	if len(issuer.calls) != 1 || len(delivered) != 1 {
		t.Fatalf("a fresh token was minted or delivered again: %v, %d deliveries", issuer.calls, len(delivered))
	}

	now = start.Add(2 * time.Hour)
	pt.RefreshOnce(context.Background(), func(context.Context, string, []VolumeFile) error { return errPodCredentialsGone })
	if pt.Tracked("u-1") {
		t.Fatal("a pod that is gone is still tracked")
	}
}

func TestPodTokensDeliveryRetriesAfterAFailure(t *testing.T) {
	pt := NewPodTokens(&fakeTokenIssuer{})
	pt.Track(PodRef{Namespace: "team-a", Name: "p", UID: "u-1"}, []PodToken{{Volume: "v", Path: "token", ExpirationSeconds: 3600}})
	fail := true
	var got []VolumeFile
	deliver := func(_ context.Context, _ string, files []VolumeFile) error {
		if fail {
			return errors.New("runner unreachable")
		}
		got = files
		return nil
	}
	pt.RefreshOnce(context.Background(), deliver)
	fail = false
	pt.RefreshOnce(context.Background(), deliver)
	if len(got) != 1 || string(got[0].Data) != "token-1" {
		t.Fatalf("got %v, want the token minted in the first round", got)
	}
}

// fakeKube serves Secrets, ConfigMaps, tokens and the API server Service the way the Kubernetes API would.
type fakeKube struct {
	mapSecretResolver
	mapConfigMapResolver
	*fakeTokenIssuer
}

func (fakeKube) APIServerService(context.Context) (string, int32, error) {
	return "172.20.0.1", 443, nil
}

func (fakeKube) ServiceAccountAutomount(context.Context, string, string) (*bool, error) {
	return nil, nil
}

func TestServer_CreateRunsThePodLikeAKubeletAndRefreshesItsToken(t *testing.T) {
	srv := NewServer(NewMockProvider(), "test-cluster")
	runner := NewMockContainerRunner()
	srv.SetContainerRunner(runner)
	kube := fakeKube{mapSecretResolver{}, mapConfigMapResolver{"team-a/kube-root-ca.crt": {"ca.crt": []byte("CA")}}, &fakeTokenIssuer{}}
	srv.SetSecretResolver(kube)
	tokens := NewPodTokens(kube)
	srv.SetPodTokens(tokens)
	handler := srv.Routes()
	post := func(path string, body []byte) {
		t.Helper()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body)))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s returned %d: %s", path, rec.Code, rec.Body.String())
		}
	}

	var pod Pod
	if err := json.Unmarshal(credentialPodJSON("uid-k", nil), &pod); err != nil {
		t.Fatal(err)
	}
	pod.Spec.InitContainers = []ContainerSpec{{Name: "setup", Image: "alpine"}}
	pod.Spec.Containers[0].Env = []EnvVar{fieldEnv("POD_IP", "status.podIP")}
	pod.Spec.HostPID = true
	body, _ := json.Marshal(pod)
	post("/create", body)

	req := runner.GetStartRequest("uid-k")
	if req == nil || req.CgroupParent != "/kubepods/besteffort/poduid-k" || !req.HostPID || len(req.InitContainers) != 1 {
		t.Fatalf("start request = %+v", req)
	}
	if req.Env["KUBERNETES_SERVICE_HOST"] != "172.20.0.1" || req.InitContainers[0].Env["KUBERNETES_SERVICE_PORT"] != "443" ||
		!reflect.DeepEqual(req.HostIPEnv, []string{"POD_IP"}) {
		t.Fatalf("env = %v, init env = %v, host IP env = %v", req.Env, req.InitContainers[0].Env, req.HostIPEnv)
	}
	if len(req.Volumes) != 1 || len(req.Volumes[0].Files) != 3 || string(req.Volumes[0].Files[2].Data) != "token-1" ||
		len(req.Mounts) != 1 || req.Mounts[0].MountPath != serviceAccountMountPath || len(req.InitContainers[0].Mounts) != 1 {
		t.Fatalf("volumes = %v, mounts = %+v", req.Volumes, req.Mounts)
	}

	tokens.now = func() time.Time { return time.Now().Add(50 * time.Minute) }
	tokens.RefreshOnce(context.Background(), srv.deliverPodVolumeFiles)
	if pushed := runner.PushedVolumeFiles("uid-k"); len(pushed) != 1 || string(pushed[0].Data) != "token-2" || pushed[0].Path != "token" {
		t.Fatalf("pushed = %v", pushed)
	}

	// After a restart forgets the pod, the next status poll resumes refreshing its token.
	tokens.Forget("uid-k")
	post("/status", []byte("["+string(body)+"]"))
	if !tokens.Tracked("uid-k") {
		t.Fatal("expected the status poll to resume refreshing the pod's token")
	}
	post("/delete", body)
	if tokens.Tracked("uid-k") {
		t.Fatal("expected deletion to stop refreshing the token")
	}
}
