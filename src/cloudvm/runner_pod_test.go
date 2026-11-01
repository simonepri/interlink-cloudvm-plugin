// Runs the embedded in-VM runner against a fake nerdctl to verify it runs a pod as the kubelet would: QoS cgroup,
// host namespaces, security flags, volumes, init containers in order, container IDs, and token file refreshes.
package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeContainerID is the 64-hex ID the fake nerdctl gives every container.
const fakeContainerID = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// fakePodNerdctl records each run's global and run arguments in $state/run-<name> and the run order in $state/order,
// answers wait with the next exit code listed in $state/exit-<name> (0 when none is left), image inspect with the
// user in $state/user, and inspect, ps and rm from the containers it created.
const fakePodNerdctl = `#!/usr/bin/env bash
state="$FAKE_NERDCTL_STATE"
globals=()
while [[ "$1" == --* ]]; do globals+=("$1"); shift; done
case "$1" in
  pull) exit 0 ;;
  image) if [ -f "$state/user" ]; then cat "$state/user"; else echo; fi; exit 0 ;;
  run)
    name=""; prev=""
    for a in "$@"; do [ "$prev" = "--name" ] && name="$a"; prev="$a"; done
    { for g in "${globals[@]}"; do echo "$g"; done; printf '%s\n' "$@"; } > "$state/run-$name"
    echo "$name" >> "$state/order"
    touch "$state/exists-$name"; exit 0 ;;
  wait)
    f="$state/exit-$2"; code=0
    if [ -s "$f" ]; then code=$(head -n 1 "$f"); tail -n +2 "$f" > "$f.tmp"; mv "$f.tmp" "$f"; fi
    echo "$code"; exit 0 ;;
  inspect)
    if [ -f "$state/exists-$2" ]; then
      printf '[{"Id":"` + fakeContainerID + `","State":{"Status":"running","Running":true,"ExitCode":0,"StartedAt":"2026-01-01T00:00:00Z","FinishedAt":"0001-01-01T00:00:00Z"}}]'
      exit 0
    fi
    echo "Error: No such object: $2" >&2; exit 1 ;;
  ps)
    filter=""; prev=""
    for a in "$@"; do [ "$prev" = "--filter" ] && filter="${a#name=}"; prev="$a"; done
    for f in "$state"/exists-*; do [ -e "$f" ] || continue; n="${f##*/exists-}"; case "$n" in "$filter"*) echo "$n" ;; esac; done
    exit 0 ;;
  rm) shift; [ "$1" = -f ] && shift; rm -f "$state/exists-$1"; exit 0 ;;
  *) exit 0 ;;
esac
`

// fakeMount records each mount and umount call instead of mounting.
const fakeMount = `#!/usr/bin/env bash
echo "$(basename "$0") $*" >> "$FAKE_NERDCTL_STATE/mounts"
`

type podRunnerHarness struct {
	*runnerHarness
	state, podDirs, filesDir string
}

func newPodRunnerHarness(t *testing.T) *podRunnerHarness {
	t.Helper()
	h := newRunnerHarness(t)
	dir := filepath.Dir(h.script)
	for name, body := range map[string]string{"nerdctl": fakePodNerdctl, "mount": fakeMount, "umount": fakeMount} {
		if err := os.WriteFile(filepath.Join(dir, "bin", name), []byte(body), 0o755); err != nil { //nolint:gosec // test fixture must be executable
			t.Fatal(err)
		}
	}
	p := &podRunnerHarness{runnerHarness: h, state: filepath.Join(dir, "state"), podDirs: filepath.Join(dir, "pods"), filesDir: filepath.Join(dir, "volumes")}
	h.env = append(h.env, "CLOUDVM_POD_DIRS="+p.podDirs, "CLOUDVM_SECRET_VOLUMES_DIR="+p.filesDir, "CLOUDVM_HOST_IP=192.0.2.10",
		"CLOUDVM_RESTART_BACKOFF_START=0.1", "CLOUDVM_RESTART_BACKOFF_MAX=0.2")
	return p
}

func (p *podRunnerHarness) startPod(req *StartContainerRequest) int {
	p.t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		p.t.Fatal(err)
	}
	resp := p.do(http.MethodPost, "/containers/start", string(body))
	_ = resp.Body.Close()
	return resp.StatusCode
}

func (p *podRunnerHarness) runArgs(name string) []string {
	p.t.Helper()
	data, err := os.ReadFile(filepath.Join(p.state, "run-"+name)) //nolint:gosec // test-controlled path
	if err != nil {
		p.t.Fatalf("container %s never ran: %v", name, err)
	}
	return strings.Split(strings.TrimSpace(string(data)), "\n")
}

func (p *podRunnerHarness) file(name string) string {
	data, _ := os.ReadFile(filepath.Join(p.state, name)) //nolint:gosec // test-controlled path
	return string(data)
}

// flagPairs joins each argument with the next one, so a flag and its value can be looked up as one string.
func flagPairs(args []string) map[string]bool {
	out := map[string]bool{}
	for i, a := range args {
		out[a] = true
		if i+1 < len(args) {
			out[a+" "+args[i+1]] = true
		}
	}
	return out
}

func TestRunnerScript_RunsPodLikeAKubelet(t *testing.T) {
	p := newPodRunnerHarness(t)
	hostDir := filepath.Join(t.TempDir(), "host", "logs")
	if err := os.WriteFile(filepath.Join(p.state, "exit-cloudvm-uid-1-init-0"), []byte("1\n0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	defer p.start()()

	gid := int64(os.Getgid())
	req := &StartContainerRequest{
		PodUID: "uid-1", PodName: "p", Namespace: "team-a", ContainerName: "main", Image: "busybox",
		Args: []string{"--ip=$(POD_IP)"}, Env: map[string]string{"POD_IP": "", "ADDR": "$(POD_IP):80", "A": "b"},
		HostIPEnv:    []string{"POD_IP"},
		CgroupParent: "/kubepods/burstable/poduid-1", HostPID: true, RestartPolicy: "OnFailure", FSGroup: &gid,
		Security: &ContainerSecurity{
			RunAsUser: int64p(0), Privileged: true, CapAdd: []string{"SYS_PTRACE"}, CapDrop: []string{"ALL"},
			NoNewPrivileges: true, ReadOnlyRootFilesystem: true, SupplementalGroups: []int64{5},
		},
		Volumes: []PodVolume{
			{Name: "scratch", Kind: VolumeKindEmptyDir},
			{Name: "shm", Kind: VolumeKindEmptyDir, Medium: "Memory", SizeLimitBytes: 1 << 20},
			{Name: "logs", Kind: VolumeKindHostPath, Path: hostDir, Type: "DirectoryOrCreate"},
			{Name: "kube-api-access-x", Kind: VolumeKindFiles, Files: []SecretFile{{Path: "token", Data: []byte("t1"), Mode: 0o644}}},
		},
		Mounts: []ContainerMount{
			{Name: "scratch", MountPath: "/scratch", SubPath: "work"}, {Name: "shm", MountPath: "/dev/shm"},
			{Name: "logs", MountPath: "/host/logs", ReadOnly: true, Propagation: "rslave"},
			{Name: "kube-api-access-x", MountPath: serviceAccountMountPath, ReadOnly: true},
		},
		InitContainers: []ContainerRun{
			{Name: "migrate", Image: "alpine", Command: []string{"sh", "-c", "exit 0"}, Mounts: []ContainerMount{{Name: "scratch", MountPath: "/scratch"}}},
			{Name: "warm", Image: "alpine", Security: &ContainerSecurity{RunAsUser: int64p(1000), RunAsGroup: int64p(2000)}},
		},
	}
	if code := p.startPod(req); code != http.StatusOK {
		t.Fatalf("start returned HTTP %d", code)
	}
	waitStatus(t, p.runnerHarness, "uid-1", func(r ContainerStatusResult) bool { return r.Running })

	if got, want := p.file("order"), "cloudvm-uid-1-init-0\ncloudvm-uid-1-init-0\ncloudvm-uid-1-init-1\ncloudvm-uid-1\n"; got != want {
		t.Fatalf("run order = %q, want the failed init container retried, then the next one, then the main container: %q", got, want)
	}
	args := p.runArgs("cloudvm-uid-1")
	if args[0] != "--cgroup-manager=cgroupfs" || args[1] != "run" {
		t.Fatalf("the cgroupfs manager must be a global flag before run, got %v", args[:2])
	}
	emptyDir := filepath.Join(p.podDirs, "uid-1", "volumes", "kubernetes.io~empty-dir")
	flags := flagPairs(args)
	for _, want := range []string{
		"--cgroup-parent=/kubepods/burstable/poduid-1", "--pid=host", "--net=host", "--privileged", "--cgroupns=host",
		"--cap-add=SYS_PTRACE", "--cap-drop=ALL", "--security-opt=no-new-privileges", "--read-only", "--user=0", "--group-add=5",
		"-v " + filepath.Join(emptyDir, "scratch", "work") + ":/scratch:rw", "-v " + filepath.Join(emptyDir, "shm") + ":/dev/shm:rw",
		"-v " + hostDir + ":/host/logs:ro,rslave",
		"-v " + filepath.Join(p.filesDir, "uid-1", "kube-api-access-x") + ":" + serviceAccountMountPath + ":ro",
		"-e POD_IP=192.0.2.10", "-e ADDR=192.0.2.10:80", "--ip=192.0.2.10",
	} {
		if !flags[want] {
			t.Errorf("main container run lacks %q:\n%s", want, strings.Join(args, "\n"))
		}
	}
	for _, unwanted := range []string{"--cap-drop=SETUID", "--user=65532:65532"} {
		if flags[unwanted] {
			t.Errorf("main container run has the old default %q", unwanted)
		}
	}
	if init1 := flagPairs(p.runArgs("cloudvm-uid-1-init-1")); !init1["--user=1000:2000"] || !init1["--cgroup-parent=/kubepods/burstable/poduid-1"] || init1["--privileged"] {
		t.Errorf("init container runs with its own security context in the pod's cgroup, got %v", p.runArgs("cloudvm-uid-1-init-1"))
	}
	if info, err := os.Stat(hostDir); err != nil || !info.IsDir() {
		t.Errorf("DirectoryOrCreate must create the host directory: %v", err)
	}
	if info, err := os.Stat(filepath.Join(emptyDir, "scratch")); err != nil || info.Mode().Perm() != 0o777 || info.Mode()&os.ModeSetgid == 0 {
		t.Errorf("an emptyDir with an fsGroup is world-writable and setgid, got %v, %v", info, err)
	}
	if mounts := p.file("mounts"); !strings.Contains(mounts, "mount -t tmpfs -o size=1048576 tmpfs "+filepath.Join(emptyDir, "shm")) {
		t.Errorf("a memory emptyDir is a tmpfs of its size limit, mounts: %q", mounts)
	}

	st := p.status("uid-1")
	if st.ContainerID != "containerd://"+fakeContainerID {
		t.Errorf("containerID = %q", st.ContainerID)
	}
	if len(st.InitContainers) != 2 || st.InitContainers[0].Name != "migrate" || !st.InitContainers[0].Done || st.InitContainers[0].ExitCode != 0 ||
		st.InitContainers[0].RestartCount != 1 || st.InitContainers[0].ContainerID != "containerd://"+fakeContainerID || !st.InitContainers[1].Done {
		t.Errorf("init containers = %+v", st.InitContainers)
	}

	// The plugin replaces a refreshed token in place.
	files, _ := json.Marshal([]VolumeFile{{Volume: "kube-api-access-x", Path: "token", Data: []byte("t2"), Mode: 0o644}})
	resp := p.do(http.MethodPost, "/containers/volume-files?podUID=uid-1", string(files))
	_ = resp.Body.Close()
	token, err := os.ReadFile(filepath.Join(p.filesDir, "uid-1", "kube-api-access-x", "token"))
	if resp.StatusCode != http.StatusOK || err != nil || string(token) != "t2" {
		t.Fatalf("volume-files HTTP %d; token = %q, %v", resp.StatusCode, token, err)
	}
	unknown, _ := json.Marshal([]VolumeFile{{Volume: "other", Path: "token", Data: []byte("x")}})
	if resp = p.do(http.MethodPost, "/containers/volume-files?podUID=uid-1", string(unknown)); resp.StatusCode != http.StatusNotFound {
		t.Errorf("a volume the pod lacks returned HTTP %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	resp = p.do(http.MethodPost, "/containers/stop?podUID=uid-1", "")
	_ = resp.Body.Close()
	for _, gone := range []string{filepath.Join(p.podDirs, "uid-1"), filepath.Join(p.filesDir, "uid-1"), filepath.Join(p.state, "exists-cloudvm-uid-1-init-0")} {
		if _, err := os.Stat(gone); !os.IsNotExist(err) {
			t.Errorf("stop must remove %s, stat: %v", gone, err)
		}
	}
}

func TestRunnerScript_RunsAsTheImageUserUnlessTheSpecSetsOne(t *testing.T) {
	for _, tc := range []struct {
		name, imageUser string
		sec             *ContainerSecurity
		wantUser        string
		wantError       string
	}{
		{"no user runs the image's user", "app", &ContainerSecurity{}, "", ""},
		{"a group alone keeps the image's user", "1000:1000", &ContainerSecurity{RunAsGroup: int64p(7)}, "--user=1000:7", ""},
		{"runAsNonRoot accepts a numeric non-root image user", "1000", &ContainerSecurity{RunAsNonRoot: true}, "", ""},
		{"runAsNonRoot refuses an image that runs as root", "", &ContainerSecurity{RunAsNonRoot: true}, "", "image will run as root"},
		{"runAsNonRoot refuses a named image user", "app", &ContainerSecurity{RunAsNonRoot: true}, "", "non-numeric user (app)"},
		{"runAsNonRoot refuses runAsUser 0", "", &ContainerSecurity{RunAsNonRoot: true, RunAsUser: int64p(0)}, "", "breaks non-root policy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newPodRunnerHarness(t)
			if err := os.WriteFile(filepath.Join(p.state, "user"), []byte(tc.imageUser), 0o600); err != nil {
				t.Fatal(err)
			}
			defer p.start()()
			if code := p.startPod(&StartContainerRequest{PodUID: "u", Image: "app", CgroupParent: "/kubepods/besteffort/podu", Security: tc.sec}); code != http.StatusOK {
				t.Fatalf("start returned HTTP %d", code)
			}
			if tc.wantError != "" {
				waitStatus(t, p.runnerHarness, "u", func(r ContainerStatusResult) bool { return r.Reason == "CreateContainerConfigError" })
				if st := p.status("u"); st.Status != "dead" || !strings.Contains(st.Message, tc.wantError) {
					t.Fatalf("status = %+v, want a terminal error mentioning %q", st, tc.wantError)
				}
				return
			}
			waitStatus(t, p.runnerHarness, "u", func(r ContainerStatusResult) bool { return r.Running })
			var user []string
			for _, a := range p.runArgs("cloudvm-u") {
				if strings.HasPrefix(a, "--user") {
					user = append(user, a)
				}
			}
			if strings.Join(user, " ") != tc.wantUser {
				t.Fatalf("user flags = %v, want %q", user, tc.wantUser)
			}
			if !flagPairs(p.runArgs("cloudvm-u"))["--cgroup-parent=/kubepods/besteffort/podu"] {
				t.Fatalf("a BestEffort pod runs under /kubepods/besteffort: %v", p.runArgs("cloudvm-u"))
			}
		})
	}
}

func TestRunnerScript_FailedInitContainerFailsAPodThatNeverRestarts(t *testing.T) {
	p := newPodRunnerHarness(t)
	if err := os.WriteFile(filepath.Join(p.state, "exit-cloudvm-u-init-0"), []byte("3\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	defer p.start()()
	req := &StartContainerRequest{PodUID: "u", Image: "app", RestartPolicy: "Never", InitContainers: []ContainerRun{{Name: "setup", Image: "alpine"}}}
	if code := p.startPod(req); code != http.StatusOK {
		t.Fatalf("start returned HTTP %d", code)
	}
	waitStatus(t, p.runnerHarness, "u", func(r ContainerStatusResult) bool { return r.Reason == "Init:Error" })
	st := p.status("u")
	if st.Status != "dead" || len(st.InitContainers) != 1 || st.InitContainers[0].ExitCode != 3 || st.InitContainers[0].Reason != "Error" {
		t.Fatalf("status = %+v", st)
	}
	if order := p.file("order"); order != "cloudvm-u-init-0\n" {
		t.Fatalf("run order = %q, want only the failed init container", order)
	}
}

func TestRunnerScript_RefusesAHostPathOfTheWrongType(t *testing.T) {
	p := newPodRunnerHarness(t)
	defer p.start()()
	missing := filepath.Join(t.TempDir(), "absent")
	code := p.startPod(&StartContainerRequest{PodUID: "u", Image: "app",
		Volumes: []PodVolume{{Name: "h", Kind: VolumeKindHostPath, Path: missing, Type: "Directory"}},
		Mounts:  []ContainerMount{{Name: "h", MountPath: "/h"}}})
	if code != http.StatusBadRequest {
		t.Fatalf("start returned HTTP %d, want 400", code)
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("type Directory must not create the path: %v", err)
	}
}
