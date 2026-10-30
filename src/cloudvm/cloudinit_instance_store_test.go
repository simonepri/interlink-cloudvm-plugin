// Tests that the boot script moves container storage onto the NVMe instance store before containerd starts, by running
// its cloudvm_instance_store function against fake disk tools, and keeps the root volume when there is none.
package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// fakeDiskTool logs each call as "<tool> <args>" and plays lsblk from the files the test writes.
const fakeDiskTool = `#!/usr/bin/env bash
tool=$(basename "$0")
echo "$tool $*" >> "$STUB_DIR/calls"
case " $STUB_FAIL " in *" $tool "*) exit 1 ;; esac
if [ "$tool" = lsblk ]; then
  case "$*" in
    *MODEL*) cat "$STUB_DIR/disks" ;;
    *MOUNTPOINT*) cat "$STUB_DIR/mounts" 2>/dev/null ;;
  esac
fi
exit 0
`

const (
	ebsDisk   = `NAME="/dev/nvme0n1" MODEL="Amazon Elastic Block Store              "` + "\n"
	storeDisk = `NAME="/dev/nvme1n1" MODEL="Amazon EC2 NVMe Instance Storage        "` + "\n"
	storeTwo  = `NAME="/dev/nvme2n1" MODEL="Amazon EC2 NVMe Instance Storage        "` + "\n"
)

// extractInstanceStoreFunction returns the cloudvm_instance_store function of a container mode boot script.
func extractInstanceStoreFunction(t *testing.T) string {
	t.Helper()
	script, err := RenderCloudInit(&CloudInitConfig{PodName: "p", PodUID: "uid", RunnerAuthToken: "tok"})
	if err != nil {
		t.Fatalf("render cloud-init: %v", err)
	}
	start := strings.Index(script, "cloudvm_instance_store() {\n")
	end := strings.Index(script[start:], "\n}\n")
	if start < 0 || end < 0 {
		t.Fatal("cloudvm_instance_store function not found in cloud-init")
	}
	return script[start : start+end+3]
}

// runInstanceStore runs cloudvm_instance_store with the given lsblk disks and mounts and only the fake tools in tools on
// PATH, and returns the logged tool calls and the function's output.
func runInstanceStore(t *testing.T, disks, mounts string, tools []string, fail string) (calls []string, output string) {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o750); err != nil {
		t.Fatal(err)
	}
	stub := filepath.Join(dir, "stub")
	if err := os.WriteFile(stub, []byte(fakeDiskTool), 0o755); err != nil { //nolint:gosec // test fixture must be executable
		t.Fatal(err)
	}
	for _, tool := range tools {
		if err := os.Symlink(stub, filepath.Join(bin, tool)); err != nil {
			t.Fatal(err)
		}
	}
	for _, tool := range []string{"awk", "grep", "basename", "cat", "env", "bash"} {
		path, err := exec.LookPath(tool)
		if err != nil {
			t.Skipf("%s not available", tool)
		}
		if err := os.Symlink(path, filepath.Join(bin, tool)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "disks"), []byte(disks), 0o600); err != nil {
		t.Fatal(err)
	}
	if mounts != "" {
		if err := os.WriteFile(filepath.Join(dir, "mounts"), []byte(mounts), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(bash, "-c", extractInstanceStoreFunction(t)+"cloudvm_instance_store") //nolint:gosec // script rendered by the test
	cmd.Env = []string{"PATH=" + bin, "STUB_DIR=" + dir, "STUB_FAIL=" + fail}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("cloudvm_instance_store failed: %v\n%s", err, out)
	}
	logged, _ := os.ReadFile(filepath.Join(dir, "calls")) //nolint:gosec // test-controlled path
	for _, line := range strings.Split(strings.TrimSpace(string(logged)), "\n") {
		if !strings.HasPrefix(line, "lsblk ") && line != "" {
			calls = append(calls, line)
		}
	}
	return calls, string(out)
}

var allDiskTools = []string{"lsblk", "mdadm", "mkfs.xfs", "mkfs.ext4", "mkdir", "mount", "systemctl"}

// bindCalls are the calls that move the runtime state below base, after containerd stopped.
func bindCalls(base string) []string {
	out := []string{"systemctl stop containerd stargz-snapshotter"}
	for _, dir := range []string{"containerd", "containerd-stargz-grpc", "nerdctl"} {
		out = append(out,
			"mkdir -p "+base+"/cloudvm/"+dir+" /var/lib/"+dir,
			"mount --bind "+base+"/cloudvm/"+dir+" /var/lib/"+dir)
	}
	return out
}

func TestCloudInitInstanceStore(t *testing.T) {
	const mnt = "/mnt/cloudvm-instance-store"
	tests := []struct {
		name   string
		disks  string
		mounts string
		tools  []string
		fail   string
		want   []string
	}{
		{
			name:  "no instance store keeps the root volume",
			disks: ebsDisk,
			tools: allDiskTools,
		},
		{
			name:  "one disk is formatted and mounted",
			disks: ebsDisk + storeDisk,
			tools: allDiskTools,
			want: append([]string{
				"mkfs.xfs -q -K /dev/nvme1n1", "mkdir -p " + mnt, "mount -o noatime /dev/nvme1n1 " + mnt,
			}, bindCalls(mnt)...),
		},
		{
			name:  "several disks are striped with mdadm",
			disks: ebsDisk + storeDisk + storeTwo,
			tools: allDiskTools,
			want: append([]string{
				"mdadm --create /dev/md/cloudvm-store --level=0 --raid-devices=2 --run /dev/nvme1n1 /dev/nvme2n1",
				"mkfs.xfs -q -K /dev/md/cloudvm-store", "mkdir -p " + mnt, "mount -o noatime /dev/md/cloudvm-store " + mnt,
			}, bindCalls(mnt)...),
		},
		{
			name:  "several disks without mdadm use the first",
			disks: ebsDisk + storeDisk + storeTwo,
			tools: []string{"lsblk", "mkfs.xfs", "mkdir", "mount", "systemctl"},
			want: append([]string{
				"mkfs.xfs -q -K /dev/nvme1n1", "mkdir -p " + mnt, "mount -o noatime /dev/nvme1n1 " + mnt,
			}, bindCalls(mnt)...),
		},
		{
			name:  "ext4 without xfsprogs",
			disks: storeDisk,
			tools: []string{"lsblk", "mkfs.ext4", "mkdir", "mount", "systemctl"},
			want: append([]string{
				"mkfs.ext4 -q -m 0 /dev/nvme1n1", "mkdir -p " + mnt, "mount -o noatime /dev/nvme1n1 " + mnt,
			}, bindCalls(mnt)...),
		},
		{
			name:   "a disk the image already mounted keeps its mount",
			disks:  ebsDisk + storeDisk,
			mounts: "\n/opt/dlami/nvme\n",
			tools:  allDiskTools,
			want:   bindCalls("/opt/dlami/nvme"),
		},
		{
			name:  "a disk that cannot be formatted keeps the root volume",
			disks: storeDisk,
			tools: allDiskTools,
			fail:  "mkfs.xfs mkfs.ext4",
			want:  []string{"mkfs.xfs -q -K /dev/nvme1n1", "mkfs.ext4 -q -m 0 /dev/nvme1n1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls, out := runInstanceStore(t, tt.disks, tt.mounts, tt.tools, tt.fail)
			if strings.Join(calls, "\n") != strings.Join(tt.want, "\n") {
				t.Fatalf("calls:\n%s\nwant:\n%s\noutput:\n%s", strings.Join(calls, "\n"), strings.Join(tt.want, "\n"), out)
			}
		})
	}
}

func TestCloudInitInstanceStoreRunsBeforeContainerdStarts(t *testing.T) {
	script, err := RenderCloudInit(&CloudInitConfig{PodName: "p", PodUID: "uid", RunnerAuthToken: "tok"})
	if err != nil {
		t.Fatalf("render cloud-init: %v", err)
	}
	call := strings.Index(script, "\ncloudvm_instance_store\n")
	if call < 0 {
		t.Fatal("container mode boot script never calls cloudvm_instance_store")
	}
	for _, start := range []string{"systemctl enable --now stargz-snapshotter.service", "systemctl restart containerd"} {
		if i := strings.Index(script, start); i < call {
			t.Errorf("%q runs before the instance store is mounted", start)
		}
	}
	if !strings.Contains(script, `/MODEL="Amazon EC2 NVMe Instance Storage/`) {
		t.Error("boot script does not select disks by the instance store model")
	}
	assertBashSyntax(t, script)
	assertUserDataFits(t, script)

	native, err := RenderCloudInit(&CloudInitConfig{PodName: "p", PodUID: "uid", RunnerAuthToken: "tok", UseNativeDLAMI: true})
	if err != nil {
		t.Fatalf("render cloud-init: %v", err)
	}
	if strings.Contains(native, "cloudvm_instance_store") {
		t.Error("native DLAMI boot script without containerd must leave the instance store alone")
	}
}
