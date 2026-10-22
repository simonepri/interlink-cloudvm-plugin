// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
)

// CloudInitConfig contains the parameters needed to render the VM bootstrap script.
type CloudInitConfig struct {
	PodName              string
	PodNamespace         string
	RayHeadAddress       string
	RayVersion           string
	TargetContainerImage string
	Command              []string
	Args                 []string
	Env                  map[string]string
	UseNativeDLAMI       bool // When true, runs native Python/Ray from DLAMI instead of pulling Docker container
	NumGPUs              int
	MeshAuthKey          string
	Region               string
	InstanceType         string
}

const cloudInitTemplate = `#!/usr/bin/env bash
exec > /var/log/cloudvm-bootstrap.log 2>&1
echo "=== INTERLINK CLOUDVM BOOTSTRAP: $(date) ==="
echo "CLOUDVM_STATUS: {\"status\": \"BOOTING\", \"pod\": \"{{.PodName}}\", \"region\": \"{{.Region}}\", \"type\": \"{{.InstanceType}}\"}" > /dev/console 2>/dev/null || true

# 1. Arm Self-Destruct Watchdog (Terminates VM if workload fails to verify ready within 300s)
cat << 'EOF' > /usr/local/bin/bootstrap-watchdog.sh
#!/usr/bin/env bash
sleep 300
if [ ! -f /tmp/workload-ready ]; then
  echo "[cloudvm-bootstrap] FAIL-SAFE: Workload failed to verify ready within 300s. Terminating VM to prevent zombie billing..."
  echo "CLOUDVM_STATUS: {\"status\": \"WATCHDOG_TIMEOUT\", \"pod\": \"{{.PodName}}\"}" > /dev/console 2>/dev/null || true
  shutdown -h now || poweroff
fi
EOF
chmod +x /usr/local/bin/bootstrap-watchdog.sh
nohup /usr/local/bin/bootstrap-watchdog.sh > /var/log/bootstrap-watchdog.log 2>&1 &

# 2. Join Private Network Mesh (Tailscale / WireGuard)
if [ -n "{{.MeshAuthKey}}" ]; then
  echo "[cloudvm-bootstrap] Connecting to private mesh network..."
  if ! command -v tailscale >/dev/null 2>&1; then
    curl -fsSL https://tailscale.com/install.sh | sh
  fi
  tailscale up --authkey="{{.MeshAuthKey}}" \
    --hostname="cloudvm-{{.PodName}}" \
    --accept-routes \
    --ssh
  echo "[cloudvm-bootstrap] Connected to mesh. Assigned IP: $(tailscale ip -4)"
fi

# 3. Configure Multi-Cloud Spot Interruption Watcher (AWS IMDSv2 & GCP Metadata)
cat << 'EOF' > /usr/local/bin/spot-monitor.sh
#!/usr/bin/env bash
while true; do
  # Check AWS EC2 spot termination notice with IMDSv2 token
  TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" --connect-timeout 1 2>/dev/null || true)
  AWS_CODE=$(curl -s -o /tmp/aws-spot.json -w "%{http_code}" -H "X-aws-ec2-metadata-token: $TOKEN" --connect-timeout 1 http://169.254.169.254/latest/meta-data/spot/instance-action 2>/dev/null || true)
  if [ "$AWS_CODE" -eq 200 ]; then
    echo "[cloudvm-bootstrap] AWS SPOT INTERRUPTION NOTICE! Draining workload..."
    echo "CLOUDVM_STATUS: {\"status\": \"SPOT_INTERRUPTION\", \"provider\": \"aws\"}" > /dev/console 2>/dev/null || true
    if command -v ray >/dev/null 2>&1; then ray stop --grace-period 90 || true; fi
    shutdown -h now || poweroff
    exit 0
  fi

  # Check GCP preempted notice
  GCP_PREEMPTED=$(curl -s -f -H "Metadata-Flavor: Google" --connect-timeout 1 "http://metadata.google.internal/computeMetadata/v1/instance/preempted" 2>/dev/null || echo "FALSE")
  if [ "$GCP_PREEMPTED" = "TRUE" ]; then
    echo "[cloudvm-bootstrap] GCP SPOT PREEMPTION NOTICE! Draining workload..."
    echo "CLOUDVM_STATUS: {\"status\": \"SPOT_INTERRUPTION\", \"provider\": \"gcp\"}" > /dev/console 2>/dev/null || true
    if command -v ray >/dev/null 2>&1; then ray stop --grace-period 25 || true; fi
    shutdown -h now || poweroff
    exit 0
  fi

  sleep 5
done
EOF
chmod +x /usr/local/bin/spot-monitor.sh
nohup /usr/local/bin/spot-monitor.sh > /var/log/spot-monitor.log 2>&1 &

# 4. Asynchronous Readiness Watcher
(
  for i in $(seq 1 90); do
    if [ -f /tmp/workload-ready ] || (command -v ray >/dev/null 2>&1 && ray status >/dev/null 2>&1); then
      touch /tmp/workload-ready
      echo "[cloudvm-bootstrap] Workload verified healthy!"
      echo "CLOUDVM_STATUS: {\"status\": \"RUNNING\", \"pod\": \"{{.PodName}}\", \"region\": \"{{.Region}}\"}" > /dev/console 2>/dev/null || true
      break
    fi
    sleep 2
  done
) &

{{if .UseNativeDLAMI}}
# 5. Native DLAMI Mode (Fast PyTorch/Ray cold start ~20-30s)
echo "[cloudvm-bootstrap] Launching workload in native DLAMI environment..."
PRIVATE_IP=$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4 2>/dev/null || hostname -I | awk '{print $1}')
{{if .RayHeadAddress}}
/opt/pytorch/bin/pip install -q "ray[default]=={{if .RayVersion}}{{.RayVersion}}{{else}}2.44.0{{end}}"
touch /tmp/workload-ready
/opt/pytorch/bin/ray start \
  --address="{{.RayHeadAddress}}" \
  --node-ip-address="$PRIVATE_IP" \
  --num-gpus="{{.NumGPUs}}" \
  --resources='{"opportunistic": 1, "region/{{.Region}}": 1, "instance/{{.InstanceType}}": 1}' \
  --block || true
{{else}}
touch /tmp/workload-ready
{{range $k, $v := .Env}}export {{$k}}="{{$v}}"
{{end}}
{{.RenderCommand}}
{{end}}

{{else}}
# 5. Container Execution Mode (Docker / Containerd)
echo "[cloudvm-bootstrap] Launching containerized workload..."
GPU_FLAGS=""
{{if gt .NumGPUs 0}}
GPU_FLAGS="--gpus all"
{{end}}

touch /tmp/workload-ready
docker run --rm \
  --net=host \
  --ipc=host \
  --pid=host \
  $GPU_FLAGS \
{{range $k, $v := .Env}}  -e {{$k}}="{{$v}}" \
{{end}}  "{{.TargetContainerImage}}" \
  {{.RenderContainerCommand}} || true
{{end}}

# 6. Clean self-shutdown on normal process exit
echo "[cloudvm-bootstrap] Workload process completed. Shutting down VM..."
echo "CLOUDVM_STATUS: {\"status\": \"COMPLETED\", \"pod\": \"{{.PodName}}\"}" > /dev/console 2>/dev/null || true
shutdown -h now || poweroff
`

// RenderCommand formats Command and Args for bash execution.
func (c *CloudInitConfig) RenderCommand() string {
	var parts []string
	parts = append(parts, c.Command...)
	parts = append(parts, c.Args...)
	if len(parts) == 0 {
		return "true"
	}
	return strings.Join(parts, " ")
}

// RenderContainerCommand formats Command and Args for docker run.
func (c *CloudInitConfig) RenderContainerCommand() string {
	if c.RayHeadAddress != "" && len(c.Command) == 0 {
		return fmt.Sprintf("ray start --address=%q --num-gpus=%d --block", c.RayHeadAddress, c.NumGPUs)
	}
	var parts []string
	parts = append(parts, c.Command...)
	parts = append(parts, c.Args...)
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " ")
}

// GenerateCloudInit renders the bash/cloud-init user-data payload for a cloud VM worker.
func GenerateCloudInit(cfg *CloudInitConfig) (string, error) {
	if cfg.TargetContainerImage == "" && !cfg.UseNativeDLAMI {
		cfg.TargetContainerImage = "ubuntu:24.04"
	}
	if cfg.RayVersion == "" {
		cfg.RayVersion = "2.44.0"
	}
	tmpl, err := template.New("cloudinit").Parse(cloudInitTemplate)
	if err != nil {
		return "", fmt.Errorf("failed to parse cloud-init template: %w", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, cfg); err != nil {
		return "", fmt.Errorf("failed to execute cloud-init template: %w", err)
	}
	return buf.String(), nil
}
