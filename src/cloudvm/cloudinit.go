// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"
)

// CloudInitConfig contains the parameters needed to render the VM bootstrap script.
type CloudInitConfig struct {
	PodName               string
	PodNamespace          string
	PodUID                string
	RayHeadAddress        string
	RayVersion            string
	TargetContainerImage  string
	Command               []string
	Args                  []string
	Env                   map[string]string
	UseNativeDLAMI        bool // When true, runs native Python/Ray from DLAMI instead of pulling Docker container
	NumGPUs               int
	AssignedGPUs          []int
	CPUMillicores         int64
	MemoryBytes           int64
	RunnerAuthToken       string
	KMSEncryptedToken     bool
	KMSKeyID              string
	KMSCiphertext         string
	MeshAuthKey           string
	Region                string
	InstanceType          string
	EgressBandwidth       string
	EgressBurst           string
	RunAsUser             *int64
	RunAsGroup            *int64
	DockerConfigJSON      string
	RegistryMirrors       []string
	OtelEndpoint          string
	NodeProblemDetector   bool
	CustomBootstrapScript string
}

// IsKMSEncrypted reports whether the runner token is encrypted via AWS KMS.
// Ciphertext returns the encrypted runner token string for KMS decrypt.
func (c *CloudInitConfig) Ciphertext() string {
	return c.KMSCiphertext
}

// Validate ensures cloud-init configuration integrity.
func (c *CloudInitConfig) Validate() error {
	if c.IsKMSEncrypted() && c.KMSCiphertext == "" {
		return fmt.Errorf("KMS token encryption is enabled but KMSCiphertext is empty")
	}
	return nil
}

func (c *CloudInitConfig) IsKMSEncrypted() bool {
	return c.KMSEncryptedToken || c.KMSCiphertext != ""
}

// EffectivePodUID returns PodUID if set, or defaults to PodName.
func (c *CloudInitConfig) EffectivePodUID() string {
	if c.PodUID != "" {
		return c.PodUID
	}
	return c.PodName
}

// CPULimit formats CPUMillicores into decimal cores for docker --cpus flag.
func (c *CloudInitConfig) CPULimit() string {
	if c.CPUMillicores <= 0 {
		return ""
	}
	return fmt.Sprintf("%.2f", float64(c.CPUMillicores)/1000.0)
}

// GPUFlag returns the exact docker --gpus flag based on discrete device allocations.
func (c *CloudInitConfig) GPUFlag() string {
	if len(c.AssignedGPUs) > 1 {
		var parts []string
		for _, g := range c.AssignedGPUs {
			parts = append(parts, fmt.Sprintf("%d", g))
		}
		return fmt.Sprintf(`--gpus '"device=%s"'`, strings.Join(parts, ","))
	}
	if len(c.AssignedGPUs) == 1 {
		return fmt.Sprintf("--gpus device=%d", c.AssignedGPUs[0])
	}
	if c.NumGPUs > 0 {
		return "--gpus all"
	}
	return ""
}

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

// RegistryMirrorsJSON returns the JSON array representation of RegistryMirrors.
func (c *CloudInitConfig) RegistryMirrorsJSON() string {
	if len(c.RegistryMirrors) == 0 {
		return "[]"
	}
	b, err := json.Marshal(c.RegistryMirrors)
	if err != nil {
		return "[]"
	}
	return string(b)
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

const cloudInitTemplate = `#!/usr/bin/env bash
exec > /var/log/cloudvm-bootstrap.log 2>&1
echo "=== INTERLINK CLOUDVM BOOTSTRAP: $(date) ==="
echo "CLOUDVM_STATUS: {\"status\": \"BOOTING\", \"pod\": \"{{.PodName}}\", \"region\": \"{{.Region}}\", \"type\": \"{{.InstanceType}}\"}" > /dev/console 2>/dev/null || true

# 1. Arm Self-Destruct Startup Watchdog (terminates VM if workload fails to reach ready within 600s)
cat << 'EOF' > /usr/local/bin/bootstrap-watchdog.sh
#!/usr/bin/env bash
sleep 600
if [ ! -f /tmp/workload-ready ]; then
  echo "[cloudvm-bootstrap] FAIL-SAFE: Workload failed to verify ready within 600s. Terminating VM to prevent zombie billing..."
  echo "CLOUDVM_STATUS: {\"status\": \"WATCHDOG_TIMEOUT\", \"pod\": \"{{.PodName}}\"}" > /dev/console 2>/dev/null || true
  shutdown -h now || poweroff
  exit 0
fi
EOF
chmod +x /usr/local/bin/bootstrap-watchdog.sh
nohup /usr/local/bin/bootstrap-watchdog.sh > /var/log/bootstrap-watchdog.log 2>&1 &

# Ensure AWS instance terminates on poweroff to prevent zombie storage billing
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null || true)
INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || true)
if [ -n "$INSTANCE_ID" ] && command -v aws >/dev/null 2>&1; then
  aws ec2 modify-instance-attribute --instance-id "$INSTANCE_ID" --instance-initiated-shutdown-behavior terminate --region "{{.Region}}" 2>/dev/null || true
fi

# Prevent non-root container workloads from accessing cloud metadata services (IMDS)
iptables -C OUTPUT -d 169.254.169.254 -m owner ! --uid-owner root -j DROP 2>/dev/null || iptables -I OUTPUT 1 -d 169.254.169.254 -m owner ! --uid-owner root -j DROP 2>/dev/null || true

# Configure ECR credential helper if available in AMI
if command -v docker-credential-ecr-login >/dev/null 2>&1; then
  mkdir -p /root/.docker
  if [ ! -f /root/.docker/config.json ]; then
    echo '{"credsStore":"ecr-login"}' > /root/.docker/config.json
  fi
fi

# Runner port 9090 dedicated firewall chain inserted at head of INPUT
iptables -N CLOUDVM_RUNNER 2>/dev/null || true
iptables -F CLOUDVM_RUNNER 2>/dev/null || true
iptables -C INPUT -p tcp --dport 9090 -j CLOUDVM_RUNNER 2>/dev/null || iptables -I INPUT 1 -p tcp --dport 9090 -j CLOUDVM_RUNNER 2>/dev/null || true
iptables -A CLOUDVM_RUNNER -i lo -j ACCEPT 2>/dev/null || true
iptables -A CLOUDVM_RUNNER -i tailscale0 -j ACCEPT 2>/dev/null || true
iptables -A CLOUDVM_RUNNER -s 10.0.0.0/8 -j ACCEPT 2>/dev/null || true
iptables -A CLOUDVM_RUNNER -s 172.16.0.0/12 -j ACCEPT 2>/dev/null || true
iptables -A CLOUDVM_RUNNER -s 192.168.0.0/16 -j ACCEPT 2>/dev/null || true
iptables -A CLOUDVM_RUNNER -s 100.64.0.0/10 -j ACCEPT 2>/dev/null || true
iptables -A CLOUDVM_RUNNER -j DROP 2>/dev/null || true

if command -v iptables-save >/dev/null 2>&1; then
  mkdir -p /etc/iptables
  iptables-save > /etc/iptables/rules.v4 2>/dev/null || true
fi

# 2. Join Private Network Mesh (Tailscale / WireGuard)
if [ -n "{{.MeshAuthKey}}" ]; then
  echo "[cloudvm-bootstrap] Connecting to private mesh network..."
  if ! command -v tailscale >/dev/null 2>&1; then
    if command -v apt-get >/dev/null 2>&1; then
      mkdir -p --mode=0755 /usr/share/keyrings
      curl -fsSL --retry 3 https://pkgs.tailscale.com/stable/ubuntu/jammy.noarmor.gpg -o /usr/share/keyrings/tailscale-archive-keyring.gpg 2>/dev/null || true
      curl -fsSL --retry 3 https://pkgs.tailscale.com/stable/ubuntu/jammy.tailscale-keyring.list -o /etc/apt/sources.list.d/tailscale.list 2>/dev/null || true
      apt-get update -y && apt-get install -y tailscale || true
    elif command -v yum >/dev/null 2>&1; then
      yum install -y yum-utils 2>/dev/null || true
      yum-config-manager --add-repo https://pkgs.tailscale.com/stable/amazon-linux/2/tailscale.repo 2>/dev/null || true
      yum install -y tailscale || true
    fi
  fi
  if command -v tailscale >/dev/null 2>&1; then
    tailscale up --authkey="{{.MeshAuthKey}}" \
      --hostname="cloudvm-{{.PodName}}" \
      --accept-routes || { echo "[cloudvm-bootstrap] Error: tailscale mesh join failed" >&2; exit 1; }
    echo "[cloudvm-bootstrap] Connected to mesh. Assigned IP: $(tailscale ip -4 2>/dev/null || echo unknown)"
  else
    echo "[cloudvm-bootstrap] Error: tailscale package installation failed" >&2
    exit 1
  fi
fi

{{if .EgressBandwidth}}
# Network Egress Spend Governor (Selective Token Bucket Filter via Linux TC)
echo "[cloudvm-bootstrap] Configuring network egress spend governor: {{.EgressBandwidth}} (burst: {{if .EgressBurst}}{{.EgressBurst}}{{else}}100mb{{end}})..."
DEV=$(ip -4 route show default 2>/dev/null | awk '{print $5}' || echo "eth0")
if [ -n "$DEV" ] && command -v tc >/dev/null 2>&1; then
  tc qdisc del dev "$DEV" root 2>/dev/null || true
  tc qdisc add dev "$DEV" root handle 1: prio bands 2 priomap 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 2>/dev/null || true
  tc qdisc add dev "$DEV" parent 1:2 handle 20: tbf rate {{.EgressBandwidth}} burst {{if .EgressBurst}}{{.EgressBurst}}{{else}}100mb{{end}} latency 50ms 2>/dev/null || true
  tc filter add dev "$DEV" protocol ip parent 1: prio 1 u32 match ip protocol 17 0xff match ip dport 41641 0xffff flowid 1:1 2>/dev/null || true
  tc filter add dev "$DEV" protocol ip parent 1: prio 1 u32 match ip protocol 17 0xff match ip dport 51820 0xffff flowid 1:1 2>/dev/null || true
  echo "[cloudvm-bootstrap] Egress spend governor applied to $DEV (mesh UDP traffic classified to unthrottled band)"
fi
{{end}}

# 3. Configure Multi-Cloud Spot Interruption Watcher (AWS IMDSv2 & GCP Metadata)
cat << 'EOF' > /usr/local/bin/spot-monitor.sh
#!/usr/bin/env bash
while true; do
  TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" --connect-timeout 1 2>/dev/null || true)
  AWS_CODE=$(curl -s -o /tmp/aws-spot.json -w "%{http_code}" -H "X-aws-ec2-metadata-token: $TOKEN" --connect-timeout 1 http://169.254.169.254/latest/meta-data/spot/instance-action 2>/dev/null || true)
  if [ "$AWS_CODE" -eq 200 ]; then
    echo "[cloudvm-bootstrap] AWS SPOT INTERRUPTION NOTICE! Draining workload..."
    echo "CLOUDVM_STATUS: {\"status\": \"SPOT_INTERRUPTION\", \"provider\": \"aws\"}" > /dev/console 2>/dev/null || true
    if command -v ray >/dev/null 2>&1; then ray stop --grace-period 90 || true; fi
    shutdown -h now || poweroff
    exit 0
  fi

  GCP_PREEMPTED=$(curl -s -f -H "Metadata-Flavor: Google" --connect-timeout 1 "http://metadata.google.internal/computeMetadata/v1/instance/preempted" 2>/dev/null || echo "FALSE")
  if [ "$GCP_PREEMPTED" = "TRUE" ]; then
    echo "[cloudvm-bootstrap] GCP SPOT PREEMPTION NOTICE (30s window)! Draining workload..."
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
  for i in $(seq 1 300); do
    {{if .UseNativeDLAMI}}
    if [ -f /tmp/workload-ready ] || (command -v ray >/dev/null 2>&1 && ray status >/dev/null 2>&1); then
      touch /tmp/workload-ready
      echo "[cloudvm-bootstrap] Workload verified healthy!"
      echo "CLOUDVM_STATUS: {\"status\": \"RUNNING\", \"pod\": \"{{.PodName}}\", \"region\": \"{{.Region}}\"}" > /dev/console 2>/dev/null || true
      break
    fi
    {{else}}
    if docker ps -q --filter "name=cloudvm" 2>/dev/null | grep -q .; then
      touch /tmp/workload-ready
      echo "[cloudvm-bootstrap] Container workload running and ready!"
      echo "CLOUDVM_STATUS: {\"status\": \"RUNNING\", \"pod\": \"{{.PodName}}\", \"region\": \"{{.Region}}\"}" > /dev/console 2>/dev/null || true
      break
    fi
    {{end}}
    sleep 2
  done
) &

{{if .RegistryMirrors}}
# Configure Docker registry mirrors
echo "[cloudvm-bootstrap] Configuring container registry mirrors..."
mkdir -p /etc/docker
if command -v python3 >/dev/null 2>&1; then
  python3 -c '
import json, os
path = "/etc/docker/daemon.json"
mirrors = {{.RegistryMirrorsJSON}}
data = {}
if os.path.exists(path):
    try:
        with open(path, "r") as f:
            data = json.load(f)
    except Exception:
        data = {}
data["registry-mirrors"] = mirrors
with open(path, "w") as f:
    json.dump(data, f, indent=2)
'
else
  if [ -f /etc/docker/daemon.json ]; then
    grep -v '"registry-mirrors"' /etc/docker/daemon.json | grep -v '^}' > /tmp/daemon.json.tmp || true
    echo '  "registry-mirrors": {{.RegistryMirrorsJSON}}' >> /tmp/daemon.json.tmp
    echo '}' >> /tmp/daemon.json.tmp
    mv /tmp/daemon.json.tmp /etc/docker/daemon.json
  else
    echo '{"registry-mirrors": {{.RegistryMirrorsJSON}}}' > /etc/docker/daemon.json
  fi
fi
if systemctl is-active --quiet docker 2>/dev/null; then
  systemctl restart docker 2>/dev/null || true
fi
{{end}}

{{if .OtelEndpoint}}
# Configure OpenTelemetry host metrics forwarding
echo "[cloudvm-bootstrap] Configuring OpenTelemetry metrics forwarder for {{.OtelEndpoint}}..."
cat << 'OTEL_EOF' > /usr/local/bin/cloudvm-otel-forwarder.sh
#!/usr/bin/env bash
ENDPOINT="{{.OtelEndpoint}}"
if [[ "$ENDPOINT" != http://* && "$ENDPOINT" != https://* ]]; then
  TARGET_URL="http://${ENDPOINT}/v1/metrics"
else
  TARGET_URL="${ENDPOINT}/v1/metrics"
fi

while true; do
  TIMESTAMP_NS=$(date +%s%N 2>/dev/null || echo "$(date +%s)000000000")
  MEM_TOTAL=$(awk '/MemTotal:/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)
  MEM_AVAIL=$(awk '/MemAvailable:/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)
  MEM_USED_KB=$(( MEM_TOTAL - MEM_AVAIL ))
  if [ "$MEM_USED_KB" -lt 0 ]; then MEM_USED_KB=0; fi
  LOAD_1M=$(awk '{print $1}' /proc/loadavg 2>/dev/null || echo 0)

  read -r -d '' PAYLOAD << JSON_EOF
{
  "resourceMetrics": [
    {
      "resource": {
        "attributes": [
          {"key": "host.name", "value": {"stringValue": "$(hostname)"}},
          {"key": "k8s.pod.name", "value": {"stringValue": "{{.PodName}}"}},
          {"key": "service.name", "value": {"stringValue": "cloudvm-host"}}
        ]
      },
      "scopeMetrics": [
        {
          "metrics": [
            {
              "name": "system.memory.usage",
              "unit": "By",
              "gauge": {
                "dataPoints": [
                  {"timeUnixNano": "${TIMESTAMP_NS}", "asInt": "$(( MEM_USED_KB * 1024 ))"}
                ]
              }
            },
            {
              "name": "system.cpu.load_average.1m",
              "unit": "1",
              "gauge": {
                "dataPoints": [
                  {"timeUnixNano": "${TIMESTAMP_NS}", "asDouble": ${LOAD_1M:-0}}
                ]
              }
            }
          ]
        }
      }
    }
  ]
}
JSON_EOF

  curl -s -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$TARGET_URL" --connect-timeout 2 --max-time 5 >/dev/null 2>&1 || true
  sleep 15
done
OTEL_EOF
chmod +x /usr/local/bin/cloudvm-otel-forwarder.sh

cat << 'SERVICE_EOF' > /etc/systemd/system/cloudvm-otel.service
[Unit]
Description=CloudVM OpenTelemetry Host Metrics Forwarder
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Environment="OTEL_EXPORTER_OTLP_ENDPOINT={{.OtelEndpoint}}"
ExecStart=/usr/local/bin/cloudvm-otel-forwarder.sh
Restart=always
RestartSec=15

[Install]
WantedBy=multi-user.target
SERVICE_EOF

if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload 2>/dev/null || true
  systemctl enable --now cloudvm-otel.service 2>/dev/null || true
fi
if ! systemctl is-active --quiet cloudvm-otel.service 2>/dev/null; then
  nohup /usr/local/bin/cloudvm-otel-forwarder.sh > /var/log/cloudvm-otel.log 2>&1 &
fi
{{end}}

{{if .NodeProblemDetector}}
# Node Problem Detector (NPD) standalone mode
echo "[cloudvm-bootstrap] Launching node-problem-detector standalone container..."
mkdir -p /etc/node-problem-detector
cat << 'NPD_GPU_EOF' > /etc/node-problem-detector/gpu-monitor.json
{
  "plugin": "kmsg",
  "logPath": "/dev/kmsg",
  "lookback": "5m",
  "bufferSize": 10,
  "source": "gpu-monitor",
  "conditions": [
    {
      "type": "GPUProblem",
      "reason": "GPUHealthy",
      "message": "NVIDIA GPUs are functioning normally"
    }
  ],
  "rules": [
    {
      "type": "permanent",
      "condition": "GPUProblem",
      "reason": "GPUFallenOffBus",
      "pattern": ".*(GPU has fallen off the bus|RmInitAdapter failed|NVRM: Xid.*: (79|92)).*"
    },
    {
      "type": "permanent",
      "condition": "GPUProblem",
      "reason": "GPUECCUncorrectableError",
      "pattern": ".*(NVRM: Xid.*: (48|63|64)).*"
    }
  ]
}
NPD_GPU_EOF

if command -v systemctl >/dev/null 2>&1; then
  systemctl start docker 2>/dev/null || true
elif command -v service >/dev/null 2>&1; then
  service docker start 2>/dev/null || true
fi

docker run -d   --name node-problem-detector   --restart always   --net=host   --privileged   -v /dev:/dev   -v /dev/kmsg:/dev/kmsg   -v /var/log:/var/log:ro   -v /etc/node-problem-detector:/config/custom:ro   registry.k8s.io/node-problem-detector/node-problem-detector:v0.8.20   --enable-k8s-exporter=false   --address=0.0.0.0   --port=20256   --config.system-log-monitor=/config/kernel-monitor.json,/config/custom/gpu-monitor.json || true
{{end}}

{{if .UseNativeDLAMI}}
# 5. Native DLAMI Mode (Fast PyTorch/Ray cold start ~20-30s)
echo "[cloudvm-bootstrap] Launching workload in native DLAMI environment..."
PRIVATE_IP=$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4 2>/dev/null || hostname -I | awk '{print $1}')
{{if .RayHeadAddress}}
/opt/pytorch/bin/pip install -q "ray[default]=={{if .RayVersion}}{{.RayVersion}}{{else}}2.44.0{{end}}"
/opt/pytorch/bin/ray start \
  --address="{{.RayHeadAddress}}" \
  --node-ip-address="$PRIVATE_IP" \
  --num-gpus="{{.NumGPUs}}" \
  --resources='{"opportunistic": 1, "region/{{.Region}}": 1, "instance/{{.InstanceType}}": 1}' \
  --block || true
{{else}}
{{range $k, $v := .Env}}export {{$k}}="{{$v}}"
{{end}}
( sleep 5 && touch /tmp/workload-ready ) &
{{.RenderCommand}}
{{end}}

{{else}}
# 5. Container Execution Mode with In-VM Micro-Runner Daemon (:9090)
echo "[cloudvm-bootstrap] Starting in-VM micro-runner daemon on port 9090..."

mkdir -p /etc/cloudvm
chmod 700 /etc/cloudvm
{{if .IsKMSEncrypted}}
CIPHERTEXT="{{.Ciphertext}}"
REGION="{{.Region}}"
aws kms decrypt --ciphertext-blob fileb://<(echo "$CIPHERTEXT" | base64 -d) --output text --query Plaintext --region $REGION | base64 -d > /etc/cloudvm/runner-token
chmod 600 /etc/cloudvm/runner-token
{{else}}
echo -n "{{.RunnerAuthToken}}" > /etc/cloudvm/runner-token
chmod 600 /etc/cloudvm/runner-token
{{end}}

cat << 'RUNNER_EOF' > /usr/local/bin/cloudvm-runner.py
import datetime
import hmac
import http.server
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.parse

PORT = 9090
AUTH_TOKEN = ""
try:
    with open('/etc/cloudvm/runner-token', 'r') as f:
        AUTH_TOKEN = f.read().strip()
except Exception:
    pass

pod_locks = {}
pod_locks_guard = threading.Lock()
starting_pods = set()
start_failures = {}

def get_pod_lock(uid):
    with pod_locks_guard:
        if uid not in pod_locks:
            pod_locks[uid] = threading.Lock()
        return pod_locks[uid]

def record_heartbeat():
    try:
        with open('/tmp/cloudvm-heartbeat', 'w') as f:
            f.write(str(time.time()))
    except Exception:
        pass

class RunnerHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def check_auth(self):
        global AUTH_TOKEN
        if not AUTH_TOKEN:
            try:
                with open('/etc/cloudvm/runner-token', 'r') as f:
                    AUTH_TOKEN = f.read().strip()
            except Exception:
                pass
        if not AUTH_TOKEN:
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"error":"auth_token_not_configured"}')
            return False
        auth_header = self.headers.get('Authorization', '')
        expected = f"Bearer {AUTH_TOKEN}"
        if hmac.compare_digest(auth_header, expected):
            record_heartbeat()
            return True
        self.send_response(401)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(b'{"error":"unauthorized"}')
        return False

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == '/healthz':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
            return

        if not self.check_auth():
            return

        if parsed.path == '/containers/list':
            res = subprocess.run(['docker', 'ps', '--filter', 'name=cloudvm-', '--filter', 'status=running', '--format', '{{print "{{"}}.ID{{print "}}"}}	{{print "{{"}}.Names{{print "}}"}}	{{print "{{"}}.State{{print "}}"}}	{{print "{{"}}.Status{{print "}}"}}'], capture_output=True, text=True)
            results = []
            if res.returncode == 0:
                for line in res.stdout.strip().splitlines():
                    if not line:
                        continue
                    parts = line.split('	')
                    if len(parts) >= 3:
                        name = parts[1]
                        state = parts[2]
                        uid = name.removeprefix('cloudvm-')
                        results.append({
                            "podUID": uid,
                            "running": True,
                            "reason": state,
                        })
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(results).encode('utf-8'))
            return

        if parsed.path == '/containers/status':
            params = urllib.parse.parse_qs(parsed.query)
            pod_uid = params.get('podUID', [''])[0]
            container_name = f"cloudvm-{pod_uid}"
            res = subprocess.run(['docker', 'inspect', container_name], capture_output=True, text=True)
            if res.returncode == 0:
                try:
                    data = json.loads(res.stdout)[0]
                    state = data.get('State', {})
                    status_str = state.get('Status', '')
                    running = state.get('Running', False)
                    exit_code = state.get('ExitCode', 0)
                    started_at = state.get('StartedAt', '')
                    finished_at = state.get('FinishedAt', '')
                    reason = "Running" if running else (
                        "ContainerCreating" if status_str in ("created", "starting") else (
                            ("Completed" if exit_code == 0 else "Error") if status_str in ("exited", "dead") else status_str.capitalize()
                        )
                    )
                    resp = {
                        "podUID": pod_uid,
                        "running": running,
                        "status": status_str,
                        "exitCode": exit_code,
                        "reason": reason,
                        "startedAt": started_at,
                        "finishedAt": finished_at,
                    }
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps(resp).encode('utf-8'))
                    return
                except Exception:
                    pass

            plock = get_pod_lock(pod_uid)
            with plock:
                if pod_uid in start_failures:
                    reason_val, err_val = start_failures[pod_uid]
                    resp = {
                        "podUID": pod_uid,
                        "running": False,
                        "status": "dead",
                        "exitCode": 1,
                        "reason": reason_val,
                        "message": err_val,
                        "finishedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    }
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps(resp).encode('utf-8'))
                    return
                if pod_uid in starting_pods:
                    resp = {
                        "podUID": pod_uid,
                        "running": False,
                        "status": "created",
                        "exitCode": 0,
                        "reason": "ContainerCreating",
                        "message": "Pulling container image or initializing...",
                    }
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps(resp).encode('utf-8'))
                    return

            self.send_response(404)
            self.end_headers()
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if not self.check_auth():
            return

        if parsed.path == '/containers/stop':
            params = urllib.parse.parse_qs(parsed.query)
            pod_uid = params.get('podUID', [''])[0]
            if pod_uid:
                plock = get_pod_lock(pod_uid)
                with plock:
                    starting_pods.discard(pod_uid)
                    start_failures.pop(pod_uid, None)
                subprocess.run(['docker', 'stop', f"cloudvm-{pod_uid}"], capture_output=True)
            self.send_response(200)
            self.end_headers()
            return

        if parsed.path == '/containers/start':
            length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(length)
            try:
                req = json.loads(body.decode('utf-8'))
            except Exception:
                self.send_response(400)
                self.end_headers()
                return

            pod_uid = str(req.get('podUID', 'unknown'))
            container_name = f"cloudvm-{pod_uid}"

            run_as_user = req.get('runAsUser')
            run_as_group = req.get('runAsGroup')

            user_val = None
            group_val = None

            if run_as_user is not None:
                try:
                    user_val = int(run_as_user)
                except (ValueError, TypeError):
                    self.send_response(400)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(b'{"error":"invalid runAsUser"}')
                    return
                if user_val == 0:
                    self.send_response(400)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(b'{"error":"runAsUser 0 is forbidden on host network"}')
                    return

            if run_as_group is not None:
                try:
                    group_val = int(run_as_group)
                except (ValueError, TypeError):
                    self.send_response(400)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(b'{"error":"invalid runAsGroup"}')
                    return
                if group_val == 0:
                    self.send_response(400)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(b'{"error":"runAsGroup 0 is forbidden on host network"}')
                    return

            plock = get_pod_lock(pod_uid)
            with plock:
                if pod_uid in starting_pods:
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(b'{"status":"already_starting"}')
                    return

                inspect_res = subprocess.run(['docker', 'inspect', container_name], capture_output=True, text=True)
                if inspect_res.returncode == 0:
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(b'{"status":"already_exists"}')
                    return

                starting_pods.add(pod_uid)
                start_failures.pop(pod_uid, None)

            image = str(req.get('image', ''))
            gpus = req.get('assignedGPUs', [])
            env = req.get('env', {})
            cmd = req.get('command', [])
            args = req.get('args', [])
            cpu_milli = req.get('cpuMillicores', 0)
            mem_bytes = req.get('memoryBytes', 0)

            shm_flag = '--shm-size=16g'
            if mem_bytes and mem_bytes > 0:
                shm_bytes = min(mem_bytes // 2, 64 * 1024 * 1024 * 1024)
                shm_flag = f"--shm-size={shm_bytes}"
            else:
                try:
                    with open('/proc/meminfo') as mf:
                        for line in mf:
                            if line.startswith('MemTotal:'):
                                total_kb = int(line.split()[1])
                                host_shm = min(total_kb * 1024 // 4, 16 * 1024 * 1024 * 1024)
                                shm_flag = f"--shm-size={host_shm}"
                                break
                except Exception:
                    shm_flag = '--shm-size=16g'

            if user_val is not None:
                grp = group_val if group_val is not None else user_val
                user_flag = f"--user={user_val}:{grp}"
            else:
                user_flag = "--user=65532:65532"

            docker_cmd = [
                'docker', 'run', '-d',
                '--name', container_name,
                '--net=host',
                '--cap-drop=SETUID',
                '--cap-drop=SETGID',
                '--security-opt=no-new-privileges',
                user_flag,
                shm_flag,
            ]
            if cpu_milli and cpu_milli > 0:
                docker_cmd.extend(['--cpus', f"{cpu_milli / 1000.0:.2f}"])
            if mem_bytes and mem_bytes > 0:
                docker_cmd.extend(['--memory', str(mem_bytes)])
            if gpus:
                gpu_devs = ','.join(str(g) for g in gpus)
                gpu_spec = f'"device={gpu_devs}"' if len(gpus) > 1 else f"device={gpu_devs}"
                docker_cmd.extend(['--gpus', gpu_spec])
            for k, v in env.items():
                docker_cmd.extend(['-e', f"{k}={v}"])
            docker_cmd.append(image)
            if isinstance(cmd, list):
                docker_cmd.extend(cmd)
            if isinstance(args, list):
                docker_cmd.extend(args)

            docker_cfg = req.get('dockerConfigJSON')

            def run_async(uid, img, c, auth_cfg):
                # 0. Isolate credentials in an ephemeral per-pod directory (never shared with co-tenants)
                cfg_dir = None
                if auth_cfg:
                    try:
                        cfg_dir = tempfile.mkdtemp(prefix=f"docker-cfg-{uid}-")
                        cfg_path = os.path.join(cfg_dir, 'config.json')
                        with open(cfg_path, 'w', encoding='utf-8') as cf:
                            cf.write(auth_cfg if isinstance(auth_cfg, str) else json.dumps(auth_cfg))
                        os.chmod(cfg_path, 0o600)
                    except Exception:
                        cfg_dir = None

                # 1. Pull container image using isolated ephemeral docker config
                pull_cmd = ['docker']
                if cfg_dir:
                    pull_cmd.extend(['--config', cfg_dir])
                pull_cmd.extend(['pull', img])
                pull_res = subprocess.run(pull_cmd, capture_output=True, text=True)

                # Clean up isolated credentials directory immediately
                if cfg_dir:
                    try:
                        shutil.rmtree(cfg_dir)
                    except Exception:
                        pass
                if pull_res.returncode != 0:
                    err_msg = pull_res.stderr.strip() or pull_res.stdout.strip() or "image pull failed"
                    with get_pod_lock(uid):
                        starting_pods.discard(uid)
                        start_failures[uid] = ("ErrImagePull", err_msg)
                    return

                # 2. Run container
                res = subprocess.run(c, capture_output=True, text=True)
                with get_pod_lock(uid):
                    starting_pods.discard(uid)
                    if res.returncode != 0:
                        err_msg = res.stderr.strip() or res.stdout.strip() or "container start failed"
                        start_failures[uid] = ("StartError", err_msg)

            threading.Thread(target=run_async, args=(pod_uid, image, docker_cmd, docker_cfg), daemon=True).start()

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status":"starting"}')
            return

        self.send_response(404)
        self.end_headers()

if __name__ == '__main__':
    server = http.server.ThreadingHTTPServer(('0.0.0.0', PORT), RunnerHandler)
    server.serve_forever()
RUNNER_EOF
chmod 700 /usr/local/bin/cloudvm-runner.py
if command -v python3 >/dev/null 2>&1; then
  nohup python3 /usr/local/bin/cloudvm-runner.py > /var/log/cloudvm-runner.log 2>&1 &
fi


# 6. Orphaned Node Idle Fail-Safe Watchdog
# Powers off only after the node has had zero active containers AND no provider heartbeat for >300s
(
  while true; do
    ACTIVE=$(docker ps -q --filter "name=cloudvm-" 2>/dev/null | wc -l || echo 0)
    if [ "$ACTIVE" -eq 0 ]; then
      HB_AGE=9999
      if [ -f /tmp/cloudvm-heartbeat ]; then
        HB_TIME=$(cat /tmp/cloudvm-heartbeat 2>/dev/null | tr -dc '0-9.' || echo "")
        if [ -n "$HB_TIME" ]; then
          NOW=$(date +%s)
          HB_INT=${HB_TIME%.*}
          if [ -n "$HB_INT" ] && [ "$HB_INT" -gt 0 ] 2>/dev/null; then
            HB_AGE=$(( NOW - HB_INT ))
          fi
        fi
      fi
      if [ "$HB_AGE" -gt 300 ]; then
        sleep 60
        ACTIVE_AFTER=$(docker ps -q --filter "name=cloudvm-" 2>/dev/null | wc -l || echo 0)
        if [ "$ACTIVE_AFTER" -eq 0 ]; then
          echo "[cloudvm-bootstrap] Node has been idle with zero active containers and no provider heartbeat for >300s. Terminating VM..."
          echo "CLOUDVM_STATUS: {\"status\": \"IDLE_TIMEOUT\", \"pod\": \"{{.PodName}}\"}" > /dev/console 2>/dev/null || true
          shutdown -h now || poweroff
          exit 0
        fi
      fi
    fi
    sleep 30
  done
) &
{{end}}

{{if .CustomBootstrapScript}}
# Custom user bootstrap script
echo "[cloudvm-bootstrap] Executing custom bootstrap script..."
{{.CustomBootstrapScript}}

{{end}}

{{if .UseNativeDLAMI}}
# Clean self-shutdown on normal process exit for native DLAMI mode
echo "[cloudvm-bootstrap] Workload process completed. Shutting down VM..."
echo "CLOUDVM_STATUS: {\"status\": \"COMPLETED\", \"pod\": \"{{.PodName}}\"}" > /dev/console 2>/dev/null || true
shutdown -h now || poweroff
{{end}}
`

// RenderCloudInit fills the template with runtime parameters.
func RenderCloudInit(cfg *CloudInitConfig) (string, error) {
	if cfg == nil {
		return "", fmt.Errorf("cloud-init config is nil")
	}
	if err := cfg.Validate(); err != nil {
		return "", err
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

// GenerateCloudInit renders the bootstrap cloud-init script (alias for RenderCloudInit).
func GenerateCloudInit(cfg *CloudInitConfig) (string, error) {
	return RenderCloudInit(cfg)
}
