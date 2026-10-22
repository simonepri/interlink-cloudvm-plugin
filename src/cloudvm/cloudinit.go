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
	PodUID               string
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
	EgressBandwidth      string
	EgressBurst          string
}

const cloudInitTemplate = `#!/usr/bin/env bash
exec > /var/log/cloudvm-bootstrap.log 2>&1
echo "=== INTERLINK CLOUDVM BOOTSTRAP: $(date) ==="
echo "CLOUDVM_STATUS: {\"status\": \"BOOTING\", \"pod\": \"{{.PodName}}\", \"region\": \"{{.Region}}\", \"type\": \"{{.InstanceType}}\"}" > /dev/console 2>/dev/null || true

# 1. Arm Self-Destruct Watchdog & Post-Startup Dead-Man's Switch
cat << 'EOF' > /usr/local/bin/bootstrap-watchdog.sh
#!/usr/bin/env bash
# Phase 1: Startup watchdog (terminates VM if workload fails to reach ready within 300s)
sleep 300
if [ ! -f /tmp/workload-ready ]; then
  echo "[cloudvm-bootstrap] FAIL-SAFE: Workload failed to verify ready within 300s. Terminating VM to prevent zombie billing..."
  echo "CLOUDVM_STATUS: {\"status\": \"WATCHDOG_TIMEOUT\", \"pod\": \"{{.PodName}}\"}" > /dev/console 2>/dev/null || true
  shutdown -h now || poweroff
  exit 0
fi

# Phase 2: Post-startup dead-man's switch (terminates VM if workload process terminates)
while true; do
  sleep 60
  if [ -f /tmp/workload-ready ]; then
    WORKLOAD_ALIVE=false
    {{if .UseNativeDLAMI}}
    if pgrep -f "ray" >/dev/null 2>&1 || pgrep -f "python" >/dev/null 2>&1; then
      WORKLOAD_ALIVE=true
    fi
    {{else}}
    if docker inspect -f '{{print "{{"}}.State.Running{{print "}}"}}' cloudvm-workload 2>/dev/null | grep -q "true"; then
      WORKLOAD_ALIVE=true
    fi
    {{end}}
    if [ "$WORKLOAD_ALIVE" = "false" ]; then
      echo "[cloudvm-bootstrap] Workload process exited. Terminating VM..."
      echo "CLOUDVM_STATUS: {\"status\": \"WORKLOAD_EXITED\", \"pod\": \"{{.PodName}}\"}" > /dev/console 2>/dev/null || true
      shutdown -h now || poweroff
      exit 0
    fi
  fi
done
EOF
chmod +x /usr/local/bin/bootstrap-watchdog.sh
nohup /usr/local/bin/bootstrap-watchdog.sh > /var/log/bootstrap-watchdog.log 2>&1 &

# Ensure AWS instance terminates on poweroff to prevent zombie storage billing
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null || true)
INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || true)
if [ -n "$INSTANCE_ID" ] && command -v aws >/dev/null 2>&1; then
  aws ec2 modify-instance-attribute --instance-id "$INSTANCE_ID" --instance-initiated-shutdown-behavior terminate --region "{{.Region}}" 2>/dev/null || true
fi

# 2. Join Private Network Mesh (Tailscale / WireGuard)
if [ -n "{{.MeshAuthKey}}" ]; then
  echo "[cloudvm-bootstrap] Connecting to private mesh network..."
  if ! command -v tailscale >/dev/null 2>&1; then
    curl -fsSL https://tailscale.com/install.sh | sh
  fi
  tailscale up --authkey="{{.MeshAuthKey}}" \
    --hostname="cloudvm-{{.PodName}}" \
    --accept-routes
  echo "[cloudvm-bootstrap] Connected to mesh. Assigned IP: $(tailscale ip -4)"
fi

{{if .EgressBandwidth}}
# Network Egress Spend Governor (Selective Token Bucket Filter via Linux TC)
echo "[cloudvm-bootstrap] Configuring network egress spend governor: {{.EgressBandwidth}} (burst: {{if .EgressBurst}}{{.EgressBurst}}{{else}}100mb{{end}})..."
DEV=$(ip -4 route show default 2>/dev/null | awk '{print $5}' || echo "eth0")
if [ -n "$DEV" ] && command -v tc >/dev/null 2>&1; then
  tc qdisc del dev "$DEV" root 2>/dev/null || true
  # Create a 2-band priority queue: Band 1 unthrottled for mesh UDP traffic, Band 2 rate-limited for public internet egress
  tc qdisc add dev "$DEV" root handle 1: prio bands 2 priomap 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 2>/dev/null || true
  tc qdisc add dev "$DEV" parent 1:2 handle 20: tbf rate {{.EgressBandwidth}} burst {{if .EgressBurst}}{{.EgressBurst}}{{else}}100mb{{end}} latency 50ms 2>/dev/null || true
  # Classify WireGuard (UDP 51820) and Tailscale (UDP 41641) into unthrottled Band 1
  tc filter add dev "$DEV" protocol ip parent 1: prio 1 u32 match ip protocol 17 0xff match ip dport 41641 0xffff flowid 1:1 2>/dev/null || true
  tc filter add dev "$DEV" protocol ip parent 1: prio 1 u32 match ip protocol 17 0xff match ip dport 51820 0xffff flowid 1:1 2>/dev/null || true
  echo "[cloudvm-bootstrap] Egress spend governor applied to $DEV (mesh UDP traffic classified to unthrottled band)"
fi
{{end}}

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

# Clean self-shutdown on normal process exit for native DLAMI mode
echo "[cloudvm-bootstrap] Workload process completed. Shutting down VM..."
echo "CLOUDVM_STATUS: {\"status\": \"COMPLETED\", \"pod\": \"{{.PodName}}\"}" > /dev/console 2>/dev/null || true
shutdown -h now || poweroff

{{else}}
# 5. Container Execution Mode with In-VM Micro-Runner Daemon (:9090)
echo "[cloudvm-bootstrap] Starting in-VM micro-runner daemon on port 9090..."

cat << 'RUNNER_EOF' > /usr/local/bin/cloudvm-runner.py
import http.server
import json
import subprocess
import urllib.parse

PORT = 9090

class RunnerHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == '/healthz':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
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
                    running = state.get('Running', False)
                    exit_code = state.get('ExitCode', 0)
                    resp = {
                        "podUID": pod_uid,
                        "running": running,
                        "exitCode": exit_code,
                        "reason": "Running" if running else ("Completed" if exit_code == 0 else "Error"),
                    }
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps(resp).encode('utf-8'))
                    return
                except Exception:
                    pass
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == '/containers/stop':
            params = urllib.parse.parse_qs(parsed.query)
            pod_uid = params.get('podUID', [''])[0]
            if pod_uid:
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

            pod_uid = req.get('podUID', 'unknown')
            image = req.get('image', '')
            gpus = req.get('assignedGPUs', [])
            env = req.get('env', {})
            cmd = req.get('command', [])
            args = req.get('args', [])

            container_name = f"cloudvm-{pod_uid}"
            docker_cmd = ['docker', 'run', '-d', '--name', container_name, '--net=host', '--ipc=host', '--pid=host']
            if gpus:
                gpu_devs = ','.join(str(g) for g in gpus)
                docker_cmd.extend(['--gpus', f"\"device={gpu_devs}\""])
            for k, v in env.items():
                docker_cmd.extend(['-e', f"{k}={v}"])
            docker_cmd.append(image)
            docker_cmd.extend(cmd)
            docker_cmd.extend(args)

            cmd_str = ' '.join(docker_cmd)
            res = subprocess.run(cmd_str, shell=True, capture_output=True, text=True)
            if res.returncode == 0:
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(b'{"status":"started"}')
            else:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(res.stderr.encode('utf-8'))
            return

        self.send_response(404)
        self.end_headers()

if __name__ == '__main__':
    server = http.server.HTTPServer(('0.0.0.0', PORT), RunnerHandler)
    server.serve_forever()
RUNNER_EOF
chmod +x /usr/local/bin/cloudvm-runner.py
if command -v python3 >/dev/null 2>&1; then
  nohup python3 /usr/local/bin/cloudvm-runner.py > /var/log/cloudvm-runner.log 2>&1 &
fi

# Launch initial container workload if image is provided
if [ -n "{{.TargetContainerImage}}" ]; then
  echo "[cloudvm-bootstrap] Launching initial container workload..."
  GPU_FLAGS=""
  {{if gt .NumGPUs 0}}
  GPU_FLAGS="--gpus all"
  {{end}}

  docker run -d --name="cloudvm-{{.EffectivePodUID}}" \
    --net=host \
    --ipc=host \
    --pid=host \
    $GPU_FLAGS \
  {{range $k, $v := .Env}}  -e {{$k}}="{{$v}}" \
  {{end}}  "{{.TargetContainerImage}}" \
    {{.RenderContainerCommand}} || true
fi

# 6. Multi-Pod Idle Watchdog: Shuts down VM only when all containers finish and idle grace period expires
echo "[cloudvm-bootstrap] Workload started. Monitoring active containers for node shutdown..."
while true; do
  ACTIVE=$(docker ps -q --filter "name=cloudvm" 2>/dev/null | wc -l || echo 0)
  if [ "$ACTIVE" -eq 0 ]; then
    echo "[cloudvm-bootstrap] No active cloudvm containers running. Waiting 60s idle grace period..."
    sleep 60
    ACTIVE_AFTER=$(docker ps -q --filter "name=cloudvm" 2>/dev/null | wc -l || echo 0)
    if [ "$ACTIVE_AFTER" -eq 0 ]; then
      echo "[cloudvm-bootstrap] Node idle grace expired. Shutting down VM..."
      echo "CLOUDVM_STATUS: {\"status\": \"COMPLETED\", \"pod\": \"{{.PodName}}\"}" > /dev/console 2>/dev/null || true
      shutdown -h now || poweroff
      exit 0
    fi
  fi
  sleep 10
done
{{end}}
`

// EffectivePodUID returns PodUID if set, or defaults to PodName.
func (c *CloudInitConfig) EffectivePodUID() string {
	if c.PodUID != "" {
		return c.PodUID
	}
	return c.PodName
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

// RenderCloudInit fills the template with runtime parameters.
func RenderCloudInit(cfg *CloudInitConfig) (string, error) {
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
