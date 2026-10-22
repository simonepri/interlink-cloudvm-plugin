// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"

	"k8s.io/apimachinery/pkg/api/resource"
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
	AssignedGPUs         []int
	CPUMillicores        int64
	MemoryBytes          int64
	RunnerAuthToken      string
	MeshAuthKey          string
	Region               string
	InstanceType         string
	EgressBandwidth      string
	EgressBurst          string
	RunAsUser            *int64
	RunAsGroup           *int64
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

// InitialContainerJSON serializes the initial pod spec into JSON for the runner,
// avoiding bash variable expansion and shell injection vulnerabilities.
func (c *CloudInitConfig) InitialContainerJSON() string {
	if c.TargetContainerImage == "" {
		return ""
	}
	cmd := c.Command
	if c.RayHeadAddress != "" && len(cmd) == 0 {
		cmd = []string{"ray", "start", fmt.Sprintf("--address=%s", c.RayHeadAddress), fmt.Sprintf("--num-gpus=%d", c.NumGPUs), "--block"}
	}
	req := StartContainerRequest{
		PodUID:        c.EffectivePodUID(),
		Namespace:     c.PodNamespace,
		PodName:       c.PodName,
		ContainerName: "worker",
		Image:         c.TargetContainerImage,
		Command:       cmd,
		Args:          c.Args,
		Env:           c.Env,
		AssignedGPUs:  c.AssignedGPUs,
		MemoryBytes:   c.MemoryBytes,
		RunAsUser:     c.RunAsUser,
		RunAsGroup:    c.RunAsGroup,
	}
	if c.CPULimit() != "" {
		if q, err := resource.ParseQuantity(c.CPULimit()); err == nil {
			req.CPUMillicores = q.MilliValue()
		}
	}
	b, err := json.Marshal(req)
	if err != nil {
		return ""
	}
	return string(b)
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
iptables -I OUTPUT -d 169.254.169.254 -m owner ! --uid-owner root -j DROP 2>/dev/null || true

# Runner port 9090 dedicated firewall chain inserted at head of INPUT
iptables -N CLOUDVM_RUNNER 2>/dev/null || iptables -F CLOUDVM_RUNNER
iptables -I INPUT 1 -p tcp --dport 9090 -j CLOUDVM_RUNNER 2>/dev/null || true
iptables -A CLOUDVM_RUNNER -i lo -j ACCEPT 2>/dev/null || true
iptables -A CLOUDVM_RUNNER -i tailscale0 -j ACCEPT 2>/dev/null || true
iptables -A CLOUDVM_RUNNER -s 10.0.0.0/8 -j ACCEPT 2>/dev/null || true
iptables -A CLOUDVM_RUNNER -s 172.16.0.0/12 -j ACCEPT 2>/dev/null || true
iptables -A CLOUDVM_RUNNER -s 192.168.0.0/16 -j ACCEPT 2>/dev/null || true
iptables -A CLOUDVM_RUNNER -s 100.64.0.0/10 -j ACCEPT 2>/dev/null || true
iptables -A CLOUDVM_RUNNER -j DROP 2>/dev/null || true

# 2. Join Private Network Mesh (Tailscale / WireGuard)
if [ -n "{{.MeshAuthKey}}" ]; then
  echo "[cloudvm-bootstrap] Connecting to private mesh network..."
  if ! command -v tailscale >/dev/null 2>&1; then
    mkdir -p --mode=0755 /usr/share/keyrings
    curl -fsSL --retry 3 https://pkgs.tailscale.com/stable/ubuntu/jammy.noarmor.gpg | tee /usr/share/keyrings/tailscale-archive-keyring.gpg >/dev/null 2>&1 || true
    curl -fsSL --retry 3 https://pkgs.tailscale.com/stable/ubuntu/jammy.tailscale-keyring.list | tee /etc/apt/sources.list.d/tailscale.list 2>/dev/null || true
    apt-get update -y && apt-get install -y tailscale || curl -fsSL https://tailscale.com/install.sh | sh
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
import hmac
import http.server
import json
import subprocess
import threading
import time
import urllib.parse

PORT = 9090
AUTH_TOKEN = "{{.RunnerAuthToken}}"

start_lock = threading.Lock()
starting_pods = set()
start_failures = {}

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
                    }
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps(resp).encode('utf-8'))
                    return
                except Exception:
                    pass

            with start_lock:
                if pod_uid in start_failures:
                    reason_val, err_val = start_failures[pod_uid]
                    resp = {
                        "podUID": pod_uid,
                        "running": False,
                        "status": "dead",
                        "exitCode": 1,
                        "reason": reason_val,
                        "message": err_val,
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
                with start_lock:
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

            with start_lock:
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
            run_as_user = req.get('runAsUser')
            run_as_group = req.get('runAsGroup')

            shm_flag = '--shm-size=16g'
            if mem_bytes and mem_bytes > 0:
                shm_bytes = min(mem_bytes // 2, 64 * 1024 * 1024 * 1024)
                shm_flag = f"--shm-size={shm_bytes}"

            if run_as_user is not None:
                grp = run_as_group if run_as_group is not None else run_as_user
                user_flag = f"--user={run_as_user}:{grp}"
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

            def run_async(uid, c):
                res = subprocess.run(c, capture_output=True, text=True)
                with start_lock:
                    starting_pods.discard(uid)
                    if res.returncode != 0:
                        err_msg = res.stderr.strip() or "container start failed"
                        low = err_msg.lower()
                        if "pull" in low or "not found" in low or "manifest" in low or "repository does not exist" in low:
                            reason = "ErrImagePull"
                        else:
                            reason = "StartError"
                        start_failures[uid] = (reason, err_msg)

            threading.Thread(target=run_async, args=(pod_uid, docker_cmd), daemon=True).start()

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

# Launch initial container workload via runner (eliminates bash env expansion & shell injection)
{{if .InitialContainerJSON}}
mkdir -p /etc/cloudvm
cat << 'INITIAL_POD_EOF' > /etc/cloudvm/initial-pod.json
{{.InitialContainerJSON}}
INITIAL_POD_EOF

if [ -s /etc/cloudvm/initial-pod.json ]; then
  echo "[cloudvm-bootstrap] Launching initial container workload via runner..."
  python3 -c "
import json, urllib.request, time
for _ in range(15):
    try:
        with open('/etc/cloudvm/initial-pod.json', 'rb') as f:
            data = f.read()
        req = urllib.request.Request(
            'http://127.0.0.1:9090/containers/start',
            data=data,
            headers={
                'Authorization': 'Bearer {{.RunnerAuthToken}}',
                'Content-Type': 'application/json'
            }
        )
        urllib.request.urlopen(req, timeout=30)
        print('[cloudvm-bootstrap] Initial pod successfully started via runner')
        break
    except Exception as e:
        time.sleep(1)
" || true
fi
{{end}}

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
`

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
