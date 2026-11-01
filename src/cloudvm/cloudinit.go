// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strings"
	"text/template"
)

// runnerScript is the in-VM runner that starts, inspects, and stops pod containers on the VM.
//
//go:embed runner.py
var runnerScript string

// tunnelAgentScript is the in-VM reverse tunnel agent that bridges gateway streams to the local runner.
//
//go:embed tunnel_agent.py
var tunnelAgentScript string

// CloudInitConfig contains the parameters needed to render the VM bootstrap script.
type CloudInitConfig struct {
	PodName                string
	PodNamespace           string
	PodUID                 string
	RayHeadAddress         string
	RayVersion             string
	TargetContainerImage   string
	Command                []string
	Args                   []string
	Env                    map[string]string
	UseNativeDLAMI         bool // When true, runs native Python/Ray from DLAMI instead of pulling Docker container
	NumGPUs                int
	AssignedGPUs           []int
	CPUMillicores          int64
	MemoryBytes            int64
	RunnerAuthToken        string
	KMSEncryptedToken      bool
	KMSKeyID               string
	KMSCiphertext          string
	MeshAuthKey            string
	MeshAuthKeyCiphertext  string
	MeshTags               string
	TunnelEndpoint         string
	TunnelTransport        string
	TunnelGatewayIP        string
	TunnelDirectCIDRs      []string
	TunnelCIDRs            []string
	TunnelServiceCIDRs     []string
	TunnelRedirects        []TunnelRedirect
	RunnerDirectHTTP       bool
	Region                 string
	InstanceType           string
	EgressBandwidth        string
	EgressBurst            string
	RunAsUser              *int64
	RunAsGroup             *int64
	DockerConfigJSON       string
	RegistryMirrors        []string
	ImageMirrors           map[string]string
	CustomBootstrapScript  string
	IdleGracePeriodSeconds int `json:"idleGracePeriodSeconds,omitempty"`
	RunnerConcurrency      int `json:"runnerConcurrency,omitempty"`
	// NodeClaim boots a VM for a Karpenter NodeClaim: it starts with no pod, so the boot watchdog, which powers off a
	// VM whose first container does not start, stays off; the plugin terminates a claim VM that never becomes ready.
	NodeClaim bool `json:"nodeClaim,omitempty"`
	// DisableLazyPulls leaves the stargz snapshotter off the VM, so every image pulls in full.
	DisableLazyPulls bool `json:"disableLazyPulls,omitempty"`
	// StargzConfig is TOML that opens the stargz snapshotter config, ahead of the Docker Hub mirrors the plugin adds.
	StargzConfig string `json:"stargzConfig,omitempty"`
	// Provider is the VM's cloud, "aws" or "gcp", which the traffic meter classifies peers against.
	Provider string `json:"provider,omitempty"`
	// TransferHome is the provider/region the cluster runs in; empty takes the transfer cost model's home.
	TransferHome string `json:"transferHome,omitempty"`
}

// IsKMSEncrypted reports whether the runner token is encrypted via AWS KMS.
// Ciphertext returns the encrypted runner token string for KMS decrypt.
func (c *CloudInitConfig) Ciphertext() string {
	return c.KMSCiphertext
}

// Validate ensures cloud-init configuration integrity.
func (c *CloudInitConfig) Validate() error {
	if c.IdleGracePeriodSeconds <= 0 {
		c.IdleGracePeriodSeconds = 600
	}
	if c.RunnerConcurrency < 0 {
		c.RunnerConcurrency = 0
	}
	if c.IsKMSEncrypted() && c.KMSCiphertext == "" {
		return fmt.Errorf("KMS token encryption is enabled but KMSCiphertext is empty")
	}
	if c.TunnelTransport == string(TunnelTransportRouter) && net.ParseIP(c.TunnelGatewayIP) == nil {
		return fmt.Errorf("router tunnel transport needs a gateway IP, got %q", c.TunnelGatewayIP)
	}
	return nil
}

// KMSRegion returns the region encoded in a KMS key ARN, falling back to the VM region for bare key IDs and aliases.
func (c *CloudInitConfig) KMSRegion() string {
	return kmsKeyRegion(c.KMSKeyID, c.Region)
}

// kmsKeyRegion returns the region named in a KMS key ARN, or fallback for a bare key ID or alias, because a KMS
// endpoint only serves keys of its own region.
func kmsKeyRegion(keyID, fallback string) string {
	if parts := strings.Split(keyID, ":"); len(parts) >= 6 && parts[0] == "arn" && parts[2] == "kms" && parts[3] != "" {
		return parts[3]
	}
	return fallback
}

// DirectCIDRList returns the comma-separated private networks the worker's agent dials without the tunnel.
func (c *CloudInitConfig) DirectCIDRList() string {
	return strings.Join(c.TunnelDirectCIDRs, ",")
}

// TunnelHost returns the hostname the worker dials and verifies the gateway certificate against.
func (c *CloudInitConfig) TunnelHost() string {
	return tunnelHost(c.TunnelEndpoint)
}

// RunnerScript returns the in-VM container runner source with blank and comment-only lines stripped, which workers
// without a tunnel endpoint receive in their user data.
func (c *CloudInitConfig) RunnerScript() string {
	return stripPythonComments(runnerScript)
}

// RunnerScriptSHA256 returns the hex sha256 a tunnel worker fetches and verifies the runner by.
func (c *CloudInitConfig) RunnerScriptSHA256() string {
	return scriptSHA256(runnerScript)
}

// SpotWatcherScriptSHA256 returns the hex sha256 a tunnel worker fetches and verifies the spot watcher by.
func (c *CloudInitConfig) SpotWatcherScriptSHA256() string {
	return scriptSHA256(spotWatcherScript)
}

// TrafficHome returns the provider/region of the cluster the traffic meter tells home_region peers by.
func (c *CloudInitConfig) TrafficHome() string {
	if c.TransferHome != "" {
		return c.TransferHome
	}
	return GlobalTransferCost.HomeKey()
}

// TrafficMeshPort returns the UDP port tailnet workers carry the tunnel on, or 0 when the worker joins no tailnet.
func (c *CloudInitConfig) TrafficMeshPort() int {
	if c.TunnelTransport == string(TunnelTransportRouter) || c.TunnelTransport == string(TunnelTransportPublic) || c.MeshAuthKey == "" && c.MeshAuthKeyCiphertext == "" {
		return 0
	}
	return tailscaleWireGuardPort
}

// tailscaleWireGuardPort is the UDP port tailscaled listens on by default.
const tailscaleWireGuardPort = 41641

// TunnelCIDRList returns the comma-separated networks the worker's agent sends through the tunnel.
func (c *CloudInitConfig) TunnelCIDRList() string {
	return strings.Join(c.TunnelCIDRs, ",")
}

// TransparentProxy reports whether the bootstrap redirects connections into the agent's transparent port.
func (c *CloudInitConfig) TransparentProxy() bool {
	return len(c.TunnelServiceCIDRs) > 0 || len(c.TunnelRedirects) > 0
}

// TransparentProxyPort is the loopback port where the worker's agent accepts the connections the bootstrap redirects
// to it from the Service networks.
func (c *CloudInitConfig) TransparentProxyPort() int {
	return transparentProxyPort
}

// TunnelAgentScriptSHA256 returns the hex sha256 a tunnel worker fetches and verifies the tunnel agent by.
func (c *CloudInitConfig) TunnelAgentScriptSHA256() string {
	return scriptSHA256(tunnelAgentScript)
}

// stripPythonComments drops blank and comment-only lines to keep the user data under the EC2 size limit. It keeps the
// shebang and encoding lines and every line inside a triple-quoted string.
func stripPythonComments(src string) string {
	var kept []string
	inString := false
	for i, line := range strings.Split(src, "\n") {
		trimmed := strings.TrimSpace(line)
		header := i < 2 && (strings.HasPrefix(trimmed, "#!") || strings.Contains(trimmed, "coding"))
		keep := inString || header || (trimmed != "" && !strings.HasPrefix(trimmed, "#"))
		if strings.Count(line, `"""`)%2 == 1 || strings.Count(line, "'''")%2 == 1 {
			inString = !inString
			keep = true
		}
		if keep {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "\n")
}

// RunnerBindAddress returns the interface the in-VM runner listens on: loopback when only the tunnel agent may reach it.
func (c *CloudInitConfig) RunnerBindAddress() string {
	if c.TunnelEndpoint != "" && !c.RunnerDirectHTTP {
		return "127.0.0.1"
	}
	return "0.0.0.0"
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

// CPULimit formats CPUMillicores into decimal cores for the nerdctl --cpus flag.
func (c *CloudInitConfig) CPULimit() string {
	if c.CPUMillicores <= 0 {
		return ""
	}
	return fmt.Sprintf("%.2f", float64(c.CPUMillicores)/1000.0)
}

// GPUFlag returns the exact nerdctl --gpus flag based on discrete device allocations.
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

// StargzConfigTOML returns the stargz snapshotter config: the operator's StargzConfig, such as fetch and prefetch
// tuning, then the Docker Hub mirrors normal pulls use for lazy fetches. The plugin writes only entries of the
// [[resolver.host."docker.io".mirrors]] array, which join any the operator lists, so the operator may declare any
// table without a duplicate that would stop the snapshotter. Fetches authenticate through the host docker config,
// whose ECR credential helper mints a fresh token for each fetch.
func (c *CloudInitConfig) StargzConfigTOML() string {
	var b strings.Builder
	if extra := strings.TrimSpace(c.StargzConfig); extra != "" {
		b.WriteString(extra + "\n")
	}
	for _, m := range c.RegistryMirrors {
		if !strings.Contains(m, "://") {
			m = "https://" + m
		}
		u, err := url.Parse(m)
		if err != nil || u.Host == "" {
			continue
		}
		fmt.Fprintf(&b, "[[resolver.host.\"docker.io\".mirrors]]\nhost = %q\n", u.Host+strings.TrimRight(u.Path, "/"))
		if u.Scheme == "http" {
			b.WriteString("insecure = true\n")
		}
	}
	return b.String()
}

// ImageMirrorsJSON returns the JSON object mapping upstream registry hosts to mirror prefixes for the in-VM runner.
func (c *CloudInitConfig) ImageMirrorsJSON() string {
	b, err := json.Marshal(c.ImageMirrors)
	if err != nil || len(c.ImageMirrors) == 0 {
		return "{}"
	}
	return string(b)
}

// RenderContainerCommand formats Command and Args for nerdctl run.
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
# Mirror the bootstrap and tunnel agent logs to the serial console so EC2 get-console-output shows them.
CLOUDVM_SERIAL=/dev/console
[ -w /dev/ttyS0 ] && CLOUDVM_SERIAL=/dev/ttyS0
# cloudvm_mirror appends stdin to the file in $1 and reopens the console for every line: the serial getty hangs up
# ttyS0 while booting, which silences any writer that holds it open.
cloudvm_mirror() {
  while IFS= read -r line || [ -n "$line" ]; do
    printf '%s\n' "$line" >> "$1"
    printf '%s\n' "$line" > "$CLOUDVM_SERIAL" 2>/dev/null || true
  done
}
exec > >(cloudvm_mirror /var/log/cloudvm-bootstrap.log) 2>&1
# cloudvm_mark records when the boot phase in $1 ended; the tunnel agent reports the marks in its handshake.
cloudvm_mark() { mkdir -p /run/cloudvm && printf '%s %s\n' "$1" "$(date +%s.%N)" >> /run/cloudvm/boot-marks; }
cloudvm_mark os_boot
# cloudvm_bootstrap_failed reports the reason in $1 and stops the bootstrap, so the VM never runs a pod without a guard it needs.
cloudvm_bootstrap_failed() {
  echo "[cloudvm-bootstrap] FATAL: $1"
  echo "CLOUDVM_STATUS: {\"status\": \"BOOTSTRAP_FAILED\", \"pod\": \"{{.PodName}}\"}" > /dev/console 2>/dev/null || true
  exit 1
}
echo "=== INTERLINK CLOUDVM BOOTSTRAP: $(date) ==="
echo "CLOUDVM_STATUS: {\"status\": \"BOOTING\", \"pod\": \"{{.PodName}}\", \"region\": \"{{.Region}}\", \"type\": \"{{.InstanceType}}\"}" > /dev/console 2>/dev/null || true

# Initialize heartbeat file with boot timestamp so new nodes are not considered idle prematurely
touch /tmp/cloudvm-heartbeat 2>/dev/null || true
echo "$(date +%s)" > /tmp/cloudvm-heartbeat 2>/dev/null || true

{{if not .NodeClaim}}
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
{{end}}

# Ensure AWS instance terminates on poweroff to prevent zombie storage billing
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null || true)
INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || true)
if [ -n "$INSTANCE_ID" ] && command -v aws >/dev/null 2>&1; then
  aws ec2 modify-instance-attribute --instance-id "$INSTANCE_ID" --instance-initiated-shutdown-behavior terminate --region "{{.Region}}" 2>/dev/null || true
fi

# Images such as the plain Amazon Linux 2023 AMI ship without iptables, which every guard below needs.
if ! command -v iptables >/dev/null 2>&1; then
  dnf install -y iptables-nft || yum install -y iptables-nft || { apt-get update && apt-get install -y iptables; } || true
fi
command -v iptables >/dev/null 2>&1 || cloudvm_bootstrap_failed "no iptables to keep containers away from the instance metadata service"

# Prevent non-root container workloads from accessing cloud metadata services (IMDS)
iptables -C OUTPUT -d 169.254.169.254 -m owner ! --uid-owner root -j DROP 2>/dev/null || iptables -I OUTPUT 1 -d 169.254.169.254 -m owner ! --uid-owner root -j DROP || cloudvm_bootstrap_failed "cannot block the instance metadata service for non-root users"

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

# The runner's traffic meter splits bytes by peer and pod with nftables; without nft the VM reports only its NIC totals.
command -v nft >/dev/null 2>&1 || dnf install -y nftables || yum install -y nftables || apt-get install -y nftables || echo "[cloudvm-bootstrap] Warning: no nftables; traffic is not split by peer" >&2

if command -v iptables-save >/dev/null 2>&1; then
  mkdir -p /etc/iptables
  iptables-save > /etc/iptables/rules.v4 2>/dev/null || true
fi

# 2. Reach the tunnel gateway: join the tailnet, or pin the gateway host to the address the worker VPC routes through its router pair
{{if eq .TunnelTransport "router"}}
echo "[cloudvm-bootstrap] Router transport: resolving {{.TunnelHost}} to {{.TunnelGatewayIP}}"
CLOUDVM_HOSTS=$(grep -vE '[[:space:]]{{.TunnelHost}}([[:space:]]|$)' /etc/hosts || true)
printf '%s\n%s %s\n' "$CLOUDVM_HOSTS" "{{.TunnelGatewayIP}}" "{{.TunnelHost}}" > /etc/hosts
# The gateway address can sit inside a container bridge range (172.17.0.0/16); a host route keeps it on the VPC uplink.
CLOUDVM_UPLINK_GW=$(ip -4 route show default 2>/dev/null | awk '{print $3; exit}')
CLOUDVM_UPLINK_DEV=$(ip -4 route show default 2>/dev/null | awk '{print $5; exit}')
if [ -n "$CLOUDVM_UPLINK_GW" ] && [ -n "$CLOUDVM_UPLINK_DEV" ]; then
  ip route replace "{{.TunnelGatewayIP}}/32" via "$CLOUDVM_UPLINK_GW" dev "$CLOUDVM_UPLINK_DEV"
fi
echo "[cloudvm-bootstrap] Gateway route: $(ip -4 route get "{{.TunnelGatewayIP}}" 2>&1 | head -n 1)"
{{else if or .MeshAuthKey .MeshAuthKeyCiphertext}}
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
if ! command -v tailscale >/dev/null 2>&1; then
  echo "[cloudvm-bootstrap] Error: tailscale package installation failed" >&2
  exit 1
fi
if command -v systemctl >/dev/null 2>&1; then
  systemctl enable --now tailscaled 2>/dev/null || true
fi
mkdir -p /etc/cloudvm
chmod 700 /etc/cloudvm
# The key is read from a 0600 file so it never appears in process arguments or this log.
(
  umask 077
{{- if .MeshAuthKeyCiphertext}}
  aws kms decrypt --ciphertext-blob fileb://<(echo "{{.MeshAuthKeyCiphertext}}" | base64 -d) --output text --query Plaintext --region "{{.KMSRegion}}" | base64 -d > /etc/cloudvm/mesh-authkey
{{- else}}
  printf '%s' '{{.MeshAuthKey}}' > /etc/cloudvm/mesh-authkey
{{- end}}
)
tailscale up --auth-key=file:/etc/cloudvm/mesh-authkey \
  --hostname="cloudvm-{{.PodName}}" \
{{- if .MeshTags}}
  --advertise-tags="{{.MeshTags}}" \
{{- end}}
  --accept-routes || { rm -f /etc/cloudvm/mesh-authkey; echo "[cloudvm-bootstrap] Error: tailscale mesh join failed" >&2; exit 1; }
rm -f /etc/cloudvm/mesh-authkey
echo "[cloudvm-bootstrap] Connected to mesh. Assigned IP: $(tailscale ip -4 2>/dev/null || echo unknown)"
# Leave the tailnet on shutdown so the ephemeral node disappears immediately rather than after its offline timeout.
if command -v systemctl >/dev/null 2>&1; then
  cat << 'TSLOGOUT_EOF' > /etc/systemd/system/cloudvm-tailscale-logout.service
[Unit]
Description=Remove this ephemeral worker from the tailnet on shutdown
After=tailscaled.service
Requires=tailscaled.service

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/true
ExecStop=/usr/bin/tailscale logout

[Install]
WantedBy=multi-user.target
TSLOGOUT_EOF
  systemctl daemon-reload 2>/dev/null || true
  systemctl enable --now cloudvm-tailscale-logout.service 2>/dev/null || true
fi
{{end}}

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

# 3. Configure Multi-Cloud Spot Interruption Watcher (AWS IMDSv2 & GCP Metadata). Tunnel workers instead report notices to
# the plugin through the spot watcher, so Ray drains the worker before the VM goes away.
cloudvm_mark network
{{if not .TunnelEndpoint}}
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
{{end}}

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
    if nerdctl ps -q --filter "name=cloudvm" 2>/dev/null | grep -q .; then
      touch /tmp/workload-ready
      echo "[cloudvm-bootstrap] Container workload running and ready!"
      echo "CLOUDVM_STATUS: {\"status\": \"RUNNING\", \"pod\": \"{{.PodName}}\", \"region\": \"{{.Region}}\"}" > /dev/console 2>/dev/null || true
      break
    fi
    {{end}}
    sleep 2
  done
) &

{{if not .UseNativeDLAMI}}
# Container runtime: containerd, pinned nerdctl, and the stargz snapshotter for lazy pulls
CLOUDVM_NERDCTL_VERSION="2.4.1"
CLOUDVM_NERDCTL_SHA256="f6bbf02fd8a299cbcbc71a0bad113c1b63b3f33689bbb7d51b0fa98c6d361d5e"
CLOUDVM_STARGZ_VERSION="0.18.2"
CLOUDVM_STARGZ_SHA256="515a3c3af0012f192ace31fb79e910597977c77227e976680aeaaef6e9ae50a9"
CLOUDVM_ECR_HELPER_VERSION="0.12.0"
CLOUDVM_ECR_HELPER_SHA256="a22ab6d59dbac777b8fb31984709461da00a4c778b6ae14c8d3729e8d5ded1a6"
# cloudvm_fetch URL SHA256 MEMBER downloads a tarball, verifies its checksum, and extracts MEMBER into /usr/local/bin.
cloudvm_fetch() {
  curl -fsSL --retry 5 "$1" -o /tmp/cloudvm-fetch.tgz && echo "$2  /tmp/cloudvm-fetch.tgz" | sha256sum -c - >/dev/null && tar -xzf /tmp/cloudvm-fetch.tgz -C /usr/local/bin "$3"
}
if [ "$(uname -m)" != "x86_64" ]; then
  echo "[cloudvm-bootstrap] Error: the pinned nerdctl and stargz builds are linux-amd64 only" >&2
  exit 1
fi
if ! command -v containerd >/dev/null 2>&1; then
  dnf install -y containerd || yum install -y containerd || { apt-get update -y && apt-get install -y containerd; } || true
fi
if ! command -v containerd >/dev/null 2>&1; then
  echo "[cloudvm-bootstrap] Error: containerd installation failed" >&2
  exit 1
fi
# cloudvm_instance_store moves the containerd, stargz and nerdctl state onto the NVMe instance store, striping several
# disks with mdadm when the image has it, because the root EBS volume caps image unpacking at its throughput.
cloudvm_instance_store() {
  local devs base target dir
  devs=$(lsblk -dnpPo NAME,MODEL | awk -F'"' '/MODEL="Amazon EC2 NVMe Instance Storage/ {print $2}')
  if [ -z "$devs" ]; then
    echo "[cloudvm-bootstrap] No NVMe instance store; container storage stays on the root volume"
    return 0
  fi
  # An image that mounts the instance store itself, such as the AWS Deep Learning AMIs, keeps its mount.
  base=$(lsblk -nrpo MOUNTPOINT $devs | grep -m 1 .)
  if [ -z "$base" ]; then
    set -- $devs
    target=$1
    if [ $# -gt 1 ] && command -v mdadm >/dev/null 2>&1; then
      mdadm --create /dev/md/cloudvm-store --level=0 --raid-devices=$# --run "$@" < /dev/null && target=/dev/md/cloudvm-store
    fi
    base=/mnt/cloudvm-instance-store
    if ! { mkfs.xfs -q -K "$target" || mkfs.ext4 -q -m 0 "$target"; } < /dev/null || ! mkdir -p "$base" || ! mount -o noatime "$target" "$base"; then
      echo "[cloudvm-bootstrap] Warning: cannot mount instance store $target; container storage stays on the root volume" >&2
      return 0
    fi
  fi
  systemctl stop containerd stargz-snapshotter 2>/dev/null || true
  for dir in containerd containerd-stargz-grpc nerdctl; do
    mkdir -p "$base/cloudvm/$dir" "/var/lib/$dir" && mount --bind "$base/cloudvm/$dir" "/var/lib/$dir" || echo "[cloudvm-bootstrap] Warning: /var/lib/$dir stays on the root volume" >&2
  done
  echo "[cloudvm-bootstrap] Container storage on instance store $base (${devs//$'\n'/ })"
}
cloudvm_instance_store
if ! nerdctl --version 2>/dev/null | grep -q "version ${CLOUDVM_NERDCTL_VERSION}$"; then
  cloudvm_fetch "https://github.com/containerd/nerdctl/releases/download/v${CLOUDVM_NERDCTL_VERSION}/nerdctl-${CLOUDVM_NERDCTL_VERSION}-linux-amd64.tar.gz" "$CLOUDVM_NERDCTL_SHA256" nerdctl || { echo "[cloudvm-bootstrap] Error: nerdctl install failed" >&2; exit 1; }
fi
# nerdctl and the stargz snapshotter authenticate ECR pulls with the instance role through this helper; the runner maps
# each ECR registry host to it in /root/.docker/config.json.
mkdir -p /root/.docker
[ -f /root/.docker/config.json ] || echo '{}' > /root/.docker/config.json
if [ -n "$INSTANCE_ID" ] && ! command -v docker-credential-ecr-login >/dev/null 2>&1; then
  if curl -fsSL --retry 5 "https://amazon-ecr-credential-helper-releases.s3.us-east-2.amazonaws.com/${CLOUDVM_ECR_HELPER_VERSION}/linux-amd64/docker-credential-ecr-login" -o /tmp/docker-credential-ecr-login && echo "$CLOUDVM_ECR_HELPER_SHA256  /tmp/docker-credential-ecr-login" | sha256sum -c - >/dev/null; then
    install -m 0755 /tmp/docker-credential-ecr-login /usr/local/bin/docker-credential-ecr-login
  else
    echo "[cloudvm-bootstrap] Warning: ECR credential helper install failed; ECR pulls will be unauthenticated" >&2
  fi
fi
mkdir -p /etc/containerd
[ -f /etc/containerd/config.toml ] || echo 'version = 2' > /etc/containerd/config.toml
grep -q '# cloudvm-unpack-overlayfs' /etc/containerd/config.toml || cat << 'CONTAINERD_EOF' >> /etc/containerd/config.toml

# cloudvm-unpack-overlayfs
[[plugins."io.containerd.transfer.v1.local".unpack_config]]
  platform = "linux/amd64"
  snapshotter = "overlayfs"
CONTAINERD_EOF
{{if not .DisableLazyPulls}}
# The snapshotter pulls eStargz layers lazily (other images pull in full) and keeps its layer cache on the containerd disk.
if [ ! -x /usr/local/bin/containerd-stargz-grpc ]; then
  cloudvm_fetch "https://github.com/containerd/stargz-snapshotter/releases/download/v${CLOUDVM_STARGZ_VERSION}/stargz-snapshotter-v${CLOUDVM_STARGZ_VERSION}-linux-amd64.tar.gz" "$CLOUDVM_STARGZ_SHA256" containerd-stargz-grpc || echo "[cloudvm-bootstrap] Warning: stargz snapshotter install failed; images pull in full" >&2
fi
if [ -x /usr/local/bin/containerd-stargz-grpc ]; then
  mkdir -p /etc/containerd-stargz-grpc /etc/systemd/system/containerd.service.d
  cat << 'STARGZ_EOF' > /etc/containerd-stargz-grpc/config.toml
{{.StargzConfigTOML}}STARGZ_EOF
  grep -q 'proxy_plugins.stargz' /etc/containerd/config.toml || cat << 'CONTAINERD_EOF' >> /etc/containerd/config.toml

[proxy_plugins.stargz]
  type = "snapshot"
  address = "/run/containerd-stargz-grpc/containerd-stargz-grpc.sock"
  [proxy_plugins.stargz.exports]
    root = "/var/lib/containerd-stargz-grpc"
    # The transfer service passes the image reference and layer digests to the snapshotter only with this export;
    # without them the snapshotter cannot mount a layer remotely, and containerd downloads and unpacks every layer.
    enable_remote_snapshot_annotations = "true"
CONTAINERD_EOF
  # containerd 2.x pulls through the transfer service, which needs unpack platforms for each snapshotter it unpacks into.
  grep -q '# cloudvm-unpack-stargz' /etc/containerd/config.toml || cat << 'CONTAINERD_EOF' >> /etc/containerd/config.toml

# cloudvm-unpack-stargz
[[plugins."io.containerd.transfer.v1.local".unpack_config]]
  platform = "linux/amd64"
  snapshotter = "stargz"
CONTAINERD_EOF
  printf '[Unit]\nWants=stargz-snapshotter.service\nAfter=stargz-snapshotter.service\n' > /etc/systemd/system/containerd.service.d/10-stargz.conf
  # Lazy fetches authenticate with the host docker config, whose ECR helper mints a fresh token each time one expires.
  cat << 'STARGZ_EOF' > /etc/systemd/system/stargz-snapshotter.service
[Unit]
Description=stargz snapshotter
Wants=network-online.target
After=network-online.target
Before=containerd.service

[Service]
Type=notify
Environment=HOME=/root DOCKER_CONFIG=/root/.docker PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin
ExecStart=/usr/local/bin/containerd-stargz-grpc --address=/run/containerd-stargz-grpc/containerd-stargz-grpc.sock --config=/etc/containerd-stargz-grpc/config.toml
Restart=always
RestartSec=1
RuntimeDirectory=containerd-stargz-grpc
StateDirectory=containerd-stargz-grpc

[Install]
WantedBy=multi-user.target
STARGZ_EOF
  systemctl daemon-reload
  systemctl enable --now stargz-snapshotter.service || echo "[cloudvm-bootstrap] Warning: stargz snapshotter did not start; images pull in full" >&2
fi
{{end}}

{{if .RegistryMirrors}}
# Docker Hub pulls try these mirrors first
mkdir -p /etc/containerd/certs.d/docker.io
cat << 'MIRRORS_EOF' > /etc/containerd/certs.d/docker.io/hosts.toml
server = "https://registry-1.docker.io"
{{range .RegistryMirrors}}
[host.{{printf "%q" .}}]
  capabilities = ["pull", "resolve"]
{{end}}
MIRRORS_EOF
{{end}}

systemctl enable containerd
systemctl restart containerd || true
for i in $(seq 1 30); do
  [ -S /run/containerd/containerd.sock ] && break
  sleep 1
done
{{end}}
cloudvm_mark container_runtime

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
# Ensure Python 3 is installed on base Linux distributions
if ! command -v python3 >/dev/null 2>&1; then
  echo "[cloudvm-bootstrap] Installing Python 3..."
  if command -v dnf >/dev/null 2>&1; then
    dnf install -y python3 || true
  elif command -v yum >/dev/null 2>&1; then
    yum install -y python3 || true
  elif command -v apt-get >/dev/null 2>&1; then
    apt-get update -y && apt-get install -y python3 || true
  fi
fi

# 5. Container Execution Mode with In-VM Micro-Runner Daemon (:9090)
echo "[cloudvm-bootstrap] Starting in-VM micro-runner daemon on port 9090..."

mkdir -p /etc/cloudvm
chmod 700 /etc/cloudvm
{{if .IsKMSEncrypted}}
CIPHERTEXT="{{.Ciphertext}}"
aws kms decrypt --ciphertext-blob fileb://<(echo "$CIPHERTEXT" | base64 -d) --output text --query Plaintext --region "{{.KMSRegion}}" | base64 -d > /etc/cloudvm/runner-token
chmod 600 /etc/cloudvm/runner-token
{{else}}
echo -n "{{.RunnerAuthToken}}" > /etc/cloudvm/runner-token
chmod 600 /etc/cloudvm/runner-token
{{end}}

cat << 'IMAGE_MIRRORS_EOF' > /etc/cloudvm/image-mirrors.json
{{.ImageMirrorsJSON}}
IMAGE_MIRRORS_EOF
chmod 600 /etc/cloudvm/image-mirrors.json

{{if .TunnelEndpoint}}
# cloudvm_fetch_script downloads the script with sha256 $1 from the tunnel gateway to $2, reaching the gateway host the way the
# tunnel agent does, and fails the bootstrap when the download fails or its hash differs.
cloudvm_fetch_script() {
  if curl -fsS --retry 10 --retry-delay 3 --retry-all-errors --connect-timeout 10 --max-time 60 -o "$2" \
      "https://{{.TunnelEndpoint}}/bootstrap/$1" && echo "$1  $2" | sha256sum -c --status -; then
    return 0
  fi
  rm -f "$2"
  echo "[cloudvm-bootstrap] FATAL: cannot fetch $2 with sha256 $1 from {{.TunnelEndpoint}}"
  echo "CLOUDVM_STATUS: {\"status\": \"BOOTSTRAP_FAILED\", \"pod\": \"{{.PodName}}\"}" > /dev/console 2>/dev/null || true
  exit 1
}
cloudvm_fetch_script {{.RunnerScriptSHA256}} /usr/local/bin/cloudvm-runner.py
cloudvm_fetch_script {{.TrafficMeterScriptSHA256}} /usr/local/bin/cloudvm_traffic_meter.py
{{else}}
cat << 'RUNNER_EOF' > /usr/local/bin/cloudvm-runner.py
{{.RunnerScript}}
RUNNER_EOF
{{end}}
chmod 700 /usr/local/bin/cloudvm-runner.py
if command -v python3 >/dev/null 2>&1; then
  CLOUDVM_RUNNER_BIND="{{.RunnerBindAddress}}" CLOUDVM_RUNNER_CONCURRENCY="{{if .RunnerConcurrency}}{{.RunnerConcurrency}}{{end}}" \
    CLOUDVM_TRAFFIC_PROVIDER="{{.Provider}}" CLOUDVM_TRAFFIC_REGION="{{.Region}}" CLOUDVM_TRAFFIC_HOME="{{.TrafficHome}}" \
    CLOUDVM_TRAFFIC_TUNNEL_HOST="{{.TunnelHost}}" CLOUDVM_TRAFFIC_TUNNEL_IP="{{.TunnelGatewayIP}}" CLOUDVM_TRAFFIC_MESH_PORT="{{.TrafficMeshPort}}" \
    CLOUDVM_TRAFFIC_PROXY_PORTS="{{if .TunnelEndpoint}}{{.TrafficProxyPorts}}{{end}}" \
    nohup python3 /usr/local/bin/cloudvm-runner.py > /var/log/cloudvm-runner.log 2>&1 &
fi
cloudvm_mark runner
{{if .TunnelEndpoint}}
# 5b. Reverse Tunnel Agent: dials the tunnel gateway outbound and carries every provider call to the local runner
echo "[cloudvm-bootstrap] Starting reverse tunnel agent towards {{.TunnelEndpoint}}..."
TUNNEL_PLATFORM="aws"
IMDS_TOKEN=$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null || true)
TUNNEL_INSTANCE_ID=$(curl -sf -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || true)
if [ -n "$TUNNEL_INSTANCE_ID" ]; then
  # The signed identity document rides in the handshake; public workers send the RSA-2048 envelope the gateway verifies.
  ( umask 077; curl -sf -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" http://169.254.169.254/latest/dynamic/instance-identity/{{if eq .TunnelTransport "public"}}rsa2048{{else}}pkcs7{{end}} > /etc/cloudvm/attestation 2>/dev/null || rm -f /etc/cloudvm/attestation )
else
  TUNNEL_PLATFORM="gcp"
  TUNNEL_INSTANCE_ID=$(curl -sf -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/name 2>/dev/null || hostname)
fi
cloudvm_fetch_script {{.SpotWatcherScriptSHA256}} /usr/local/bin/cloudvm-spot-watcher.py
CLOUDVM_PLATFORM="$TUNNEL_PLATFORM" nohup python3 /usr/local/bin/cloudvm-spot-watcher.py >> {{.SpotWatcherLogPath}} 2>&1 < /dev/null &
cloudvm_fetch_script {{.TunnelAgentScriptSHA256}} /usr/local/bin/cloudvm-tunnel-agent.py
chmod 700 /usr/local/bin/cloudvm-tunnel-agent.py
{{if .TransparentProxy}}
# Connections to cluster Services and allow-listed external hosts enter the agent, which tunnels them, so clients need no
# proxy; the gateway stays direct.
iptables -t nat -N CLOUDVM_SERVICES 2>/dev/null || iptables -t nat -F CLOUDVM_SERVICES || cloudvm_bootstrap_failed "cannot create the Service redirect chain"
{{if .TunnelGatewayIP}}iptables -t nat -A CLOUDVM_SERVICES -d {{.TunnelGatewayIP}} -j RETURN || cloudvm_bootstrap_failed "cannot exempt the tunnel gateway from the Service redirect"
{{end}}{{range .TunnelServiceCIDRs}}iptables -t nat -A CLOUDVM_SERVICES -p tcp -d {{.}} -j REDIRECT --to-ports {{$.TransparentProxyPort}} || cloudvm_bootstrap_failed "cannot redirect Service network {{.}} into the tunnel agent"
{{end}}{{range .TunnelRedirects}}iptables -t nat -A CLOUDVM_SERVICES -p tcp -d {{.Network}}{{if .Port}} --dport {{.Port}}{{end}} -j REDIRECT --to-ports {{$.TransparentProxyPort}} || cloudvm_bootstrap_failed "cannot redirect {{.Network}} into the tunnel agent"
{{end}}iptables -t nat -C OUTPUT -j CLOUDVM_SERVICES 2>/dev/null || iptables -t nat -A OUTPUT -j CLOUDVM_SERVICES || cloudvm_bootstrap_failed "cannot hook the Service redirect into OUTPUT"
{{end}}
CLOUDVM_CONTAINER_CLI="nerdctl" CLOUDVM_TUNNEL_ENDPOINT="{{.TunnelEndpoint}}" CLOUDVM_INSTANCE_ID="$TUNNEL_INSTANCE_ID" CLOUDVM_PLATFORM="$TUNNEL_PLATFORM" \
  CLOUDVM_DIRECT_CIDRS="{{.DirectCIDRList}}" CLOUDVM_TUNNEL_CIDRS="{{.TunnelCIDRList}}" \
  CLOUDVM_TRANSPARENT_PORT="{{if .TransparentProxy}}{{.TransparentProxyPort}}{{else}}0{{end}}" CLOUDVM_DNS_LISTEN="127.0.0.153:53" CLOUDVM_DNS_TAKEOVER="1" \
  CLOUDVM_LOG_FILES="bootstrap=/var/log/cloudvm-bootstrap.log,runner=/var/log/cloudvm-runner.log,{{.SpotWatcherLogFile}}" \
  nohup python3 /usr/local/bin/cloudvm-tunnel-agent.py 2>&1 < /dev/null | cloudvm_mirror /var/log/cloudvm-tunnel-agent.log &
{{end}}


# 6. Orphaned Node Idle Fail-Safe Watchdog
# Powers off only after the node has had zero active containers AND no provider heartbeat for >{{if .IdleGracePeriodSeconds}}{{.IdleGracePeriodSeconds}}{{else}}600{{end}}s
(
  while true; do
    ACTIVE=0
    if command -v nerdctl >/dev/null 2>&1; then
      ACTIVE=$(nerdctl ps -q --filter "name=cloudvm-" 2>/dev/null | wc -l || echo 0)
    fi
    if [ "$ACTIVE" -eq 0 ]; then
      NOW=$(date +%s)
      HB_AGE=0
      if [ -f /tmp/cloudvm-heartbeat ]; then
        HB_TIME=$(cat /tmp/cloudvm-heartbeat 2>/dev/null | tr -dc '0-9.' || echo "")
        HB_INT=${HB_TIME%.*}
        if [ -n "$HB_INT" ] && [ "$HB_INT" -gt 0 ] 2>/dev/null; then
          HB_AGE=$(( NOW - HB_INT ))
        fi
      fi
      if [ "$HB_AGE" -gt {{if .IdleGracePeriodSeconds}}{{.IdleGracePeriodSeconds}}{{else}}600{{end}} ]; then
        sleep 60
        ACTIVE_AFTER=0
        if command -v nerdctl >/dev/null 2>&1; then
          ACTIVE_AFTER=$(nerdctl ps -q --filter "name=cloudvm-" 2>/dev/null | wc -l || echo 0)
        fi
        if [ -f /tmp/cloudvm-heartbeat ]; then
          HB_TIME=$(cat /tmp/cloudvm-heartbeat 2>/dev/null | tr -dc '0-9.' || echo "")
          HB_INT=${HB_TIME%.*}
          if [ -n "$HB_INT" ] && [ "$HB_INT" -gt 0 ] 2>/dev/null; then
            HB_AGE=$(( $(date +%s) - HB_INT ))
          fi
        fi
        if [ "$ACTIVE_AFTER" -eq 0 ] && [ "$HB_AGE" -gt {{if .IdleGracePeriodSeconds}}{{.IdleGracePeriodSeconds}}{{else}}600{{end}} ]; then
          echo "[cloudvm-bootstrap] Node has been idle with zero active containers and no provider heartbeat for >{{if .IdleGracePeriodSeconds}}{{.IdleGracePeriodSeconds}}{{else}}600{{end}}s. Terminating VM..."
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
	if cfg.IdleGracePeriodSeconds <= 0 {
		cfg.IdleGracePeriodSeconds = 600
	}
	if cfg.RunnerConcurrency < 0 {
		cfg.RunnerConcurrency = 0
	}
	// Router and public workers never join the tailnet, so a mesh key carried for other regions stays out of their user data.
	if cfg.TunnelTransport == string(TunnelTransportRouter) || cfg.TunnelTransport == string(TunnelTransportPublic) {
		cfg.MeshAuthKey, cfg.MeshAuthKeyCiphertext, cfg.MeshTags = "", "", ""
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
