// Runs the embedded runner's traffic meter against a fake nft CLI and published-range fixtures to verify the ruleset it
// installs, how it splits overlapping prefixes, and the per-peer bytes it reports.
package main

import (
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeNft records every script it loads and lists the counters in $FAKE_NFT_COUNTERS.
const fakeNft = `#!/usr/bin/env bash
if [ "$1" = "-f" ]; then cat >> "$FAKE_NFT_STATE/rules"; echo >> "$FAKE_NFT_STATE/rules"; exit 0; fi
if [ "$1" = "-j" ]; then cat "$FAKE_NFT_COUNTERS"; exit 0; fi
exit 0
`

const fakeAWSRanges = `{"prefixes":[
 {"ip_prefix":"3.0.0.0/15","region":"us-east-1","service":"AMAZON"},
 {"ip_prefix":"3.0.0.0/16","region":"us-east-1","service":"S3"},
 {"ip_prefix":"3.0.0.0/16","region":"us-east-1","service":"AMAZON"},
 {"ip_prefix":"3.1.0.0/16","region":"eu-south-2","service":"EC2"},
 {"ip_prefix":"13.32.0.0/15","region":"GLOBAL","service":"CLOUDFRONT"}],
 "ipv6_prefixes":[{"ipv6_prefix":"2600:1f18::/33","region":"us-east-1","service":"AMAZON"}]}`

const fakeGCPRanges = `{"prefixes":[{"ipv4Prefix":"34.1.208.0/20","service":"Google Cloud","scope":"europe-west4"}]}`

const fakeNftCounters = `{"nftables":[{"metainfo":{"version":"1.0.9"}},
 {"counter":{"family":"inet","name":"tx_aws_us_east_1","table":"cloudvm_traffic","handle":1,"packets":3,"bytes":4096}},
 {"counter":{"family":"inet","name":"rx_aws_us_east_1","table":"cloudvm_traffic","handle":2,"packets":9,"bytes":65536}},
 {"counter":{"family":"inet","name":"tx_tunnel","table":"cloudvm_traffic","handle":3,"packets":1,"bytes":512}},
 {"counter":{"family":"inet","name":"rx_private","table":"cloudvm_traffic","handle":4,"packets":0,"bytes":0}}]}`

func TestRunnerScript_TrafficMeterInstallsPeerMapsAndReportsBytes(t *testing.T) {
	h := newRunnerHarness(t)
	dir := filepath.Dir(h.script)
	write := func(name, body string, mode os.FileMode) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
		return p
	}
	nft := write("bin/nft", fakeNft, 0o755)
	write("cloudvm_traffic_meter.py", trafficMeterScript, 0o600)
	aws := write("ranges/aws.json", fakeAWSRanges, 0o600)
	gcp := write("ranges/gcp.json", fakeGCPRanges, 0o600)
	counters := write("nft-counters.json", fakeNftCounters, 0o600)
	write("net/eth0/device/vendor", "0x1d0f", 0o600)
	write("net/lo/mtu", "65536", 0o600)
	h.env = append(h.env,
		"CLOUDVM_NFT="+nft, "FAKE_NFT_STATE="+dir, "FAKE_NFT_COUNTERS="+counters,
		"CLOUDVM_TRAFFIC_PROVIDER=aws", "CLOUDVM_TRAFFIC_REGION=eu-south-2", "CLOUDVM_TRAFFIC_HOME=aws/us-east-1",
		"CLOUDVM_TRAFFIC_TUNNEL_IP=172.20.0.10", "CLOUDVM_TRAFFIC_RANGES=aws=file://"+aws+",gcp=file://"+gcp,
		"CLOUDVM_SYS_NET="+filepath.Join(dir, "net"), "CLOUDVM_CGROUP_ROOT="+filepath.Join(dir, "no-cgroup2"))
	defer h.start()()

	var rules string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		b, _ := os.ReadFile(filepath.Join(dir, "rules")) //nolint:gosec // test path
		if rules = string(b); strings.Contains(rules, "goto tx_internet") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, want := range []string{
		"add rule inet cloudvm_traffic out oifname != { \"eth0\" } return",
		"add element inet cloudvm_traffic tunnel4 { 172.20.0.10 }",
		"add rule inet cloudvm_traffic in ip saddr @tunnel4 goto rx_tunnel",
		// The eu-south-2 /16 inside the us-east-1 /15 splits it, the longer prefix wins, and S3 wins a tie with its region.
		"3.0.0.0-3.0.255.255 : goto tx_aws_us_east_1_storage, 3.1.0.0-3.1.255.255 : goto tx_aws_eu_south_2",
		"34.1.208.0-34.1.223.255 : goto tx_gcp_europe_west4",
		"10.0.0.0-10.255.255.255 : goto tx_private",
		"2600:1f18::-2600:1f18:7fff:ffff:ffff:ffff:ffff:ffff : goto rx_aws_us_east_1",
		"add rule inet cloudvm_traffic out goto tx_internet",
	} {
		if !strings.Contains(rules, want) {
			t.Errorf("ruleset lacks %q:\n%s", want, rules)
		}
	}
	// Before the ranges load, unmatched peers count as unclassified; global prefixes never get a region.
	if !strings.Contains(rules, "add rule inet cloudvm_traffic out goto tx_unclassified") {
		t.Error("first ruleset does not count unmatched peers as unclassified")
	}
	if strings.Contains(rules, "global") || strings.Contains(rules, "13.32.0.0") {
		t.Error("ruleset gives a global prefix a region")
	}
	if strings.Contains(rules, "socket cgroupv2") {
		t.Error("ruleset matches pod cgroups on a host without cgroup v2")
	}

	resp := h.do(http.MethodGet, "/host/stats", "")
	defer func() { _ = resp.Body.Close() }()
	var stats HostStatsResult
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatal(err)
	}
	if stats.Traffic == nil || !stats.Traffic.Classified {
		t.Fatalf("host stats traffic = %+v, want a classified reading", stats.Traffic)
	}
	if got := stats.Traffic.Peers["aws/us-east-1"]; got.Tx != 4096 || got.Rx != 65536 {
		t.Errorf("us-east-1 bytes = %+v", got)
	}
	if got := stats.Traffic.Peers["tunnel"]; got.Tx != 512 {
		t.Errorf("tunnel bytes = %+v", got)
	}
	if _, ok := stats.Traffic.Peers["private"]; ok {
		t.Error("a peer with no bytes is reported")
	}
}

// writeTrafficMeter writes the embedded traffic meter to a temporary file and returns its path.
func writeTrafficMeter(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}
	p := filepath.Join(t.TempDir(), "cloudvm_traffic_meter.py")
	if err := os.WriteFile(p, []byte(trafficMeterScript), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestRunnerScript_TrafficPeerClass(t *testing.T) {
	meter := writeTrafficMeter(t)
	probe := `import runpy, json, sys
m = runpy.run_path(sys.argv[1])
c = m['traffic_peer_class']
print(json.dumps([c(p, 'aws', 'eu-south-2', 'aws/us-east-1') for p in sys.argv[2:]]))`
	peers := []string{"private", "tunnel", "internet", "unclassified", "aws/eu-south-2", "aws/us-east-1", "aws/us-east-1/storage", "aws/eu-central-2", "aws/eu-central-2/storage", "gcp/europe-west4"}
	out, err := exec.Command("python3", append([]string{"-I", "-c", probe, meter}, peers...)...).Output() //nolint:gosec // script written by the test
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	want := []string{"same_region", "tunnel", "internet", "unclassified", "same_region", "home_region", "home_storage", "cross_region", "cross_region", "internet"}
	loc, home := TransferLocation{"aws", "eu-south-2"}, TransferLocation{"aws", "us-east-1"}
	for i, peer := range peers {
		if got[i] != want[i] {
			t.Errorf("runner class of %s = %s, want %s", peer, got[i], want[i])
		}
		// The plugin's classes for the VM's peers agree with the runner's for its pods.
		if goClass, _, _, _ := classifyTrafficPeer(peer, loc, home, true); peer != "tunnel" && goClass != want[i] {
			t.Errorf("plugin class of %s = %s, want %s", peer, goClass, want[i])
		}
	}
}

func TestRunnerScript_TrafficRulesetCountsPodsByCgroup(t *testing.T) {
	meter := writeTrafficMeter(t)
	probe := `import runpy, sys
m = runpy.run_path(sys.argv[1])
print(m['render_traffic_ruleset'](['tunnel', 'private', 'internet', 'unclassified', 'aws/us-east-1', 'aws/us-east-1/storage'], {4: [], 6: []}, [], [], 41641, 'internet',
    {'o0': ['system.slice/nerdctl-abc.scope'], 'o_pulls': ['system.slice/containerd.service']}, [13128, 13129]))`
	cmd := exec.Command("python3", "-I", "-c", probe, meter) //nolint:gosec // script written by the test
	cmd.Env = append(os.Environ(), "CLOUDVM_TRAFFIC_PROVIDER=aws", "CLOUDVM_TRAFFIC_REGION=eu-south-2", "CLOUDVM_TRAFFIC_HOME=aws/us-east-1")
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	rules := string(out)
	for _, want := range []string{
		`add element inet cloudvm_traffic own_tx_home_region_l2 { "system.slice/nerdctl-abc.scope" : "o0_tx_home_region", "system.slice/containerd.service" : "o_pulls_tx_home_region" }`,
		"add rule inet cloudvm_traffic tx_aws_us_east_1 counter name socket cgroupv2 level 2 map @own_tx_home_region_l2",
		"add rule inet cloudvm_traffic rx_private counter name socket cgroupv2 level 2 map @own_rx_same_region_l2",
		"add rule inet cloudvm_traffic out udp sport 41641 goto tx_tunnel",
		"add rule inet cloudvm_traffic in udp dport 41641 goto rx_tunnel",
		`add rule inet cloudvm_traffic in iifname { "lo", "tailscale0" } return`,
		// Object storage in the home region and the tunnel are separate pod classes.
		"add rule inet cloudvm_traffic tx_aws_us_east_1_storage counter name socket cgroupv2 level 2 map @own_tx_home_storage_l2",
		`add rule inet cloudvm_traffic out oifname "lo" meta l4proto tcp ct reply proto-src { 13128, 13129 } goto tx_pod_tunnel`,
		"add rule inet cloudvm_traffic rx_pod_tunnel counter name socket cgroupv2 level 2 map @own_rx_tunnel_l2",
	} {
		if !strings.Contains(rules, want) {
			t.Errorf("ruleset lacks %q:\n%s", want, rules)
		}
	}
	// Pods never own the tunnel's sockets, so the tunnel chains count only the VM.
	if strings.Contains(rules, "tx_tunnel counter name socket") {
		t.Error("tunnel chain matches pod cgroups")
	}
}

func TestRenderCloudInit_ConfiguresTheTrafficMeter(t *testing.T) {
	prev := GlobalTransferCost.cfg
	GlobalTransferCost.Configure(TransferCostConfig{HomeProvider: "aws", HomeRegion: "us-east-1"})
	defer GlobalTransferCost.Configure(prev)
	for _, tc := range []struct {
		name string
		cfg  CloudInitConfig
		want []string
	}{
		{"tailnet worker", CloudInitConfig{Provider: "aws", Region: "eu-south-2", TunnelEndpoint: "gw.example.com:7443", TunnelTransport: "tailnet", MeshAuthKey: "key"}, []string{
			`CLOUDVM_TRAFFIC_PROVIDER="aws" CLOUDVM_TRAFFIC_REGION="eu-south-2" CLOUDVM_TRAFFIC_HOME="aws/us-east-1"`,
			`CLOUDVM_TRAFFIC_TUNNEL_HOST="gw.example.com" CLOUDVM_TRAFFIC_TUNNEL_IP="" CLOUDVM_TRAFFIC_MESH_PORT="41641"`,
		}},
		{"router worker", CloudInitConfig{Provider: "gcp", Region: "europe-west4", TransferHome: "gcp/us-central1", TunnelEndpoint: "gw.example.com:7443", TunnelTransport: "router", TunnelGatewayIP: "172.20.0.10", MeshAuthKey: "key"}, []string{
			`CLOUDVM_TRAFFIC_PROVIDER="gcp" CLOUDVM_TRAFFIC_REGION="europe-west4" CLOUDVM_TRAFFIC_HOME="gcp/us-central1"`,
			`CLOUDVM_TRAFFIC_TUNNEL_IP="172.20.0.10" CLOUDVM_TRAFFIC_MESH_PORT="0"`,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.PodName, cfg.RunnerAuthToken = "p", "tok"
			script, err := RenderCloudInit(&cfg)
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range append(tc.want, "command -v nft >/dev/null 2>&1 || dnf install -y nftables",
				"cloudvm_fetch_script "+cfg.TrafficMeterScriptSHA256()+" /usr/local/bin/cloudvm_traffic_meter.py",
				`CLOUDVM_TRAFFIC_PROXY_PORTS="13128,13129"`) {
				if !strings.Contains(script, want) {
					t.Errorf("bootstrap lacks %q", want)
				}
			}
		})
	}
}
