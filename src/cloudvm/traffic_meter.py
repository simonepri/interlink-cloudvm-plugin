"""In-VM traffic meter: nftables counters on the VM's NICs split its bytes by direction and peer, and counters keyed by
the sending or receiving socket's cgroup split the same bytes by pod and by image pulls; the runner reports them.

A peer is the tunnel, a private address, a cloud region (or a region's object storage) read from the clouds' published
address ranges, or the internet. The bootstrap sets the CLOUDVM_TRAFFIC_* settings.
"""
import ipaddress
import json
import os
import re
import shutil
import socket
import subprocess
import threading
import time
import urllib.request

CONTAINER_CLI = os.environ.get("CLOUDVM_CONTAINER_CLI", 'nerdctl')
TRAFFIC_TABLE = 'cloudvm_traffic'
NFT_CLI = os.environ.get("CLOUDVM_NFT", 'nft')
TRAFFIC_PROVIDER = os.environ.get("CLOUDVM_TRAFFIC_PROVIDER", '')
TRAFFIC_REGION = os.environ.get("CLOUDVM_TRAFFIC_REGION", '')
TRAFFIC_HOME = os.environ.get("CLOUDVM_TRAFFIC_HOME", '')
TRAFFIC_TUNNEL_HOST = os.environ.get("CLOUDVM_TRAFFIC_TUNNEL_HOST", '')
TRAFFIC_TUNNEL_IP = os.environ.get("CLOUDVM_TRAFFIC_TUNNEL_IP", '')
TRAFFIC_MESH_PORT = int(os.environ.get("CLOUDVM_TRAFFIC_MESH_PORT") or 0)
# Loopback ports of the tunnel agent's proxies: a pod's connections to them are its tunnel traffic.
TRAFFIC_PROXY_PORTS = [int(p) for p in (os.environ.get("CLOUDVM_TRAFFIC_PROXY_PORTS") or '').split(',') if p.strip().isdigit()]
TRAFFIC_RANGES = os.environ.get("CLOUDVM_TRAFFIC_RANGES", 'aws=https://ip-ranges.amazonaws.com/ip-ranges.json,gcp=https://www.gstatic.com/ipranges/cloud.json')
TRAFFIC_SYS_NET = os.environ.get("CLOUDVM_SYS_NET", '/sys/class/net')
TRAFFIC_CGROUP_ROOT = os.environ.get("CLOUDVM_CGROUP_ROOT", '/sys/fs/cgroup')
TRAFFIC_PRIVATE = ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16', '100.64.0.0/10', '169.254.0.0/16', 'fc00::/7', 'fe80::/10')
# Image pulls run in containerd and the stargz snapshotter, outside any pod.
TRAFFIC_PULL_CGROUPS = ('system.slice/containerd.service', 'system.slice/stargz-snapshotter.service')
TRAFFIC_PULLS = 'pulls'
# Classes of a pod's peers relative to the VM; the plugin's classifyTrafficPeer gives the VM's peers the same classes.
TRAFFIC_CLASSES = ('same_region', 'home_region', 'home_storage', 'cross_region', 'internet', 'unclassified', 'tunnel')
# Published services whose prefixes become a region's object storage peer, provider/region/storage.
TRAFFIC_STORAGE_SERVICES = {'aws': 'S3'}
TRAFFIC_RETIRED_TTL = 600
TRAFFIC_CACHE_SECONDS = 5

def traffic_peer_class(peer, provider=None, region=None, home=None):
    """Returns the class of peer relative to a VM in provider/region whose cluster runs in home."""
    provider = TRAFFIC_PROVIDER if provider is None else provider
    region = TRAFFIC_REGION if region is None else region
    home = TRAFFIC_HOME if home is None else home
    if peer == 'private':
        return 'same_region'
    if peer in ('tunnel', 'internet', 'unclassified'):
        return peer
    parts = peer.split('/')
    location = '/'.join(parts[:2])
    if location == f"{provider}/{region}":
        return 'same_region'
    if location == home:
        return 'home_storage' if parts[2:] == ['storage'] else 'home_region'
    if parts[0] == provider:
        return 'cross_region'
    return 'internet'

def traffic_name(value):
    return re.sub(r'[^a-z0-9]', '_', value.lower())

def parse_cloud_ranges(provider, doc):
    """Returns (network, peer) pairs for every regional prefix in a cloud's published address ranges; prefixes of the
    provider's object storage service name the region's storage peer."""
    out = []
    if provider == 'aws':
        entries = [(p.get('ip_prefix'), p.get('region'), p.get('service')) for p in doc.get('prefixes', [])]
        entries += [(p.get('ipv6_prefix'), p.get('region'), p.get('service')) for p in doc.get('ipv6_prefixes', [])]
    elif provider == 'gcp':
        entries = [(p.get('ipv4Prefix') or p.get('ipv6Prefix'), p.get('scope'), p.get('service')) for p in doc.get('prefixes', [])]
    else:
        return out
    storage = TRAFFIC_STORAGE_SERVICES.get(provider)
    for prefix, region, service in entries:
        # Global prefixes, such as CDN edges, belong to no region.
        if not prefix or not region or region.lower() == 'global':
            continue
        peer = f"{provider}/{region}/storage" if storage and service == storage else f"{provider}/{region}"
        try:
            out.append((ipaddress.ip_network(prefix, strict=False), peer))
        except ValueError:
            continue
    return out

def partition_networks(entries):
    """Returns non-overlapping (first, last, peer) address ranges per IP version, where the longest prefix wins and,
    between equal prefixes, a storage peer wins over its region."""
    result = {4: [], 6: []}
    for version in (4, 6):
        items = sorted(((int(n.network_address), int(n.broadcast_address), n.prefixlen, peer) for n, peer in entries if n.version == version),
                       key=lambda t: (t[0], t[2], t[3].endswith('/storage')))
        out = result[version]
        def emit(first, last, peer):
            if first > last:
                return
            if out and out[-1][2] == peer and out[-1][1] + 1 == first:
                out[-1] = (out[-1][0], last, peer)
            else:
                out.append((first, last, peer))
        stack = []
        pos = 0
        for first, last, _, peer in items:
            while stack and stack[-1][0] < first:
                end, outer = stack.pop()
                emit(pos, end, outer)
                pos = max(pos, end + 1)
            if stack:
                emit(pos, first - 1, stack[-1][1])
            pos = max(pos, first)
            stack.append((last, peer))
        while stack:
            end, outer = stack.pop()
            emit(pos, end, outer)
            pos = max(pos, end + 1)
        addr = ipaddress.IPv4Address if version == 4 else ipaddress.IPv6Address
        result[version] = [(addr(f), addr(l), p) for f, l, p in out]
    return result

def physical_nics():
    """Returns the network interfaces backed by a device, which carry the VM's traffic on the wire."""
    try:
        return sorted(n for n in os.listdir(TRAFFIC_SYS_NET) if os.path.exists(os.path.join(TRAFFIC_SYS_NET, n, 'device')))
    except OSError:
        return []

def cgroup_of_pid(pid):
    try:
        with open(f'/proc/{int(pid)}/cgroup') as f:
            for line in f:
                if line.startswith('0::'):
                    return line[3:].strip().strip('/')
    except (OSError, ValueError):
        pass
    return ''

def container_pid(name):
    try:
        res = subprocess.run([CONTAINER_CLI, 'inspect', '--format', '{{.State.Pid}}', name], capture_output=True, text=True)
        return int(res.stdout.strip() or 0)
    except (OSError, ValueError):
        return 0

def render_traffic_ruleset(peers, ranges, nics, tunnel_ips, mesh_port, fallback, owners, proxy_ports=()):
    """Returns the nft script that (re)builds the traffic table in place, keeping the counters it already has.

    peers lists every peer with a counter, ranges maps 4 and 6 to (first, last, peer) ranges, owners maps the counter
    prefix of an owner, a pod or the image pulls, to its cgroup paths, each relative to the cgroup root, and proxy_ports
    are the tunnel agent's loopback ports."""
    t = f"inet {TRAFFIC_TABLE}"
    lines = [f"add table {t}"]
    levels = sorted({p.count('/') + 1 for paths in owners.values() for p in paths})
    for d in ('tx', 'rx'):
        for peer in peers:
            lines.append(f"add counter {t} {d}_{traffic_name(peer)}")
        for prefix in owners:
            for c in TRAFFIC_CLASSES:
                lines.append(f"add counter {t} {prefix}_{d}_{c}")
        for c in TRAFFIC_CLASSES:
            for level in levels:
                m = f"own_{d}_{c}_l{level}"
                lines.append(f"add map {t} {m} {{ type cgroupsv2 : counter; }}")
                lines.append(f"flush map {t} {m}")
        for v in (4, 6):
            lines.append(f"add map {t} peers{v}_{d} {{ type ipv{v}_addr : verdict; flags interval; }}")
            lines.append(f"flush map {t} peers{v}_{d}")
            lines.append(f"add set {t} tunnel{v} {{ type ipv{v}_addr; }}")
            lines.append(f"flush set {t} tunnel{v}")
        for chain in [f"{d}_{traffic_name(peer)}" for peer in peers] + [f"{d}_pod_tunnel"]:
            lines.append(f"add chain {t} {chain}")
            lines.append(f"flush chain {t} {chain}")
    lines.append(f"add chain {t} out {{ type filter hook output priority 300; policy accept; }}")
    lines.append(f"add chain {t} in {{ type filter hook input priority 300; policy accept; }}")
    lines.append(f"flush chain {t} out")
    lines.append(f"flush chain {t} in")
    for d in ('tx', 'rx'):
        for c in TRAFFIC_CLASSES:
            for level in levels:
                elems = [f'"{p}" : "{o}_{d}_{c}"' for o, paths in owners.items() for p in paths if p.count('/') + 1 == level]
                if elems:
                    lines.append(f"add element {t} own_{d}_{c}_l{level} {{ {', '.join(elems)} }}")
        for v in (4, 6):
            elems = [f"{f}-{l} : goto {d}_{traffic_name(p)}" if f != l else f"{f} : goto {d}_{traffic_name(p)}" for f, l, p in ranges.get(v, [])]
            if elems:
                lines.append(f"add element {t} peers{v}_{d} {{ {', '.join(elems)} }}")
        for peer in peers:
            chain = f"{d}_{traffic_name(peer)}"
            lines.append(f'add rule {t} {chain} counter name "{chain}"')
            # The tunnel's NIC bytes belong to the agent; pods' tunnel bytes are their loopback bytes to its proxies.
            if peer == 'tunnel':
                continue
            for level in levels:
                lines.append(f"add rule {t} {chain} counter name socket cgroupv2 level {level} map @own_{d}_{traffic_peer_class(peer)}_l{level}")
        for level in levels:
            lines.append(f"add rule {t} {d}_pod_tunnel counter name socket cgroupv2 level {level} map @own_{d}_tunnel_l{level}")
    for v in (4, 6):
        ips = [ip for ip in tunnel_ips if ipaddress.ip_address(ip).version == v]
        if ips:
            lines.append(f"add element {t} tunnel{v} {{ {', '.join(ips)} }}")
    for d, chain, iface, addr, port in (('tx', 'out', 'oifname', 'daddr', 'sport'), ('rx', 'in', 'iifname', 'saddr', 'dport')):
        if proxy_ports and levels:
            ports = ', '.join(str(int(p)) for p in proxy_ports)
            lines.append(f'add rule {t} {chain} {iface} "lo" meta l4proto tcp ct reply proto-src {{ {ports} }} goto {d}_pod_tunnel')
        if nics:
            lines.append(f"add rule {t} {chain} {iface} != {{ {', '.join(json.dumps(n) for n in nics)} }} return")
        else:
            lines.append(f'add rule {t} {chain} {iface} {{ "lo", "tailscale0" }} return')
        lines.append(f"add rule {t} {chain} ip {addr} @tunnel4 goto {d}_tunnel")
        lines.append(f"add rule {t} {chain} ip6 {addr} @tunnel6 goto {d}_tunnel")
        if mesh_port:
            lines.append(f"add rule {t} {chain} udp {port} {int(mesh_port)} goto {d}_tunnel")
        lines.append(f"add rule {t} {chain} ip {addr} vmap @peers4_{d}")
        lines.append(f"add rule {t} {chain} ip6 {addr} vmap @peers6_{d}")
        lines.append(f"add rule {t} {chain} goto {d}_{traffic_name(fallback)}")
    return '\n'.join(lines) + '\n'

class TrafficMeter:
    """Keeps the traffic table in step with the VM's peers and pods and reads its counters."""

    def __init__(self):
        self.lock = threading.Lock()
        self.enabled = bool(TRAFFIC_REGION) and shutil.which(NFT_CLI) is not None
        self.cgroups = os.path.exists(os.path.join(TRAFFIC_CGROUP_ROOT, 'cgroup.controllers'))
        self.ranges = partition_networks([(ipaddress.ip_network(n), 'private') for n in TRAFFIC_PRIVATE])
        self.loaded = False
        self.applied = False
        self.levels = set()
        self.slots = {}
        self.tunnel_ips = [TRAFFIC_TUNNEL_IP] if TRAFFIC_TUNNEL_IP else []
        self.owners = {}
        self.pods = {}
        self.retired = {}
        self.cache = (0, None)
        self.owner_rules = self.cgroups

    def peers(self):
        found = {p for _, _, p in self.ranges[4] + self.ranges[6]}
        return ['tunnel', 'private', 'internet', 'unclassified'] + sorted(found - {'private'})

    def apply(self):
        """Rebuilds the table; when the kernel refuses the per-pod rules, falls back to VM counters only."""
        if not self.enabled:
            return False
        owners = {}
        if self.owner_rules:
            # A cgroup that is gone, such as an exited container's, would fail the whole transaction.
            for owner, paths in self.owners.items():
                live = [p for p in paths if os.path.isdir(os.path.join(TRAFFIC_CGROUP_ROOT, p))]
                if live:
                    owners[self.prefix(owner)] = live
        script = render_traffic_ruleset(self.peers(), self.ranges, physical_nics(), self.tunnel_ips, TRAFFIC_MESH_PORT,
                                        'internet' if self.loaded else 'unclassified', owners, TRAFFIC_PROXY_PORTS)
        res = subprocess.run([NFT_CLI, '-f', '-'], input=script, capture_output=True, text=True)
        if res.returncode != 0 and owners:
            # Kernels without socket cgroup matching in the output hook refuse the per-pod rules.
            print(f"[cloudvm-runner] per-pod traffic counters refused, counting the VM only this time: {res.stderr.strip()}", flush=True)
            permanent = set(owners) == {self.prefix(TRAFFIC_PULLS)}
            self.owner_rules = False
            ok = self.apply()
            self.owner_rules = not permanent
            return ok
        if res.returncode != 0:
            # A table that never loaded means the kernel or nft cannot meter; a later refusal keeps the previous table.
            print(f"[cloudvm-runner] cannot update the traffic meter: {res.stderr.strip()}", flush=True)
            self.enabled = self.applied
            return False
        self.applied = True
        self.levels = {p.count('/') + 1 for paths in owners.values() for p in paths}
        self.cache = (0, None)
        return True

    def load_ranges(self):
        entries = [(ipaddress.ip_network(n), 'private') for n in TRAFFIC_PRIVATE]
        for spec in TRAFFIC_RANGES.split(','):
            provider, _, url = spec.partition('=')
            if not url:
                continue
            with urllib.request.urlopen(url, timeout=30) as resp:
                entries += parse_cloud_ranges(provider.strip(), json.loads(resp.read()))
        ranges = partition_networks(entries)
        with self.lock:
            self.ranges, self.loaded = ranges, True
            self.apply()

    def resolve_tunnel(self):
        if not TRAFFIC_TUNNEL_HOST:
            return
        try:
            ips = sorted({ai[4][0] for ai in socket.getaddrinfo(TRAFFIC_TUNNEL_HOST, None)} | set(self.tunnel_ips))
        except OSError:
            return
        with self.lock:
            if ips != self.tunnel_ips:
                self.tunnel_ips = ips
                self.apply()

    def run(self):
        """Installs the table with private peers at once, then loads the cloud ranges, retrying until they load."""
        with self.lock:
            if not self.enabled:
                return
            self.owners[TRAFFIC_PULLS] = list(TRAFFIC_PULL_CGROUPS)
            self.apply()
        self.adopt_running_pods()
        delay = 30
        while True:
            self.resolve_tunnel()
            if not self.loaded:
                try:
                    self.load_ranges()
                except Exception as e:
                    print(f"[cloudvm-runner] cannot load cloud address ranges, peers stay unclassified: {e}", flush=True)
                    time.sleep(delay)
                    delay = min(delay * 2, 600)
                    continue
            time.sleep(300)

    def adopt_running_pods(self):
        """Meters the pod containers a previous runner started, whose names and namespaces it no longer knows."""
        try:
            res = subprocess.run([CONTAINER_CLI, 'ps', '--filter', 'name=cloudvm-', '--format', '{{.Names}}'], capture_output=True, text=True)
        except OSError:
            return
        for name in res.stdout.split():
            # Init containers, cloudvm-<uid>-init-<index>-<name>, belong to the pod their main container meters.
            if '-init-' not in name:
                self.add_pod(name.removeprefix('cloudvm-'), '', '')

    def prefix(self, owner):
        """Returns the counter name prefix of an owner: pods take numbered slots, because the kernel may keep a counter
        busy after the elements naming it are gone, so counters are reset and reused instead of deleted."""
        if owner == TRAFFIC_PULLS:
            return f"o_{TRAFFIC_PULLS}"
        if owner not in self.slots:
            used = set(self.slots.values())
            self.slots[owner] = next(i for i in range(len(used) + 1) if i not in used)
        return f"o{self.slots[owner]}"

    def owner_change(self, owner, paths, add):
        """Adds or removes one owner's map elements, resetting its counters on add, without rebuilding the peer maps,
        and returns whether nft accepted the change."""
        t = f"inet {TRAFFIC_TABLE}"
        lines = []
        for d in ('tx', 'rx'):
            for c in TRAFFIC_CLASSES:
                name = f"{self.prefix(owner)}_{d}_{c}"
                if add:
                    lines.append(f"add counter {t} {name}")
                    lines.append(f"reset counter {t} {name}")
                for p in paths:
                    m = f"own_{d}_{c}_l{p.count('/') + 1}"
                    lines.append(f'add element {t} {m} {{ "{p}" : "{name}" }}' if add else f'delete element {t} {m} {{ "{p}" }}')
        res = subprocess.run([NFT_CLI, '-f', '-'], input='\n'.join(lines) + '\n', capture_output=True, text=True)
        self.cache = (0, None)
        return res.returncode == 0

    def add_pod(self, uid, namespace, name, pid=None):
        """Meters the pod whose container runs as cloudvm-<uid>, or as pid."""
        path = cgroup_of_pid(container_pid(f"cloudvm-{uid}") if pid is None else pid)
        with self.lock:
            if not self.enabled or not path:
                return
            self.owners[uid] = [path]
            self.pods[uid] = {k: v for k, v in (('namespace', namespace), ('pod', name)) if v}
            # A pod whose cgroup depth has no rules yet needs a rebuild that adds them.
            if not self.owner_rules or path.count('/') + 1 not in self.levels or not self.owner_change(uid, [path], True):
                self.apply()

    def remove_pod(self, uid):
        with self.lock:
            if uid not in self.owners:
                return
            final = self.read_locked().get('owners', {}).get(uid)
            if final:
                self.retired[uid] = (time.time(), final)
            paths = self.owners.pop(uid)
            self.pods.pop(uid, None)
            # Removing elements fails once the cgroup is gone, as after the container exited; a rebuild drops them.
            if self.owner_rules and not self.owner_change(uid, paths, False):
                self.apply()
            self.slots.pop(uid, None)

    def read_locked(self):
        res = subprocess.run([NFT_CLI, '-j', 'list', 'counters', 'table', 'inet', TRAFFIC_TABLE], capture_output=True, text=True)
        if res.returncode != 0:
            return {}
        counts = {}
        for obj in json.loads(res.stdout).get('nftables', []):
            c = obj.get('counter')
            if c:
                counts[c.get('name')] = int(c.get('bytes') or 0)
        peers = {}
        for p in self.peers():
            rx, tx = counts.get(f"rx_{traffic_name(p)}", 0), counts.get(f"tx_{traffic_name(p)}", 0)
            if rx or tx:
                peers[p] = {'rx': rx, 'tx': tx}
        owners = {}
        for o in self.owners:
            classes = {}
            for c in TRAFFIC_CLASSES:
                rx, tx = counts.get(f"{self.prefix(o)}_rx_{c}", 0), counts.get(f"{self.prefix(o)}_tx_{c}", 0)
                if rx or tx:
                    classes[c] = {'rx': rx, 'tx': tx}
            owners[o] = dict(self.pods.get(o, {}), peers=classes)
        return {'peers': peers, 'owners': owners}

    def report(self, pod_uid=None):
        """Returns the cumulative bytes per peer and per owner, or None while the meter is off. With pod_uid only that
        pod and the image pulls are listed."""
        with self.lock:
            if not self.enabled:
                return None
            at, cached = self.cache
            if cached is None or time.time() - at > TRAFFIC_CACHE_SECONDS:
                cached = self.read_locked()
                now = time.time()
                for uid, (retired_at, final) in list(self.retired.items()):
                    if now - retired_at > TRAFFIC_RETIRED_TTL:
                        del self.retired[uid]
                    else:
                        cached.setdefault('owners', {}).setdefault(uid, final)
                cached['classified'] = self.loaded
                self.cache = (time.time(), cached)
        if pod_uid is None:
            return cached
        owners = cached.get('owners', {})
        return dict(cached, owners={k: v for k, v in owners.items() if k in (pod_uid, TRAFFIC_PULLS)})
