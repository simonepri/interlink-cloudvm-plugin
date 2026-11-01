"""In-VM reverse tunnel agent: dials the plugin's tunnel gateway over TLS, bridges Yamux streams to the local runner and local ports, streams worker logs, and serves DNS.

Attributes every connection it tunnels to the pod whose process opened it, through /proc, so the gateway authorizes it per pod.

Implements the client half of the github.com/hashicorp/yamux framing on one asyncio loop (stdlib only, Python 3.7+).
"""
import asyncio
import base64
import collections
import datetime
import ipaddress
import json
import os
import random
import re
import shutil
import socket
import ssl
import struct
import sys
import threading
import time

VERSION = 0
T_DATA, T_WINDOW, T_PING, T_GOAWAY = 0, 1, 2, 3
F_SYN, F_ACK, F_FIN, F_RST = 1, 2, 4, 8
HEADER = struct.Struct(">BBHII")
INITIAL_WINDOW = 256 * 1024
# Each stream grants its peer this much room when it opens; at about 130ms between continents it allows roughly 60 MB/s.
STREAM_WINDOW = 8 * 1024 * 1024
MAX_FRAME = 16 * 1024
# The gateway pings every 30s; a link that stays silent for this long is treated as dead.
READ_IDLE_TIMEOUT = 90
# The first line of the agent-opened log stream; newline-delimited JSON log records follow it.
LOG_STREAM_HEADER = b'{"stream":"logs","version":1}\n'
# A stream whose first line is a tcp header carries one plain TCP connection to the header's target.
TCP_STREAM_KIND = "tcp"
MAX_STREAM_HEADER = 4096
DEFAULT_TCP_PROXY_PORT = "13128"
# A dns stream carries one DNS query, ended by the agent's FIN, and its reply.
DNS_STREAM_HEADER = b'{"stream":"dns","version":1}\n'
DNS_TIMEOUT = 5
MAX_LOG_MESSAGE = 16 * 1024
LOG_READ_CHUNK = 64 * 1024
LOG_POLL_SECONDS = 1
CONTAINER_POLL_SECONDS = 5
CONTAINER_PREFIX = "cloudvm-"
DEFAULT_LOG_FILES = "bootstrap=/var/log/cloudvm-bootstrap.log,runner=/var/log/cloudvm-runner.log"
# The bootstrap appends "<phase> <epoch seconds>" here as each boot phase ends.
DEFAULT_BOOT_MARKS = "/run/cloudvm/boot-marks"
MAX_BOOT_MARKS = 16
# A process in a pod runs in the pod's cgroup, /kubepods/<qos>/pod<uid>/<container> or its systemd-slice spelling.
POD_CGROUP = re.compile(
    r"kubepods\b.*?[/-]pod([0-9a-fA-F]{8}[-_][0-9a-fA-F]{4}[-_][0-9a-fA-F]{4}[-_][0-9a-fA-F]{4}[-_][0-9a-fA-F]{12})"
    r"(?:\.slice)?(?:/|$)")
# The pod a stream names when the agent cannot tell which process opened its connection; the gateway refuses it.
UNATTRIBUTED_POD = "unattributed"
# How long the agent remembers which pod holds a socket or a process.
ATTRIBUTION_TTL = 10
MAX_ATTRIBUTION_CACHE = 4096
# Bounds the file descriptors one attribution reads before it gives up, so a VM with huge fd tables stays responsive.
MAX_ATTRIBUTION_FDS = 200000
HOT_PIDS = 64
# Answers to the allow-listed external hostnames come from the RFC 2544 benchmarking range the bootstrap redirects.
SYNTHETIC_NETWORK = ipaddress.ip_network("198.18.0.0/15")
SYNTHETIC_TTL = 60


def log(msg):
    sys.stderr.write("[cloudvm-tunnel-agent] %s\n" % msg)
    sys.stderr.flush()


class Stream(object):
    """One Yamux stream bridged to a TCP connection: a gateway-opened stream dials the runner or the target its tcp
    header names on this host, and an agent-opened stream carries a connection accepted by the local CONNECT proxy."""

    def __init__(self, session, sid, local=None):
        self.session = session
        self.sid = sid
        self.inbound = asyncio.Queue()
        self.send_window = INITIAL_WINDOW
        self.window_open = asyncio.Event()
        self.window_open.set()
        self.task = None
        self.local = local

    def grow_window(self, delta):
        self.send_window += delta
        if self.send_window > 0:
            self.window_open.set()

    def on_data(self, payload):
        self.inbound.put_nowait(payload)

    def on_fin(self):
        self.inbound.put_nowait(None)

    async def run(self):
        if self.local is not None:
            local_reader, local_writer = self.local
        else:
            target, rest, consumed = await self.read_target()
            if target is None:
                self.reset()
                return
            try:
                local_reader, local_writer = await asyncio.wait_for(asyncio.open_connection(*target), 10)
            except (OSError, asyncio.TimeoutError) as e:
                log("stream %d: cannot reach %s:%d: %s" % (self.sid, target[0], target[1], e))
                self.reset()
                return
            if rest:
                local_writer.write(rest)
            if consumed:
                self.session.send(T_WINDOW, 0, self.sid, consumed)
        pump_in = asyncio.ensure_future(self.pump_in(local_writer))
        try:
            await self.pump_out(local_reader)
            await pump_in
        except (OSError, AssertionError) as e:
            # The gateway end only learns that this connection ended from a reset.
            log("stream %d: %s" % (self.sid, e))
            self.reset()
        finally:
            pump_in.cancel()
            local_writer.close()
            self.session.streams.pop(self.sid, None)

    def reset(self):
        self.session.send(T_WINDOW, F_RST, self.sid, 0)
        self.session.streams.pop(self.sid, None)

    async def read_target(self):
        """Returns where to bridge this gateway-opened stream: the address of this VM its tcp header names, or the runner.

        Also returns the bytes read past the header and how many received bytes to credit back to the gateway.
        """
        first = await self.inbound.get()
        if first is None:
            return None, b"", 0
        if not first.startswith(b"{"):
            return self.session.runner_addr, first, len(first)
        buf = first
        while b"\n" not in buf and len(buf) < MAX_STREAM_HEADER:
            chunk = await self.inbound.get()
            if chunk is None:
                return None, b"", 0
            buf += chunk
        line, _, rest = buf.partition(b"\n")
        try:
            header = json.loads(line.decode())
            host, _, port = header["target"].rpartition(":")
            host, port = host.strip("[]"), int(port)
        except (ValueError, KeyError, AttributeError) as e:
            log("stream %d: invalid stream header: %s" % (self.sid, e))
            return None, b"", 0
        if header.get("stream") != TCP_STREAM_KIND or not is_own_address(host):
            log("stream %d: refused stream to %s" % (self.sid, header.get("target")))
            return None, b"", 0
        return (host, port), rest, len(buf)

    async def pump_in(self, local_writer):
        """Copies tunnel data into the local runner and returns receive window credit to the gateway."""
        while True:
            chunk = await self.inbound.get()
            if chunk is None:
                if local_writer.can_write_eof():
                    try:
                        local_writer.write_eof()
                    except OSError:
                        pass  # the local end already closed
                return
            local_writer.write(chunk)
            await local_writer.drain()
            self.session.send(T_WINDOW, 0, self.sid, len(chunk))

    async def pump_out(self, local_reader):
        """Copies runner responses into the tunnel without exceeding the gateway's receive window."""
        while True:
            await self.window_open.wait()
            data = await local_reader.read(min(self.send_window, MAX_FRAME))
            if not data:
                self.session.send(T_WINDOW, F_FIN, self.sid, 0)
                return
            self.send_window -= len(data)
            if self.send_window <= 0:
                self.window_open.clear()
            self.session.send(T_DATA, 0, self.sid, len(data), data)
            await self.session.drain()


class LogStream(object):
    """Agent-opened Yamux stream that carries log records to the gateway within its receive window."""

    def __init__(self, session, sid):
        self.session = session
        self.sid = sid
        self.send_window = INITIAL_WINDOW
        self.window_open = asyncio.Event()
        self.window_open.set()
        self.closed = False
        self.task = None

    def grow_window(self, delta):
        self.send_window += delta
        if self.send_window > 0:
            self.window_open.set()

    def on_data(self, payload):
        pass

    def on_fin(self):
        self.closed = True
        self.window_open.set()

    async def write(self, data):
        while data:
            await self.window_open.wait()
            if self.closed:
                raise EOFError("gateway closed the log stream")
            n = min(len(data), self.send_window, MAX_FRAME)
            self.send_window -= n
            if self.send_window <= 0:
                self.window_open.clear()
            self.session.send(T_DATA, 0, self.sid, n, data[:n])
            await self.session.drain()
            data = data[n:]


class DNSStream(object):

    def __init__(self, sid):
        self.sid, self.task, self.received, self.done = sid, None, b"", asyncio.Event()

    def grow_window(self, delta):
        pass

    def on_data(self, payload):
        self.received += payload

    def on_fin(self):
        self.done.set()


class Session(object):
    def __init__(self, reader, writer, runner_addr):
        self.reader = reader
        self.writer = writer
        self.runner_addr = runner_addr
        self.streams = {}
        self.next_sid = 1  # Yamux clients open odd stream IDs
        # On Python 3.9, the system Python of Amazon Linux 2023, a second concurrent drain() fails an assertion.
        self.drain_lock = asyncio.Lock()

    def send(self, ftype, flags, sid, length, payload=b""):
        self.writer.write(HEADER.pack(VERSION, ftype, flags, sid, length) + payload)

    async def drain(self):
        """Waits until the session socket accepts more frames, letting one stream wait at a time."""
        async with self.drain_lock:
            await self.writer.drain()

    def open_tcp_stream(self, target, local_reader, local_writer, pod=""):
        """Opens a stream asking the gateway to connect to target for pod, bridged to an accepted local connection.

        The caller starts the stream once it has answered its client.
        """
        stream = Stream(self, self.next_sid, local=(local_reader, local_writer))
        self.next_sid += 2
        self.streams[stream.sid] = stream
        self.send(T_WINDOW, F_SYN, stream.sid, STREAM_WINDOW - INITIAL_WINDOW)
        header = stream_header({"stream": TCP_STREAM_KIND, "version": 1, "target": target}, pod)
        stream.send_window -= len(header)
        self.send(T_DATA, 0, stream.sid, len(header), header)
        return stream

    def open_dns_stream(self, query, pod=""):
        stream = DNSStream(self.next_sid)
        self.next_sid += 2
        self.streams[stream.sid] = stream
        self.send(T_WINDOW, F_SYN, stream.sid, 0)
        data = (stream_header({"stream": "dns", "version": 1}, pod) if pod else DNS_STREAM_HEADER) + query
        # HACK(simonepri): yamux marks the stream closed before it copies a FIN frame's payload, so the gateway can read EOF
        # and drop the query; send the FIN in its own frame. https://github.com/hashicorp/yamux/issues/184
        self.send(T_DATA, 0, stream.sid, len(data), data)
        self.send(T_DATA, F_FIN, stream.sid, 0)
        return stream

    def open_log_stream(self, collector):
        """Opens the session's log stream and starts shipping the collector's records over it."""
        stream = LogStream(self, self.next_sid)
        self.next_sid += 2
        self.streams[stream.sid] = stream
        self.send(T_WINDOW, F_SYN, stream.sid, 0)
        stream.task = asyncio.ensure_future(collector.ship(stream))

    async def serve(self):
        try:
            while True:
                header = await asyncio.wait_for(self.reader.readexactly(HEADER.size), READ_IDLE_TIMEOUT)
                version, ftype, flags, sid, length = HEADER.unpack(header)
                if version != VERSION:
                    raise ValueError("unsupported yamux version %d" % version)
                if ftype == T_PING:
                    if flags & F_SYN:
                        self.send(T_PING, F_ACK, 0, length)
                    continue
                if ftype == T_GOAWAY:
                    raise EOFError("gateway sent go-away (code %d)" % length)
                payload = b""
                if ftype == T_DATA and length:
                    payload = await asyncio.wait_for(self.reader.readexactly(length), READ_IDLE_TIMEOUT)
                self.on_stream_frame(ftype, flags, sid, length, payload)
        finally:
            tasks = [s.task for s in self.streams.values() if s.task is not None]
            for task in tasks:
                task.cancel()
            self.streams.clear()
            if tasks:
                # Let cancelled shippers stop their log followers before the loop pauses for the reconnect backoff.
                await asyncio.wait(tasks, timeout=5)

    def on_stream_frame(self, ftype, flags, sid, length, payload):
        stream = self.streams.get(sid)
        if stream is None and flags & F_SYN:
            stream = Stream(self, sid)
            self.streams[sid] = stream
            self.send(T_WINDOW, F_ACK, sid, STREAM_WINDOW - INITIAL_WINDOW)
            stream.task = asyncio.ensure_future(stream.run())
        if stream is None:
            return
        if ftype == T_WINDOW and length:
            stream.grow_window(length)
        if payload:
            stream.on_data(payload)
        if flags & F_RST:
            self.streams.pop(sid, None)
            if stream.task is not None:
                stream.task.cancel()
        elif flags & F_FIN:
            stream.on_fin()


def stream_header(header, pod):
    """Encodes the first line of an agent-opened stream, naming pod when the connection came from one."""
    if pod:
        header = dict(header, pod=pod)
    return (json.dumps(header) + "\n").encode()


def proc_address(raw):
    """Decodes an address of /proc/net/{tcp,udp}{,6}, whose 32-bit words are in host byte order."""
    data = bytes.fromhex(raw)
    if sys.byteorder == "little":
        data = b"".join(data[i:i + 4][::-1] for i in range(0, len(data), 4))
    ip = ipaddress.ip_address(data)
    return ip.ipv4_mapped or ip if ip.version == 6 else ip


def pod_of_cgroup(text):
    """Returns the UID of the pod whose cgroup a /proc/<pid>/cgroup file names, or "" for a process outside every pod."""
    for line in text.splitlines():
        match = POD_CGROUP.search(line)
        if match:
            return match.group(1).replace("_", "-").lower()
    return ""


class PodResolver(object):
    """Attributes a local socket to the pod whose process holds it: /proc/net gives the socket's inode, /proc/<pid>/fd
    the process holding that inode, and /proc/<pid>/cgroup the process's pod.

    Containers share the VM's network, so the socket, not its address, tells pods apart. Lookups run on executor
    threads; the caches and the recently matched processes, which are checked before a full scan, sit behind a lock.
    """

    def __init__(self, proc_root="/proc"):
        self.proc_root = proc_root
        self.own_pid = str(os.getpid())
        self.lock = threading.Lock()
        self.by_inode = {}
        self.by_pid = {}
        self.hot = collections.OrderedDict()

    def pod_of(self, proto, peer):
        """Returns the pod UID of the process holding the local proto ("tcp" or "udp") socket with address peer, "" for
        a process of the VM itself, or None when no process can be found."""
        inode = self.socket_inode(proto, peer)
        if inode is None:
            return None
        now = time.monotonic()
        pod = self.cached(self.by_inode, inode, now)
        if pod is not None:
            return pod
        pid = self.socket_pid(inode)
        if pid is None:
            return None
        pod = self.cached(self.by_pid, pid, now)
        if pod is None:
            try:
                with open(os.path.join(self.proc_root, pid, "cgroup")) as f:
                    pod = pod_of_cgroup(f.read())
            except OSError:
                return None
            self.remember(self.by_pid, pid, pod, now)
        self.remember(self.by_inode, inode, pod, now)
        return pod

    def cached(self, cache, key, now):
        with self.lock:
            entry = cache.get(key)
        return entry[1] if entry and entry[0] > now else None

    def remember(self, cache, key, pod, now):
        with self.lock:
            if len(cache) >= MAX_ATTRIBUTION_CACHE:
                for k in [k for k, v in cache.items() if v[0] <= now]:
                    del cache[k]
                if len(cache) >= MAX_ATTRIBUTION_CACHE:
                    cache.clear()
            cache[key] = (now + ATTRIBUTION_TTL, pod)

    def socket_inode(self, proto, peer):
        """Returns the inode of the socket bound to peer in /proc/net; a UDP socket may be bound to a wildcard address."""
        ip, port = ipaddress.ip_address(peer[0].split("%")[0]), int(peer[1])
        if ip.version == 6 and ip.ipv4_mapped:
            ip = ip.ipv4_mapped
        wildcard = None
        for name in (proto, proto + "6"):
            try:
                with open(os.path.join(self.proc_root, "net", name)) as f:
                    next(f, None)
                    for line in f:
                        fields = line.split()
                        if len(fields) < 10 or fields[9] == "0":
                            continue
                        local, _, local_port = fields[1].partition(":")
                        if int(local_port, 16) != port:
                            continue
                        addr = proc_address(local)
                        if addr == ip:
                            return fields[9]
                        if proto == "udp" and addr.is_unspecified and wildcard is None:
                            wildcard = fields[9]
            except (OSError, ValueError):
                continue
        return wildcard

    def holds(self, pid, link):
        fd_dir = os.path.join(self.proc_root, pid, "fd")
        try:
            fds = os.listdir(fd_dir)
        except OSError:
            return False, 0
        for fd in fds:
            try:
                if os.readlink(os.path.join(fd_dir, fd)) == link:
                    return True, len(fds)
            except OSError:
                pass
        return False, len(fds)

    def socket_pid(self, inode):
        """Returns the PID of a process holding the socket inode, checking the recently matched processes first."""
        link = "socket:[%s]" % inode
        with self.lock:
            hot = list(reversed(self.hot))
        for pid in hot:
            if self.holds(pid, link)[0]:
                self.touch(pid)
                return pid
        try:
            pids = [p for p in os.listdir(self.proc_root) if p.isdigit() and p != self.own_pid and p not in hot]
        except OSError:
            return None
        budget = MAX_ATTRIBUTION_FDS
        for pid in pids:
            found, read = self.holds(pid, link)
            if found:
                self.touch(pid)
                return pid
            budget -= read
            if budget <= 0:
                log("gave up attributing socket %s after reading %d file descriptors" % (inode, MAX_ATTRIBUTION_FDS))
                return None
        return None

    def touch(self, pid):
        with self.lock:
            self.hot.pop(pid, None)
            self.hot[pid] = True
            while len(self.hot) > HOT_PIDS:
                self.hot.popitem(last=False)


def is_own_address(host):
    """Reports whether host is localhost or an IP address of this VM, which only a local address can bind to."""
    if host == "localhost":
        return True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    if ip.is_unspecified or ip.is_multicast:
        return False
    try:
        with socket.socket(socket.AF_INET6 if ip.version == 6 else socket.AF_INET) as s:
            s.bind((host, 0))
    except OSError:
        return False
    return True


def now_rfc3339():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def log_record(source, message, time_str=None, pod=None, fd=None):
    rec = {"time": time_str or now_rfc3339(), "src": source, "msg": message[:MAX_LOG_MESSAGE]}
    if pod:
        rec["pod"] = pod
    if fd:
        rec["fd"] = fd
    return (json.dumps(rec) + "\n").encode()


class LogCollector(object):
    """Follows the bootstrap and runner log files and the runner's container output, resuming where the last session stopped.

    Progress advances only after a record is written to a session, so records queued when a session drops are resent on the next.
    The files and Docker's own log store are the buffer; the in-memory queue stays small and producers wait when it is full.
    """

    def __init__(self, files, container_cli):
        self.files = files
        self.container_cli = container_cli
        self.offsets = {}
        self.container_since = {}
        self.finished = set()

    async def ship(self, stream):
        queue = asyncio.Queue(maxsize=256)
        tasks = [asyncio.ensure_future(self.follow_file(source, path, queue)) for source, path in self.files]
        if self.container_cli:
            tasks.append(asyncio.ensure_future(self.watch_containers(queue)))
        try:
            await stream.write(LOG_STREAM_HEADER)
            while True:
                record, commit = await queue.get()
                await stream.write(record)
                commit()
        except EOFError as e:
            log("log stream ended: %s" % e)
        finally:
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.wait(tasks, timeout=5)

    def commit_offset(self, path, offset):
        def commit():
            self.offsets[path] = offset
        return commit

    async def follow_file(self, source, path, queue):
        pos = self.offsets.get(path, 0)
        while True:
            try:
                with open(path, "rb") as f:
                    if os.fstat(f.fileno()).st_size < pos:
                        pos = 0  # truncated or replaced
                    f.seek(pos)
                    data = f.read(LOG_READ_CHUNK)
            except OSError:
                data = b""
            end = data.rfind(b"\n") + 1
            if end == 0 and len(data) == LOG_READ_CHUNK:
                end = len(data)  # a line longer than one chunk ships in pieces
            if end == 0:
                await asyncio.sleep(LOG_POLL_SECONDS)
                continue
            start = 0
            while start < end:
                newline = data.find(b"\n", start, end)
                stop = end if newline < 0 else newline + 1
                text = data[start:stop].rstrip(b"\r\n").decode("utf-8", "replace")
                start = stop
                await queue.put((log_record(source, text), self.commit_offset(path, pos + stop)))
            pos += end

    async def watch_containers(self, queue):
        followers = {}
        try:
            while True:
                for name in await list_containers(self.container_cli):
                    if name not in followers and name not in self.finished:
                        followers[name] = asyncio.ensure_future(self.follow_container(name, queue))
                for name in [n for n, t in followers.items() if t.done()]:
                    del followers[name]
                await asyncio.sleep(CONTAINER_POLL_SECONDS)
        finally:
            for task in followers.values():
                task.cancel()

    def commit_since(self, name, ts):
        def commit():
            self.container_since[name] = ts
        return commit

    async def follow_container(self, name, queue):
        since = self.container_since.get(name)
        cmd = [self.container_cli, "logs", "--follow", "--timestamps"]
        if since:
            cmd += ["--since", since]
        cmd.append(name)
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            limit=LOG_READ_CHUNK)
        try:
            pod = name[len(CONTAINER_PREFIX):]
            await asyncio.gather(
                self.pipe_container(proc.stdout, name, pod, "stdout", since, queue),
                self.pipe_container(proc.stderr, name, pod, "stderr", since, queue))
            if await proc.wait() == 0:
                self.finished.add(name)  # the container exited and its output is complete
        finally:
            if proc.returncode is None:
                proc.kill()

    async def pipe_container(self, reader, name, pod, fd, since, queue):
        since_key = since and log_time_key(since)
        while True:
            try:
                line = await reader.readline()
            except ValueError:
                continue  # readline already discarded the overlong line
            if not line:
                return
            ts, _, text = line.decode("utf-8", "replace").rstrip("\r\n").partition(" ")
            if since_key and log_time_key(ts) <= since_key:
                continue  # --since repeats sent records: Docker the last one, nerdctl its whole second
            await queue.put((log_record("container", text, ts, pod, fd), self.commit_since(name, ts)))


def log_time_key(ts):
    head, _, frac = ts.partition(".")
    return head[:19] + frac.rstrip("Zz").ljust(9, "0")


async def list_containers(cli):
    try:
        proc = await asyncio.create_subprocess_exec(
            cli, "ps", "--all", "--filter", "name=" + CONTAINER_PREFIX, "--format", "{{.Names}}",
            stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        out, _ = await proc.communicate()
    except OSError:
        return []
    return [n for n in out.decode().split() if n.startswith(CONTAINER_PREFIX)]


def boot_marks(path, uptime_path="/proc/uptime"):
    """Returns when the VM's boot phases ended: instance_start when the kernel booted, then the bootstrap's own marks."""
    def iso(t):
        return datetime.datetime.fromtimestamp(t, datetime.timezone.utc).isoformat()
    marks = []
    try:
        with open(uptime_path) as f:
            marks.append({"phase": "instance_start", "at": iso(time.time() - float(f.read().split()[0]))})
    except (OSError, ValueError, IndexError):
        pass
    try:
        with open(path) as f:
            for line in f:
                parts = line.split()
                if len(parts) == 2 and len(marks) < MAX_BOOT_MARKS:
                    marks.append({"phase": parts[0], "at": iso(float(parts[1]))})
    except (OSError, ValueError):
        pass
    return marks


def parse_log_files(spec):
    files = []
    for item in spec.split(","):
        source, _, path = item.strip().partition("=")
        if source and path:
            files.append((source, path))
    return files


def read_file(path):
    try:
        with open(path, "rb") as f:
            return f.read().strip()
    except OSError:
        return b""


class TCPProxy(object):
    """Local HTTP CONNECT proxy: tunnels targets in the tunnel networks (without any, those outside the direct
    networks) and connects the rest itself."""

    def __init__(self, direct_networks=(), tunnel_networks=(), pods=None):
        self.session = None
        self.direct_networks = list(direct_networks)
        self.tunnel_networks = list(tunnel_networks)
        self.pods = pods
        self.synthetic_names, self.synthetic_ips, self.external_addresses = {}, {}, set()

    def set_destinations(self, synthetic_hosts, external_addresses):
        """Takes the allow-listed external destinations from the gateway's handshake: the synthetic address of each
        hostname, which the DNS forwarder answers, and the IPv4 host:port destinations."""
        names = {}
        for name, addr in (synthetic_hosts or {}).items():
            try:
                if ipaddress.ip_address(addr) in SYNTHETIC_NETWORK:
                    names[str(name).lower().rstrip(".")] = addr
            except ValueError:
                log("ignoring synthetic address %r of %r" % (addr, name))
        self.synthetic_names = names
        self.synthetic_ips = {addr: name for name, addr in names.items()}
        self.external_addresses = set(external_addresses or ())

    def tunnel_target(self, target):
        """Maps a synthetic address in target back to the hostname it answers for."""
        host, _, port = target.rpartition(":")
        name = self.synthetic_ips.get(host.strip("[]"))
        return "%s:%s" % (name, port) if name else target

    async def attribute(self, proto, peer):
        """Returns the pod whose process holds the local socket at peer: "" for the VM's own processes and when
        attribution is off, and UNATTRIBUTED_POD when no process can be found."""
        if self.pods is None or not peer:
            return ""
        try:
            pod = await asyncio.get_event_loop().run_in_executor(None, self.pods.pod_of, proto, peer)
        except (OSError, ValueError) as e:
            log("cannot attribute %s connection from %s: %s" % (proto, peer, e))
            pod = None
        return UNATTRIBUTED_POD if pod is None else pod

    async def route(self, target):
        host, _, port = target.rpartition(":")
        name = host.strip("[]").lower().rstrip(".")
        if name in self.synthetic_names:
            return True, "%s:%s" % (name, port)
        try:
            ip = ipaddress.ip_address(host.strip("[]"))
        except ValueError:
            if not self.tunnel_networks:
                return True, target
            info = await asyncio.get_event_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
            ip = ipaddress.ip_address(info[0][4][0])
            target = "[%s]:%s" % (ip, port)
        if str(ip) in self.synthetic_ips:
            return True, self.tunnel_target("%s:%s" % (ip, port))
        if "%s:%s" % (ip, port) in self.external_addresses:
            return True, "%s:%s" % (ip, port)
        if any(ip in network for network in self.direct_networks):
            return False, target
        return not self.tunnel_networks or any(ip in network for network in self.tunnel_networks), target

    async def connect_direct(self, target, reader, writer):
        host, _, port = target.rpartition(":")
        try:
            peer_reader, peer_writer = await asyncio.wait_for(asyncio.open_connection(host.strip("[]"), int(port)), 10)
        except (OSError, ValueError, asyncio.TimeoutError) as e:
            log("direct connection to %s failed: %s" % (target, e))
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            writer.close()
            return
        writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        await asyncio.gather(copy(reader, peer_writer), copy(peer_reader, writer))

    async def handle(self, reader, writer):
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError, OSError):
            writer.close()
            return
        parts = head.split(b"\r\n", 1)[0].split()
        route = None
        if len(parts) >= 2 and parts[0] == b"CONNECT":
            try:
                route = await self.route(parts[1].decode())
            except (OSError, ValueError) as e:
                log("CONNECT %s: %s" % (parts[1], e))
        if route is not None and not route[0]:
            await self.connect_direct(route[1], reader, writer)
            return
        pod = await self.attribute("tcp", writer.get_extra_info("peername")) if route is not None else ""
        session = self.session
        if route is None or session is None:
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            writer.close()
            return
        stream = session.open_tcp_stream(route[1], reader, writer, pod)
        writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        await writer.drain()
        stream.task = asyncio.ensure_future(stream.run())


    async def handle_redirected(self, reader, writer):
        """Tunnels a connection the bootstrap's REDIRECT rule sent here to the address its client dialed, or to the
        hostname whose synthetic address it dialed."""
        try:
            target = self.tunnel_target(original_destination(writer.get_extra_info("socket")))
        except OSError as e:
            log("redirected connection without a destination: %s" % e)
            target = None
        pod = await self.attribute("tcp", writer.get_extra_info("peername")) if target is not None else ""
        session = self.session
        if target is None or session is None:
            writer.close()
            return
        stream = session.open_tcp_stream(target, reader, writer, pod)
        stream.task = asyncio.ensure_future(stream.run())


SO_ORIGINAL_DST = 80


def original_destination(sock):
    """Returns the IPv4 host:port a REDIRECTed connection was addressed to, from its conntrack entry."""
    raw = sock.getsockopt(socket.SOL_IP, SO_ORIGINAL_DST, 16)
    port, = struct.unpack("!H", raw[2:4])
    return "%s:%d" % (socket.inet_ntoa(raw[4:8]), port)


def dns_question(query):
    """Returns the lower-case name and type of a query's question and where the question ends, or None when malformed."""
    labels, i = [], 12
    while i < len(query):
        n = query[i]
        if n == 0:
            break
        if n & 0xC0 or i + 1 + n > len(query):
            return None
        labels.append(query[i + 1:i + 1 + n].decode("ascii", "replace"))
        i += 1 + n
    if i + 5 > len(query):
        return None
    qtype, = struct.unpack("!H", query[i + 1:i + 3])
    return ".".join(labels).lower(), qtype, i + 5


def synthetic_reply(query, end, qtype, addr):
    """Answers a query for an allow-listed external hostname: its synthetic IPv4 address for A, no records otherwise."""
    answers = qtype in (1, 255)
    reply = query[:2] + bytes([0x84 | query[2] & 0x79, 0x80]) + struct.pack("!HHHH", 1, int(answers), 0, 0) + query[12:end]
    if answers:
        reply += b"\xc0\x0c" + struct.pack("!HHIH", 1, 1, SYNTHETIC_TTL, 4) + socket.inet_aton(addr)
    return reply


def dns_exchange(upstream, query):
    with socket.socket(socket.AF_INET6 if ":" in upstream[0] else socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(DNS_TIMEOUT)
        s.connect(upstream)
        s.send(query)
        return s.recv(65535)


class DNSForwarder(asyncio.DatagramProtocol):
    """UDP DNS forwarder: answers allow-listed external hostnames with their synthetic addresses, asks the gateway for
    cluster names and the VM's resolvers for the rest."""

    def __init__(self, domain, upstreams, proxy):
        self.domain = b"".join(bytes([len(l)]) + l.encode() for l in domain.split(".")) + b"\0"
        self.upstreams, self.proxy, self.transport = upstreams, proxy, None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        if len(data) > 12:
            asyncio.ensure_future(self.answer(data, addr))

    async def answer(self, query, addr):
        end = query.find(b"\0", 12) + 5
        question = dns_question(query)
        if question is not None and question[0] in self.proxy.synthetic_names:
            self.transport.sendto(synthetic_reply(query, question[2], question[1], self.proxy.synthetic_names[question[0]]), addr)
            return
        reply, session = b"", self.proxy.session
        if not query[12:end - 4].lower().endswith(self.domain):
            for upstream in self.upstreams:
                try:
                    reply = await asyncio.get_event_loop().run_in_executor(None, dns_exchange, upstream, query)
                    break
                except OSError as e:
                    log("DNS %s: %s" % (upstream, e))
        elif session is not None:
            pod = await self.proxy.attribute("udp", addr)
            session = self.proxy.session
            if session is not None:
                stream = session.open_dns_stream(query, pod)
                try:
                    await asyncio.wait_for(stream.done.wait(), DNS_TIMEOUT)
                    reply = stream.received
                except asyncio.TimeoutError:
                    pass
                session.streams.pop(stream.sid, None)
        if len(reply) < 12:  # SERVFAIL
            reply = query[:2] + bytes([0x80 | query[2] & 0x79, 0x82]) + b"\0\1\0\0\0\0\0\0" + query[12:end]
        self.transport.sendto(reply, addr)


async def start_dns_forwarder(listen, proxy):
    """Serves DNS on host:port listen; CLOUDVM_DNS_TAKEOVER=1 puts it before two fallbacks in resolv.conf."""
    host, _, port = listen.rpartition(":")
    path = os.environ.get("CLOUDVM_DNS_RESOLV_CONF", "/etc/resolv.conf")
    with open(path) as f:
        lines = f.read().splitlines()
    servers = [l.split()[1] for l in lines if l.startswith("nameserver ") and l.split()[1] != host]
    upstreams = [(s, 53) for s in servers]
    if os.environ.get("CLOUDVM_DNS_UPSTREAM"):
        h, _, p = os.environ["CLOUDVM_DNS_UPSTREAM"].rpartition(":")
        upstreams = [(h, int(p))]
    domain = os.environ.get("CLOUDVM_CLUSTER_DOMAIN", "cluster.local").strip(".").lower()
    forwarder = DNSForwarder(domain, upstreams, proxy)
    await asyncio.get_event_loop().create_datagram_endpoint(lambda: forwarder, local_addr=(host, int(port)))
    if os.environ.get("CLOUDVM_DNS_TAKEOVER") == "1":
        search = ["svc." + domain, domain]
        search += [d for l in lines if l.startswith(("search ", "domain ")) for d in l.split()[1:] if d not in search]
        lines = ["nameserver " + s for s in [host] + servers[:2]] + ["search " + " ".join(search)] + [
            l for l in lines if l.startswith("options")]
        with open(path + ".tmp", "w") as f:
            f.write("\n".join(lines) + "\n")
        os.chmod(path + ".tmp", 0o644)
        os.replace(path + ".tmp", path)  # replaces a symlink, such as one into systemd-resolved, not its target
    return forwarder


async def copy(reader, writer):
    """Copies reader into writer until either side ends, then closes writer."""
    try:
        while True:
            data = await reader.read(64 * 1024)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except OSError:
        pass
    finally:
        writer.close()


def parse_networks(raw):
    """Parses a comma-separated list of CIDRs, skipping entries that are not CIDRs."""
    networks = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            networks.append(ipaddress.ip_network(item, strict=False))
        except ValueError:
            log("ignoring network %r: not a CIDR" % item)
    return networks


async def connect_once(cfg):
    host, _, port = cfg["endpoint"].rpartition(":")
    # The gateway presents a publicly trusted certificate; CLOUDVM_TUNNEL_CA_FILE only narrows trust to a private bundle.
    ctx = ssl.create_default_context(cafile=cfg["ca_file"])
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, int(port), ssl=ctx, server_hostname=host), 15)
    try:
        hello = {
            "instanceId": cfg["instance_id"],
            "authToken": read_file(cfg["token_file"]).decode(),
            "platform": cfg["platform"],
            "boot": boot_marks(cfg["boot_marks_file"]),
        }
        attestation = read_file(cfg["attestation_file"])
        if attestation:
            hello["attestation"] = base64.b64encode(attestation).decode()
        writer.write((json.dumps(hello) + "\n").encode())
        line = await asyncio.wait_for(reader.readline(), 15)
        resp = json.loads(line.decode() or "{}")
        if resp.get("status") != "ok":
            raise RuntimeError("handshake rejected: %s" % resp.get("error", "no response"))
        log("connected to %s" % cfg["endpoint"])
        cfg["proxy"].set_destinations(resp.get("syntheticHosts"), resp.get("externalAddresses"))
        session = Session(reader, writer, cfg["runner_addr"])
        session.open_log_stream(cfg["collector"])
        cfg["proxy"].session = session
        await session.serve()
    finally:
        cfg["proxy"].session = None
        writer.close()


def main():
    container_cli = os.environ.get("CLOUDVM_CONTAINER_CLI", "docker")
    runner_host, _, runner_port = os.environ.get("CLOUDVM_RUNNER_ADDR", "127.0.0.1:9090").rpartition(":")
    cfg = {
        "endpoint": os.environ["CLOUDVM_TUNNEL_ENDPOINT"],
        "instance_id": os.environ["CLOUDVM_INSTANCE_ID"],
        "platform": os.environ.get("CLOUDVM_PLATFORM", "aws"),
        "token_file": os.environ.get("CLOUDVM_RUNNER_TOKEN_FILE", "/etc/cloudvm/runner-token"),
        "attestation_file": os.environ.get("CLOUDVM_ATTESTATION_FILE", "/etc/cloudvm/attestation"),
        "boot_marks_file": os.environ.get("CLOUDVM_BOOT_MARKS_FILE", DEFAULT_BOOT_MARKS),
        "runner_addr": (runner_host, int(runner_port)),
        "ca_file": os.environ.get("CLOUDVM_TUNNEL_CA_FILE") or None,
        "collector": LogCollector(
            parse_log_files(os.environ.get("CLOUDVM_LOG_FILES", DEFAULT_LOG_FILES)),
            os.environ.get("CLOUDVM_CONTAINER_LOGS", "1") != "0" and shutil.which(container_cli) and container_cli),
    }
    proc_root = os.environ.get("CLOUDVM_PROC_ROOT", "/proc")
    cfg["proxy"] = TCPProxy(
        parse_networks(os.environ.get("CLOUDVM_DIRECT_CIDRS", "")),
        parse_networks(os.environ.get("CLOUDVM_TUNNEL_CIDRS", "")),
        PodResolver(proc_root) if os.path.exists(os.path.join(proc_root, "net", "tcp")) else None)
    max_backoff = float(os.environ.get("CLOUDVM_TUNNEL_MAX_BACKOFF", "30"))
    backoff = 0.5
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    proxy_port = int(os.environ.get("CLOUDVM_TCP_PROXY_PORT", DEFAULT_TCP_PROXY_PORT))
    if proxy_port > 0:
        loop.run_until_complete(asyncio.start_server(cfg["proxy"].handle, "127.0.0.1", proxy_port))
    transparent_port = int(os.environ.get("CLOUDVM_TRANSPARENT_PORT") or 0)
    if transparent_port > 0:
        loop.run_until_complete(asyncio.start_server(cfg["proxy"].handle_redirected, "127.0.0.1", transparent_port))
    if os.environ.get("CLOUDVM_DNS_LISTEN"):
        try:
            loop.run_until_complete(start_dns_forwarder(os.environ["CLOUDVM_DNS_LISTEN"], cfg["proxy"]))
        except (OSError, ValueError) as e:
            log("cannot serve DNS: %s" % e)
    while True:
        started = time.time()
        try:
            loop.run_until_complete(connect_once(cfg))
        except Exception as e:  # noqa: BLE001 - every failure leads to a reconnect
            log("disconnected: %s" % e)
        if time.time() - started > 60:
            backoff = 0.5
        loop.run_until_complete(asyncio.sleep(backoff * random.uniform(0.8, 1.2)))
        backoff = min(backoff * 2, max_backoff)


if __name__ == "__main__":
    main()
