"""In-VM reverse tunnel agent: dials the plugin's tunnel gateway over TLS, bridges Yamux streams to the local runner and local ports, and streams worker logs.

Implements the client half of the github.com/hashicorp/yamux framing on one asyncio loop (stdlib only, Python 3.7+).
"""
import asyncio
import base64
import datetime
import json
import os
import random
import shutil
import ssl
import struct
import sys
import time

VERSION = 0
T_DATA, T_WINDOW, T_PING, T_GOAWAY = 0, 1, 2, 3
F_SYN, F_ACK, F_FIN, F_RST = 1, 2, 4, 8
HEADER = struct.Struct(">BBHII")
INITIAL_WINDOW = 256 * 1024
MAX_FRAME = 16 * 1024
# The gateway pings every 30s; a link that stays silent for this long is treated as dead.
READ_IDLE_TIMEOUT = 90
# The first line of the agent-opened log stream; newline-delimited JSON log records follow it.
LOG_STREAM_HEADER = b'{"stream":"logs","version":1}\n'
# A stream whose first line is a tcp header carries one plain TCP connection to the header's target.
TCP_STREAM_KIND = "tcp"
MAX_STREAM_HEADER = 4096
DEFAULT_TCP_PROXY_PORT = "13128"
MAX_LOG_MESSAGE = 16 * 1024
LOG_READ_CHUNK = 64 * 1024
LOG_POLL_SECONDS = 1
CONTAINER_POLL_SECONDS = 5
CONTAINER_PREFIX = "cloudvm-"
DEFAULT_LOG_FILES = "bootstrap=/var/log/cloudvm-bootstrap.log,runner=/var/log/cloudvm-runner.log"


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
        finally:
            pump_in.cancel()
            local_writer.close()
            self.session.streams.pop(self.sid, None)

    def reset(self):
        self.session.send(T_WINDOW, F_RST, self.sid, 0)
        self.session.streams.pop(self.sid, None)

    async def read_target(self):
        """Returns where to bridge this gateway-opened stream: the local port its tcp header names, or the runner.

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
            port = int(port)
        except (ValueError, KeyError, AttributeError) as e:
            log("stream %d: invalid stream header: %s" % (self.sid, e))
            return None, b"", 0
        if header.get("stream") != TCP_STREAM_KIND or host not in ("127.0.0.1", "localhost"):
            log("stream %d: refused stream to %s" % (self.sid, header.get("target")))
            return None, b"", 0
        return (host, port), rest, len(buf)

    async def pump_in(self, local_writer):
        """Copies tunnel data into the local runner and returns receive window credit to the gateway."""
        while True:
            chunk = await self.inbound.get()
            if chunk is None:
                if local_writer.can_write_eof():
                    local_writer.write_eof()
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
            await self.session.writer.drain()


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
            await self.session.writer.drain()
            data = data[n:]


class Session(object):
    def __init__(self, reader, writer, runner_addr):
        self.reader = reader
        self.writer = writer
        self.runner_addr = runner_addr
        self.streams = {}
        self.next_sid = 1  # Yamux clients open odd stream IDs

    def send(self, ftype, flags, sid, length, payload=b""):
        self.writer.write(HEADER.pack(VERSION, ftype, flags, sid, length) + payload)

    def open_tcp_stream(self, target, local_reader, local_writer):
        """Opens a stream asking the gateway to connect to target, bridged to an accepted local connection.

        The caller starts the stream once it has answered its client.
        """
        stream = Stream(self, self.next_sid, local=(local_reader, local_writer))
        self.next_sid += 2
        self.streams[stream.sid] = stream
        self.send(T_WINDOW, F_SYN, stream.sid, 0)
        header = (json.dumps({"stream": TCP_STREAM_KIND, "version": 1, "target": target}) + "\n").encode()
        stream.send_window -= len(header)
        self.send(T_DATA, 0, stream.sid, len(header), header)
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
            self.send(T_WINDOW, F_ACK, sid, 0)
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

    def __init__(self, files, containers):
        self.files = files
        self.containers = containers
        self.offsets = {}
        self.container_since = {}
        self.finished = set()

    async def ship(self, stream):
        queue = asyncio.Queue(maxsize=256)
        tasks = [asyncio.ensure_future(self.follow_file(source, path, queue)) for source, path in self.files]
        if self.containers:
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
                for name in await list_containers():
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
        cmd = ["docker", "logs", "--follow", "--timestamps"]
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
        while True:
            try:
                line = await reader.readline()
            except ValueError:
                continue  # readline already discarded the overlong line
            if not line:
                return
            ts, _, text = line.decode("utf-8", "replace").rstrip("\r\n").partition(" ")
            if ts == since:
                continue  # --since includes the last record the previous session already sent
            await queue.put((log_record("container", text, ts, pod, fd), self.commit_since(name, ts)))


async def list_containers():
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "ps", "--all", "--filter", "name=" + CONTAINER_PREFIX, "--format", "{{.Names}}",
            stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        out, _ = await proc.communicate()
    except OSError:
        return []
    return [n for n in out.decode().split() if n.startswith(CONTAINER_PREFIX)]


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
    """Local HTTP CONNECT proxy that carries each accepted connection to its target over the current tunnel session."""

    def __init__(self):
        self.session = None

    async def handle(self, reader, writer):
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError, OSError):
            writer.close()
            return
        parts = head.split(b"\r\n", 1)[0].split()
        session = self.session
        if len(parts) < 2 or parts[0] != b"CONNECT" or session is None:
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            writer.close()
            return
        stream = session.open_tcp_stream(parts[1].decode(), reader, writer)
        writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        await writer.drain()
        stream.task = asyncio.ensure_future(stream.run())


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
        session = Session(reader, writer, cfg["runner_addr"])
        session.open_log_stream(cfg["collector"])
        cfg["proxy"].session = session
        await session.serve()
    finally:
        cfg["proxy"].session = None
        writer.close()


def main():
    runner_host, _, runner_port = os.environ.get("CLOUDVM_RUNNER_ADDR", "127.0.0.1:9090").rpartition(":")
    cfg = {
        "endpoint": os.environ["CLOUDVM_TUNNEL_ENDPOINT"],
        "instance_id": os.environ["CLOUDVM_INSTANCE_ID"],
        "platform": os.environ.get("CLOUDVM_PLATFORM", "aws"),
        "token_file": os.environ.get("CLOUDVM_RUNNER_TOKEN_FILE", "/etc/cloudvm/runner-token"),
        "attestation_file": os.environ.get("CLOUDVM_ATTESTATION_FILE", "/etc/cloudvm/attestation"),
        "runner_addr": (runner_host, int(runner_port)),
        "ca_file": os.environ.get("CLOUDVM_TUNNEL_CA_FILE") or None,
        "collector": LogCollector(
            parse_log_files(os.environ.get("CLOUDVM_LOG_FILES", DEFAULT_LOG_FILES)),
            os.environ.get("CLOUDVM_CONTAINER_LOGS", "1") != "0" and shutil.which("docker") is not None),
    }
    cfg["proxy"] = TCPProxy()
    max_backoff = float(os.environ.get("CLOUDVM_TUNNEL_MAX_BACKOFF", "30"))
    backoff = 0.5
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    proxy_port = int(os.environ.get("CLOUDVM_TCP_PROXY_PORT", DEFAULT_TCP_PROXY_PORT))
    if proxy_port > 0:
        loop.run_until_complete(asyncio.start_server(cfg["proxy"].handle, "127.0.0.1", proxy_port))
    while True:
        started = time.time()
        try:
            loop.run_until_complete(connect_once(cfg))
        except Exception as e:  # noqa: BLE001 - every failure leads to a reconnect
            log("disconnected: %s" % e)
        if time.time() - started > 60:
            backoff = 0.5
        time.sleep(backoff * random.uniform(0.8, 1.2))
        backoff = min(backoff * 2, max_backoff)


if __name__ == "__main__":
    main()
