"""In-VM reverse tunnel agent: dials the cell's tunnel gateway over TLS and bridges Yamux streams to the local runner.

Implements the client half of the github.com/hashicorp/yamux framing on one asyncio loop (stdlib only, Python 3.7+).
"""
import asyncio
import base64
import json
import os
import random
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


def log(msg):
    sys.stderr.write("[cloudvm-tunnel-agent] %s\n" % msg)
    sys.stderr.flush()


class Stream(object):
    """One gateway-opened Yamux stream bridged to a fresh TCP connection to the local runner."""

    def __init__(self, session, sid):
        self.session = session
        self.sid = sid
        self.inbound = asyncio.Queue()
        self.send_window = INITIAL_WINDOW
        self.window_open = asyncio.Event()
        self.window_open.set()
        self.task = None

    def grow_window(self, delta):
        self.send_window += delta
        if self.send_window > 0:
            self.window_open.set()

    async def run(self):
        try:
            host, port = self.session.runner_addr
            local_reader, local_writer = await asyncio.wait_for(asyncio.open_connection(host, port), 10)
        except (OSError, asyncio.TimeoutError) as e:
            log("stream %d: cannot reach local runner: %s" % (self.sid, e))
            self.session.send(T_WINDOW, F_RST, self.sid, 0)
            self.session.streams.pop(self.sid, None)
            return
        pump_in = asyncio.ensure_future(self.pump_in(local_writer))
        try:
            await self.pump_out(local_reader)
            await pump_in
        finally:
            pump_in.cancel()
            local_writer.close()
            self.session.streams.pop(self.sid, None)

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


class Session(object):
    def __init__(self, reader, writer, runner_addr):
        self.reader = reader
        self.writer = writer
        self.runner_addr = runner_addr
        self.streams = {}

    def send(self, ftype, flags, sid, length, payload=b""):
        self.writer.write(HEADER.pack(VERSION, ftype, flags, sid, length) + payload)

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
            for stream in list(self.streams.values()):
                if stream.task is not None:
                    stream.task.cancel()
            self.streams.clear()

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
            stream.inbound.put_nowait(payload)
        if flags & F_RST:
            self.streams.pop(sid, None)
            if stream.task is not None:
                stream.task.cancel()
        elif flags & F_FIN:
            stream.inbound.put_nowait(None)


def read_file(path):
    try:
        with open(path, "rb") as f:
            return f.read().strip()
    except OSError:
        return b""


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
        await Session(reader, writer, cfg["runner_addr"]).serve()
    finally:
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
    }
    max_backoff = float(os.environ.get("CLOUDVM_TUNNEL_MAX_BACKOFF", "30"))
    backoff = 0.5
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
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
