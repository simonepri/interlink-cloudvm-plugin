"""In-VM container runner: serves the plugin's create, status, logs, and delete calls by driving the VM's containerd through nerdctl.

The bootstrap sets CLOUDVM_RUNNER_BIND and CLOUDVM_RUNNER_CONCURRENCY; the plugin reaches it through the tunnel agent.
"""
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
CONTAINER_CLI = 'nerdctl'
STARGZ_SOCKET = '/run/containerd-stargz-grpc/containerd-stargz-grpc.sock'
IMAGE_MIRRORS_PATH = '/etc/cloudvm/image-mirrors.json'
DOCKER_HUB_HOSTS = ('docker.io', 'index.docker.io', 'registry-1.docker.io')
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
DEFAULT_RUNNER_CONCURRENCY = max(4, os.cpu_count() or 4)
RUNNER_CONCURRENCY = int(os.environ.get("CLOUDVM_RUNNER_CONCURRENCY") or DEFAULT_RUNNER_CONCURRENCY)
CONTAINER_SEMAPHORE = threading.Semaphore(RUNNER_CONCURRENCY)

def snapshotter_args():
    # Lazy pulls need the stargz snapshotter; without it containerd's default snapshotter pulls in full.
    return ['--snapshotter=stargz'] if os.path.exists(STARGZ_SOCKET) else []

def split_image_ref(ref):
    """Returns the registry host and the repository path plus tag or digest of an image reference."""
    first, sep, rest = ref.partition('/')
    if sep and ('.' in first or ':' in first or first == 'localhost'):
        host, path = first, rest
    else:
        host, path = 'docker.io', ref
    if host in DOCKER_HUB_HOSTS:
        host = 'docker.io'
        if '/' not in path:
            path = 'library/' + path
    return host, path

def rewrite_image(ref):
    """Points a reference at the regional mirror configured for its registry; tags and digests carry over."""
    try:
        with open(IMAGE_MIRRORS_PATH, 'r') as f:
            prefix = json.load(f).get(split_image_ref(ref)[0])
    except Exception:
        return ref
    return prefix.rstrip('/') + '/' + split_image_ref(ref)[1] if prefix else ref

def pull_config(auth_cfg, image):
    """Adds the ECR credential helper for the image's registry to the pod's pull secrets."""
    try:
        cfg = json.loads(auth_cfg) if isinstance(auth_cfg, str) else dict(auth_cfg)
    except Exception:
        cfg = {}
    host = image.split('/')[0]
    if '.dkr.ecr.' in host:
        cfg.setdefault('credHelpers', {})[host] = 'ecr-login'
    return cfg

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

def get_network_bytes():
    rx_bytes = 0
    tx_bytes = 0
    try:
        with open('/proc/net/dev', 'r') as f:
            for line in f:
                if ':' not in line:
                    continue
                iface, data = line.split(':', 1)
                iface = iface.strip()
                if iface == 'lo':
                    continue
                fields = data.split()
                if len(fields) >= 9:
                    rx_bytes += int(fields[0])
                    tx_bytes += int(fields[8])
    except Exception:
        pass
    return rx_bytes, tx_bytes

class RunnerHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def reply(self, code, body):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(body if isinstance(body, bytes) else body.encode('utf-8'))

    def check_auth(self):
        global AUTH_TOKEN
        if not AUTH_TOKEN:
            try:
                with open('/etc/cloudvm/runner-token', 'r') as f:
                    AUTH_TOKEN = f.read().strip()
            except Exception:
                pass
        if not AUTH_TOKEN:
            self.reply(500, b'{"error":"auth_token_not_configured"}')
            return False
        auth_header = self.headers.get('Authorization', '')
        expected = f"Bearer {AUTH_TOKEN}"
        if hmac.compare_digest(auth_header, expected):
            record_heartbeat()
            return True
        self.reply(401, b'{"error":"unauthorized"}')
        return False

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path in ('/healthz', '/status'):
            if self.headers.get('Authorization', ''):
                if not self.check_auth():
                    return
            self.reply(200, b'{"status":"ok"}')
            return

        if not self.check_auth():
            return

        if parsed.path in ('/host/stats', '/pinglink'):
            rx_bytes, tx_bytes = get_network_bytes()
            resp = {
                "status": "ok",
                "networkRxBytes": rx_bytes,
                "networkTxBytes": tx_bytes,
            }
            self.reply(200, json.dumps(resp))
            return

        if parsed.path == '/containers/list':
            results = []
            try:
                res = subprocess.run([CONTAINER_CLI, 'ps', '--filter', 'name=cloudvm-', '--filter', 'status=running', '--format', '{{.ID}}\t{{.Names}}\t{{.Status}}'], capture_output=True, text=True)
                if res.returncode == 0:
                    for line in res.stdout.strip().splitlines():
                        if not line:
                            continue
                        parts = line.split('\t')
                        if len(parts) >= 2:
                            name = parts[1]
                            state = 'running'
                            uid = name.removeprefix('cloudvm-')
                            results.append({
                                "podUID": uid,
                                "running": True,
                                "reason": state,
                            })
            except Exception:
                pass
            self.reply(200, json.dumps(results))
            return

        if parsed.path == '/containers/status':
            params = urllib.parse.parse_qs(parsed.query)
            pod_uid = params.get('podUID', [''])[0]
            container_name = f"cloudvm-{pod_uid}"
            rx_bytes, tx_bytes = get_network_bytes()
            # A recorded start failure wins over inspect: a failed run can leave the container in "created".
            with get_pod_lock(pod_uid):
                failure = start_failures.get(pod_uid)
            if failure is not None:
                reason_val, err_val = failure
                resp = {
                    "podUID": pod_uid,
                    "running": False,
                    "status": "dead",
                    "exitCode": 1,
                    "reason": reason_val,
                    "message": err_val,
                    "finishedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "networkRxBytes": rx_bytes,
                    "networkTxBytes": tx_bytes,
                }
                self.reply(200, json.dumps(resp))
                return
            try:
                res = subprocess.run([CONTAINER_CLI, 'inspect', container_name], capture_output=True, text=True)
                if res.returncode == 0:
                    data = json.loads(res.stdout)[0]
                    state = data.get('State', {})
                    status_str = state.get('Status', '')
                    running = state.get('Running', False)
                    exit_code = state.get('ExitCode', 0)
                    started_at = state.get('StartedAt', '')
                    finished_at = state.get('FinishedAt', '')
                    start_error = state.get('Error', '')
                    message = ''
                    if not running and start_error:
                        # The engine may record why it could not start the container, such as a missing GPU device driver.
                        status_str = 'dead'
                        exit_code = exit_code or 1
                        reason = "StartError"
                        message = start_error
                    else:
                        reason = "Running" if running else (
                            "ContainerCreating" if status_str in ("created", "starting") else (
                                ("Completed" if exit_code == 0 else "Error") if status_str in ("exited", "stopped", "dead") else status_str.capitalize()
                            )
                        )
                    resp = {
                        "podUID": pod_uid,
                        "running": running,
                        "status": status_str,
                        "exitCode": exit_code,
                        "reason": reason,
                        "message": message,
                        "startedAt": started_at,
                        "finishedAt": finished_at,
                        "networkRxBytes": rx_bytes,
                        "networkTxBytes": tx_bytes,
                    }
                    self.reply(200, json.dumps(resp))
                    return
            except Exception:
                pass

            plock = get_pod_lock(pod_uid)
            with plock:
                if pod_uid in starting_pods:
                    resp = {
                        "podUID": pod_uid,
                        "running": False,
                        "status": "created",
                        "exitCode": 0,
                        "reason": "ContainerCreating",
                        "message": "Pulling container image or initializing...",
                        "networkRxBytes": rx_bytes,
                        "networkTxBytes": tx_bytes,
                    }
                    self.reply(200, json.dumps(resp))
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
                try:
                    subprocess.run([CONTAINER_CLI, 'stop', f"cloudvm-{pod_uid}"], capture_output=True)
                except Exception:
                    pass
            self.send_response(200)
            self.end_headers()
            return

        if parsed.path == '/containers/start':
            if os.path.exists('/tmp/cloudvm-shutting-down'):
                self.reply(503, b'{"error": "node is shutting down"}')
                return

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

            ids = {}
            for field in ('runAsUser', 'runAsGroup'):
                if req.get(field) is None:
                    continue
                try:
                    ids[field] = int(req[field])
                except (ValueError, TypeError):
                    self.reply(400, json.dumps({"error": f"invalid {field}"}))
                    return
                if ids[field] == 0:
                    self.reply(400, json.dumps({"error": f"{field} 0 is forbidden on host network"}))
                    return
            user_val = ids.get('runAsUser')
            group_val = ids.get('runAsGroup')

            plock = get_pod_lock(pod_uid)
            with plock:
                if pod_uid in starting_pods:
                    self.reply(200, b'{"status":"already_starting"}')
                    return

                try:
                    inspect_res = subprocess.run([CONTAINER_CLI, 'inspect', container_name], capture_output=True, text=True)
                    if inspect_res.returncode == 0:
                        self.reply(200, b'{"status":"already_exists"}')
                        return
                except Exception:
                    pass

                starting_pods.add(pod_uid)
                start_failures.pop(pod_uid, None)

            image = rewrite_image(str(req.get('image', '')))
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

            run_cmd = [
                CONTAINER_CLI, *snapshotter_args(), 'run', '-d',
                '--name', container_name,
                '--net=host',
                '--cap-drop=SETUID',
                '--cap-drop=SETGID',
                '--security-opt=no-new-privileges',
                user_flag,
                shm_flag,
            ]
            if cpu_milli and cpu_milli > 0:
                run_cmd.extend(['--cpus', f"{cpu_milli / 1000.0:.2f}"])
            if mem_bytes and mem_bytes > 0:
                run_cmd.extend(['--memory', str(mem_bytes)])
            if gpus:
                gpu_devs = ','.join(str(g) for g in gpus)
                gpu_spec = f'"device={gpu_devs}"'
                run_cmd.extend(['--gpus', gpu_spec])
            for k, v in env.items():
                run_cmd.extend(['-e', f"{k}={v}"])
            run_cmd.append(image)
            if isinstance(cmd, list):
                run_cmd.extend(cmd)
            if isinstance(args, list):
                run_cmd.extend(args)

            docker_cfg = req.get('dockerConfigJSON')

            def run_async(uid, img, c, auth_cfg):
                # 0. Isolate credentials in an ephemeral per-pod directory (never shared with co-tenants)
                cfg_dir = None
                if auth_cfg:
                    try:
                        cfg_dir = tempfile.mkdtemp(prefix=f"docker-cfg-{uid}-")
                        cfg_path = os.path.join(cfg_dir, 'config.json')
                        with open(cfg_path, 'w', encoding='utf-8') as cf:
                            json.dump(pull_config(auth_cfg, img), cf)
                        os.chmod(cfg_path, 0o600)
                    except Exception:
                        cfg_dir = None

                # 1. Pull container image using the isolated ephemeral credential config
                pull_res = None
                try:
                    pull_env = dict(os.environ)
                    if cfg_dir:
                        pull_env['DOCKER_CONFIG'] = cfg_dir
                    pull_cmd = [CONTAINER_CLI, *snapshotter_args(), 'pull', img]
                    pull_res = subprocess.run(pull_cmd, capture_output=True, text=True, env=pull_env)
                except Exception as e:
                    with get_pod_lock(uid):
                        starting_pods.discard(uid)
                        start_failures[uid] = ("ErrImagePull", str(e))
                    if cfg_dir:
                        try:
                            shutil.rmtree(cfg_dir)
                        except Exception:
                            pass
                    return

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
                try:
                    with CONTAINER_SEMAPHORE:
                        res = subprocess.run(c, capture_output=True, text=True)
                    with get_pod_lock(uid):
                        starting_pods.discard(uid)
                        if res.returncode != 0:
                            err_msg = res.stderr.strip() or res.stdout.strip() or "container start failed"
                            start_failures[uid] = ("StartError", err_msg)
                except Exception as e:
                    with get_pod_lock(uid):
                        starting_pods.discard(uid)
                        start_failures[uid] = ("StartError", str(e))

            threading.Thread(target=run_async, args=(pod_uid, image, run_cmd, docker_cfg), daemon=True).start()

            self.reply(200, b'{"status":"starting"}')
            return

        self.send_response(404)
        self.end_headers()

if __name__ == '__main__':
    server = http.server.ThreadingHTTPServer((os.environ.get("CLOUDVM_RUNNER_BIND", "127.0.0.1"), PORT), RunnerHandler)
    server.serve_forever()
