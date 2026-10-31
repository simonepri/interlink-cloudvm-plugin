"""In-VM container runner: serves the plugin's create, status, logs, and delete calls by driving the VM's containerd through nerdctl.

The bootstrap sets CLOUDVM_RUNNER_BIND and CLOUDVM_RUNNER_CONCURRENCY, and on profiled workers CLOUDVM_PROFILING; the plugin
reaches it through the tunnel agent.
It also serves each pod whose service account has an IAM role the credentials the plugin sends for it on a loopback endpoint.
"""
import base64
import datetime
import hmac
import http.server
import json
import os
import re
import secrets
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.parse

PORT = 9090
CONTAINER_CLI = 'nerdctl'
STARGZ_SOCKET = os.environ.get("CLOUDVM_STARGZ_SOCKET", '/run/containerd-stargz-grpc/containerd-stargz-grpc.sock')
IMAGE_MIRRORS_PATH = os.environ.get("CLOUDVM_IMAGE_MIRRORS", '/etc/cloudvm/image-mirrors.json')
HOST_DOCKER_CONFIG_DIR = os.environ.get("CLOUDVM_DOCKER_CONFIG", '/root/.docker')
PULL_BACKOFF_START = float(os.environ.get("CLOUDVM_PULL_BACKOFF_START") or 10)
PULL_BACKOFF_MAX = float(os.environ.get("CLOUDVM_PULL_BACKOFF_MAX") or 300)
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
pull_failures = {}
# pod_timings holds when each pod's start reached acceptedAt, pullStartedAt and pullFinishedAt, for the plugin's traces.
pod_timings = {}
pod_timings_guard = threading.Lock()
host_config_lock = threading.Lock()
DEFAULT_RUNNER_CONCURRENCY = max(4, os.cpu_count() or 4)
RUNNER_CONCURRENCY = int(os.environ.get("CLOUDVM_RUNNER_CONCURRENCY") or DEFAULT_RUNNER_CONCURRENCY)
CONTAINER_SEMAPHORE = threading.Semaphore(RUNNER_CONCURRENCY)

# Containers share the host network, so they reach the credential endpoint on loopback. A privileged port keeps a
# non-root container from binding it while the runner is down.
CREDENTIALS_ADDR = os.environ.get("CLOUDVM_CREDENTIALS_ADDR", "127.0.0.1:170")
pod_credentials = {}
pod_credentials_guard = threading.Lock()
# Secret volume files stay on tmpfs, as the kubelet keeps them, and each pod's tree is removed with its container.
SECRET_VOLUMES_DIR = os.environ.get("CLOUDVM_SECRET_VOLUMES_DIR", '/run/cloudvm/volumes')
# On a profiled worker the runner tells the host's parca-agent which pod each container belongs to through these
# variables, and exposes the pod labels CLOUDVM_PROFILING_POD_LABELS names by their sanitized names.
PROFILING = os.environ.get("CLOUDVM_PROFILING") == "1"
PROFILING_POD_LABELS = frozenset(l for l in os.environ.get("CLOUDVM_PROFILING_POD_LABELS", "").split(",") if l)
# A container whose CUDA_INJECTION64_PATH names this library gets the host's copy at that path, or loses the variable
# when the host has none, so CUDA never tries to load a missing library.
PARCAGPU_LIBRARY_NAME = 'libparcagpucupti.so'
HOST_RESOLV_CONF = os.environ.get("CLOUDVM_RESOLV_CONF", '/etc/resolv.conf')
PARCAGPU_LIBRARY = os.environ.get("CLOUDVM_PARCAGPU_LIBRARY", '/opt/cloudvm/parcagpu/' + PARCAGPU_LIBRARY_NAME)

def safe_relpath(path):
    """Returns path when it is relative and stays below its root, or raises ValueError."""
    if not isinstance(path, str) or os.path.isabs(path) or '..' in path.split('/'):
        raise ValueError(f"unsafe path {path!r}")
    return path

def pod_volumes_dir(pod_uid):
    return os.path.join(SECRET_VOLUMES_DIR, re.sub(r'[^A-Za-z0-9_.-]', '_', pod_uid))

def write_secret_volumes(pod_uid, volumes):
    """Writes a pod's Secret volumes below its own root-only directory and returns the read-only mount flags."""
    flags = []
    if not volumes:
        return flags
    root = pod_volumes_dir(pod_uid)
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root, mode=0o700)
    for i, vol in enumerate(volumes):
        vol_dir = os.path.join(root, str(i))
        os.makedirs(vol_dir, mode=0o755)
        for f in vol.get('files') or []:
            dest = os.path.join(vol_dir, safe_relpath(f.get('path')))
            os.makedirs(os.path.dirname(dest), mode=0o755, exist_ok=True)
            mode = int(f.get('mode') or 0o644) & 0o777
            fd = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
            with os.fdopen(fd, 'wb') as out:
                out.write(base64.b64decode(f.get('data') or ''))
            os.chmod(dest, mode)
        source = vol_dir
        if vol.get('subPath'):
            source = os.path.join(vol_dir, safe_relpath(vol['subPath']))
        mount_path = str(vol.get('mountPath', ''))
        if not mount_path.startswith('/') or ':' in mount_path or ',' in mount_path:
            raise ValueError(f"invalid mountPath {mount_path!r}")
        flags.extend(['-v', f"{source}:{mount_path}:ro"])
    return flags

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

def is_ecr_host(host):
    return '.dkr.ecr.' in host

def ensure_ecr_helper(host):
    """Maps an ECR registry host to the credential helper in the host config nerdctl and the stargz snapshotter read."""
    if not is_ecr_host(host):
        return
    path = os.path.join(HOST_DOCKER_CONFIG_DIR, 'config.json')
    with host_config_lock:
        try:
            with open(path, 'r') as f:
                cfg = json.load(f)
        except Exception:
            cfg = {}
        if cfg.get('credHelpers', {}).get(host) == 'ecr-login':
            return
        cfg.setdefault('credHelpers', {})[host] = 'ecr-login'
        os.makedirs(HOST_DOCKER_CONFIG_DIR, exist_ok=True)
        with open(path + '.tmp', 'w') as f:
            json.dump(cfg, f)
        os.replace(path + '.tmp', path)

def pull_config(auth_cfg, image):
    """Adds the ECR credential helper for the image's registry to the pod's pull secrets."""
    try:
        cfg = json.loads(auth_cfg) if isinstance(auth_cfg, str) else dict(auth_cfg)
    except Exception:
        cfg = {}
    host = image.split('/')[0]
    if is_ecr_host(host):
        cfg.setdefault('credHelpers', {})[host] = 'ecr-login'
    return cfg

def pull_image(img, auth_cfg, lazy):
    """Pulls img and returns None, or the error text. Pod pull secrets live in a per-pod directory removed afterwards."""
    cfg_dir = None
    try:
        ensure_ecr_helper(img.split('/')[0])
        env = dict(os.environ, DOCKER_CONFIG=HOST_DOCKER_CONFIG_DIR)
        if auth_cfg:
            cfg_dir = tempfile.mkdtemp(prefix="docker-cfg-")
            cfg_path = os.path.join(cfg_dir, 'config.json')
            with open(cfg_path, 'w', encoding='utf-8') as cf:
                json.dump(pull_config(auth_cfg, img), cf)
            os.chmod(cfg_path, 0o600)
            env['DOCKER_CONFIG'] = cfg_dir
        res = subprocess.run([CONTAINER_CLI, *(snapshotter_args() if lazy else []), 'pull', img], capture_output=True, text=True, env=env)
        if res.returncode != 0:
            return res.stderr.strip() or res.stdout.strip() or "image pull failed"
        return None
    except Exception as e:
        return str(e)
    finally:
        if cfg_dir:
            shutil.rmtree(cfg_dir, ignore_errors=True)

def clean_time(value):
    """Returns value when it is an RFC 3339 timestamp, else Go's zero time, which the plugin decodes as unset."""
    if isinstance(value, str) and re.match(r'^\d{4}-\d{2}-\d{2}T[\d:.]+(Z|[+-]\d{2}:\d{2})$', value):
        return value
    return "0001-01-01T00:00:00Z"

def now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def record_timing(uid, **fields):
    with pod_timings_guard:
        if uid in pod_timings:
            pod_timings[uid].update(fields)

def pod_timing(uid):
    with pod_timings_guard:
        return dict(pod_timings.get(uid) or {})

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

def set_pod_credentials(pod_uid, creds):
    """Records the role credentials the plugin sent for the pod, keeping those that expire last, and returns the
    secret that authorizes the pod's container at the credential endpoint."""
    with pod_credentials_guard:
        entry = pod_credentials.get(pod_uid)
        if entry is None:
            entry = {'secret': secrets.token_urlsafe(32), 'creds': {}}
            pod_credentials[pod_uid] = entry
        if creds.get('accessKeyId') and int(creds.get('expiration') or 0) >= int(entry['creds'].get('expiration') or 0):
            entry['creds'] = dict(creds)
        return entry['secret']

def forget_pod_credentials(pod_uid):
    with pod_credentials_guard:
        pod_credentials.pop(pod_uid, None)

def sanitize_label_name(name):
    """Returns name with every character outside [a-zA-Z0-9_] replaced by an underscore, as Prometheus labels need."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)

def profiling_env(req):
    """Returns the variables that name the container's pod to the host's parca-agent."""
    env = {
        'CLOUDVM_POD_NAMESPACE': str(req.get('namespace') or ''),
        'CLOUDVM_POD_NAME': str(req.get('podName') or ''),
        'CLOUDVM_POD_UID': str(req.get('podUID') or ''),
        'CLOUDVM_CONTAINER_NAME': str(req.get('containerName') or ''),
        'CLOUDVM_CONTAINER_IMAGE': str(req.get('image') or ''),
        'CLOUDVM_NODE_NAME': str(req.get('nodeName') or ''),
    }
    for key, value in (req.get('podLabels') or {}).items():
        name = sanitize_label_name(str(key))
        if name in PROFILING_POD_LABELS:
            env['CLOUDVM_POD_LABEL_' + name] = str(value)
    return {k: v for k, v in env.items() if v}

def parcagpu_mounts(env):
    """Mounts the host's ParcaGPU library where the container's CUDA_INJECTION64_PATH expects it, or drops the variable."""
    path = env.get('CUDA_INJECTION64_PATH', '')
    if os.path.basename(path) != PARCAGPU_LIBRARY_NAME or not os.path.isabs(path):
        return []
    if os.path.isfile(PARCAGPU_LIBRARY):
        return ['-v', f"{PARCAGPU_LIBRARY}:{path}:ro"]
    env.pop('CUDA_INJECTION64_PATH', None)
    env.pop('PARCAGPU_PC_SAMPLING_RATE', None)
    return []

def cluster_dns_search(namespace, path=None):
    """Returns the kubelet's ClusterFirst search list for a pod of namespace, then the host's other search domains.

    The tunnel agent puts svc.<cluster domain> first in the host's search list when it takes over DNS; without it, or
    without a namespace, the container keeps the host's list and this returns an empty list.
    """
    try:
        with open(path or HOST_RESOLV_CONF) as f:
            lines = f.read().splitlines()
    except OSError:
        return []
    search = [d for l in lines if l.startswith(("search ", "domain ")) for d in l.split()[1:]]
    if not namespace or not search or not search[0].startswith("svc."):
        return []
    domain = search[0][len("svc."):]
    first = [namespace + ".svc." + domain, "svc." + domain, domain]
    return first + [d for d in search if d not in first]

def credentials_env(secret):
    """Returns the env that points every AWS SDK in a container at the pod's credential endpoint."""
    return {
        'AWS_CONTAINER_CREDENTIALS_FULL_URI': 'http://%s/v1/credentials' % CREDENTIALS_ADDR,
        'AWS_CONTAINER_AUTHORIZATION_TOKEN': secret,
    }

def credentials_for_secret(secret):
    """Returns the pod UID and credentials whose endpoint secret matches, comparing every secret in constant time."""
    match = None
    with pod_credentials_guard:
        for uid, entry in pod_credentials.items():
            if hmac.compare_digest(entry['secret'].encode(), secret.encode()):
                match = (uid, entry['creds'])
    return match

class CredentialsHandler(http.server.BaseHTTPRequestHandler):
    """Serves a pod's role credentials in the AWS container credentials format to the holder of its endpoint secret."""
    def log_message(self, format, *args):
        pass

    def reply(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if urllib.parse.urlparse(self.path).path != '/v1/credentials':
            self.reply(404, {'code': 'NotFound', 'message': 'unknown path'})
            return
        match = credentials_for_secret(self.headers.get('Authorization', ''))
        if match is None:
            self.reply(401, {'code': 'AccessDenied', 'message': 'unknown authorization token'})
            return
        uid, creds = match
        expiration = int(creds.get('expiration') or 0)
        if not creds.get('accessKeyId') or expiration <= time.time():
            print(f"[cloudvm-runner] no current AWS credentials for {uid}", flush=True)
            self.reply(503, {'code': 'CredentialsUnavailable', 'message': 'the plugin has not delivered current credentials'})
            return
        self.reply(200, {
            'AccessKeyId': creds['accessKeyId'],
            'SecretAccessKey': creds['secretAccessKey'],
            'Token': creds['sessionToken'],
            'Expiration': datetime.datetime.fromtimestamp(expiration, datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        })

def serve_credentials():
    host, _, port = CREDENTIALS_ADDR.rpartition(':')
    while True:
        try:
            http.server.ThreadingHTTPServer((host, int(port)), CredentialsHandler).serve_forever()
        except Exception as e:
            print(f"[cloudvm-runner] credential endpoint on {CREDENTIALS_ADDR} failed: {e}", flush=True)
        time.sleep(5)

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
                    "timings": pod_timing(pod_uid),
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
                    started_at = clean_time(state.get('StartedAt'))
                    finished_at = clean_time(state.get('FinishedAt'))
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
                        "timings": pod_timing(pod_uid),
                    }
                    self.reply(200, json.dumps(resp))
                    return
            except Exception:
                pass

            plock = get_pod_lock(pod_uid)
            with plock:
                if pod_uid in starting_pods:
                    pull_reason, pull_error = pull_failures.get(pod_uid, ("ContainerCreating", "Pulling container image or initializing..."))
                    resp = {
                        "podUID": pod_uid,
                        "running": False,
                        "status": "created",
                        "exitCode": 0,
                        "reason": pull_reason,
                        "message": pull_error,
                        "networkRxBytes": rx_bytes,
                        "networkTxBytes": tx_bytes,
                        "timings": pod_timing(pod_uid),
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

        if parsed.path == '/containers/credentials':
            pod_uid = urllib.parse.parse_qs(parsed.query).get('podUID', [''])[0]
            try:
                creds = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))).decode('utf-8'))
            except Exception:
                creds = None
            if not pod_uid or not isinstance(creds, dict):
                self.reply(400, b'{"error":"invalid credentials request"}')
                return
            set_pod_credentials(pod_uid, creds)
            self.reply(200, b'{"status":"ok"}')
            return

        if parsed.path == '/containers/stop':
            # Removing the container drops its writable layer and logs, so a later pod on this VM finds only the image cache.
            params = urllib.parse.parse_qs(parsed.query)
            pod_uid = params.get('podUID', [''])[0]
            if pod_uid:
                forget_pod_credentials(pod_uid)
                plock = get_pod_lock(pod_uid)
                with plock:
                    starting_pods.discard(pod_uid)
                    start_failures.pop(pod_uid, None)
                    pull_failures.pop(pod_uid, None)
                    with pod_timings_guard:
                        pod_timings.pop(pod_uid, None)
                container_name = f"cloudvm-{pod_uid}"
                try:
                    subprocess.run([CONTAINER_CLI, 'stop', container_name], capture_output=True)
                    subprocess.run([CONTAINER_CLI, 'rm', '-f', container_name], capture_output=True)
                    remains = subprocess.run([CONTAINER_CLI, 'inspect', container_name], capture_output=True).returncode == 0
                except Exception:
                    remains = True
                if remains:
                    self.reply(500, b'{"error":"container was not removed"}')
                    return
                shutil.rmtree(pod_volumes_dir(pod_uid), ignore_errors=True)
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
                pull_failures.pop(pod_uid, None)
                with pod_timings_guard:
                    pod_timings[pod_uid] = {"acceptedAt": now_iso()}

            original_image = str(req.get('image', ''))
            image = rewrite_image(original_image)
            gpus = req.get('assignedGPUs', [])
            env = dict(req.get('env') or {})
            if isinstance(req.get('awsCredentials'), dict):
                env.update(credentials_env(set_pod_credentials(pod_uid, req['awsCredentials'])))
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
            for domain in cluster_dns_search(str(req.get('namespace') or '')):
                run_cmd.extend(['--dns-search', domain])
            if cpu_milli and cpu_milli > 0:
                run_cmd.extend(['--cpus', f"{cpu_milli / 1000.0:.2f}"])
            if mem_bytes and mem_bytes > 0:
                run_cmd.extend(['--memory', str(mem_bytes)])
            if gpus:
                gpu_devs = ','.join(str(g) for g in gpus)
                gpu_spec = f'"device={gpu_devs}"'
                run_cmd.extend(['--gpus', gpu_spec])
            if PROFILING:
                env.update(profiling_env(req))
            run_cmd.extend(parcagpu_mounts(env))
            for k, v in env.items():
                run_cmd.extend(['-e', f"{k}={v}"])
            try:
                run_cmd.extend(write_secret_volumes(pod_uid, req.get('secretVolumes') or []))
            except (OSError, ValueError, TypeError) as e:
                shutil.rmtree(pod_volumes_dir(pod_uid), ignore_errors=True)
                with plock:
                    starting_pods.discard(pod_uid)
                self.reply(400, json.dumps({"error": f"cannot write secret volumes: {e}"}))
                return
            image_index = len(run_cmd)
            run_cmd.append(image)
            if isinstance(cmd, list):
                run_cmd.extend(cmd)
            if isinstance(args, list):
                run_cmd.extend(args)

            docker_cfg = req.get('dockerConfigJSON')

            def run_async(uid, imgs, c, idx, auth_cfg):
                # 1. Pull: try the mirrored reference, then the original, and back off exponentially while the pod stays Pending
                delay = PULL_BACKOFF_START
                lazy = True
                attempts = 0
                record_timing(uid, pullStartedAt=now_iso())
                while True:
                    err_msg = "image pull failed"
                    for img in imgs:
                        attempts += 1
                        err = pull_image(img, auth_cfg, lazy)
                        if err and lazy and re.search(r'unpack|snapshotter', err, re.I):
                            print(f"[cloudvm-runner] lazy pull of {img} for {uid} failed, retrying with the default snapshotter: {err}", flush=True)
                            lazy = False
                            attempts += 1
                            err = pull_image(img, auth_cfg, lazy)
                        if err is None:
                            record_timing(uid, pullFinishedAt=now_iso(), image=img, pullAttempts=attempts, lazyPull=lazy and bool(snapshotter_args()))
                            print(f"[cloudvm-runner] pulled {img} for {uid}" + (" (mirror failed, used upstream)" if img != imgs[0] else ""), flush=True)
                            c[idx] = img
                            if not lazy and c[1] == '--snapshotter=stargz':
                                del c[1]
                            break
                        print(f"[cloudvm-runner] pull of {img} for {uid} failed: {err}", flush=True)
                        err_msg = err
                    else:
                        with get_pod_lock(uid):
                            if uid not in starting_pods:
                                return
                            pull_failures[uid] = ("ErrImagePull", err_msg)
                        deadline = time.time() + delay
                        while time.time() < deadline:
                            time.sleep(min(1, PULL_BACKOFF_START))
                            with get_pod_lock(uid):
                                if uid not in starting_pods:
                                    pull_failures.pop(uid, None)
                                    return
                        with get_pod_lock(uid):
                            pull_failures[uid] = ("ImagePullBackOff", err_msg)
                        delay = min(delay * 2, PULL_BACKOFF_MAX)
                        continue
                    break
                with get_pod_lock(uid):
                    pull_failures.pop(uid, None)

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

            threading.Thread(target=run_async, args=(pod_uid, [image] + ([original_image] if original_image != image else []), run_cmd, image_index, docker_cfg), daemon=True).start()

            self.reply(200, b'{"status":"starting"}')
            return

        self.send_response(404)
        self.end_headers()

if __name__ == '__main__':
    if CREDENTIALS_ADDR:
        threading.Thread(target=serve_credentials, daemon=True).start()
    server = http.server.ThreadingHTTPServer((os.environ.get("CLOUDVM_RUNNER_BIND", "127.0.0.1"), PORT), RunnerHandler)
    server.serve_forever()
