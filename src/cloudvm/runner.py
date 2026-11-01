"""In-VM container runner: serves the plugin's create, status, logs, and delete calls by driving the VM's containerd through nerdctl.

The bootstrap sets CLOUDVM_RUNNER_BIND and CLOUDVM_RUNNER_CONCURRENCY; the plugin reaches it through the tunnel agent.
Status and host stats carry the readings of the traffic meter (cloudvm_traffic_meter.py) when the bootstrap installed it.
It runs a pod the way the kubelet would: its init containers in order, then its first container, all under the pod's
/kubepods/<qos>/pod<uid> cgroup, with its volumes and security settings. It also serves each pod whose service account
has an IAM role the credentials the plugin sends for it on a loopback endpoint.
"""
import base64
import datetime
import hashlib
import hmac
import http.server
import json
import os
import re
import secrets
import shutil
import socket
import stat
import subprocess
import tempfile
import threading
import time
import urllib.parse

PORT = 9090
CONTAINER_CLI = 'nerdctl'
SNAPSHOT_CLI = 'ctr'
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
waiting_reasons = {}
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
# emptyDir volumes live where the kubelet keeps them, on the disk that holds container storage.
POD_DIRS = os.environ.get("CLOUDVM_POD_DIRS", '/var/lib/kubelet/pods')
RESTART_BACKOFF_START = float(os.environ.get("CLOUDVM_RESTART_BACKOFF_START") or 10)
RESTART_BACKOFF_MAX = float(os.environ.get("CLOUDVM_RESTART_BACKOFF_MAX") or 300)
# init_states holds each pod's init container reports, in order, for the status endpoint.
init_states = {}
init_states_guard = threading.Lock()
# pod_fs_groups holds the fsGroup that owns each pod's volume files, for files the plugin replaces later.
pod_fs_groups = {}
CGROUP_PARENT_RE = re.compile(r'^/kubepods/(besteffort|burstable)/pod[A-Za-z0-9_.-]+$')
VOLUME_NAME_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]*$')
def is_kind(test):
    return lambda p: os.path.exists(p) and test(os.stat(p).st_mode)
HOST_PATH_CHECKS = {
    'Directory': os.path.isdir,
    'DirectoryOrCreate': os.path.isdir,
    'File': os.path.isfile,
    'FileOrCreate': os.path.isfile,
    'Socket': is_kind(stat.S_ISSOCK),
    'CharDevice': is_kind(stat.S_ISCHR),
    'BlockDevice': is_kind(stat.S_ISBLK),
}

class ConfigError(Exception):
    """A pod spec setting the runner cannot apply, reported as CreateContainerConfigError like the kubelet's."""
HOST_RESOLV_CONF = os.environ.get("CLOUDVM_RESOLV_CONF", '/etc/resolv.conf')

def safe_relpath(path):
    """Returns path when it is relative and stays below its root, or raises ValueError."""
    if not isinstance(path, str) or os.path.isabs(path) or '..' in path.split('/'):
        raise ValueError(f"unsafe path {path!r}")
    return path

def pod_volumes_dir(pod_uid):
    return os.path.join(SECRET_VOLUMES_DIR, re.sub(r'[^A-Za-z0-9_.-]', '_', pod_uid))

def safe_volume_name(name):
    if not isinstance(name, str) or not VOLUME_NAME_RE.match(name):
        raise ValueError(f"invalid volume name {name!r}")
    return name

def safe_mount_path(path):
    path = str(path or '')
    if not path.startswith('/') or ':' in path or ',' in path:
        raise ValueError(f"invalid path {path!r}")
    return path

def empty_dir_path(pod_uid, name):
    return os.path.join(POD_DIRS, re.sub(r'[^A-Za-z0-9_.-]', '_', pod_uid), 'volumes', 'kubernetes.io~empty-dir', name)

def write_volume_file(vol_dir, f, fs_group):
    """Writes one file of a file volume, replacing an existing one atomically so a container never reads half of it."""
    rel = safe_relpath(f.get('path'))
    dest = os.path.join(vol_dir, rel)
    os.makedirs(os.path.dirname(dest), mode=0o755, exist_ok=True)
    mode = int(f.get('mode') or 0o644) & 0o777
    tmp = os.path.join(os.path.dirname(dest), '.' + os.path.basename(dest) + '.tmp')
    if os.path.lexists(tmp):
        os.unlink(tmp)
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    with os.fdopen(fd, 'wb') as out:
        out.write(base64.b64decode(f.get('data') or ''))
    os.chmod(tmp, mode)
    if fs_group is not None:
        os.chown(tmp, -1, fs_group)
    os.replace(tmp, dest)

def check_host_path(path, kind):
    """Creates or checks a hostPath the way the kubelet does for its type."""
    if kind == 'DirectoryOrCreate' and not os.path.exists(path):
        os.makedirs(path, mode=0o755)
    if kind == 'FileOrCreate' and not os.path.exists(path):
        os.makedirs(os.path.dirname(path), mode=0o755, exist_ok=True)
        with open(path, 'a'):
            pass
        os.chmod(path, 0o644)
    check = HOST_PATH_CHECKS.get(kind)
    if kind and check is None:
        raise ConfigError(f"hostPath type {kind!r} is not supported")
    if check is not None and not check(path):
        raise ConfigError(f"hostPath {path} is not a {kind}")

def prepare_volumes(pod_uid, volumes, fs_group):
    """Materializes the pod's volumes and returns each volume's host path and kind by name."""
    paths = {}
    files_root = pod_volumes_dir(pod_uid)
    shutil.rmtree(files_root, ignore_errors=True)
    for vol in volumes:
        name = safe_volume_name(vol.get('name'))
        kind = vol.get('kind')
        if kind == 'emptyDir':
            path = empty_dir_path(pod_uid, name)
            os.makedirs(path, exist_ok=True)
            if vol.get('medium') == 'Memory' and not os.path.ismount(path):
                opts = 'size=%d' % int(vol['sizeLimitBytes']) if vol.get('sizeLimitBytes') else 'mode=0777'
                res = subprocess.run(['mount', '-t', 'tmpfs', '-o', opts, 'tmpfs', path], capture_output=True, text=True)
                if res.returncode != 0:
                    raise ConfigError(f"cannot mount tmpfs for emptyDir {name}: {res.stderr.strip()}")
            # The kubelet makes emptyDir world-writable, and with an fsGroup group-owned by it with the setgid bit.
            os.chmod(path, 0o777)
            if fs_group is not None:
                os.chown(path, -1, fs_group)
                os.chmod(path, 0o2777)
        elif kind == 'hostPath':
            path = safe_mount_path(vol.get('path'))
            check_host_path(path, vol.get('type') or '')
        elif kind == 'files':
            os.makedirs(files_root, mode=0o700, exist_ok=True)
            path = os.path.join(files_root, name)
            os.makedirs(path, mode=0o755)
            for f in vol.get('files') or []:
                write_volume_file(path, f, fs_group)
            if fs_group is not None:
                os.chown(path, -1, fs_group)
        else:
            raise ValueError(f"volume {name} has unknown kind {kind!r}")
        paths[name] = (path, kind)
    return paths

def remove_pod_volumes(pod_uid):
    """Unmounts the pod's memory-backed emptyDirs and removes every volume directory the runner made for it."""
    pod_dir = os.path.join(POD_DIRS, re.sub(r'[^A-Za-z0-9_.-]', '_', pod_uid))
    empty_dirs = os.path.join(pod_dir, 'volumes', 'kubernetes.io~empty-dir')
    for name in (os.listdir(empty_dirs) if os.path.isdir(empty_dirs) else []):
        path = os.path.join(empty_dirs, name)
        if os.path.ismount(path):
            subprocess.run(['umount', path], capture_output=True)
    shutil.rmtree(pod_dir, ignore_errors=True)
    shutil.rmtree(pod_volumes_dir(pod_uid), ignore_errors=True)

def mount_flags(mounts, paths):
    """Returns the -v flags of a container's volume mounts, creating a missing subPath in a writable volume."""
    flags = []
    for m in mounts or []:
        name = m.get('name')
        if name not in paths:
            raise ValueError(f"volume mount {name!r} names no volume")
        source, kind = paths[name]
        if m.get('subPath'):
            source = os.path.join(source, safe_relpath(m['subPath']))
            if kind in ('emptyDir', 'hostPath') and not os.path.exists(source):
                os.makedirs(source, mode=0o755)
        opts = ['ro' if m.get('readOnly') else 'rw']
        if m.get('propagation') in ('rslave', 'rshared'):
            opts.append(m['propagation'])
        flags.extend(['-v', f"{safe_mount_path(source)}:{safe_mount_path(m.get('mountPath'))}:{','.join(opts)}"])
    return flags

def image_user(img):
    """Returns the user the image's config names, empty for root."""
    res = subprocess.run([CONTAINER_CLI, 'image', 'inspect', '--format', '{{.Config.User}}', img], capture_output=True, text=True)
    if res.returncode != 0:
        raise ConfigError(f"cannot read the user of image {img}: {res.stderr.strip()}")
    return res.stdout.strip()

def user_flags(sec, img):
    """Returns the --user flag, which runs the image's own user unless the spec sets one, and enforces runAsNonRoot
    with the kubelet's checks and messages."""
    uid, gid = sec.get('runAsUser'), sec.get('runAsGroup')
    if uid is not None:
        if sec.get('runAsNonRoot') and int(uid) == 0:
            raise ConfigError(f"container's runAsUser breaks non-root policy")
        return [f"--user={int(uid)}" + (f":{int(gid)}" if gid is not None else '')]
    if gid is None and not sec.get('runAsNonRoot'):
        return []
    user = image_user(img).split(':')[0]
    if sec.get('runAsNonRoot'):
        if user in ('', '0', 'root'):
            raise ConfigError(f"container has runAsNonRoot and image will run as root")
        if not user.isdigit():
            raise ConfigError(f"container has runAsNonRoot and image has non-numeric user ({user}), cannot verify user is non-root")
    return [f"--user={user or '0'}:{int(gid)}"] if gid is not None else []

def security_flags(sec):
    """Returns the flags of a container's privileges. A privileged container shares the host's cgroup namespace, as
    containerd's CRI plugin does, so node agents in it read the host's cgroup paths."""
    flags = []
    if sec.get('privileged'):
        flags.extend(['--privileged', '--cgroupns=host'])
    for flag, caps in (('--cap-drop', sec.get('capDrop')), ('--cap-add', sec.get('capAdd'))):
        flags.extend(f"{flag}={cap}" for cap in caps or [])
    if sec.get('noNewPrivileges'):
        flags.append('--security-opt=no-new-privileges')
    if sec.get('readOnlyRootFilesystem'):
        flags.append('--read-only')
    if sec.get('seccompUnconfined'):
        flags.append('--security-opt=seccomp=unconfined')
    if sec.get('appArmorUnconfined'):
        flags.append('--security-opt=apparmor=unconfined')
    for gid in sec.get('supplementalGroups') or []:
        flags.append(f"--group-add={int(gid)}")
    return flags

def host_ip():
    """Returns the worker's primary address, which every container shares as its pod IP and host IP."""
    if os.environ.get("CLOUDVM_HOST_IP"):
        return os.environ["CLOUDVM_HOST_IP"]
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(('10.255.255.255', 1))
            return sock.getsockname()[0]
    except OSError:
        return '127.0.0.1'

def fill_host_ip(names, env, args):
    """Sets the variables the plugin left for the worker's address and expands their $(NAME) references."""
    if not names:
        return args
    ip = host_ip()
    def expand(value):
        for name in names:
            value = value.replace(f"$({name})", ip)
        return value
    for key in list(env):
        env[key] = ip if key in names else expand(str(env[key]))
    return [expand(str(a)) for a in args]

def pod_cgroup_flags(req):
    """Returns the global and run flags that put a container under the pod's cgroup and host namespaces."""
    global_flags, run_flags = [], ['--net=host']
    parent = str(req.get('cgroupParent') or '')
    if parent:
        if not CGROUP_PARENT_RE.match(parent):
            raise ValueError(f"invalid cgroupParent {parent!r}")
        # The cgroupfs manager puts the container at <parent>/<64-hex container id>, the kubelet's cgroupfs layout.
        global_flags.append('--cgroup-manager=cgroupfs')
        run_flags.append(f"--cgroup-parent={parent}")
    if req.get('hostPID'):
        run_flags.append('--pid=host')
    if req.get('hostIPC'):
        run_flags.append('--ipc=host')
    return global_flags, run_flags

def container_id(name):
    """Returns the runtime ID of the named container as containerd://<id>, or an empty string."""
    try:
        res = subprocess.run([CONTAINER_CLI, 'inspect', name], capture_output=True, text=True)
        if res.returncode == 0:
            cid = json.loads(res.stdout)[0].get('Id') or ''
            return 'containerd://' + cid if cid else ''
    except Exception:
        pass
    return ''

def init_container_name(pod_uid, index, name):
    """Names an init container's runtime container after its pod, its position and its Kubernetes name, which the
    tunnel agent reads back to label its log records for kubectl logs -c."""
    return f"cloudvm-{pod_uid}-init-{index}-{re.sub(r'[^a-z0-9-]', '-', str(name or '').lower())}"

def pod_init_states(pod_uid):
    with init_states_guard:
        return [dict(s) for s in init_states.get(pod_uid) or []]

def set_init_state(pod_uid, index, **fields):
    with init_states_guard:
        states = init_states.get(pod_uid)
        if states is not None and index < len(states):
            states[index].update(fields)

def pull_with_backoff(uid, imgs, auth_cfg, timed):
    """Pulls the first of imgs that succeeds, backing off exponentially while the pod stays Pending, and returns the
    reference pulled and whether the stargz snapshotter serves it, or None when the pod was stopped meanwhile."""
    delay = PULL_BACKOFF_START
    lazy = True
    attempts = 0
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
                remote, layers = remote_layer_count(img) if lazy and snapshotter_args() else (0, 0)
                if timed:
                    record_timing(uid, pullFinishedAt=now_iso(), image=img, pullAttempts=attempts, lazyPull=remote > 0)
                print(f"[cloudvm-runner] pulled {img} for {uid}" + (" (mirror failed, used upstream)" if img != imgs[0] else "")
                      + (f"; {remote} of {layers} layers mounted remotely" if lazy and snapshotter_args() else ""), flush=True)
                return img, lazy
            print(f"[cloudvm-runner] pull of {img} for {uid} failed: {err}", flush=True)
            err_msg = err
        with get_pod_lock(uid):
            if uid not in starting_pods:
                return None
            waiting_reasons[uid] = ("ErrImagePull", err_msg)
        if not sleep_while_starting(uid, delay):
            return None
        with get_pod_lock(uid):
            waiting_reasons[uid] = ("ImagePullBackOff", err_msg)
        delay = min(delay * 2, PULL_BACKOFF_MAX)

def sleep_while_starting(uid, delay):
    """Sleeps for delay seconds and reports whether the pod is still starting."""
    deadline = time.time() + delay
    while time.time() < deadline:
        time.sleep(min(1, delay))
        with get_pod_lock(uid):
            if uid not in starting_pods:
                waiting_reasons.pop(uid, None)
                return False
    return True

def run_init_containers(uid, req, cmds):
    """Runs each init container to completion in order. A failed one fails the pod under restartPolicy Never and is
    otherwise run again after an exponential back-off, as the kubelet does. Returns whether every one succeeded."""
    for i, cmd in enumerate(cmds):
        name = init_container_name(uid, i, req['initContainers'][i].get('name'))
        delay = RESTART_BACKOFF_START
        restarts = 0
        while True:
            with get_pod_lock(uid):
                if uid not in starting_pods:
                    return False
                waiting_reasons[uid] = ("PodInitializing", "")
            subprocess.run([CONTAINER_CLI, 'rm', '-f', name], capture_output=True)
            with CONTAINER_SEMAPHORE:
                res = subprocess.run(cmd, capture_output=True, text=True)
            exit_code, reason, message = 0, 'Completed', ''
            if res.returncode != 0:
                exit_code, reason = 128, 'StartError'
                message = res.stderr.strip() or res.stdout.strip() or "container start failed"
            else:
                set_init_state(uid, i, running=True, containerID=container_id(name), startedAt=now_iso(), reason='', restartCount=restarts)
                waited = subprocess.run([CONTAINER_CLI, 'wait', name], capture_output=True, text=True)
                try:
                    exit_code = int(waited.stdout.strip().splitlines()[-1])
                except (ValueError, IndexError):
                    exit_code = 1
                    message = waited.stderr.strip()
                if exit_code != 0:
                    reason = 'Error'
            set_init_state(uid, i, running=False, done=True, exitCode=exit_code, reason=reason, message=message,
                           finishedAt=now_iso(), restartCount=restarts)
            if exit_code == 0:
                break
            if req.get('restartPolicy') == 'Never':
                with get_pod_lock(uid):
                    if uid not in starting_pods:
                        return False
                    starting_pods.discard(uid)
                    start_failures[uid] = ("Init:Error", f"init container {req['initContainers'][i].get('name')} exited with code {exit_code}")
                return False
            if not sleep_while_starting(uid, delay):
                return False
            delay = min(delay * 2, RESTART_BACKOFF_MAX)
            restarts += 1
    return True

def snapshotter_args():
    # Lazy pulls need the stargz snapshotter; without it containerd's default snapshotter pulls in full.
    return ['--snapshotter=stargz'] if os.path.exists(STARGZ_SOCKET) else []

def remote_layer_count(img):
    """Counts the layers of img that the stargz snapshotter mounted remotely; 0 when it cannot tell.

    The snapshotter labels each layer snapshot it mounts from the registry, and names layer snapshots by chain ID. A
    layer it could not mount remotely is downloaded and unpacked like any other, so the pull succeeds either way.
    """
    try:
        # HACK(simonepri): nerdctl image inspect matches a name@digest reference only to an image stored with the tag
        # latest, so it cannot find one pulled by digest; the digest alone finds it. No upstream issue tracks this:
        # https://github.com/containerd/nerdctl/blob/v2.4.1/pkg/cmd/image/inspect.go#L141-L145
        res = subprocess.run([CONTAINER_CLI, *snapshotter_args(), 'image', 'inspect', '--format', '{{json .RootFS.Layers}}', img.rpartition('@')[2]],
                             capture_output=True, text=True)
        # A reference naming several stored images prints a line for each.
        lines = res.stdout.split() if res.returncode == 0 else []
        if not lines:
            raise ValueError(res.stderr.strip())
        diff_ids = json.loads(lines[0]) or []
        count, chain = 0, ''
        for diff_id in diff_ids:
            chain = diff_id if not chain else 'sha256:' + hashlib.sha256(f"{chain} {diff_id}".encode()).hexdigest()
            info = subprocess.run([SNAPSHOT_CLI, '--namespace', 'default', 'snapshots', '--snapshotter', 'stargz', 'info', chain],
                                  capture_output=True, text=True)
            if info.returncode == 0 and 'containerd.io/snapshot/remote' in (json.loads(info.stdout).get('Labels') or {}):
                count += 1
        return count, len(diff_ids)
    except Exception as e:
        print(f"[cloudvm-runner] cannot count remote layers of {img}: {e}", flush=True)
        return 0, 0

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

class NoTrafficMeter:
    """Stands in for the traffic meter on workers that did not receive it."""

    def run(self):
        pass

    def report(self, pod_uid=None):
        return None

    def add_pod(self, uid, namespace, name):
        pass

    def remove_pod(self, uid):
        pass

# Tunnel workers fetch the traffic meter next to the runner; others count only NIC totals.
try:
    import cloudvm_traffic_meter
    traffic_meter = cloudvm_traffic_meter.TrafficMeter()
except ImportError:
    traffic_meter = NoTrafficMeter()

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

def shm_flag(mem_bytes):
    if mem_bytes and mem_bytes > 0:
        return f"--shm-size={min(mem_bytes // 2, 64 * 1024 * 1024 * 1024)}"
    try:
        with open('/proc/meminfo') as mf:
            for line in mf:
                if line.startswith('MemTotal:'):
                    return f"--shm-size={min(int(line.split()[1]) * 1024 // 4, 16 * 1024 * 1024 * 1024)}"
    except Exception:
        pass
    return '--shm-size=16g'

def container_plan(pod_uid, req, c, name, paths, main):
    """Returns how to run one container: its images to try, and a function that builds its run command once the
    image is pulled."""
    original_image = str(c.get('image', ''))
    image = rewrite_image(original_image)
    env = {str(k): str(v) for k, v in (c.get('env') or {}).items()}
    if isinstance(req.get('awsCredentials'), dict):
        env.update(credentials_env(set_pod_credentials(pod_uid, req['awsCredentials'])))
    args = fill_host_ip(c.get('hostIPEnv') or [], env, [str(a) for a in (c.get('command') or [])] + [str(a) for a in (c.get('args') or [])])
    global_flags, pod_flags = pod_cgroup_flags(req)
    sec = c.get('security') or {'runAsUser': c.get('runAsUser'), 'runAsGroup': c.get('runAsGroup')}
    flags = [*pod_flags, *security_flags(sec)]
    if not req.get('hostIPC'):
        flags.append(shm_flag(c.get('memoryBytes', 0)))
    for domain in cluster_dns_search(str(req.get('namespace') or '')):
        flags.extend(['--dns-search', domain])
    cpu_milli, mem_bytes = c.get('cpuMillicores', 0), c.get('memoryBytes', 0)
    if cpu_milli and cpu_milli > 0:
        flags.extend(['--cpus', f"{cpu_milli / 1000.0:.2f}"])
    if mem_bytes and mem_bytes > 0:
        flags.extend(['--memory', str(mem_bytes)])
    if main and req.get('assignedGPUs'):
        flags.extend(['--gpus', '"device=%s"' % ','.join(str(g) for g in req['assignedGPUs'])])
    for k, v in env.items():
        flags.extend(['-e', f"{k}={v}"])
    flags.extend(mount_flags(c.get('mounts'), paths))

    def build(img, lazy):
        user = user_flags(sec, img)
        return [CONTAINER_CLI, *global_flags, *(snapshotter_args() if lazy else []), 'run', '-d', '--name', name,
                *flags, *user, img, *args]
    return [image] + ([original_image] if original_image != image else []), build

def plan_pod(pod_uid, req):
    """Prepares the pod's volumes and returns the plan of each init container and of the main container."""
    fs_group = req.get('fsGroup')
    paths = prepare_volumes(pod_uid, req.get('volumes') or [], int(fs_group) if fs_group is not None else None)
    with get_pod_lock(pod_uid):
        pod_fs_groups[pod_uid] = fs_group
    inits = req.get('initContainers') or []
    plans = [container_plan(pod_uid, req, c, init_container_name(pod_uid, i, c.get('name')), paths, False) for i, c in enumerate(inits)]
    with init_states_guard:
        init_states[pod_uid] = [{'name': str(c.get('name') or ''), 'reason': 'PodInitializing'} for c in inits]
    plans.append(container_plan(pod_uid, req, req, f"cloudvm-{pod_uid}", paths, True))
    return plans

def start_pod(uid, req, plans):
    """Pulls every image, runs the init containers in order, then starts the main container."""
    auth_cfg = req.get('dockerConfigJSON')
    record_timing(uid, pullStartedAt=now_iso())
    cmds = []
    try:
        for i, (imgs, build) in enumerate(plans):
            pulled = pull_with_backoff(uid, imgs, auth_cfg, timed=i == len(plans) - 1)
            if pulled is None:
                return
            cmds.append(build(*pulled))
    except (ConfigError, ValueError) as e:
        with get_pod_lock(uid):
            starting_pods.discard(uid)
            start_failures[uid] = ("CreateContainerConfigError", str(e))
        return
    with get_pod_lock(uid):
        waiting_reasons.pop(uid, None)
    if not run_init_containers(uid, req, cmds[:-1]):
        return
    with get_pod_lock(uid):
        if uid not in starting_pods:
            return
    try:
        with CONTAINER_SEMAPHORE:
            res = subprocess.run(cmds[-1], capture_output=True, text=True)
        with get_pod_lock(uid):
            starting_pods.discard(uid)
            waiting_reasons.pop(uid, None)
            if res.returncode != 0:
                start_failures[uid] = ("StartError", res.stderr.strip() or res.stdout.strip() or "container start failed")
        if res.returncode == 0:
            traffic_meter.add_pod(uid, str(req.get('namespace') or ''), str(req.get('podName') or ''))
    except Exception as e:
        with get_pod_lock(uid):
            starting_pods.discard(uid)
            start_failures[uid] = ("StartError", str(e))

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
            traffic = traffic_meter.report()
            if traffic is not None:
                resp["traffic"] = traffic
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
                        if len(parts) >= 2 and '-init-' not in parts[1]:
                            results.append({"podUID": parts[1].removeprefix('cloudvm-'), "running": True, "reason": 'running'})
            except Exception:
                pass
            self.reply(200, json.dumps(results))
            return

        if parsed.path == '/containers/status':
            params = urllib.parse.parse_qs(parsed.query)
            pod_uid = params.get('podUID', [''])[0]
            container_name = f"cloudvm-{pod_uid}"
            rx_bytes, tx_bytes = get_network_bytes()
            traffic = traffic_meter.report(pod_uid)
            def reply_status(**fields):
                resp = dict(fields, podUID=pod_uid, networkRxBytes=rx_bytes, networkTxBytes=tx_bytes,
                            timings=pod_timing(pod_uid), initContainers=pod_init_states(pod_uid))
                if traffic is not None:
                    resp["traffic"] = traffic
                self.reply(200, json.dumps(resp))
            # A recorded start failure wins over inspect: a failed run can leave the container in "created".
            def reply_failure():
                with get_pod_lock(pod_uid):
                    failure = start_failures.get(pod_uid)
                if failure is not None:
                    reply_status(running=False, status="dead", exitCode=1, reason=failure[0], message=failure[1],
                                 finishedAt=datetime.datetime.now(datetime.timezone.utc).isoformat())
                return failure is not None
            if reply_failure():
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
                    reply_status(running=running, status=status_str, exitCode=exit_code, reason=reason, message=message,
                                 startedAt=started_at, finishedAt=finished_at,
                                 containerID='containerd://' + data['Id'] if data.get('Id') else '')
                    return
            except Exception:
                pass

            plock = get_pod_lock(pod_uid)
            with plock:
                if pod_uid in starting_pods:
                    pull_reason, pull_error = waiting_reasons.get(pod_uid, ("ContainerCreating", "Pulling container image or initializing..."))
                    reply_status(running=False, status="created", exitCode=0, reason=pull_reason, message=pull_error)
                    return
            # The start may have failed while inspect ran.
            if reply_failure():
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

        if parsed.path == '/containers/volume-files':
            pod_uid = urllib.parse.parse_qs(parsed.query).get('podUID', [''])[0]
            try:
                files = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))).decode('utf-8'))
                with get_pod_lock(pod_uid):
                    fs_group = pod_fs_groups.get(pod_uid)
                for f in files:
                    vol_dir = os.path.join(pod_volumes_dir(pod_uid), safe_volume_name(f.get('volume')))
                    if not pod_uid or not os.path.isdir(vol_dir):
                        self.reply(404, b'{"error":"unknown pod volume"}')
                        return
                    write_volume_file(vol_dir, f, int(fs_group) if fs_group is not None else None)
            except (OSError, ValueError, TypeError, AttributeError) as e:
                self.reply(400, json.dumps({"error": f"cannot replace volume files: {e}"}))
                return
            self.reply(200, b'{"status":"ok"}')
            return

        if parsed.path == '/containers/stop':
            # Removing the container drops its writable layer and logs, so a later pod on this VM finds only the image cache.
            params = urllib.parse.parse_qs(parsed.query)
            pod_uid = params.get('podUID', [''])[0]
            if pod_uid:
                forget_pod_credentials(pod_uid)
                traffic_meter.remove_pod(pod_uid)
                plock = get_pod_lock(pod_uid)
                with plock:
                    starting_pods.discard(pod_uid)
                    start_failures.pop(pod_uid, None)
                    waiting_reasons.pop(pod_uid, None)
                    with pod_timings_guard:
                        pod_timings.pop(pod_uid, None)
                    with init_states_guard:
                        init_states.pop(pod_uid, None)
                    pod_fs_groups.pop(pod_uid, None)
                container_name = f"cloudvm-{pod_uid}"
                try:
                    subprocess.run([CONTAINER_CLI, 'stop', container_name], capture_output=True)
                    # Removing the container deletes its output, so wait up to 10 seconds for the tunnel agent to ship it:
                    # the agent keeps a file in /run/cloudvm/log-drain for each container whose output it has not shipped.
                    for _ in range(100):
                        if not os.path.exists('/run/cloudvm/log-drain/' + container_name):
                            break
                        time.sleep(0.1)
                    subprocess.run([CONTAINER_CLI, 'rm', '-f', container_name], capture_output=True)
                    listed = subprocess.run([CONTAINER_CLI, 'ps', '-a', '--filter', f"name={container_name}-init-", '--format', '{{.Names}}'],
                                            capture_output=True, text=True)
                    for init_name in listed.stdout.split() if listed.returncode == 0 else []:
                        subprocess.run([CONTAINER_CLI, 'rm', '-f', init_name], capture_output=True)
                    remains = subprocess.run([CONTAINER_CLI, 'inspect', container_name], capture_output=True).returncode == 0
                except Exception:
                    remains = True
                if remains:
                    self.reply(500, b'{"error":"container was not removed"}')
                    return
                remove_pod_volumes(pod_uid)
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
            for field in ('runAsUser', 'runAsGroup'):
                if req.get(field) is not None and not isinstance(req.get(field), int):
                    self.reply(400, json.dumps({"error": f"invalid {field}"}))
                    return

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
                waiting_reasons.pop(pod_uid, None)
                with pod_timings_guard:
                    pod_timings[pod_uid] = {"acceptedAt": now_iso()}

            try:
                plan = plan_pod(pod_uid, req)
            except (OSError, ValueError, TypeError, KeyError, ConfigError) as e:
                remove_pod_volumes(pod_uid)
                with plock:
                    starting_pods.discard(pod_uid)
                self.reply(400, json.dumps({"error": f"cannot prepare the pod: {e}"}))
                return

            threading.Thread(target=start_pod, args=(pod_uid, req, plan), daemon=True).start()

            self.reply(200, b'{"status":"starting"}')
            return

        self.send_response(404)
        self.end_headers()

if __name__ == '__main__':
    if CREDENTIALS_ADDR:
        threading.Thread(target=serve_credentials, daemon=True).start()
    threading.Thread(target=traffic_meter.run, daemon=True).start()
    server = http.server.ThreadingHTTPServer((os.environ.get("CLOUDVM_RUNNER_BIND", "127.0.0.1"), PORT), RunnerHandler)
    server.serve_forever()
