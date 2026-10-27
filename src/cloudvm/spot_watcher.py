"""In-VM spot interruption watcher: polls the cloud metadata service and prints each new spot interruption notice as one JSON line.

The tunnel agent ships the lines to the plugin, which asks KubeRay to drain the worker's Ray pods (stdlib only, Python 3.9+).
"""
import datetime
import json
import os
import sys
import time
import urllib.error
import urllib.request

EVENT = "spot-interruption"
POLL_SECONDS = float(os.environ.get("CLOUDVM_SPOT_POLL_SECONDS", "5"))
AWS_METADATA_URL = os.environ.get("CLOUDVM_AWS_METADATA_URL", "http://169.254.169.254")
GCP_METADATA_URL = os.environ.get("CLOUDVM_GCP_METADATA_URL", "http://metadata.google.internal")
REQUEST_TIMEOUT = 2
AWS_TOKEN_TTL = 21600
# GCP holds a wait_for_change request open for up to this long and answers as soon as the value changes.
GCP_WAIT_SECONDS = 60


def log(msg):
    sys.stderr.write("[cloudvm-spot-watcher] %s\n" % msg)
    sys.stderr.flush()


def now_rfc3339():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def request(url, method="GET", headers=None, timeout=REQUEST_TIMEOUT):
    """Returns (status, body, headers); status is 0 when the service cannot be reached."""
    req = urllib.request.Request(url, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", "replace"), resp.headers
    except urllib.error.HTTPError as e:
        return e.code, "", e.headers
    except (OSError, ValueError):
        return 0, "", {}


class AWSMetadata(object):
    """Reads /latest/meta-data/spot/instance-action through IMDSv2; the path answers 404 until EC2 schedules an interruption."""

    def __init__(self, base):
        self.base = base
        self.token = None
        self.token_expiry = 0

    def refresh_token(self):
        status, body, _ = request(self.base + "/latest/api/token", method="PUT",
                                  headers={"X-aws-ec2-metadata-token-ttl-seconds": str(AWS_TOKEN_TTL)})
        if status != 200:
            return False
        self.token = body.strip()
        self.token_expiry = time.monotonic() + AWS_TOKEN_TTL / 2
        return True

    def notice(self):
        """Polls once and returns (action, time) while an interruption is scheduled; each call takes one poll interval."""
        try:
            return self.poll()
        finally:
            time.sleep(POLL_SECONDS)

    def poll(self):
        if (self.token is None or time.monotonic() >= self.token_expiry) and not self.refresh_token():
            return None
        status, body, _ = request(self.base + "/latest/meta-data/spot/instance-action",
                                  headers={"X-aws-ec2-metadata-token": self.token})
        if status == 401:
            self.token = None
        if status != 200:
            return None
        try:
            action = json.loads(body)
        except ValueError:
            log("ignoring malformed instance-action: %r" % body[:200])
            return None
        return action.get("action", ""), action.get("time", "")


class GCPMetadata(object):
    """Reads instance/preempted, which turns TRUE when Compute Engine starts preempting the VM, waiting on its ETag."""

    def __init__(self, base):
        self.base = base
        self.etag = None
        self.preempted_at = None

    def notice(self):
        """Returns ("preempt", time) once the VM is being preempted; a call returns early when the value changes."""
        url = self.base + "/computeMetadata/v1/instance/preempted"
        timeout = REQUEST_TIMEOUT
        waited = self.etag is not None
        if waited:
            url += "?wait_for_change=true&timeout_sec=%d&last_etag=%s" % (GCP_WAIT_SECONDS, self.etag)
            timeout += GCP_WAIT_SECONDS
        status, body, headers = request(url, headers={"Metadata-Flavor": "Google"}, timeout=timeout)
        self.etag = headers.get("ETag") if status == 200 else None
        if not (waited and self.etag):
            time.sleep(POLL_SECONDS)
        if status != 200 or body.strip().upper() != "TRUE":
            return None
        if self.preempted_at is None:
            self.preempted_at = now_rfc3339()
        return "preempt", self.preempted_at


def detect_platform():
    """Returns the metadata reader for the platform named by CLOUDVM_PLATFORM, or the first metadata service that answers."""
    platform = os.environ.get("CLOUDVM_PLATFORM", "")
    if platform == "aws":
        return AWSMetadata(AWS_METADATA_URL)
    if platform == "gcp":
        return GCPMetadata(GCP_METADATA_URL)
    while True:
        aws = AWSMetadata(AWS_METADATA_URL)
        if aws.refresh_token():
            return aws
        status, _, _ = request(GCP_METADATA_URL + "/computeMetadata/v1/instance/id", headers={"Metadata-Flavor": "Google"})
        if status == 200:
            return GCPMetadata(GCP_METADATA_URL)
        time.sleep(POLL_SECONDS)


def main():
    metadata = detect_platform()
    log("watching %s for spot interruption notices" % metadata.base)
    last = None
    while True:
        notice = metadata.notice()
        if notice is None or notice == last:
            continue
        last = notice
        action, notice_time = notice
        sys.stdout.write(json.dumps({"event": EVENT, "action": action, "time": notice_time or now_rfc3339()}) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
