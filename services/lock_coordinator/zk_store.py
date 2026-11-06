"""Raw ZooKeeper operations for pool locks. Uses no kazoo lock recipes (task f)."""
import json
import os
import threading
import re
import time

from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NoNodeError

LOCK_ROOT = "/locks"
DEFAULT_HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
LOCK_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]+/lock-[0-9]{10}")
ABANDON_GRACE_MS = 5000

def connect(hosts=None):
    """Open a ZooKeeper session. Any of the listed copies can answer."""
    client = KazooClient(hosts=hosts or os.environ.get("ZK_HOSTS", DEFAULT_HOSTS))
    client.start(timeout=10)
    return client


def join_queue(client, pool_id, owner_id, acquire_id, lease_ms, wait_timeout_ms):
    """Create this request's place in line and return its full path."""
    data = {
        "owner_id": owner_id,
        "acquire_id": acquire_id,
        "lease_ms": lease_ms,
        "expires_at": None,  # Stays empty until this request gets the key.
        "abandon_at": now_ms() + wait_timeout_ms + ABANDON_GRACE_MS,
    }
    return client.create(
        f"{LOCK_ROOT}/{pool_id}/lock-",
        json.dumps(data).encode(),
        ephemeral=True,
        sequence=True,
        makepath=True,
    )


def queue(client, pool_id):
    """Return the names of everyone in line, first in line first."""
    return sorted(client.get_children(f"{LOCK_ROOT}/{pool_id}"))


def fencing_token(client, path):
    """Return the znode's czxid, which is the lock's fencing token."""
    _, stat = client.get(path)
    return stat.czxid
#wait_for_turn avoids "herd effect", where every request floods zookeeper.
#instead, here, a request only looks at the the one ahead of it in line and zookeeper taps on its shoulder when the one ahead is done.
def wait_for_turn(client, pool_id, path, timeout_s):
    """Wait until this request is first in line. Return False if time runs out."""
    my_name = path.rsplit("/", 1)[-1]
    deadline = time.monotonic() + timeout_s

    while True:
        names = queue(client, pool_id)
        if my_name not in names:
            return False  # Our place in line no longer exists.

        position = names.index(my_name)
        if position == 0:
            return True  # We are first, so we hold the key.

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False

        # Watch only the request directly ahead of us.
        ahead = f"{LOCK_ROOT}/{pool_id}/{names[position - 1]}"
        changed = threading.Event()
        if client.exists(ahead, watch=lambda event: changed.set()) is None:
            continue  # They already left, so check the line again.
        changed.wait(remaining)


def leave_queue(client, path):
    """Remove this request from the line. Being gone already is fine."""
    try:
        client.delete(path)
    except NoNodeError:
        pass

def now_ms():
    """Return the current time in epoch milliseconds."""
    return int(time.time() * 1000)


def read_lock(client, path):
    """Return the lock's data and stat, or (None, None) if it is gone."""
    try:
        raw, stat = client.get(path)
    except NoNodeError:
        return None, None
    return json.loads(raw), stat


def start_lease(client, path):
    """Start the timer once a request gets the key. Return expires_at."""
    data, stat = read_lock(client, path)
    if data is None:
        return None
    if data["expires_at"] is not None:
        return data["expires_at"]  # A retried Acquire already started it.    
    data["expires_at"] = now_ms() + data["lease_ms"]
    client.set(path, json.dumps(data).encode(), version=stat.version)
    return data["expires_at"]


def renew_lease(client, path, owner_id, lease_ms):
    """Extend a live lease. Return the new expires_at, or None if refused."""
    data, stat = read_lock(client, path)
    if data is None or data["owner_id"] != owner_id:
        return None
    if data["expires_at"] is None or data["expires_at"] <= now_ms():
        return None
    data["expires_at"] = now_ms() + lease_ms
    try:
        client.set(path, json.dumps(data).encode(), version=stat.version)
    except (NoNodeError, BadVersionError):
        return None
    return data["expires_at"]


def release(client, path, owner_id):
    """Give the key back. Only its owner can do this."""
    data, _ = read_lock(client, path)
    if data is None or data["owner_id"] != owner_id:
        return False
    leave_queue(client, path)
    return True


def validate(client, path, owner_id, token):
    """Check a lease is still good. Return (valid, reason)."""
    data, stat = read_lock(client, path)
    if data is None or data["expires_at"] is None or data["expires_at"] <= now_ms():
        return False, "LEASE_NOT_HELD"
    if data["owner_id"] != owner_id:
        return False, "NOT_OWNER"
    if stat.czxid != token:
        return False, "STALE_FENCING_TOKEN"
    return True, ""

#“The reaper’s delete is conditional on the znode version, 
# so a renewal that lands between its read and its delete makes the delete fail, and a live lease is never reaped.”
def reap_expired(client):
    """Delete every lock whose lease ran out. Return how many were deleted."""
    reaped = 0
    try:
        pools = client.get_children(LOCK_ROOT)
    except NoNodeError:
        return 0

    for pool_id in pools:
        for name in client.get_children(f"{LOCK_ROOT}/{pool_id}"):
            path = f"{LOCK_ROOT}/{pool_id}/{name}"
            data, stat = read_lock(client, path)
            if data is None:
                continue
            # A holder is judged by its lease, a waiter by when it should have given up.
            deadline = data["abandon_at"] if data["expires_at"] is None else data["expires_at"]
            if deadline > now_ms():
                continue
            try:
                # The version check means a renew that got in first wins.
                client.delete(path, version=stat.version)
                reaped += 1
            except (NoNodeError, BadVersionError):
                pass
    return reaped

def lock_id_for(path):
    """Turn a znode path into the lock_id we give to callers."""
    return path[len(LOCK_ROOT) + 1:]


def path_for(lock_id):
    """Turn a lock_id back into a znode path. Return None if it looks wrong."""
    if not LOCK_ID_PATTERN.fullmatch(lock_id):
        return None
    return f"{LOCK_ROOT}/{lock_id}"


def find_request(client, pool_id, owner_id, acquire_id):
    """Find a live earlier attempt of this same Acquire. Return its path or None."""
    try:
        names = queue(client, pool_id)
    except NoNodeError:
        return None

    for name in names:
        path = f"{LOCK_ROOT}/{pool_id}/{name}"
        data, _ = read_lock(client, path)
        if data is None or data["acquire_id"] != acquire_id or data["owner_id"] != owner_id:
            continue
        if data["expires_at"] is None or data["expires_at"] > now_ms():
            return path
    return None