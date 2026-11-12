"""Raw ZooKeeper operations for pool locks. Uses no kazoo lock recipes (task f)."""
import hashlib
import json
import os
import re
import threading
import time

from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NodeExistsError, NoNodeError, RolledBackError

LOCK_ROOT = "/locks"
ACQUIRE_ROOT = "/acquires"   # One claim per (owner, acquire_id), so retries can't make two places in line.
RELEASED_ROOT = "/released"  # Tombstones: who released which lock, so a retried Release can succeed.
DEFAULT_HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
LOCK_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]+/lock-[0-9]{10}")
ABANDON_GRACE_MS = 5000
TOMBSTONE_TTL_MS = 10 * 60 * 1000
POOL_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]+")
MAX_ATTEMPTS = 5  # For version conflicts: read again, check again, try again.


def connect(hosts=None):
    """Open a ZooKeeper session. Any of the listed copies can answer."""
    client = KazooClient(hosts=hosts or os.environ.get("ZK_HOSTS", DEFAULT_HOSTS))
    client.start(timeout=10)
    return client


def now_ms():
    """Return the current time in epoch milliseconds."""
    return int(time.time() * 1000)


def _pool_and_name(path):
    """Split /locks/<pool>/<name> into (pool, name)."""
    _, _, pool_id, name = path.split("/")
    return pool_id, name


def _claim_path(pool_id, owner_id, acquire_id):
    """Where one (owner, acquire_id) claim lives. Hashed, so any acquire_id is a safe znode name."""
    digest = hashlib.sha256(f"{owner_id}\n{acquire_id}".encode()).hexdigest()
    return f"{ACQUIRE_ROOT}/{pool_id}/{digest}"


def _commit(transaction):
    """Commit a ZooKeeper transaction: every step happens, or none do. Raise the real error if it failed."""
    results = transaction.commit()
    for result in results:
        if isinstance(result, Exception) and not isinstance(result, RolledBackError):
            raise result
    return results


def join_queue(client, pool_id, owner_id, acquire_id, lease_ms, wait_timeout_ms):
    """Create this request's place in line and return its full path.

    With an acquire_id, the place in line and its claim are created in one transaction. If the claim
    already exists, nothing is created and NodeExistsError is raised.
    """
    data = {
        "owner_id": owner_id,
        "acquire_id": acquire_id,
        "lease_ms": lease_ms,
        "expires_at": None,  # Stays empty until this request gets the key.
        "abandon_at": now_ms() + wait_timeout_ms + ABANDON_GRACE_MS,
    }
    prefix = f"{LOCK_ROOT}/{pool_id}/lock-"
    client.ensure_path(f"{LOCK_ROOT}/{pool_id}")
    if not acquire_id:
        return client.create(prefix, json.dumps(data).encode(), ephemeral=True, sequence=True)

    client.ensure_path(f"{ACQUIRE_ROOT}/{pool_id}")
    transaction = client.transaction()
    transaction.create(prefix, json.dumps(data).encode(), ephemeral=True, sequence=True)
    transaction.create(_claim_path(pool_id, owner_id, acquire_id), b"", ephemeral=True)
    return _commit(transaction)[0]


def claim_place(client, pool_id, owner_id, acquire_id, lease_ms, wait_timeout_ms):
    """Return this request's place in line: an earlier attempt's if it's still live, otherwise a new one.

    Simultaneous retries with the same acquire_id race to create the claim. Exactly one wins, and the
    others find and share the winner's place in line.
    """
    for _ in range(50):
        if acquire_id:
            path = find_request(client, pool_id, owner_id, acquire_id)
            if path is not None:
                return path
        try:
            return join_queue(client, pool_id, owner_id, acquire_id, lease_ms, wait_timeout_ms)
        except NodeExistsError:
            # Another attempt claimed it a moment ago, or an expired attempt is about to be reaped.
            time.sleep(0.1)
    raise RuntimeError("could not claim a place in line")


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


def read_lock(client, path):
    """Return the lock's data and stat, or (None, None) if it is gone."""
    try:
        raw, stat = client.get(path)
    except NoNodeError:
        return None, None
    return json.loads(raw), stat


def _remove(client, path, data, version=-1, tombstone=None):
    """Delete a place in line and its claim in one transaction, optionally leaving a tombstone.

    version=-1 deletes whatever is there. A real version deletes only if nobody changed it since it was
    read, which raises BadVersionError otherwise. Raises NoNodeError if it's already gone.
    """
    pool_id, name = _pool_and_name(path)
    transaction = client.transaction()
    transaction.delete(path, version=version)
    if data.get("acquire_id"):
        transaction.delete(_claim_path(pool_id, data["owner_id"], data["acquire_id"]))
    if tombstone is not None:
        client.ensure_path(f"{RELEASED_ROOT}/{pool_id}")
        transaction.create(f"{RELEASED_ROOT}/{pool_id}/{name}", json.dumps(tombstone).encode())
    _commit(transaction)


def leave_queue(client, path):
    """Remove this request from the line. Being gone already is fine."""
    data, _ = read_lock(client, path)
    if data is None:
        return
    try:
        _remove(client, path, data)
    except NoNodeError:
        pass


def start_lease(client, path):
    """Start the timer once a request gets the key. Return expires_at, or None if the lock is gone."""
    for _ in range(MAX_ATTEMPTS):
        data, stat = read_lock(client, path)
        if data is None:
            return None
        if data["expires_at"] is not None:
            return data["expires_at"]  # A retried Acquire already started it.
        data["expires_at"] = now_ms() + data["lease_ms"]
        try:
            client.set(path, json.dumps(data).encode(), version=stat.version)
            return data["expires_at"]
        except BadVersionError:
            continue  # Another attempt of this same request started it first. Read it again.
        except NoNodeError:
            return None
    return None


def renew_lease(client, path, owner_id, lease_ms):
    """Extend a live lease. Return the new expires_at, or None if refused."""
    for _ in range(MAX_ATTEMPTS):
        data, stat = read_lock(client, path)
        if data is None or data["owner_id"] != owner_id:
            return None
        if data["expires_at"] is None or data["expires_at"] <= now_ms():
            return None
        data["expires_at"] = now_ms() + lease_ms
        try:
            client.set(path, json.dumps(data).encode(), version=stat.version)
            return data["expires_at"]
        except BadVersionError:
            continue  # Another renew landed at the same moment. Check ownership and expiry again.
        except NoNodeError:
            return None  # The reaper took it first: the lease had expired.
    return None


def _released_by(client, path):
    """Who released this lock, from its tombstone. None if it wasn't released (or was reaped instead)."""
    pool_id, name = _pool_and_name(path)
    try:
        raw, _ = client.get(f"{RELEASED_ROOT}/{pool_id}/{name}")
    except NoNodeError:
        return None
    return json.loads(raw)["owner_id"]


def release(client, path, owner_id):
    """Give the key back. Only its owner can do this, and retrying after a lost reply still succeeds.

    A lock that expired and was reaped reports False, so the caller knows it lost the lease.
    """
    data, _ = read_lock(client, path)
    if data is None:
        return _released_by(client, path) == owner_id  # A retry: was it this owner who released it?
    if data["owner_id"] != owner_id:
        return False
    try:
        _remove(client, path, data, tombstone={"owner_id": owner_id, "released_at": now_ms()})
    except NoNodeError:
        return _released_by(client, path) == owner_id  # A simultaneous retry released it first.
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
def reap_expired(client, on_reap=None):
    """Delete every lock whose lease ran out. Return how many were deleted.

    on_reap, if given, is called with (pool_id, "lease" or "abandoned") for each one."""
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
                _remove(client, path, data, version=stat.version)
                reaped += 1
                if on_reap:
                    on_reap(pool_id, "abandoned" if data["expires_at"] is None else "lease")
            except (NoNodeError, BadVersionError):
                pass
    return reaped


def reap_tombstones(client, max_age_ms=TOMBSTONE_TTL_MS):
    """Delete release tombstones older than max_age_ms. Return how many were deleted."""
    removed = 0
    try:
        pools = client.get_children(RELEASED_ROOT)
    except NoNodeError:
        return 0
    cutoff = now_ms() - max_age_ms
    for pool_id in pools:
        for name in client.get_children(f"{RELEASED_ROOT}/{pool_id}"):
            path = f"{RELEASED_ROOT}/{pool_id}/{name}"
            try:
                raw, _ = client.get(path)
                if json.loads(raw)["released_at"] <= cutoff:
                    client.delete(path)
                    removed += 1
            except NoNodeError:
                pass
    return removed


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