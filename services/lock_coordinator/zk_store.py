"""Raw ZooKeeper operations for pool locks. Uses no kazoo lock recipes (task f)."""
import json
import os
import threading
import time

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

LOCK_ROOT = "/locks"
DEFAULT_HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"


def connect(hosts=None):
    """Open a ZooKeeper session. Any of the listed copies can answer."""
    client = KazooClient(hosts=hosts or os.environ.get("ZK_HOSTS", DEFAULT_HOSTS))
    client.start(timeout=10)
    return client


def join_queue(client, pool_id, owner_id, acquire_id, lease_ms):
    """Create this request's place in line and return its full path."""
    data = {
        "owner_id": owner_id,
        "acquire_id": acquire_id,
        "lease_ms": lease_ms,
        "expires_at": None,  # Stays empty until this request gets the key.
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