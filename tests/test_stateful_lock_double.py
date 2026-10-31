"""Behavior of the tracked Lock Coordinator double used by local runs (not a live service)."""
import threading
import time

import pytest

from shared.observability.errors import AppException
from tests.support.stateful_lock import InProcessLockClient, LockTimeout, StatefulLockCoordinator


def test_exclusive_holders_and_monotonic_tokens():
    client = InProcessLockClient(StatefulLockCoordinator())
    inside, overlaps, tokens = [0], [], []
    guard = threading.Lock()

    def worker(i):
        lease = client.acquire("POOL-A", f"owner-{i}", f"acq-{i}", 5000, 5000)
        with guard:
            inside[0] += 1
            overlaps.append(inside[0])
            tokens.append(lease.fencing_token)
        time.sleep(0.005)
        with guard:
            inside[0] -= 1
        assert client.release(lease.lock_id, lease.owner_id)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert max(overlaps) == 1
    assert sorted(tokens) == list(range(1, 21))


def test_expiry_fences_old_owner_and_acquire_retry_is_deduplicated():
    now = [1000.0]
    coordinator = StatefulLockCoordinator(clock=lambda: now[0])
    first = coordinator.acquire("P", "a", "acq-a", 1000, 0)
    assert coordinator.acquire("P", "a", "acq-a", 1000, 0) is first  # retried Acquire, same lease
    with pytest.raises(LockTimeout):
        coordinator.acquire("P", "b", "acq-b", 1000, 0)
    now[0] += 2  # lease expires
    second = coordinator.acquire("P", "b", "acq-b", 1000, 0)
    assert second.fencing_token == first.fencing_token + 1
    assert coordinator.validate(first.lock_id, "a", "P", first.fencing_token) == (False, "NOT_OWNER")
    assert coordinator.release(first.lock_id, "a") is False
    assert coordinator.release(second.lock_id, "a") is False  # not the owner
    assert coordinator.validate(second.lock_id, "b", "P", second.fencing_token) == (True, "")


def test_client_maps_wait_timeout_to_retryable_deadline():
    client = InProcessLockClient(StatefulLockCoordinator())
    client.acquire("P", "a", "acq-a", 5000, 0)
    with pytest.raises(AppException) as error:
        client.acquire("P", "b", "acq-b", 5000, 10)
    assert error.value.code == "DEADLINE_EXCEEDED"
