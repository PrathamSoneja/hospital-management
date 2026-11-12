"""Integration tests for the Lock Coordinator against a real ZooKeeper."""
import os
import threading
import time
import uuid

import grpc
import pytest
from kazoo.client import KazooClient
from fastapi.testclient import TestClient
from kazoo.exceptions import BadVersionError
from prometheus_client import REGISTRY


from services.bed_allocation.clients.grpc_clients import GrpcLockCoordinatorClient
from services.lock_coordinator import zk_store
from services.lock_coordinator.grpc_server import create_server, reaper_loop
from services.lock_coordinator.rest import build_app
from shared.generated import lock_pb2, lock_pb2_grpc
from shared.observability.errors import AppException

ZK_HOSTS = os.environ.get("ZK_TEST_HOSTS", "")

pytestmark = pytest.mark.skipif(not ZK_HOSTS, reason="Set ZK_TEST_HOSTS to a local ZooKeeper")

POOL = "TEST-POOL"


class Coordinator:
    """One running replica, plus the real Bed Allocation client pointed at it."""

    def __init__(self, hosts):
        self.zk = zk_store.connect(hosts)
        self.server, port = create_server(self.zk, "127.0.0.1:0", workers=64)
        self.stop_reaper = threading.Event()
        threading.Thread(target=reaper_loop, args=(self.zk, self.stop_reaper), daemon=True).start()
        self.server.start()
        self.client = GrpcLockCoordinatorClient(f"127.0.0.1:{port}")
        self.stub = lock_pb2_grpc.LockCoordinatorStub(grpc.insecure_channel(f"127.0.0.1:{port}"))

    def stop(self):
        self.stop_reaper.set()
        self.server.stop(0)
        self.zk.stop()


@pytest.fixture
def hosts():
    """Give each test its own empty area of ZooKeeper, and delete it afterwards."""
    area = f"/test-{uuid.uuid4().hex}"
    admin = KazooClient(hosts=ZK_HOSTS)
    admin.start(timeout=10)
    admin.ensure_path(area)
    yield ZK_HOSTS + area
    admin.delete(area, recursive=True)
    admin.stop()


@pytest.fixture
def replica(hosts):
    coordinator = Coordinator(hosts)
    yield coordinator
    coordinator.stop()


def test_only_one_holder_at_a_time_and_tokens_increase(replica):
    holders = 0
    peak = 0
    tokens = []
    guard = threading.Lock()

    def request_key(number):
        nonlocal holders, peak
        lease = replica.client.acquire(POOL, f"owner-{number}", f"req-{number}", 5000, 20000)
        with guard:
            holders += 1
            peak = max(peak, holders)
            tokens.append(lease.fencing_token)
        time.sleep(0.01)
        with guard:
            holders -= 1
        replica.client.release(lease.lock_id, f"owner-{number}")

    threads = [threading.Thread(target=request_key, args=(n,)) for n in range(30)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(tokens) == 30
    assert peak == 1
    assert tokens == sorted(tokens)
    assert len(set(tokens)) == 30


def test_second_replica_sees_the_same_lock(hosts):
    first = Coordinator(hosts)
    second = Coordinator(hosts)
    try:
        lease = first.client.acquire(POOL, "alice", "req-1", 60000, 1000)
        assert second.client.validate(lease.lock_id, "alice", POOL, lease.fencing_token)
        assert second.client.release(lease.lock_id, "alice")
        assert not first.client.validate(lease.lock_id, "alice", POOL, lease.fencing_token)
    finally:
        first.stop()
        second.stop()


def test_retry_with_same_acquire_id_returns_the_same_lease(replica):
    first = replica.client.acquire(POOL, "alice", "req-1", 60000, 1000)
    retry = replica.client.acquire(POOL, "alice", "req-1", 60000, 1000)

    assert retry.lock_id == first.lock_id
    assert retry.fencing_token == first.fencing_token
    assert retry.expires_at == first.expires_at


def test_wait_timeout_gives_504_and_leaves_the_line(replica):
    replica.client.acquire(POOL, "alice", "req-1", 60000, 1000)

    with pytest.raises(AppException) as error:
        replica.client.acquire(POOL, "bob", "req-2", 60000, 200)

    assert error.value.status_code == 504
    assert len(zk_store.queue(replica.zk, POOL)) == 1


def test_expired_lease_is_reaped_and_the_next_request_gets_the_key(replica):
    frozen = replica.client.acquire(POOL, "frozen", "req-1", 500, 1000)

    following = replica.client.acquire(POOL, "next", "req-2", 60000, 5000)

    assert following.fencing_token > frozen.fencing_token
    assert not replica.client.validate(frozen.lock_id, "frozen", POOL, frozen.fencing_token)


def test_renew_keeps_a_lease_alive(replica):
    lease = replica.client.acquire(POOL, "alice", "req-1", 1000, 1000)

    renewed = replica.stub.Renew(lock_pb2.RenewLockRequest(lock_id=lease.lock_id, owner_id="alice", lease_ms=60000))
    time.sleep(1.5)

    assert renewed.success
    assert renewed.expires_at > lease.expires_at
    assert replica.client.validate(lease.lock_id, "alice", POOL, lease.fencing_token)


def test_renew_is_refused_for_the_wrong_owner(replica):
    lease = replica.client.acquire(POOL, "alice", "req-1", 60000, 1000)

    renewed = replica.stub.Renew(lock_pb2.RenewLockRequest(lock_id=lease.lock_id, owner_id="mallory", lease_ms=60000))

    assert not renewed.success


def test_validate_gives_the_right_reasons(replica):
    lease = replica.client.acquire(POOL, "alice", "req-1", 60000, 1000)

    def reason(owner, token, lock_id=lease.lock_id):
        request = lock_pb2.ValidateLockRequest(lock_id=lock_id, owner_id=owner, pool_id=POOL, fencing_token=token)
        return replica.stub.Validate(request).reason

    assert reason("alice", lease.fencing_token) == ""
    assert reason("mallory", lease.fencing_token) == "NOT_OWNER"
    assert reason("alice", lease.fencing_token - 1) == "STALE_FENCING_TOKEN"
    assert reason("alice", lease.fencing_token, "not-a-lock") == "LEASE_NOT_HELD"


def test_only_the_owner_can_release(replica):
    lease = replica.client.acquire(POOL, "alice", "req-1", 60000, 1000)

    assert not replica.client.release(lease.lock_id, "mallory")
    assert replica.client.release(lease.lock_id, "alice")
    assert not replica.client.release(lease.lock_id, "mallory")  # Still not mallory's, even once released.


def test_losing_the_session_frees_the_lock(hosts):
    watcher = zk_store.connect(hosts)
    crashing = zk_store.connect(hosts)
    try:
        zk_store.join_queue(crashing, POOL, "alice", "req-1", 60000, 1000)
        assert len(zk_store.queue(watcher, POOL)) == 1

        crashing.stop()

        assert zk_store.queue(watcher, POOL) == []
    finally:
        watcher.stop()


def test_a_renew_beats_a_reaper_holding_an_old_read(replica):
    path = zk_store.join_queue(replica.zk, POOL, "alice", "req-1", 60000, 1000)
    zk_store.start_lease(replica.zk, path)
    _, old_stat = zk_store.read_lock(replica.zk, path)

    zk_store.renew_lease(replica.zk, path, "alice", 60000)

    with pytest.raises(BadVersionError):
        replica.zk.delete(path, version=old_stat.version)


def test_reaper_leaves_a_request_that_is_still_waiting(replica):
    zk_store.join_queue(replica.zk, POOL, "alice", "req-1", 60000, 60000)

    assert zk_store.reap_expired(replica.zk) == 0


def test_reaper_removes_an_abandoned_waiter(replica):
    # A negative wait puts its abandon time in the past.
    zk_store.join_queue(replica.zk, POOL, "ghost", "req-1", 60000, -zk_store.ABANDON_GRACE_MS - 1000)

    assert zk_store.reap_expired(replica.zk) == 1
    assert zk_store.queue(replica.zk, POOL) == []


def metric(name, **labels):
    """Read one metric's current value, or 0 if it hasn't been recorded yet."""
    return REGISTRY.get_sample_value(name, {"service": "lock-coordinator", **labels}) or 0


def test_metrics_count_grants_timeouts_and_expiries(replica):
    granted = metric("lock_acquire_total", pool_id=POOL, outcome="granted")
    timeouts = metric("lock_acquire_total", pool_id=POOL, outcome="timeout")
    expiries = metric("lease_expirations_total", pool_id=POOL)
    deadline_errors = metric("grpc_requests_total", method="Acquire", code="DEADLINE_EXCEEDED")

    replica.client.acquire(POOL, "frozen", "req-1", 500, 1000)
    with pytest.raises(AppException):
        replica.client.acquire(POOL, "bob", "req-2", 60000, 100)
    replica.client.acquire(POOL, "next", "req-3", 60000, 5000)  # Waits for the reaper to clear "frozen".

    assert metric("lock_acquire_total", pool_id=POOL, outcome="granted") == granted + 2
    assert metric("lock_acquire_total", pool_id=POOL, outcome="timeout") == timeouts + 1
    assert metric("lease_expirations_total", pool_id=POOL) == expiries + 1
    assert metric("grpc_requests_total", method="Acquire", code="DEADLINE_EXCEEDED") == deadline_errors + 1
    assert "lock_acquire_total" in TestClient(build_app(replica.zk)).get("/metrics").text


def test_inspector_shows_who_holds_and_who_waits(replica):
    holder = zk_store.join_queue(replica.zk, POOL, "alice", "req-1", 60000, 60000)
    zk_store.start_lease(replica.zk, holder)
    zk_store.join_queue(replica.zk, POOL, "bob", "req-2", 60000, 60000)
    http = TestClient(build_app(replica.zk))

    entries = http.get(f"/api/v1/locks/{POOL}").json()["entries"]

    assert [(e["owner_id"], e["state"], e["position"]) for e in entries] == [("alice", "HOLDING", 0), ("bob", "WAITING", 1)]
    assert entries[0]["fencing_token"] == str(zk_store.fencing_token(replica.zk, holder))
    assert POOL in http.get("/api/v1/locks").json()["pools"]
    assert http.get("/api/v1/locks/NO-SUCH-POOL").status_code == 404
    assert http.get("/readyz").status_code == 200
def run_together(count, action):
    """Start `count` threads that all call action() at the same moment. Return their results."""
    barrier = threading.Barrier(count)
    results = []
    errors = []

    def run():
        barrier.wait()
        try:
            results.append(action())
        except Exception as error:  # Surface thread errors in the test, not just the log.
            errors.append(error)

    threads = [threading.Thread(target=run) for _ in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    return results


def test_simultaneous_retries_on_two_replicas_share_one_place_in_line(hosts):
    first = Coordinator(hosts)
    second = Coordinator(hosts)
    try:
        for attempt in range(10):
            acquire_id = f"req-{attempt}"
            replicas = iter([first, second])
            leases = run_together(2, lambda: next(replicas).client.acquire(POOL, "alice", acquire_id, 60000, 5000))

            assert leases[0].lock_id == leases[1].lock_id
            assert leases[0].fencing_token == leases[1].fencing_token
            assert len(zk_store.queue(first.zk, POOL)) == 1
            assert first.client.release(leases[0].lock_id, "alice")
    finally:
        first.stop()
        second.stop()


def test_simultaneous_lease_starts_agree_on_one_expiry(replica):
    path = zk_store.join_queue(replica.zk, POOL, "alice", "req-1", 60000, 1000)

    expiries = run_together(5, lambda: zk_store.start_lease(replica.zk, path))

    assert None not in expiries
    assert len(set(expiries)) == 1


def test_simultaneous_renews_all_succeed(replica):
    lease = replica.client.acquire(POOL, "alice", "req-1", 60000, 1000)
    request = lock_pb2.RenewLockRequest(lock_id=lease.lock_id, owner_id="alice", lease_ms=60000)

    renewed = run_together(5, lambda: replica.stub.Renew(request).success)

    assert renewed == [True] * 5


def test_renew_is_refused_after_the_lease_expired(replica):
    lease = replica.client.acquire(POOL, "alice", "req-1", 300, 1000)
    time.sleep(0.6)

    renewed = replica.stub.Renew(lock_pb2.RenewLockRequest(lock_id=lease.lock_id, owner_id="alice", lease_ms=60000))

    assert not renewed.success
    assert not replica.client.validate(lease.lock_id, "alice", POOL, lease.fencing_token)


def test_a_retried_release_after_a_lost_reply_still_succeeds(replica):
    lease = replica.client.acquire(POOL, "alice", "req-1", 60000, 1000)

    assert replica.client.release(lease.lock_id, "alice")
    assert replica.client.release(lease.lock_id, "alice")  # The retry, as if the first reply was lost.


def test_simultaneous_releases_by_the_owner_all_succeed(replica):
    lease = replica.client.acquire(POOL, "alice", "req-1", 60000, 1000)

    released = run_together(5, lambda: replica.client.release(lease.lock_id, "alice"))

    assert released == [True] * 5


def test_release_reports_failure_if_the_lease_expired_first(replica):
    lease = replica.client.acquire(POOL, "frozen", "req-1", 300, 1000)
    deadline = time.monotonic() + 5
    while zk_store.queue(replica.zk, POOL) and time.monotonic() < deadline:
        time.sleep(0.1)  # Wait for the reaper.

    assert not replica.client.release(lease.lock_id, "frozen")  # Tells Allocation it lost the lease.


def test_claims_and_tombstones_are_cleaned_up(replica):
    lease = replica.client.acquire(POOL, "alice", "req-1", 60000, 1000)
    replica.client.release(lease.lock_id, "alice")

    assert replica.zk.get_children(f"{zk_store.ACQUIRE_ROOT}/{POOL}") == []
    assert zk_store.reap_tombstones(replica.zk, max_age_ms=0) == 1
    assert replica.zk.get_children(f"{zk_store.RELEASED_ROOT}/{POOL}") == []

def test_a_retry_after_the_tombstone_window_reports_not_confirmed(replica):
    lease = replica.client.acquire(POOL, "alice", "req-1", 60000, 1000)
    assert replica.client.release(lease.lock_id, "alice")

    # Simulate the 10-minute window passing: the reaper removes the tombstone.
    assert zk_store.reap_tombstones(replica.zk, max_age_ms=0) == 1

    assert not replica.client.release(lease.lock_id, "alice")  # Not confirmed any more.


def test_release_of_an_expired_but_unreaped_lock_returns_true(hosts):
    zk = zk_store.connect(hosts)  # No reaper runs here, so the expired lock stays in place.
    try:
        path = zk_store.join_queue(zk, POOL, "alice", "req-1", 50, 1000)
        zk_store.start_lease(zk, path)
        time.sleep(0.2)

        assert zk_store.validate(zk, path, "alice", zk_store.fencing_token(zk, path)) == (False, "LEASE_NOT_HELD")
        # Documented in docs/contracts.md: true here is not proof the lease was still valid.
        assert zk_store.release(zk, path, "alice")
    finally:
        zk.stop()