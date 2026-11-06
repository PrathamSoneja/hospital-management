"""Integration tests for the Lock Coordinator against a real ZooKeeper."""
import os
import threading
import time
import uuid
from concurrent import futures

import grpc
import pytest
from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError

from services.bed_allocation.clients.grpc_clients import GrpcLockCoordinatorClient
from services.lock_coordinator import zk_store
from services.lock_coordinator.grpc_server import LockCoordinatorServicer, reaper_loop
from shared.generated import lock_pb2, lock_pb2_grpc
from shared.observability.errors import AppException

ZK_HOSTS = os.environ.get("ZK_TEST_HOSTS", "")

pytestmark = pytest.mark.skipif(not ZK_HOSTS, reason="Set ZK_TEST_HOSTS to a local ZooKeeper")

POOL = "TEST-POOL"


class Coordinator:
    """One running replica, plus the real Bed Allocation client pointed at it."""

    def __init__(self, hosts):
        self.zk = zk_store.connect(hosts)
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=64))
        lock_pb2_grpc.add_LockCoordinatorServicer_to_server(LockCoordinatorServicer(self.zk), self.server)
        port = self.server.add_insecure_port("127.0.0.1:0")
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
    assert not replica.client.release(lease.lock_id, "alice")


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