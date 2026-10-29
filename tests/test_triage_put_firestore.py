"""REST PUT, idempotency, immutability, and queue coordination on Firestore emulator."""
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.triage.lock_coordinator import LockLease
from services.triage.repository.firestore import FirestoreTriageRepository
from services.triage.rest import router, set_service
from services.triage.service import TriageDomainService
from services.triage.validation import MockFencingValidator
from shared.generated.common_pb2 import MutationContext
from shared.observability.errors import register_error_handlers
from shared.observability.middleware import setup_observability_middleware


pytestmark = pytest.mark.skipif(
    not os.environ.get("FIRESTORE_EMULATOR_HOST"),
    reason="requires the local Firestore emulator; cloud Firestore is not used",
)


class SerializedTestCoordinator:
    """Test-only single-pool lease adapter; not a Coordinator correctness proof."""
    def __init__(self):
        self.locks = defaultdict(threading.Lock)
        self.sequence = defaultdict(int)
        self.held = {}
        self.calls = 0
        self.request_ids = []

    def acquire(self, pool_id, owner_id, acquire_id, lease_ms, wait_timeout_ms, request_id=""):
        self.request_ids.append(request_id)
        lock = self.locks[pool_id]
        if not lock.acquire(timeout=wait_timeout_ms / 1000):
            raise TimeoutError("test coordinator lock timeout")
        self.sequence[pool_id] += 1
        self.calls += 1
        token = self.sequence[pool_id]
        lock_id = f"test-lock-{pool_id}-{token}"
        self.held[lock_id] = lock
        return LockLease(lock_id, pool_id, owner_id, token, 0)

    def release(self, lock_id, owner_id, request_id=""):
        lock = self.held.pop(lock_id)
        lock.release()
        return True

    def check_ready(self, timeout_s=0.5):
        return True

    def close(self):
        pass


def make_service(project_id):
    repo = FirestoreTriageRepository(project_id=project_id)
    coordinator = SerializedTestCoordinator()
    service = TriageDomainService(
        repo, MockFencingValidator(accept_all=True),
        lock_client=coordinator, owner_id="test-triage",
    )
    app = FastAPI()
    register_error_handlers(app, "triage-test")
    setup_observability_middleware(app, "triage-test", ready_check=repo.check_ready)
    app.include_router(router)
    set_service(service)
    return repo, service, coordinator, TestClient(app)


def put(client, admission, key, level, pool="pool-a", request_id="req-1"):
    return client.put(
        f"/api/v1/triage/{admission}",
        headers={"Idempotency-Key": key, "X-Request-ID": request_id},
        json={"triage_level": level, "pool_id": pool},
    )


def test_put_replays_original_result_before_lock_and_mismatch_is_409():
    project = f"triage-put-{uuid.uuid4().hex}"
    repo, service, coordinator, client = make_service(project)
    initial = put(client, "admission-1", "opaque-key", 4, request_id="request-original")
    assert initial.status_code == 200
    assert initial.headers["x-request-id"] == "request-original"
    updated = put(client, "admission-1", "later-key", 2, request_id="request-update")
    assert updated.status_code == 200
    http_mismatch = put(client, "admission-1", "opaque-key", 3, request_id="request-http-mismatch")
    assert http_mismatch.status_code == 409
    assert http_mismatch.json()["error"]["code"] == "IDEMPOTENCY_MISMATCH"
    missing_key = client.put("/api/v1/triage/no-key", json={"triage_level": 1, "pool_id": "pool-a"})
    assert missing_key.status_code == 400
    assert missing_key.json()["error"]["code"] == "INVALID_ARGUMENT"

    restarted_repo = FirestoreTriageRepository(project_id=project)
    replay_service = TriageDomainService(restarted_repo, MockFencingValidator(True), lock_client=None)
    before = coordinator.calls
    replay = replay_service.put_triage("admission-1", 4, "pool-a", "opaque-key", "request-replay")
    assert replay.model_dump() == initial.json() and replay.triage_level == 4
    assert coordinator.calls == before
    with pytest.raises(Exception) as mismatch:
        service.put_triage("admission-1", 3, "pool-a", "opaque-key", "request-mismatch")
    assert getattr(mismatch.value, "code", None) == "IDEMPOTENCY_MISMATCH"

    claim_id = __import__("services.triage.idempotency", fromlist=["build_put_claim"]).build_put_claim(
        "admission-1", "opaque-key", 4, "pool-a",
    )["claim_id"]
    claim = repo.triage_idempotency_col.document(claim_id).get().to_dict()
    assert claim["state"] == "COMPLETED"
    assert claim["response"] == initial.json()
    assert claim["recorded_at"] is not None and claim["completed_at"] is not None
    assert claim["key_hash"] != "opaque-key"
    assert "request-original" in coordinator.request_ids


def test_concurrent_same_key_initial_put_has_one_logical_result():
    project = f"triage-concurrent-{uuid.uuid4().hex}"
    repo, service, coordinator, client = make_service(project)
    barrier = threading.Barrier(2)

    def issue():
        barrier.wait(timeout=5)
        return put(client, "admission-race", "same-key", 3, request_id=f"req-{uuid.uuid4().hex[:6]}")

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(lambda _: issue(), range(2)))
    assert [response.status_code for response in responses] == [200, 200]
    assert responses[0].json() == responses[1].json()
    assert responses[0].headers["x-request-id"] != responses[1].headers["x-request-id"]
    assert repo.triage_idempotency_col.stream() and coordinator.calls == 2


def test_concurrent_initial_different_pools_rejects_loser_without_mutation():
    project = f"triage-pool-race-{uuid.uuid4().hex}"
    repo, service, coordinator, client = make_service(project)
    barrier = threading.Barrier(2)

    def issue(pool_id):
        barrier.wait(timeout=5)
        return put(client, "admission-pool-race", f"key-{pool_id}", 2, pool=pool_id)

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(issue, ["pool-a", "pool-b"]))
    assert sorted(response.status_code for response in responses) == [200, 409]
    loser = next(response for response in responses if response.status_code == 409)
    assert loser.json()["error"]["code"] == "POOL_CHANGE_NOT_SUPPORTED"
    stored = repo.get_triage("admission-pool-race")
    assert stored and stored.pool_id in {"pool-a", "pool-b"}
    assert repo.pool_fence_col.document("pool-a").get().exists == (stored.pool_id == "pool-a")
    assert repo.pool_fence_col.document("pool-b").get().exists == (stored.pool_id == "pool-b")


def test_urgency_update_preserves_queue_identity_fifo_and_never_recreates_removed_entry():
    project = f"triage-urgency-{uuid.uuid4().hex}"
    repo, service, coordinator, client = make_service(project)
    assert put(client, "admission-a", "create-a", 4).status_code == 200
    assert put(client, "admission-b", "create-b", 3).status_code == 200

    def enqueue(admission, allocation, operation):
        lease = coordinator.acquire("pool-a", "test-triage", operation, 10000, 5000)
        context = MutationContext(
            operation_id=operation, pool_id="pool-a", lock_id=lease.lock_id,
            owner_id=lease.owner_id, fencing_token=lease.fencing_token,
        )
        try:
            return service.enqueue(context, allocation, admission)[0]
        finally:
            coordinator.release(lease.lock_id, lease.owner_id)

    entry_a = enqueue("admission-a", "allocation-a", "enqueue-a")
    enqueue("admission-b", "allocation-b", "enqueue-b")
    response = put(client, "admission-a", "urgency-a", 1, request_id="urgency-request")
    assert response.status_code == 200
    updated_entry = repo.get_waitlist_entry("allocation-a")
    assert updated_entry.entry_id == entry_a.entry_id
    assert updated_entry.queued_at == entry_a.queued_at
    assert updated_entry.triage_level == 1
    assert repo.peek_next_waitlist("pool-a").allocation_id == "allocation-a"

    # A different pool is immutable even when there is no active queue entry.
    lease = coordinator.acquire("pool-a", "test-triage", "remove-a", 10000, 5000)
    context = MutationContext(operation_id="remove-a", pool_id="pool-a", lock_id=lease.lock_id,
                              owner_id=lease.owner_id, fencing_token=lease.fencing_token)
    try:
        assert service.remove_entry(context, "allocation-a", "promotion")
    finally:
        coordinator.release(lease.lock_id, lease.owner_id)
    old_fence = repo.pool_fence_col.document("pool-a").get().to_dict()
    rejected = put(client, "admission-a", "move-a", 2, pool="pool-b")
    assert rejected.status_code == 409
    assert rejected.json()["error"]["code"] == "POOL_CHANGE_NOT_SUPPORTED"
    assert repo.get_waitlist_entry("allocation-a") is None
    assert repo.get_triage("admission-a").pool_id == "pool-a"
    assert repo.pool_fence_col.document("pool-a").get().to_dict() == old_fence
    assert not repo.pool_fence_col.document("pool-b").get().exists


def test_urgency_update_races_remove_under_same_pool_lease_without_recreation():
    project = f"triage-urgency-remove-race-{uuid.uuid4().hex}"
    repo, service, coordinator, client = make_service(project)
    assert put(client, "admission-race", "initial", 4).status_code == 200
    lease = coordinator.acquire("pool-a", "test-triage", "enqueue-race", 10000, 5000)
    enqueue_context = MutationContext(
        operation_id="enqueue-race", pool_id="pool-a", lock_id=lease.lock_id,
        owner_id=lease.owner_id, fencing_token=lease.fencing_token,
    )
    try:
        entry, _ = service.enqueue(enqueue_context, "allocation-race", "admission-race")
    finally:
        coordinator.release(lease.lock_id, lease.owner_id)

    barrier = threading.Barrier(2)

    def update():
        barrier.wait(timeout=5)
        return put(client, "admission-race", "urgency-race", 1, request_id="urgency-race").status_code

    def remove():
        barrier.wait(timeout=5)
        remove_lease = coordinator.acquire("pool-a", "test-triage", "remove-race", 10000, 5000)
        context = MutationContext(
            operation_id="remove-race", pool_id="pool-a", lock_id=remove_lease.lock_id,
            owner_id=remove_lease.owner_id, fencing_token=remove_lease.fencing_token,
        )
        try:
            return service.remove_entry(context, "allocation-race", "promotion")
        finally:
            coordinator.release(remove_lease.lock_id, remove_lease.owner_id)

    with ThreadPoolExecutor(max_workers=2) as pool:
        updated, removed = list(pool.map(lambda fn: fn(), [update, remove]))
    assert updated == 200 and removed is True
    assert repo.get_triage("admission-race").triage_level == 1
    assert repo.get_waitlist_entry("allocation-race") is None
    ledger = repo.operation_col.document(__import__("hashlib").sha256(b"remove-race").hexdigest()).get().to_dict()
    assert ledger["entry_id"] == entry.entry_id and ledger["result_success"] is True


def test_get_errors_pagination_and_request_id():
    project = f"triage-get-{uuid.uuid4().hex}"
    repo, service, coordinator, client = make_service(project)
    assert put(client, "admission-get-a", "key-a", 2).status_code == 200
    assert put(client, "admission-get-b", "key-b", 1).status_code == 200

    def enqueue(admission, allocation, operation):
        lease = coordinator.acquire("pool-a", "test-triage", operation, 10000, 5000)
        context = MutationContext(operation_id=operation, pool_id="pool-a", lock_id=lease.lock_id,
                                  owner_id=lease.owner_id, fencing_token=lease.fencing_token)
        try:
            return service.enqueue(context, allocation, admission)[0]
        finally:
            coordinator.release(lease.lock_id, lease.owner_id)

    entry_a = enqueue("admission-get-a", "allocation-get-a", "enqueue-get-a")
    entry_b = enqueue("admission-get-b", "allocation-get-b", "enqueue-get-b")
    missing = client.get("/api/v1/triage/absent", headers={"X-Request-ID": "get-missing"})
    assert missing.status_code == 404 and missing.json()["error"]["code"] == "NOT_FOUND"
    assert missing.headers["x-request-id"] == "get-missing"
    page1 = client.get("/api/v1/waitlists/pool-a?limit=1", headers={"X-Request-ID": "get-page"})
    assert page1.status_code == 200 and page1.headers["x-request-id"] == "get-page"
    assert page1.json()["items"][0]["entry_id"] == entry_b.entry_id
    assert page1.json()["next_cursor"] == entry_b.entry_id
    page2 = client.get(f"/api/v1/waitlists/pool-a?limit=1&cursor={entry_b.entry_id}")
    assert page2.status_code == 200 and page2.json()["items"][0]["entry_id"] == entry_a.entry_id
    assert page2.json()["next_cursor"] is None
    invalid_cursor = client.get("/api/v1/waitlists/pool-a?cursor=stale-entry")
    assert invalid_cursor.status_code == 400
