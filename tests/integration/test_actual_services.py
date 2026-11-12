"""
Allocation and Triage against the teammates' merged services, locally (no cloud):

* Lock Coordinator (Nisarg): its gRPC server on local ZooKeeper (ZK_TEST_HOSTS).
* Bed Registry (Jay): its gRPC/REST server on its own disposable PostgreSQL database.
* Patient Registry (Ruchir): its app over PostgreSQL; only document storage is in-memory
  (tests/support/patient_registry_local.py).
* Staff Scheduler (Ruchir): BLOCKED by the documented Firestore transaction defect
  (tests/test_service_boundaries.py), so the stand-in serves Staff unless SANDBOX_ACTUAL_STAFF=1,
  which verifies the proposed fix once applied.

Allocation and Triage run through their production entrypoints. Fault-injection tests need the
stand-ins and stay in test_sandbox_workflows.py. Allocators: 0 normal; 1 demo mode with renewal
(10 s lease); 2 the 5 s expiry-demo profile without renewal.
"""
import http.client
import os
import sys
import threading
import time
import urllib.error
import uuid
from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest

from shared.generated import bed_pb2, bed_pb2_grpc, common_pb2, lock_pb2, lock_pb2_grpc, triage_pb2, triage_pb2_grpc
from tests.integration.test_sandbox_workflows import _rows
from tests.sandbox.audit import audit
from tests.sandbox.fixtures import SANDBOX_ENV, Api, seed_pool
from tests.sandbox.runtime import (DEMO_ENV, EXPIRY_DEMO_ENV, REPO_ROOT, ManagedProcess, Sandbox, free_port,
                                   http_json, isolated_env)


@pytest.fixture(scope="module")
def real():
    missing = [name for name in SANDBOX_ENV + ("ZK_TEST_HOSTS",) if not os.environ.get(name, "").strip()]
    if missing:
        pytest.skip(f"actual-service integration needs {', '.join(missing)}")
    actual = ("lock", "bed", "patient") + (("staff",) if os.environ.get("SANDBOX_ACTUAL_STAFF") == "1" else ())
    sb = Sandbox(
        postgres_url=os.environ["TEST_POSTGRES_URL"], database="hospital_actual_it_test",
        firestore_host=os.environ["FIRESTORE_EMULATOR_HOST"], log_dir=REPO_ROOT / ".sandbox" / "pytest-actual",
        allocators=3, recovery_claim_seconds=8, allocator_overrides={1: DEMO_ENV, 2: EXPIRY_DEMO_ENV},
        actual=actual, zk_hosts=os.environ["ZK_TEST_HOSTS"],
    )
    sb.start()
    try:
        yield sb
    finally:
        sb.stop()


@pytest.fixture
def api(real):
    return Api(real)


@pytest.fixture
def pool(request):
    """Unique per test, without starting the stand-in session sandbox."""
    name = request.node.name.replace("test_", "")[:24].upper().replace("_", "-")
    return f"P-{name}-{uuid.uuid4().hex[:6]}"


def _invariants(sb, pool, beds):
    """Durable invariants; bed ownership as the real Bed Registry reports it."""
    report = audit(sb.database_url, sb.dependency_state(), pools=[pool])
    assert report["violations"] == [], report
    return report


def _lock_entries(sb, pool):
    status, body, _ = http_json("GET", f"{sb.rest_url('lock')}/api/v1/locks/{pool}")
    return body["entries"] if status == 200 else []


def test_admission_triage_allocation_waitlist_promotion_and_cancel(real, api, pool):
    ids = seed_pool(real, pool, beds=1, admissions=[f"{pool}-adm-{n}" for n in (1, 2, 3)],
                    levels={f"{pool}-adm-1": 3, f"{pool}-adm-2": 2, f"{pool}-adm-3": 3})
    a1, a2, a3 = (ids[f"{pool}-adm-{n}"] for n in (1, 2, 3))
    assert a1 != f"{pool}-adm-1"  # IDs come from the real Patient Registry

    status, body, _ = api.create("adm-does-not-exist", pool, f"{pool}-missing")
    assert status == 404 and body["error"]["code"] == "NOT_FOUND"  # Patient 404 maps to not found, not an outage

    status, first, _ = api.create(a1, pool, f"{pool}-k1", allocator=0)
    assert (status, first["status"], first["bed_id"]) == (201, "ALLOCATED", f"{pool}-bed-01")
    second = api.create(a2, pool, f"{pool}-k2", allocator=1)[1]
    third = api.create(a3, pool, f"{pool}-k3", allocator=0)[1]
    assert second["status"] == third["status"] == "WAITLISTED"  # pool exhausted
    assert [e["admission_id"] for e in api.waitlist(pool)] == [a2, a3]  # urgency 2 before 3

    assert api.triage(a3, 1, pool)[0] == 200  # urgency change re-orders the queue
    assert [e["admission_id"] for e in api.waitlist(pool)] == [a3, a2]

    status, released, _ = api.release(first["allocation_id"], f"{pool}-rel", allocator=1)
    assert status == 200 and released["promoted_allocation_id"] == third["allocation_id"]
    assert api.wait_status(third["allocation_id"], {"ALLOCATED"})["bed_id"] == f"{pool}-bed-01"

    status, cancelled, _ = api.cancel(second["allocation_id"], f"{pool}-cancel", allocator=0)
    assert status == 200 and cancelled["status"] == "CANCELLED" and api.waitlist(pool) == []
    report = _invariants(real, pool, beds=1)
    assert report["statuses"] == {"RELEASED": 1, "ALLOCATED": 1, "CANCELLED": 1}
    assert _lock_entries(real, pool) == []  # every lease was released


def test_same_key_retries_replay_and_conflicts_are_preserved(real, api, pool):
    ids = seed_pool(real, pool, beds=2, admissions=[f"{pool}-adm-{n}" for n in (1, 2, 3)])
    a1, a2, a3 = (ids[f"{pool}-adm-{n}"] for n in (1, 2, 3))

    first, replay = api.create(a1, pool, f"{pool}-same", allocator=0), api.create(a1, pool, f"{pool}-same", allocator=1)
    assert first[0] == replay[0] == 201 and first[1]["allocation_id"] == replay[1]["allocation_id"]
    conflict = api.create(a1, f"{pool}-other", f"{pool}-same", allocator=1)
    assert conflict[0] == 409 and conflict[1]["error"]["code"] == "IDEMPOTENCY_MISMATCH"

    with ThreadPoolExecutor(12) as executor:  # one key, concurrent across both allocators
        results = list(executor.map(lambda i: api.create(a2, pool, f"{pool}-race", allocator=i % 2), range(12)))
    unexpected = [(r[0], r[1]) for r in results if r[0] not in (201, 202)]
    assert not unexpected, f"same key and payload must replay, got: {unexpected}"
    assert len({r[1]["allocation_id"] for r in results}) == 1

    with ThreadPoolExecutor(8) as executor:  # different keys, same admission
        results = list(executor.map(lambda i: api.create(a3, pool, f"{pool}-k{i}", allocator=i % 2), range(8)))
    assert sum(r[0] in (201, 202) for r in results) == 1
    assert all(r[1]["error"]["code"] == "ACTIVE_ALLOCATION_EXISTS" for r in results if r[0] not in (201, 202))
    for row in _rows(real, "SELECT allocation_id FROM allocations WHERE pool_id = :p", p=pool):
        api.wait_status(row["allocation_id"], {"ALLOCATED", "WAITLISTED"})
    report = _invariants(real, pool, beds=2)
    assert report["allocations"] == 3 and report["distinct_allocated_beds"] == 2


def test_fifty_admissions_contend_for_ten_beds(real, api, pool):
    fixture_ids = [f"{pool}-adm-{n}" for n in range(1, 51)]
    ids = seed_pool(real, pool, beds=10, admissions=fixture_ids, staff=60,
                    levels={a: 1 + n % 5 for n, a in enumerate(fixture_ids)})
    barrier = threading.Barrier(len(fixture_ids))

    def submit(index):
        barrier.wait()
        return api.create(ids[fixture_ids[index]], pool, f"{pool}-k{index}", allocator=index % 2)

    with ThreadPoolExecutor(len(fixture_ids)) as executor:
        results = list(executor.map(submit, range(len(fixture_ids))))
    assert all(r[0] in (201, 202) for r in results), [r for r in results if r[0] not in (201, 202)][:3]
    for row in _rows(real, "SELECT allocation_id FROM allocations WHERE pool_id = :p", p=pool):
        api.wait_status(row["allocation_id"], {"ALLOCATED", "WAITLISTED"}, timeout=180)
    report = _invariants(real, pool, beds=10)  # Bed Registry ownership equals the ALLOCATED rows
    assert report["statuses"] == {"ALLOCATED": 10, "WAITLISTED": 40}
    assert report["distinct_allocated_beds"] == 10
    assert len(api.waitlist(pool)) == 40


def test_long_operation_keeps_its_lease_through_renewal(real, api, pool):
    admission = seed_pool(real, pool, beds=1, admissions=[f"{pool}-adm-1"])[f"{pool}-adm-1"]
    result = {}

    def long_request():
        started = time.monotonic()
        result["response"] = api.create(admission, pool, f"{pool}-k", allocator=1, query="?pause_ms=13000")
        result["elapsed"] = time.monotonic() - started

    worker = threading.Thread(target=long_request)
    worker.start()
    time.sleep(11.5)  # past the 10 s lease: only renewal keeps it
    entries = _lock_entries(real, pool)
    worker.join(30)
    # Allocator-1 still holds the pool. The pause outlasts the 8 s recovery claim, so a peer's recovery
    # worker may be queued behind it; it waits for the lease instead of mutating.
    assert (entries[0]["owner_id"], entries[0]["state"]) == ("sandbox-allocator-1", "HOLDING"), entries
    assert all(e["state"] == "WAITING" for e in entries[1:]), entries
    status, body, _ = result["response"]
    assert (status, body["status"]) == (201, "ALLOCATED") and result["elapsed"] >= 13
    _invariants(real, pool, beds=1)


def test_coordinator_crash_during_a_renewed_lease_settles_safely(real, api, pool):
    """Renewal is unavailable while the Coordinator is down; its ZooKeeper session (and the ephemeral
    lease) may outlive the crash or not. Either way the holder stops or finishes under fencing and
    recovery settles the allocation exactly once."""
    admission = seed_pool(real, pool, beds=1, admissions=[f"{pool}-adm-1"])[f"{pool}-adm-1"]
    result = {}
    worker = threading.Thread(target=lambda: result.setdefault(
        "response", api.create(admission, pool, f"{pool}-k", allocator=1, query="?pause_ms=6000")))
    worker.start()
    time.sleep(1.0)
    real.processes["lock"].stop(graceful=False)  # crash
    time.sleep(1.0)
    real.processes["lock"] = real._actual("lock").start()
    worker.join(40)
    assert not worker.is_alive()
    status, body, _ = result["response"]
    assert status in (201, 202) and body["status"] in ("ALLOCATED", "RECOVERING")
    settled = api.wait_status(body["allocation_id"], {"ALLOCATED"}, timeout=90)
    assert settled["bed_id"] == f"{pool}-bed-01"
    _invariants(real, pool, beds=1)


def test_expired_non_renewing_holder_sends_no_mutation_and_recovers(real, api, pool):
    ids = seed_pool(real, pool, beds=2, admissions=[f"{pool}-adm-1", f"{pool}-adm-2"])
    paused, other = ids[f"{pool}-adm-1"], ids[f"{pool}-adm-2"]
    result = {}

    def hold_past_expiry():
        started = time.monotonic()
        result["response"] = api.create(paused, pool, f"{pool}-paused", allocator=2, query="?pause_ms=7000")
        result["elapsed"] = time.monotonic() - started

    holder = threading.Thread(target=hold_past_expiry)
    holder.start()
    time.sleep(1.0)
    started = time.monotonic()
    status, body, _ = api.create(other, pool, f"{pool}-other", allocator=0)
    waited = time.monotonic() - started
    holder.join(30)

    assert status == 201 and body["status"] == "ALLOCATED" and waited < 6.5  # granted after the 5 s expiry
    status, paused_body, _ = result["response"]
    assert result["elapsed"] >= 7 and paused_body["status"] == "RECOVERING"
    settled = api.wait_status(paused_body["allocation_id"], {"ALLOCATED"}, timeout=90)
    assert settled["bed_id"] != body["bed_id"]
    _invariants(real, pool, beds=2)


def test_unsafe_lock_false_never_produces_duplicate_ownership(real, api, pool):
    admission = seed_pool(real, pool, beds=1, admissions=[f"{pool}-adm-1"])[f"{pool}-adm-1"]
    status, body, _ = api.create(admission, pool, f"{pool}-k", allocator=2, query="?lock=false")
    assert status in (201, 202)
    settled = api.wait_status(body["allocation_id"], {"ALLOCATED"}, timeout=90)
    assert settled["bed_id"] == f"{pool}-bed-01"
    _invariants(real, pool, beds=1)


def test_allocator_crash_mid_workflow_is_recovered_with_real_dependencies(real, api, pool):
    admission = seed_pool(real, pool, beds=1, admissions=[f"{pool}-adm-1"])[f"{pool}-adm-1"]
    result = {}

    def in_flight_request():
        try:
            result["response"] = api.create(admission, pool, f"{pool}-k", allocator=1, query="?pause_ms=6000")
        except (ConnectionError, urllib.error.URLError, http.client.HTTPException) as error:
            result["connection_lost"] = error  # expected: the serving process is killed mid-request
        except BaseException as error:
            result["unexpected"] = error

    worker = threading.Thread(target=in_flight_request)
    worker.start()
    time.sleep(2)  # allocator-1 holds the claim and the pool lease
    real.restart_allocator(1, graceful=False)
    worker.join(30)
    assert not worker.is_alive()
    if "unexpected" in result:
        raise result["unexpected"]
    row = _rows(real, "SELECT allocation_id FROM allocations WHERE admission_id = :a", a=admission)[0]
    # The dead holder's lease lapses in ZooKeeper and its claim (8 s) expires; a recovery worker resumes.
    settled = api.wait_status(row["allocation_id"], {"ALLOCATED"}, timeout=90)
    assert settled["bed_id"] == f"{pool}-bed-01"
    _invariants(real, pool, beds=1)


def test_stale_tokens_are_refused_by_real_bed_registry_and_triage(real, api, pool):
    admission = seed_pool(real, pool, beds=2, admissions=[f"{pool}-adm-1"])[f"{pool}-adm-1"]
    lock = lock_pb2_grpc.LockCoordinatorStub(grpc.insecure_channel(real.grpc_addr("lock")))
    beds = bed_pb2_grpc.BedServiceStub(grpc.insecure_channel(real.grpc_addr("bed")))
    triage = triage_pb2_grpc.TriageServiceStub(grpc.insecure_channel(f"127.0.0.1:{real.ports['triage_grpc']}"))

    def context(lease, op):
        return common_pb2.MutationContext(operation_id=op, request_id=op, pool_id=pool, lock_id=lease.lock_id,
                                          owner_id=lease.owner_id, fencing_token=lease.fencing_token)

    old = lock.Acquire(lock_pb2.AcquireLockRequest(pool_id=pool, owner_id="it-old", acquire_id=f"{pool}-a1",
                                                   lease_ms=1000, wait_timeout_ms=1000), timeout=5)
    triage.Enqueue(triage_pb2.EnqueueWaitlistRequest(context=context(old, f"{pool}-enq"), allocation_id=f"{pool}-alloc",
                                                     admission_id=admission), timeout=5)
    time.sleep(1.5)  # the old lease expires and is reaped
    new = lock.Acquire(lock_pb2.AcquireLockRequest(pool_id=pool, owner_id="it-new", acquire_id=f"{pool}-a2",
                                                   lease_ms=5000, wait_timeout_ms=3000), timeout=5)
    assert new.fencing_token > old.fencing_token
    assert lock.Validate(lock_pb2.ValidateLockRequest(lock_id=old.lock_id, owner_id="it-old", pool_id=pool,
                                                      fencing_token=old.fencing_token), timeout=5).valid is False

    with pytest.raises(grpc.RpcError) as stale_triage:
        triage.RemoveEntry(triage_pb2.RemoveWaitlistEntryRequest(context=context(old, f"{pool}-rm"),
                                                                 allocation_id=f"{pool}-alloc", reason="stale"), timeout=5)
    assert stale_triage.value.code() in (grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.INVALID_ARGUMENT)

    # Bed Registry fences by token order: once the newer holder has written, the older token is refused.
    fresh = beds.AllocateBed(bed_pb2.AllocateBedRequest(context=context(new, f"{pool}-new"), bed_id=f"{pool}-bed-01",
                                                        allocation_id=f"{pool}-x1"), timeout=5)
    assert fresh.success
    with pytest.raises(grpc.RpcError) as stale_bed:
        beds.AllocateBed(bed_pb2.AllocateBedRequest(context=context(old, f"{pool}-old"), bed_id=f"{pool}-bed-02",
                                                    allocation_id=f"{pool}-x2"), timeout=5)
    assert stale_bed.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    lock.Release(lock_pb2.ReleaseLockRequest(lock_id=new.lock_id, owner_id=new.owner_id), timeout=5)


# ---------- Lock Coordinator replicas (confirmed defects, owner: Nisarg) ----------

@pytest.fixture(scope="module")
def coordinator_replicas(real):
    """A second real Coordinator process on the same ZooKeeper, beside the sandbox one."""
    grpc_port, rest_port = free_port(), free_port()
    second = ManagedProcess(
        "lock-replica-2", [sys.executable, "-m", "services.lock_coordinator.grpc_server"],
        isolated_env(real.log_dir, ZK_HOSTS=real.zk_hosts, LOCK_GRPC_PORT=grpc_port, LOCK_REST_PORT=rest_port),
        real.log_dir / "lock-replica-2.log", f"http://127.0.0.1:{rest_port}/readyz").start()
    try:
        yield [lock_pb2_grpc.LockCoordinatorStub(grpc.insecure_channel(addr))
               for addr in (real.grpc_addr("lock"), f"127.0.0.1:{grpc_port}")]
    finally:
        second.stop()


def _acquire(stub, pool, owner, acquire_id, wait_ms):
    try:
        lease = stub.Acquire(lock_pb2.AcquireLockRequest(pool_id=pool, owner_id=owner, acquire_id=acquire_id,
                                                         lease_ms=3000, wait_timeout_ms=wait_ms), timeout=wait_ms / 1000 + 3)
        return lease.lock_id, lease.fencing_token
    except grpc.RpcError as error:
        return error.code().name


@pytest.mark.xfail(strict=True, raises=AssertionError, reason=(
    "Lock Coordinator defect (Nisarg): Acquire's find_request then join_queue is not atomic, so simultaneous "
    "calls with the same owner/acquire_id each create a queue entry; one is granted and the duplicates wait "
    "behind it until DEADLINE_EXCEEDED. Safety holds (one grant); the retry cannot learn it holds the lease."))
def test_simultaneous_same_acquire_id_across_replicas_returns_one_lease(coordinator_replicas, pool):
    barrier = threading.Barrier(6)

    def call(index):
        barrier.wait()
        return _acquire(coordinator_replicas[index % 2], pool, "it-owner", f"{pool}-same", 1500)

    with ThreadPoolExecutor(6) as executor:
        results = list(executor.map(call, range(6)))
    granted = {r for r in results if isinstance(r, tuple)}
    for lock_id, _ in granted:
        coordinator_replicas[0].Release(lock_pb2.ReleaseLockRequest(lock_id=lock_id, owner_id="it-owner"), timeout=5)
    assert len(granted) == 1  # never two leases for one acquire (holds today)
    assert all(isinstance(r, tuple) for r in results), results  # every same-ID call returns that lease


@pytest.mark.xfail(strict=True, raises=AssertionError, reason=(
    "Lock Coordinator defect (Nisarg): when a waiting Acquire and its same-acquire_id retry (another replica) "
    "reach the head together, both call zk_store.start_lease; the loser's versioned set raises an uncaught "
    "BadVersionError, returned as UNKNOWN. Proposed fix: docs/proposed-fixes/lock-coordinator-start-lease.patch."))
def test_waiting_acquire_and_its_retry_both_receive_the_lease(coordinator_replicas, pool):
    first, second = coordinator_replicas
    holder = first.Acquire(lock_pb2.AcquireLockRequest(pool_id=pool, owner_id="it-holder", acquire_id="h",
                                                       lease_ms=5000, wait_timeout_ms=1000), timeout=5)
    with ThreadPoolExecutor(2) as executor:
        original = executor.submit(_acquire, first, pool, "it-owner", f"{pool}-same", 4000)
        time.sleep(0.3)  # the retry finds the original's queue entry and waits on it too
        retry = executor.submit(_acquire, second, pool, "it-owner", f"{pool}-same", 4000)
        time.sleep(0.5)
        first.Release(lock_pb2.ReleaseLockRequest(lock_id=holder.lock_id, owner_id="it-holder"), timeout=5)
        results = [original.result(), retry.result()]
    for result in results:
        if isinstance(result, tuple):
            first.Release(lock_pb2.ReleaseLockRequest(lock_id=result[0], owner_id="it-owner"), timeout=5)
            break
    assert all(isinstance(r, tuple) for r in results) and results[0] == results[1], results
