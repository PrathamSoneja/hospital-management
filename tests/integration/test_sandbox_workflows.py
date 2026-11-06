"""
Cross-process Allocation/Triage behaviour through real REST and gRPC adapters.

Dependencies (Lock/Bed/Staff/Patient) are shared test stand-ins (tests/support/
sandbox_dependencies.py); results show Allocation/Triage logic across processes, not
ZooKeeper or teammate-service correctness. The documented race between external lease
validation and the Firestore commit is not addressed by these tests.
"""
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import grpc
from sqlalchemy import create_engine, text

from shared.generated import bed_pb2, bed_pb2_grpc, common_pb2, lock_pb2, lock_pb2_grpc, triage_pb2, triage_pb2_grpc
from tests.sandbox.fixtures import seed_pool
from tests.sandbox.audit import audit


def _rows(sandbox, sql, **params):
    engine = create_engine(sandbox.database_url)
    try:
        with engine.connect() as connection:
            return connection.execute(text(sql), params).mappings().all()
    finally:
        engine.dispose()


def _assert_pool_invariants(sandbox, *pools):
    report = audit(sandbox.database_url, sandbox.dependency_state(), pools=pools)
    assert report["violations"] == [], report
    return report


def test_waitlist_urgency_update_promotion_and_cancel(sandbox, api, pool):
    a1, a2, a3 = (f"{pool}-adm-{n}" for n in (1, 2, 3))
    seed_pool(sandbox, pool, beds=1, admissions=[a1, a2, a3], levels={a1: 3, a2: 2, a3: 3})

    status, first, _ = api.create(a1, pool, f"{pool}-k1", allocator=0)
    assert (status, first["status"]) == (201, "ALLOCATED")
    second = api.create(a2, pool, f"{pool}-k2", allocator=1)[1]
    third = api.create(a3, pool, f"{pool}-k3", allocator=1)[1]
    assert second["status"] == third["status"] == "WAITLISTED"

    queue = api.waitlist(pool)
    assert [e["admission_id"] for e in queue] == [a2, a3]  # urgency 2 before 3
    before = {e["admission_id"]: e for e in queue}

    # Same-pool urgency change: re-ordered, but queue identity and queued_at are preserved.
    assert api.triage(a3, 1, pool)[0] == 200
    queue = api.waitlist(pool)
    assert [e["admission_id"] for e in queue] == [a3, a2]
    assert queue[0]["entry_id"] == before[a3]["entry_id"] and queue[0]["queued_at"] == before[a3]["queued_at"]

    status, released, _ = api.release(first["allocation_id"], f"{pool}-rel", allocator=1)
    assert status == 200 and released["promoted_allocation_id"] == third["allocation_id"]
    promoted = api.wait_status(third["allocation_id"], {"ALLOCATED"})
    assert promoted["bed_id"] == first["bed_id"]
    assert [e["admission_id"] for e in api.waitlist(pool)] == [a2]

    status, cancelled, _ = api.cancel(second["allocation_id"], f"{pool}-cancel", allocator=0)
    assert status == 200 and cancelled["status"] == "CANCELLED"
    assert api.waitlist(pool) == []
    report = _assert_pool_invariants(sandbox, pool)
    assert report["statuses"] == {"RELEASED": 1, "ALLOCATED": 1, "CANCELLED": 1}


def test_idempotent_replay_conflict_and_concurrent_retries(sandbox, api, pool):
    admissions = [f"{pool}-adm-{n}" for n in range(1, 4)]
    seed_pool(sandbox, pool, beds=2, admissions=admissions)
    a1, a2, a3 = admissions

    first = api.create(a1, pool, f"{pool}-same", allocator=0)
    replay = api.create(a1, pool, f"{pool}-same", allocator=1)  # other process, same key and payload
    assert first[0] == replay[0] == 201 and first[1]["allocation_id"] == replay[1]["allocation_id"]
    # Scope = method + operation + target admission + key; same scope with a different payload conflicts.
    conflict = api.create(a1, f"{pool}-other", f"{pool}-same", allocator=1)
    assert conflict[0] == 409 and conflict[1]["error"]["code"] == "IDEMPOTENCY_MISMATCH"

    with ThreadPoolExecutor(12) as executor:  # one key, concurrent across both allocators
        results = list(executor.map(lambda i: api.create(a2, pool, f"{pool}-race", allocator=i % 2), range(12)))
    assert {r[0] for r in results} <= {201, 202}
    assert len({r[1]["allocation_id"] for r in results}) == 1

    with ThreadPoolExecutor(8) as executor:  # different keys, same admission: one active allocation only
        results = list(executor.map(lambda i: api.create(a3, pool, f"{pool}-k{i}", allocator=i % 2), range(8)))
    winners = [r for r in results if r[0] in (201, 202)]
    assert len(winners) == 1
    assert all(r[0] == 409 and r[1]["error"]["code"] == "ACTIVE_ALLOCATION_EXISTS" for r in results if r not in winners)
    rows = _rows(sandbox, "SELECT admission_id, count(*) AS n FROM allocations WHERE pool_id = :p GROUP BY 1", p=pool)
    assert {r["admission_id"]: r["n"] for r in rows} == {a1: 1, a2: 1, a3: 1}
    _assert_pool_invariants(sandbox, pool)


def test_lost_bed_acknowledgement_recovers_with_stable_operation_id(sandbox, api, pool):
    admission = f"{pool}-adm-1"
    seed_pool(sandbox, pool, beds=1, admissions=[admission])
    calls_before = sandbox.dependency_state()["calls"].get("AllocateBed", 0)
    sandbox.fault("AllocateBed", "lost_ack")  # the bed is reserved, but the response is lost

    status, body, _ = api.create(admission, pool, f"{pool}-k")
    assert status in (201, 202) and body["status"] == "RECOVERING"
    settled = api.wait_status(body["allocation_id"], {"ALLOCATED"})
    state = sandbox.dependency_state()
    assert state["calls"]["AllocateBed"] - calls_before >= 2  # retried with the same operation ID
    assert f"{body['allocation_id']}:reserve-bed" in state["operation_ids"]
    owners = [b for b in state["beds"] if b["pool_id"] == pool and b["status"] == "OCCUPIED"]
    assert [b["current_allocation_id"] for b in owners] == [body["allocation_id"]] and owners[0]["bed_id"] == settled["bed_id"]
    _assert_pool_invariants(sandbox, pool)


def test_unavailable_dependencies_fail_safely_then_recover(sandbox, api, pool):
    a1, a2 = f"{pool}-adm-1", f"{pool}-adm-2"
    seed_pool(sandbox, pool, beds=2, admissions=[a1, a2])

    sandbox.fault("GetAdmission", "unavailable")
    status, body, _ = api.create(a1, pool, f"{pool}-k1")
    assert status == 503 and body["error"]["retryable"] is True
    assert _rows(sandbox, "SELECT 1 FROM allocations WHERE admission_id = :a", a=a1) == []  # nothing accepted

    sandbox.fault("AssignStaff", "unavailable", count=3)  # unknown outcome, not capacity shortage
    status, body, _ = api.create(a2, pool, f"{pool}-k2")
    assert body["status"] == "RECOVERING"
    assert api.wait_status(body["allocation_id"], {"ALLOCATED"})["staff_assignment_id"]
    _assert_pool_invariants(sandbox, pool)


def test_allocator_crash_mid_workflow_is_recovered_by_peer_and_restart(sandbox, api, pool):
    admission = f"{pool}-adm-1"
    seed_pool(sandbox, pool, beds=1, admissions=[admission])
    sandbox.fault("AssignStaff", "delay", delay_seconds=4)
    result = {}
    worker = threading.Thread(target=lambda: result.setdefault("r", api.create(admission, pool, f"{pool}-k", allocator=0)))
    worker.start()
    time.sleep(2)  # allocator-0 holds the claim and is inside AssignStaff
    sandbox.restart_allocator(0, graceful=False)  # hard kill (crash), then start a fresh process
    worker.join(30)
    row = _rows(sandbox, "SELECT allocation_id FROM allocations WHERE admission_id = :a", a=admission)[0]
    # The claim lapses (8 s), then a recovery worker (either process) resumes with the same operation IDs.
    settled = api.wait_status(row["allocation_id"], {"ALLOCATED"}, timeout=90)
    state = sandbox.dependency_state()
    assigned = [a for a in state["assignments"].values() if a["allocation_id"] == row["allocation_id"]]
    assert len(assigned) == 1 and assigned[0]["active"]  # staff assigned once despite replay
    assert settled["staff_assignment_id"]
    _assert_pool_invariants(sandbox, pool)


def test_stale_fencing_tokens_are_rejected_by_triage_and_bed_registry(sandbox, api, pool):
    admission = f"{pool}-adm-1"
    seed_pool(sandbox, pool, beds=1, admissions=[admission])
    lock = lock_pb2_grpc.LockCoordinatorStub(grpc.insecure_channel(f"127.0.0.1:{sandbox.ports['deps_grpc']}"))
    triage = triage_pb2_grpc.TriageServiceStub(grpc.insecure_channel(f"127.0.0.1:{sandbox.ports['triage_grpc']}"))
    beds = bed_pb2_grpc.BedServiceStub(grpc.insecure_channel(f"127.0.0.1:{sandbox.ports['deps_grpc']}"))

    def context(lease, op):
        return common_pb2.MutationContext(operation_id=op, request_id=op, pool_id=pool, lock_id=lease.lock_id,
                                          owner_id=lease.owner_id, fencing_token=lease.fencing_token)

    old = lock.Acquire(lock_pb2.AcquireLockRequest(pool_id=pool, owner_id="it-old", acquire_id=f"{pool}-a1",
                                                   lease_ms=300, wait_timeout_ms=1000), timeout=5)
    triage.Enqueue(triage_pb2.EnqueueWaitlistRequest(context=context(old, f"{pool}-enq"), allocation_id=f"{pool}-alloc",
                                                     admission_id=admission), timeout=5)
    time.sleep(0.5)  # the old lease expires
    new = lock.Acquire(lock_pb2.AcquireLockRequest(pool_id=pool, owner_id="it-new", acquire_id=f"{pool}-a2",
                                                   lease_ms=5000, wait_timeout_ms=1000), timeout=5)
    assert new.fencing_token > old.fencing_token

    try:
        triage.RemoveEntry(triage_pb2.RemoveWaitlistEntryRequest(context=context(old, f"{pool}-rm"),
                                                                 allocation_id=f"{pool}-alloc", reason="stale"), timeout=5)
        raise AssertionError("stale RemoveEntry was accepted")
    except grpc.RpcError as error:
        assert error.code() in (grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.INVALID_ARGUMENT)
    assert [e["allocation_id"] for e in api.waitlist(pool)] == [f"{pool}-alloc"]  # unchanged

    stale_bed = beds.AllocateBed(bed_pb2.AllocateBedRequest(context=context(old, f"{pool}-bed"),
                                                            bed_id=f"{pool}-bed-01", allocation_id="x"), timeout=5)
    assert (stale_bed.success, stale_bed.error_message) == (False, "STALE_FENCING_TOKEN")
    lock.Release(lock_pb2.ReleaseLockRequest(lock_id=new.lock_id, owner_id=new.owner_id), timeout=5)


def test_independent_pools_and_multi_allocator_contention(sandbox, api, pool):
    pools = {f"{pool}-A": 10, f"{pool}-B": 3}
    admissions = {p: [f"{p}-adm-{n}" for n in range(1, 51 if beds == 10 else 9)] for p, beds in pools.items()}
    for p, beds in pools.items():
        seed_pool(sandbox, p, beds=beds, admissions=admissions[p], staff=200,
                  levels={a: 1 + n % 5 for n, a in enumerate(admissions[p])})
    jobs = [(a, p) for p in pools for a in admissions[p]]
    barrier = threading.Barrier(len(jobs))

    def submit(index_job):
        index, (admission, p) = index_job
        barrier.wait()
        return api.create(admission, p, f"{admission}-k", allocator=index % 2)

    with ThreadPoolExecutor(len(jobs)) as executor:
        results = list(executor.map(submit, enumerate(jobs)))
    assert all(r[0] in (201, 202) for r in results), [r for r in results if r[0] not in (201, 202)][:3]
    for p, beds in pools.items():
        rows = _rows(sandbox, "SELECT allocation_id FROM allocations WHERE pool_id = :p", p=p)
        for row in rows:
            api.wait_status(row["allocation_id"], {"ALLOCATED", "WAITLISTED"}, timeout=120)
        report = _assert_pool_invariants(sandbox, p)
        assert report["statuses"] == {"ALLOCATED": beds, "WAITLISTED": len(admissions[p]) - beds}
        assert report["distinct_allocated_beds"] == beds
        allocated = _rows(sandbox, "SELECT bed_id FROM allocations WHERE pool_id = :p AND status = 'ALLOCATED'", p=p)
        assert all(r["bed_id"].startswith(p) for r in allocated)  # beds never cross pools
