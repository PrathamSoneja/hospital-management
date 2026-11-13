"""
Demo controls through the real REST/gRPC processes. Allocators 0/1 run normally; allocator 2
runs the 5 s expiry-demo profile (ALLOCATION_DEMO_MODE=true, LOCK_LEASE_MS=5000, renewal off).
The fenced Bed/Staff/Triage safeguards stay active: these tests show them refusing unsafe or
expired holders, not being bypassed. Real Coordinator/Bed Registry behaviour is not covered.
"""
import threading
import time

from tests.integration.test_sandbox_workflows import _assert_pool_invariants, _rows
from tests.sandbox.fixtures import seed_pool


def test_demo_controls_are_rejected_outside_demo_mode(sandbox, api, pool):
    admission = f"{pool}-adm-1"
    seed_pool(sandbox, pool, beds=1, admissions=[admission])
    for query in ("?pause_ms=100", "?lock=false"):
        status, body, _ = api.create(admission, pool, f"{pool}-k{query}", allocator=0, query=query)
        assert status == 400 and body["error"]["code"] == "DEMO_CONTROLS_DISABLED"
    status, body, _ = api.create(admission, pool, f"{pool}-bound", allocator=2, query="?pause_ms=999999")
    assert status in (400, 422)  # bounded even in demo mode
    assert _rows(sandbox, "SELECT 1 FROM allocations WHERE admission_id = :a", a=admission) == []


def test_unsafe_lock_false_is_refused_by_fenced_dependencies(sandbox, api, pool):
    admission = f"{pool}-adm-1"
    seed_pool(sandbox, pool, beds=1, admissions=[admission])
    rejections_before = sandbox.dependency_state()["fence_rejections"]

    status, body, _ = api.create(admission, pool, f"{pool}-k", allocator=2, query="?lock=false")
    assert status in (201, 202) and body["status"] == "RECOVERING"  # the unlocked bed reservation was refused
    state = sandbox.dependency_state()
    assert state["fence_rejections"] > rejections_before
    assert all(b["status"] == "AVAILABLE" for b in state["beds"] if b["pool_id"] == pool)

    # Recovery always takes the real pool lease, so the request completes safely afterwards.
    settled = api.wait_status(body["allocation_id"], {"ALLOCATED"}, timeout=90)
    assert settled["bed_id"] == f"{pool}-bed-01"
    _assert_pool_invariants(sandbox, pool)


def test_paused_holder_loses_its_expired_lease_and_another_holder_proceeds(sandbox, api, pool):
    paused, other = f"{pool}-adm-1", f"{pool}-adm-2"
    seed_pool(sandbox, pool, beds=2, admissions=[paused, other])
    result = {}

    def hold_past_expiry():
        started = time.monotonic()
        result["response"] = api.create(paused, pool, f"{pool}-paused", allocator=2, query="?pause_ms=7000")
        result["elapsed"] = time.monotonic() - started

    holder = threading.Thread(target=hold_past_expiry)
    holder.start()
    time.sleep(1.0)  # the holder has the pool lease and is pausing without renewal
    started = time.monotonic()
    status, body, _ = api.create(other, pool, f"{pool}-other", allocator=0)
    waited = time.monotonic() - started
    holder.join(30)

    # The second holder got the lease once the 5 s lease expired, before the pause ended,
    # and allocated normally.
    assert status == 201 and body["status"] == "ALLOCATED" and waited < 6.5
    status, paused_body, _ = result["response"]
    assert result["elapsed"] >= 7 and paused_body["status"] == "RECOVERING"  # no mutation sent after expiry
    assert paused_body["bed_id"] != body["bed_id"]

    settled = api.wait_status(paused_body["allocation_id"], {"ALLOCATED"}, timeout=90)
    assert settled["bed_id"] != body["bed_id"]
    _assert_pool_invariants(sandbox, pool)
