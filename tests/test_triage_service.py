"""
Unit and API contract tests for Triage Service.
Verifies urgency range validation, priority ordering, FIFO tie-breaking,
duplicate enqueue, pool-change restrictions, urgency changes, and error format.
"""
import pytest
from datetime import datetime, timezone
from services.triage.models import SetTriageRequest, TriageRecord, WaitlistEntry
from services.triage.repository.memory import InMemoryTriageRepository
from services.triage.service import TriageDomainService
from services.triage.validation import MockFencingValidator
from shared.generated.common_pb2 import MutationContext
from shared.generated import triage_pb2
from services.triage.grpc_server import TriageGrpcServicer
from shared.observability.errors import AppException

@pytest.fixture
def repo():
    return InMemoryTriageRepository()

@pytest.fixture
def validator():
    return MockFencingValidator(accept_all=True)

@pytest.fixture
def domain_service(repo, validator):
    return TriageDomainService(repo=repo, validator=validator)

def test_triage_range_validation(domain_service):
    # Valid urgency 1-5
    req1 = SetTriageRequest(triage_level=1, pool_id="WARDA-ICU")
    res1 = domain_service.set_triage("adm-1", req1.triage_level, req1.pool_id)
    assert res1.triage_level == 1
    assert res1.admission_id == "adm-1"

    # Invalid urgency < 1: Pydantic validation rejects it
    with pytest.raises(Exception):
        SetTriageRequest(triage_level=0, pool_id="WARDA-ICU")

    # Invalid urgency > 5: Pydantic validation rejects it
    with pytest.raises(Exception):
        SetTriageRequest(triage_level=6, pool_id="WARDA-ICU")

def test_get_triage_not_found(domain_service):
    with pytest.raises(AppException) as exc:
        domain_service.get_triage("non-existent-adm")
    assert exc.value.code == "NOT_FOUND"
    assert exc.value.status_code == 404

def test_waitlist_priority_ordering_and_fifo(domain_service):
    # Setup admissions with varying urgencies and arrival times
    # Patient 1: Level 3 at T0
    domain_service.set_triage("adm-1", 3, "WARDA-ICU")
    ctx = MutationContext(operation_id="op-1", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=1)
    e1, _ = domain_service.enqueue(ctx, "alloc-1", "adm-1")

    # Patient 2: Level 1 (most urgent) at T1
    domain_service.set_triage("adm-2", 1, "WARDA-ICU")
    ctx2 = MutationContext(operation_id="op-2", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=2)
    e2, _ = domain_service.enqueue(ctx2, "alloc-2", "adm-2")

    # Patient 3: Level 3 at T2 (same level as Patient 1, but later arrival)
    domain_service.set_triage("adm-3", 3, "WARDA-ICU")
    ctx3 = MutationContext(operation_id="op-3", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=3)
    e3, _ = domain_service.enqueue(ctx3, "alloc-3", "adm-3")

    # Patient 4: Level 2 at T3
    domain_service.set_triage("adm-4", 2, "WARDA-ICU")
    ctx4 = MutationContext(operation_id="op-4", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=4)
    e4, _ = domain_service.enqueue(ctx4, "alloc-4", "adm-4")

    # Verify peek_next returns most critical (Patient 2, level 1)
    peeked = domain_service.peek_next("WARDA-ICU")
    assert peeked is not None
    assert peeked.admission_id == "adm-2"
    assert peeked.triage_level == 1

    # Verify list order: Level 1 (adm-2) -> Level 2 (adm-4) -> Level 3 FIFO (adm-1, then adm-3)
    entries, _ = domain_service.list_waitlist("WARDA-ICU", 50, None)
    admission_order = [e.admission_id for e in entries]
    assert admission_order == ["adm-2", "adm-4", "adm-1", "adm-3"]

def test_duplicate_enqueue_returns_existing(domain_service):
    domain_service.set_triage("adm-dup", 2, "WARDA-ICU")
    ctx = MutationContext(operation_id="op-dup", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=1)
    
    e1, existed1 = domain_service.enqueue(ctx, "alloc-dup", "adm-dup")
    assert not existed1
    
    # Repeated enqueue for same allocation
    e2, existed2 = domain_service.enqueue(ctx, "alloc-dup", "adm-dup")
    # Matching operation replay returns the original first-call result.
    assert not existed2
    assert e1.entry_id == e2.entry_id

def test_prohibit_pool_change_while_waitlisted(domain_service):
    domain_service.set_triage("adm-pool", 2, "WARDA-ICU")
    ctx = MutationContext(operation_id="op-p", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=1)
    domain_service.enqueue(ctx, "alloc-pool", "adm-pool")

    # Attempting to change pool while active in waitlist must raise 409 STATE_CONFLICT
    req = SetTriageRequest(triage_level=2, pool_id="WARDB-CCU")
    with pytest.raises(AppException) as exc_info:
        domain_service.set_triage("adm-pool", req.triage_level, req.pool_id)
    assert exc_info.value.code == "POOL_CHANGE_NOT_SUPPORTED"
    assert exc_info.value.status_code == 409
    assert "immutable" in exc_info.value.message

def test_urgency_change_while_waitlisted(domain_service):
    domain_service.set_triage("adm-urgent", 4, "WARDA-ICU")
    ctx = MutationContext(operation_id="op-u", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=1)
    e1, _ = domain_service.enqueue(ctx, "alloc-urgent", "adm-urgent")
    original_queued_at = e1.queued_at

    # Patient deteriorates: urgency elevated to Level 1
    req = SetTriageRequest(triage_level=1, pool_id="WARDA-ICU")
    domain_service.set_triage("adm-urgent", req.triage_level, req.pool_id)

    # Waitlist record should reflect new urgency 1 while preserving original queued_at
    peeked = domain_service.peek_next("WARDA-ICU")
    assert peeked.admission_id == "adm-urgent"
    assert peeked.triage_level == 1
    assert peeked.queued_at == original_queued_at

def test_stale_fencing_token_rejected(domain_service, validator):
    domain_service.set_triage("adm-fence", 2, "WARDA-ICU")
    ctx1 = MutationContext(operation_id="op-1", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=10)
    domain_service.enqueue(ctx1, "alloc-1", "adm-fence")

    ctx_stale = MutationContext(operation_id="op-2", lock_id="lk-2", owner_id="inst-2", pool_id="WARDA-ICU", fencing_token=5)
    with pytest.raises(AppException) as exc:
        domain_service.remove_entry(ctx_stale, "alloc-1")
    assert exc.value.code == "FAILED_PRECONDITION"
    assert "superseded" in exc.value.message

def test_remove_entry_replays_after_repository_service_recreation(domain_service, validator):
    domain_service.set_triage("adm-remove", 2, "WARDA-ICU")
    enqueue_ctx = MutationContext(operation_id="enqueue-remove", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=1)
    entry, _ = domain_service.enqueue(enqueue_ctx, "alloc-remove", "adm-remove")
    remove_ctx = MutationContext(operation_id="remove-once", lock_id="lk-2", owner_id="inst-2", pool_id="WARDA-ICU", fencing_token=2)
    assert domain_service.remove_entry(remove_ctx, entry.allocation_id, "promoted") is True

    # A restarted domain service over the same durable in-memory adapter models
    # the adapter contract; Firestore restart coverage is in emulator tests.
    restarted = TriageDomainService(domain_service.repo, validator)
    fresh_lock_ctx = MutationContext(operation_id="remove-once", lock_id="lk-3", owner_id="inst-3", pool_id="WARDA-ICU", fencing_token=3)
    assert restarted.remove_entry(fresh_lock_ctx, entry.allocation_id, "promoted") is True
    assert restarted.peek_next("WARDA-ICU") is None
    assert domain_service.repo._pool_fence_state["WARDA-ICU"]["fencing_token"] == "2"

def test_remove_entry_conflict_and_distinct_operation_ids_do_not_alias(domain_service):
    domain_service.set_triage("adm-remove-conflict", 1, "WARDA-ICU")
    enqueue_ctx = MutationContext(operation_id="enqueue-conflict", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=1)
    entry, _ = domain_service.enqueue(enqueue_ctx, "alloc-remove-conflict", "adm-remove-conflict")
    remove_ctx = MutationContext(operation_id="remove-conflict", lock_id="lk-2", owner_id="inst-2", pool_id="WARDA-ICU", fencing_token=2)
    assert domain_service.remove_entry(remove_ctx, entry.allocation_id, "cancelled") is True
    with pytest.raises(AppException) as mismatch:
        domain_service.remove_entry(remove_ctx, entry.allocation_id, "promoted")
    assert mismatch.value.code == "IDEMPOTENCY_MISMATCH"

    second_operation = MutationContext(operation_id="remove-distinct", lock_id="lk-2", owner_id="inst-2", pool_id="WARDA-ICU", fencing_token=3)
    with pytest.raises(AppException) as unknown:
        domain_service.remove_entry(second_operation, entry.allocation_id, "cancelled")
    assert unknown.value.code == "OUTCOME_UNKNOWN"
    first_record = domain_service.repo._queue_operation_results["remove-conflict"]
    second_record = domain_service.repo._queue_operation_results["remove-distinct"]
    assert first_record["state"] == "COMPLETED" and first_record["result_success"] is True
    assert second_record["state"] == "OUTCOME_UNKNOWN" and second_record["result_success"] is None

def test_remove_entry_unknown_is_not_reexecuted_if_queue_entry_later_appears(domain_service):
    remove_ctx = MutationContext(operation_id="remove-legacy-unknown", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=1)
    with pytest.raises(AppException) as first:
        domain_service.remove_entry(remove_ctx, "alloc-legacy-unknown", "cancelled")
    assert first.value.code == "OUTCOME_UNKNOWN"
    domain_service.set_triage("adm-legacy-unknown", 1, "WARDA-ICU")
    enqueue_ctx = MutationContext(operation_id="enqueue-later", lock_id="lk-2", owner_id="inst-2", pool_id="WARDA-ICU", fencing_token=2)
    domain_service.enqueue(enqueue_ctx, "alloc-legacy-unknown", "adm-legacy-unknown")
    fresh_retry_ctx = MutationContext(operation_id=remove_ctx.operation_id, lock_id="lk-3", owner_id="inst-3", pool_id="WARDA-ICU", fencing_token=3)
    with pytest.raises(AppException) as retry:
        domain_service.remove_entry(fresh_retry_ctx, "alloc-legacy-unknown", "cancelled")
    assert retry.value.code == "OUTCOME_UNKNOWN"
    assert domain_service.peek_next("WARDA-ICU").allocation_id == "alloc-legacy-unknown"

def test_remove_entry_pool_fence_is_atomic_and_equal_token_requires_same_lock(domain_service):
    domain_service.set_triage("adm-fence-a", 1, "WARDA-ICU")
    domain_service.set_triage("adm-fence-b", 2, "WARDA-ICU")
    enqueue_ctx = MutationContext(operation_id="enqueue-fence", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=1)
    first, _ = domain_service.enqueue(enqueue_ctx, "alloc-fence-a", "adm-fence-a")
    enqueue_ctx_b = MutationContext(operation_id="enqueue-fence-b", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=1)
    second, _ = domain_service.enqueue(enqueue_ctx_b, "alloc-fence-b", "adm-fence-b")
    high = MutationContext(operation_id="remove-high", lock_id="lk-high", owner_id="owner-high", pool_id="WARDA-ICU", fencing_token=10)
    assert domain_service.remove_entry(high, first.allocation_id, "done")
    stale = MutationContext(operation_id="remove-stale", lock_id="lk-low", owner_id="owner-low", pool_id="WARDA-ICU", fencing_token=9)
    with pytest.raises(AppException) as lower:
        domain_service.repo.remove_waitlist_entry(stale, second.allocation_id, "done")
    assert lower.value.code == "FAILED_PRECONDITION"
    equal_wrong_lock = MutationContext(operation_id="remove-equal", lock_id="different", owner_id="owner-high", pool_id="WARDA-ICU", fencing_token=10)
    with pytest.raises(AppException) as identity:
        domain_service.repo.remove_waitlist_entry(equal_wrong_lock, second.allocation_id, "done")
    assert identity.value.code == "FAILED_PRECONDITION"
    assert domain_service.repo.get_waitlist_entry(second.allocation_id) is not None

def test_grpc_servicer(domain_service):
    servicer = TriageGrpcServicer(domain_service)
    domain_service.set_triage("adm-grpc", 2, "WARDA-ICU")

    class MockContext:
        def __init__(self):
            self.code = None
            self.details = None
        def set_code(self, code):
            self.code = code
        def set_details(self, details):
            self.details = details

    # Test GetTriage
    req = triage_pb2.GetTriageRequest(admission_id="adm-grpc", request_id="req-1")
    resp = servicer.GetTriage(req, MockContext())
    assert resp.found
    assert resp.triage.triage_level == 2

    # Test Enqueue
    ctx = MutationContext(operation_id="op-g", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=1)
    enq_req = triage_pb2.EnqueueWaitlistRequest(context=ctx, allocation_id="alloc-grpc", admission_id="adm-grpc")
    enq_resp = servicer.Enqueue(enq_req, MockContext())
    assert enq_resp.entry.admission_id == "adm-grpc"
    assert not enq_resp.already_existed

    # Test PeekNext
    peek_req = triage_pb2.PeekNextWaitlistRequest(pool_id="WARDA-ICU", request_id="req-2")
    peek_resp = servicer.PeekNext(peek_req, MockContext())
    assert peek_resp.found
    assert peek_resp.entry.allocation_id == "alloc-grpc"

    # Test RemoveEntry
    remove_ctx = MutationContext(operation_id="op-g-remove", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=2)
    rem_req = triage_pb2.RemoveWaitlistEntryRequest(context=remove_ctx, allocation_id="alloc-grpc", reason="promoted")
    rem_resp = servicer.RemoveEntry(rem_req, MockContext())
    assert rem_resp.success
