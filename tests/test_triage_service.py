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
from services.triage.rest import set_triage_endpoint, get_triage_endpoint, get_waitlist_endpoint
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
    res1 = set_triage_endpoint("adm-1", req1, domain_service)
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
        get_triage_endpoint("non-existent-adm", domain_service)
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
    e2, _ = domain_service.enqueue(ctx, "alloc-2", "adm-2")

    # Patient 3: Level 3 at T2 (same level as Patient 1, but later arrival)
    domain_service.set_triage("adm-3", 3, "WARDA-ICU")
    e3, _ = domain_service.enqueue(ctx, "alloc-3", "adm-3")

    # Patient 4: Level 2 at T3
    domain_service.set_triage("adm-4", 2, "WARDA-ICU")
    e4, _ = domain_service.enqueue(ctx, "alloc-4", "adm-4")

    # Verify peek_next returns most critical (Patient 2, level 1)
    peeked = domain_service.peek_next("WARDA-ICU")
    assert peeked is not None
    assert peeked.admission_id == "adm-2"
    assert peeked.triage_level == 1

    # Verify list order: Level 1 (adm-2) -> Level 2 (adm-4) -> Level 3 FIFO (adm-1, then adm-3)
    page = get_waitlist_endpoint("WARDA-ICU", 50, None, domain_service)
    admission_order = [e.admission_id for e in page.items]
    assert admission_order == ["adm-2", "adm-4", "adm-1", "adm-3"]

def test_duplicate_enqueue_returns_existing(domain_service):
    domain_service.set_triage("adm-dup", 2, "WARDA-ICU")
    ctx = MutationContext(operation_id="op-dup", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=1)
    
    e1, existed1 = domain_service.enqueue(ctx, "alloc-dup", "adm-dup")
    assert not existed1
    
    # Repeated enqueue for same allocation
    e2, existed2 = domain_service.enqueue(ctx, "alloc-dup", "adm-dup")
    assert existed2
    assert e1.entry_id == e2.entry_id

def test_prohibit_pool_change_while_waitlisted(domain_service):
    domain_service.set_triage("adm-pool", 2, "WARDA-ICU")
    ctx = MutationContext(operation_id="op-p", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=1)
    domain_service.enqueue(ctx, "alloc-pool", "adm-pool")

    # Attempting to change pool while active in waitlist must raise 409 STATE_CONFLICT
    req = SetTriageRequest(triage_level=2, pool_id="WARDB-CCU")
    with pytest.raises(AppException) as exc_info:
        set_triage_endpoint("adm-pool", req, domain_service)
    assert exc_info.value.code == "STATE_CONFLICT"
    assert exc_info.value.status_code == 409
    assert "Cannot change pool" in exc_info.value.message

def test_urgency_change_while_waitlisted(domain_service):
    domain_service.set_triage("adm-urgent", 4, "WARDA-ICU")
    ctx = MutationContext(operation_id="op-u", lock_id="lk-1", owner_id="inst-1", pool_id="WARDA-ICU", fencing_token=1)
    e1, _ = domain_service.enqueue(ctx, "alloc-urgent", "adm-urgent")
    original_queued_at = e1.queued_at

    # Patient deteriorates: urgency elevated to Level 1
    req = SetTriageRequest(triage_level=1, pool_id="WARDA-ICU")
    set_triage_endpoint("adm-urgent", req, domain_service)

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
    rem_req = triage_pb2.RemoveWaitlistEntryRequest(context=ctx, allocation_id="alloc-grpc", reason="promoted")
    rem_resp = servicer.RemoveEntry(rem_req, MockContext())
    assert rem_resp.success
