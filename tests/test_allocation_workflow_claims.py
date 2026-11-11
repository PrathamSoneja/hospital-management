"""
Workflow ownership regressions found by the local k6 contention run (PostgreSQL).

1. A freshly accepted create is claimed by its request; the recovery worker must
   not start a second concurrent run that shares the same Coordinator lease.
2. An unconfirmed pool-lock release after all mutations must resume to the
   durable outcome instead of looping in reconciliation.
"""
import threading
from datetime import datetime, timedelta, timezone

from services.bed_allocation.db import AllocationModel
from services.bed_allocation.models import CreateAllocationRequest
from services.bed_allocation.service import BedAllocationDomainService
from services.triage.repository.memory import InMemoryTriageRepository
from services.triage.service import TriageDomainService
from shared.observability.errors import AppException
from tests.support.allocation_fakes import (
    DirectTriageClient, FakeBedRegistryClient, FakePatientRegistryClient, FakeStaffSchedulerClient,
)
from tests.support.stateful_lock import InProcessLockClient, StatefulLockCoordinator
from tests.support.triage_fakes import MockFencingValidator


class GatedLockClient(InProcessLockClient):
    """Pauses the first acquire so another actor can run in between; can drop one release."""

    def __init__(self, coordinator):
        super().__init__(coordinator)
        self.entered, self.proceed = threading.Event(), threading.Event()
        self.acquire_calls = 0
        self.fail_next_release = False

    def acquire(self, *args, **kwargs):
        self.acquire_calls += 1
        if self.acquire_calls == 1:
            self.entered.set()
            self.proceed.wait(10)
        return super().acquire(*args, **kwargs)

    def release(self, lock_id, owner_id):
        if self.fail_next_release:
            self.fail_next_release = False
            return False
        return super().release(lock_id, owner_id)


def _service(sessions, lock):
    patients = FakePatientRegistryClient()
    patients.seed_admission("adm-1", "pat-1", "OPEN")
    triage = TriageDomainService(InMemoryTriageRepository(), MockFencingValidator(True))
    triage.set_triage("adm-1", 1, "POOL-A")
    beds = FakeBedRegistryClient()
    beds.seed_bed("bed-1", "POOL-A", ward="WARD-NORTH")
    staff = FakeStaffSchedulerClient(initial_capacity=5)
    service = BedAllocationDomainService(
        session_factory=sessions, patient_client=patients, triage_client=DirectTriageClient(triage),
        lock_client=lock, bed_client=beds, staff_client=staff, owner_id="replica-1",
    )
    return service, beds, staff


def test_recovery_does_not_run_a_healthy_in_flight_create(pg_sessions):
    lock = GatedLockClient(StatefulLockCoordinator())
    lock.proceed.set()
    service, beds, _ = _service(pg_sessions, lock)
    accepted, resume = threading.Event(), threading.Event()
    original = service._run_create_workflow

    def paused_after_insert(*args, **kwargs):
        # The accepted row is committed; pause before the request thread starts its workflow.
        accepted.set()
        resume.wait(10)
        return original(*args, **kwargs)

    service._run_create_workflow = paused_after_insert
    result = {}
    thread = threading.Thread(target=lambda: result.setdefault("r", service.create_allocation(
        CreateAllocationRequest(admission_id="adm-1", pool_id="POOL-A"), "req-1", "key-1")))
    thread.start()
    assert accepted.wait(10)
    service._run_create_workflow = original
    assert service.recover_pending() == 0  # the accepting request already holds the claim
    resume.set()
    thread.join(10)
    assert result["r"].status == "ALLOCATED"
    assert lock.acquire_calls == 1


def test_unconfirmed_lock_release_resumes_to_durable_outcome(pg_sessions):
    lock = GatedLockClient(StatefulLockCoordinator())
    lock.proceed.set()
    lock.fail_next_release = True
    service, beds, staff = _service(pg_sessions, lock)
    first = service.create_allocation(CreateAllocationRequest(admission_id="adm-1", pool_id="POOL-A"), "req-1", "key-1")
    assert (first.status, first.pending_action) == ("RECOVERING", "RELEASE_POOL_LOCK")
    with pg_sessions() as session:
        row = session.query(AllocationModel).filter_by(allocation_id=first.allocation_id).one()
        row.recovery_claim_until = datetime.now(timezone.utc) - timedelta(seconds=1)
        session.commit()
    assert service.recover_pending() == 1
    settled = service.get_allocation(first.allocation_id)
    assert (settled.status, settled.bed_id, settled.pending_action) == ("ALLOCATED", "bed-1", None)
    assert beds.beds["bed-1"].current_allocation_id == first.allocation_id
    # The staff assignment was replayed from its durable result, not repeated.
    assert settled.staff_assignment_id is not None
    assert service.create_allocation(CreateAllocationRequest(admission_id="adm-1", pool_id="POOL-A"), "req-2", "key-1").status == "ALLOCATED"


class LostEnqueueAckTriage(DirectTriageClient):
    """The first enqueue commits in Triage, then its response is lost (unknown outcome)."""

    def __init__(self, domain):
        super().__init__(domain)
        self.drop_next_enqueue_ack = False

    def enqueue(self, context, allocation_id, admission_id):
        entry = super().enqueue(context, allocation_id, admission_id)
        if self.drop_next_enqueue_ack:
            self.drop_next_enqueue_ack = False
            raise AppException("SERVICE_UNAVAILABLE", "enqueue outcome unknown", 503, retryable=True)
        return entry


def _expire_claims(sessions):
    with sessions() as session:
        for row in session.query(AllocationModel).all():
            row.recovery_claim_until = datetime.now(timezone.utc) - timedelta(seconds=1)
        session.commit()


def test_release_does_not_promote_a_candidate_with_an_unresolved_create(pg_sessions):
    """Regression from the sandbox load run: promotion raced the candidate's own enqueue recovery."""
    patients = FakePatientRegistryClient()
    triage_domain = TriageDomainService(InMemoryTriageRepository(), MockFencingValidator(True))
    for n in (1, 2):
        patients.seed_admission(f"adm-{n}", f"pat-{n}", "OPEN")
        triage_domain.set_triage(f"adm-{n}", 1, "POOL-A")
    triage = LostEnqueueAckTriage(triage_domain)
    beds = FakeBedRegistryClient()
    beds.seed_bed("bed-1", "POOL-A", ward="WARD-NORTH")
    service = BedAllocationDomainService(
        session_factory=pg_sessions, patient_client=patients, triage_client=triage,
        lock_client=InProcessLockClient(StatefulLockCoordinator()), bed_client=beds,
        staff_client=FakeStaffSchedulerClient(initial_capacity=5), owner_id="replica-1")

    occupant = service.create_allocation(CreateAllocationRequest(admission_id="adm-1", pool_id="POOL-A"), "r1", "k1")
    triage.drop_next_enqueue_ack = True
    candidate = service.create_allocation(CreateAllocationRequest(admission_id="adm-2", pool_id="POOL-A"), "r2", "k2")
    assert (candidate.status, candidate.pending_action) == ("RECOVERING", "ENQUEUE_WAITLIST")
    assert triage.peek_next("POOL-A", "r").allocation_id == candidate.allocation_id  # it is queued

    service.release_allocation(occupant.allocation_id, "discharged", "r3", "k3")
    deferred = service.get_allocation(candidate.allocation_id)
    assert (deferred.status, deferred.pending_action) == ("RECOVERING", "ENQUEUE_WAITLIST")  # not promoted yet
    assert beds.beds["bed-1"].status == "AVAILABLE"

    for _ in range(3):  # candidate recovery settles WAITLISTED, then the retained PROMOTE runs
        _expire_claims(pg_sessions)
        service.recover_pending()
    promoted = service.get_allocation(candidate.allocation_id)
    assert (promoted.status, promoted.pending_action, promoted.bed_id) == ("ALLOCATED", None, "bed-1")
    assert beds.beds["bed-1"].current_allocation_id == candidate.allocation_id
    assert service.get_allocation(occupant.allocation_id).pending_action is None


def test_stale_promotion_runner_cannot_resurrect_a_released_candidate(pg_sessions):
    """Regression from the sandbox load run: a lapsed recovery claim let a second runner replay a
    completed promotion and set an already-released candidate back to ALLOCATED (double bed owner)."""
    patients = FakePatientRegistryClient()
    triage_domain = TriageDomainService(InMemoryTriageRepository(), MockFencingValidator(True))
    for n in (1, 2):
        patients.seed_admission(f"adm-{n}", f"pat-{n}", "OPEN")
        triage_domain.set_triage(f"adm-{n}", 1, "POOL-A")
    lock = InProcessLockClient(StatefulLockCoordinator())
    beds = FakeBedRegistryClient()
    beds.seed_bed("bed-1", "POOL-A", ward="WARD-NORTH")
    service = BedAllocationDomainService(
        session_factory=pg_sessions, patient_client=patients, triage_client=DirectTriageClient(triage_domain),
        lock_client=lock, bed_client=beds, staff_client=FakeStaffSchedulerClient(initial_capacity=5), owner_id="r1")

    occupant = service.create_allocation(CreateAllocationRequest(admission_id="adm-1", pool_id="POOL-A"), "r1", "k1")
    candidate = service.create_allocation(CreateAllocationRequest(admission_id="adm-2", pool_id="POOL-A"), "r2", "k2")
    assert service.release_allocation(occupant.allocation_id, "discharged", "r3", "k3").promoted_allocation_id == candidate.allocation_id
    service.release_allocation(candidate.allocation_id, "discharged", "r4", "k4")
    assert service.get_allocation(candidate.allocation_id).status == "RELEASED"

    lease = lock.acquire("POOL-A", "stale-runner", "acq-stale", 5000, 1000)
    try:
        service._promote_next_locked(occupant.allocation_id, lease, "stale")  # replay of the finished promotion
    finally:
        lock.release(lease.lock_id, lease.owner_id)
    assert service.get_allocation(candidate.allocation_id).status == "RELEASED"
    assert service.get_allocation(occupant.allocation_id).pending_action is None
    assert beds.beds["bed-1"].status == "AVAILABLE"


class FlakyLockClient(InProcessLockClient):
    def __init__(self, coordinator):
        super().__init__(coordinator)
        self.fail_next_acquire = False

    def acquire(self, *args, **kwargs):
        if self.fail_next_acquire:
            self.fail_next_acquire = False
            raise AppException("DEADLINE_EXCEEDED", "lock wait timed out", 504, retryable=True)
        return super().acquire(*args, **kwargs)


def _pool_service(sessions, admissions, beds=1, triage_cls=DirectTriageClient):
    patients = FakePatientRegistryClient()
    triage_domain = TriageDomainService(InMemoryTriageRepository(), MockFencingValidator(True))
    for admission, level in admissions.items():
        patients.seed_admission(admission, f"pat-{admission}", "OPEN")
        triage_domain.set_triage(admission, level, "POOL-A")
    bed_client = FakeBedRegistryClient()
    for n in range(1, beds + 1):
        bed_client.seed_bed(f"bed-{n}", "POOL-A", ward="WARD-NORTH")
    lock = FlakyLockClient(StatefulLockCoordinator())
    triage = triage_cls(triage_domain)
    service = BedAllocationDomainService(
        session_factory=sessions, patient_client=patients, triage_client=triage, lock_client=lock,
        bed_client=bed_client, staff_client=FakeStaffSchedulerClient(initial_capacity=10), owner_id="r1")
    return service, triage, triage_domain, lock, bed_client


def test_lock_timeout_during_recovery_keeps_the_outstanding_enqueue(pg_sessions):
    """Regression from the fresh-clone load run: resetting to CHECK_PRIORITY orphaned a committed entry."""
    service, triage, _, lock, beds = _pool_service(pg_sessions, {"adm-1": 1, "adm-2": 1}, triage_cls=LostEnqueueAckTriage)
    occupant = service.create_allocation(CreateAllocationRequest(admission_id="adm-1", pool_id="POOL-A"), "r1", "k1")
    triage.drop_next_enqueue_ack = True
    waiting = service.create_allocation(CreateAllocationRequest(admission_id="adm-2", pool_id="POOL-A"), "r2", "k2")
    assert waiting.pending_action == "ENQUEUE_WAITLIST"

    _expire_claims(pg_sessions)
    lock.fail_next_acquire = True  # the recovery attempt cannot get the pool lock
    service.recover_pending()
    assert service.get_allocation(waiting.allocation_id).pending_action == "ENQUEUE_WAITLIST"

    beds.beds["bed-1"].status = "AVAILABLE"  # even if capacity appears, the committed enqueue is honoured
    _expire_claims(pg_sessions)
    service.recover_pending()
    settled = service.get_allocation(waiting.allocation_id)
    assert (settled.status, settled.pending_action) == ("WAITLISTED", None)
    assert triage.peek_next("POOL-A", "r").allocation_id == waiting.allocation_id
    assert occupant.status == "ALLOCATED"


def test_promotion_removes_an_orphaned_head_entry_and_promotes_the_next(pg_sessions):
    service, triage, triage_domain, lock, beds = _pool_service(pg_sessions, {"adm-1": 3, "adm-2": 3, "adm-3": 2}, beds=2)
    first = service.create_allocation(CreateAllocationRequest(admission_id="adm-1", pool_id="POOL-A"), "r1", "k1")
    orphan = service.create_allocation(CreateAllocationRequest(admission_id="adm-2", pool_id="POOL-A"), "r2", "k2")
    waiting = service.create_allocation(CreateAllocationRequest(admission_id="adm-3", pool_id="POOL-A"), "r3", "k3")
    assert (first.status, orphan.status, waiting.status) == ("ALLOCATED", "ALLOCATED", "WAITLISTED")
    # Simulate the residue of a lost enqueue acknowledgement: a queue entry for an allocated admission.
    lease = lock.acquire("POOL-A", "test", "acq-orphan", 5000, 1000)
    triage_domain.set_triage("adm-2", 1, "POOL-A")
    triage.enqueue(service._promotion_context("orphan-enqueue", "r", lease), orphan.allocation_id, "adm-2")
    lock.release(lease.lock_id, lease.owner_id)
    assert triage.peek_next("POOL-A", "r").allocation_id == orphan.allocation_id

    service.release_allocation(orphan.allocation_id, "discharged", "r4", "k4")  # head is now orphaned
    for _ in range(3):
        _expire_claims(pg_sessions)
        service.recover_pending()
    promoted = service.get_allocation(waiting.allocation_id)
    assert (promoted.status, promoted.pending_action) == ("ALLOCATED", None)
    assert service.get_allocation(orphan.allocation_id).status == "RELEASED"
    assert triage.peek_next("POOL-A", "r") is None


def test_deferred_promotion_backs_off_without_taking_the_pool_lock(pg_sessions):
    """Regression from the second rehearsal: deferring releases monopolized the pool lock."""
    service, triage, _, lock, _ = _pool_service(pg_sessions, {"adm-1": 1, "adm-2": 1}, triage_cls=LostEnqueueAckTriage)
    occupant = service.create_allocation(CreateAllocationRequest(admission_id="adm-1", pool_id="POOL-A"), "r1", "k1")
    triage.drop_next_enqueue_ack = True
    service.create_allocation(CreateAllocationRequest(admission_id="adm-2", pool_id="POOL-A"), "r2", "k2")
    service.release_allocation(occupant.allocation_id, "discharged", "r3", "k3")  # defers behind the unresolved head

    acquisitions = lock.coordinator.acquisitions
    service._resume_promotion(occupant.allocation_id, "r4")
    assert lock.coordinator.acquisitions == acquisitions  # pre-check only; no pool lock taken
    with pg_sessions() as session:
        row = session.query(AllocationModel).filter_by(allocation_id=occupant.allocation_id).one()
        retry_at = row.recovery_claim_until if row.recovery_claim_until.tzinfo else row.recovery_claim_until.replace(tzinfo=timezone.utc)
        assert row.pending_action == "PROMOTE" and retry_at > datetime.now(timezone.utc) + timedelta(seconds=5)
