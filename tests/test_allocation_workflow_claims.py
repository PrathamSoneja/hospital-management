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
