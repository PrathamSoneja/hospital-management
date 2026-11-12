"""
Allocation's lease handling against the real Lock Coordinator process (local ZooKeeper, ZK_TEST_HOSTS).

Release contract (Coordinator PR #11): the owner's release succeeds; the same owner's retry also
succeeds while the release tombstone exists (10 min); a reaped lock (no tombstone) or a wrong owner
returns False. Allocation must treat False as "release unconfirmed", never as proof the work failed, and
True never as proof that the lease was still valid when it mutated. Bed/Staff/Triage/Patient are the
in-process fakes so that business effects can be counted exactly.
"""
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from services.bed_allocation.clients.grpc_clients import GrpcLockCoordinatorClient
from services.bed_allocation.db import AllocationModel
from services.bed_allocation.models import CreateAllocationRequest
from services.bed_allocation.service import BedAllocationDomainService
from services.triage.repository.memory import InMemoryTriageRepository
from services.triage.service import TriageDomainService
from tests.sandbox.runtime import REPO_ROOT, ManagedProcess, free_port, isolated_env
from tests.support.allocation_fakes import (
    DirectTriageClient, FakeBedRegistryClient, FakePatientRegistryClient, FakeStaffSchedulerClient,
)
from tests.support.triage_fakes import MockFencingValidator


@pytest.fixture(scope="module")
def coordinator_addr():
    hosts = os.environ.get("ZK_TEST_HOSTS", "").strip()
    if not hosts:
        pytest.skip("requires local ZooKeeper (ZK_TEST_HOSTS); cloud is never used")
    grpc_port, rest_port = free_port(), free_port()
    log_dir = REPO_ROOT / ".sandbox" / "pytest-coordinator"
    process = ManagedProcess(
        "lock-coordinator", [sys.executable, "-m", "services.lock_coordinator.grpc_server"],
        isolated_env(log_dir, ZK_HOSTS=hosts, LOCK_GRPC_PORT=grpc_port, LOCK_REST_PORT=rest_port),
        log_dir / "lock-coordinator.log", f"http://127.0.0.1:{rest_port}/readyz").start()
    try:
        yield f"127.0.0.1:{grpc_port}"
    finally:
        process.stop()


class RecordingLockClient(GrpcLockCoordinatorClient):
    """The real gRPC client; records leases and can lose the next Release response after it was applied."""

    def __init__(self, target):
        super().__init__(target, default_timeout_s=3.0)
        self.leases, self.lose_next_release_ack = [], False

    def acquire(self, *args, **kwargs):
        lease = super().acquire(*args, **kwargs)
        self.leases.append(lease)
        return lease

    def release(self, lock_id, owner_id):
        released = super().release(lock_id, owner_id)
        if self.lose_next_release_ack:
            self.lose_next_release_ack = False
            return False  # the Coordinator released it, but the caller never learns so
        return released


class CountingBeds(FakeBedRegistryClient):
    def __init__(self):
        super().__init__()
        self.mutations = []

    def allocate_bed(self, context, bed_id, allocation_id):
        self.mutations.append((context.operation_id, context.lock_id, context.fencing_token))
        return super().allocate_bed(context, bed_id, allocation_id)


class CountingStaff(FakeStaffSchedulerClient):
    def __init__(self):
        super().__init__(initial_capacity=5)
        self.mutations = []

    def assign_staff(self, context, allocation_id, bed_id):
        self.mutations.append((context.operation_id, context.lock_id, context.fencing_token))
        return super().assign_staff(context, allocation_id, bed_id)


def _service(sessions, lock, **kwargs):
    pool = f"P-RC-{uuid.uuid4().hex[:6]}"
    patients = FakePatientRegistryClient()
    patients.seed_admission("adm-1", "pat-1", "OPEN")
    triage = TriageDomainService(InMemoryTriageRepository(), MockFencingValidator(True))
    triage.set_triage("adm-1", 1, pool)
    beds, staff = CountingBeds(), CountingStaff()
    beds.seed_bed("bed-1", pool, ward="WARD-NORTH")
    service = BedAllocationDomainService(
        session_factory=sessions, patient_client=patients, triage_client=DirectTriageClient(triage),
        lock_client=lock, bed_client=beds, staff_client=staff, owner_id=f"rc-{uuid.uuid4().hex[:6]}", **kwargs)
    return service, pool, beds, staff


def _expire_claims(sessions):
    with sessions() as session:
        for row in session.query(AllocationModel).all():
            row.recovery_claim_until = datetime.now(timezone.utc) - timedelta(seconds=1)
        session.commit()


def test_release_contract_owner_retry_and_wrong_owner(coordinator_addr):
    lock = RecordingLockClient(coordinator_addr)
    pool = f"P-RC-{uuid.uuid4().hex[:6]}"
    lease = lock.acquire(pool, "owner-a", f"acq-{uuid.uuid4().hex}", 5000, 1000)
    assert lock.release(lease.lock_id, "owner-b") is False          # wrong owner
    assert lock.release(lease.lock_id, "owner-a") is True           # owner
    assert lock.release(lease.lock_id, "owner-a") is True           # same-owner retry: tombstone
    assert lock.release(lease.lock_id, "owner-b") is False          # wrong owner, even after release
    assert lock.validate(lease.lock_id, "owner-a", pool, lease.fencing_token) is False
    assert lock.renew(lease.lock_id, "owner-a", 5000) is None       # a released lease cannot be renewed

    reaped = lock.acquire(pool, "owner-a", f"acq-{uuid.uuid4().hex}", 1000, 1000)
    time.sleep(2.5)                                                 # expires; the reaper deletes it, no tombstone
    assert lock.release(reaped.lock_id, "owner-a") is False


def test_lost_release_ack_resumes_once_under_a_fresh_lease(pg_sessions, coordinator_addr):
    lock = RecordingLockClient(coordinator_addr)
    service, pool, beds, staff = _service(pg_sessions, lock)
    lock.lose_next_release_ack = True
    first = service.create_allocation(CreateAllocationRequest(admission_id="adm-1", pool_id=pool), "req-1", "k1")
    assert (first.status, first.pending_action) == ("RECOVERING", "RELEASE_POOL_LOCK")
    original = lock.leases[-1]
    # The release did happen: the same owner's retry is confirmed by the tombstone.
    assert lock.release(original.lock_id, original.owner_id) is True
    effects = (list(beds.mutations), list(staff.mutations))

    _expire_claims(pg_sessions)
    assert service.recover_pending() == 1
    settled = service.get_allocation(first.allocation_id)
    assert (settled.status, settled.pending_action, settled.bed_id) == ("ALLOCATED", None, "bed-1")
    fresh = lock.leases[-1]
    assert fresh.lock_id != original.lock_id and fresh.fencing_token > original.fencing_token
    assert (beds.mutations, staff.mutations) == effects     # resumed from durable outcomes: no repeat
    assert len(staff.assignments) == 1 and beds.beds["bed-1"].current_allocation_id == first.allocation_id
    assert lock.validate(fresh.lock_id, fresh.owner_id, pool, fresh.fencing_token) is False  # released


def test_release_false_for_a_reaped_lease_is_unconfirmed_cleanup_not_failure(pg_sessions, coordinator_addr):
    """Renewal off and a 1 s lease: the lock is reaped before release, so Release returns False (no
    tombstone). Confirmed outcomes stay; one recovery pass under a fresh lease settles it."""
    lock = RecordingLockClient(coordinator_addr)
    service, pool, beds, staff = _service(pg_sessions, lock, lock_lease_ms=1000, lock_renewal=False)
    original_assign = staff.assign_staff

    def slow_assign(context, allocation_id, bed_id):
        result = original_assign(context, allocation_id, bed_id)
        time.sleep(2.5)  # the outcome is recorded by staff, then the lease is reaped
        return result
    staff.assign_staff = slow_assign

    first = service.create_allocation(CreateAllocationRequest(admission_id="adm-1", pool_id=pool), "req-1", "k1")
    assert (first.status, first.pending_action) == ("RECOVERING", "RELEASE_POOL_LOCK")
    staff.assign_staff = original_assign
    effects = (list(beds.mutations), list(staff.mutations))

    _expire_claims(pg_sessions)
    assert service.recover_pending() == 1
    settled = service.get_allocation(first.allocation_id)
    assert (settled.status, settled.pending_action) == ("ALLOCATED", None)   # not undone, not failed
    assert (beds.mutations, staff.mutations) == effects
    assert len(staff.assignments) == 1 and beds.beds["bed-1"].current_allocation_id == first.allocation_id
    _expire_claims(pg_sessions)
    assert service.recover_pending() == 0                                      # nothing loops
