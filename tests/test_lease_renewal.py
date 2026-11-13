"""
Allocation pool-lease renewal against the tracked Coordinator double (unit, gRPC transport,
and PostgreSQL-backed workflow). Real Lock Coordinator verification is pending (Nisarg).
"""
import threading
import time
from concurrent import futures

import grpc
import pytest

from services.bed_allocation.clients.grpc_clients import GrpcLockCoordinatorClient
from services.bed_allocation.db import AllocationModel, OperationRecoveryModel
from services.bed_allocation.lease_renewal import LeaseRenewer
from services.bed_allocation.models import CreateAllocationRequest
from services.bed_allocation.service import BedAllocationDomainService
from services.triage.repository.memory import InMemoryTriageRepository
from services.triage.service import TriageDomainService
from shared.generated import lock_pb2_grpc
from shared.observability.errors import AppException
from tests.support.allocation_fakes import (
    DirectTriageClient, FakeBedRegistryClient, FakePatientRegistryClient, FakeStaffSchedulerClient,
)
from tests.support.stateful_lock import InProcessLockClient, StatefulLockCoordinator, StatefulLockServicer
from tests.support.triage_fakes import MockFencingValidator


class ScriptedRenewals(InProcessLockClient):
    """Coordinator double whose Renew can be made to refuse or to be unreachable."""

    def __init__(self, coordinator):
        super().__init__(coordinator)
        self.mode = "ok"
        self.renew_calls = 0

    def renew(self, lock_id, owner_id, lease_ms):
        self.renew_calls += 1
        if self.mode == "unavailable":
            raise AppException("SERVICE_UNAVAILABLE", "coordinator down", 503, retryable=True)
        if self.mode == "refuse":
            return None
        return super().renew(lock_id, owner_id, lease_ms)


def _held(lock_ms=300, mode="ok", enabled=True):
    client = ScriptedRenewals(StatefulLockCoordinator())
    client.mode = mode
    started = time.monotonic()
    lease = client.acquire("P", "owner", "acq-1", lock_ms, 1000)
    renewer = LeaseRenewer(client, lease, lock_ms, started, enabled=enabled)
    renewer.start()
    return client, lease, renewer


def test_renewal_keeps_the_lease_beyond_its_original_expiry_and_stops_cleanly():
    client, lease, renewer = _held()
    try:
        time.sleep(1.0)  # > 3 lease periods
        renewer.ensure_held()
        assert client.coordinator.validate(lease.lock_id, "owner", "P", lease.fencing_token) == (True, "")
        assert client.renew_calls >= 3
    finally:
        renewer.stop()
    assert not renewer._thread.is_alive()  # no leaked thread
    calls = client.renew_calls
    time.sleep(0.3)
    assert client.renew_calls == calls  # stopped before release: no further renewals
    assert client.release(lease.lock_id, "owner") is True


def test_refused_renewal_stops_new_mutations_immediately():
    client, _, renewer = _held(mode="refuse")
    try:
        time.sleep(0.2)
        with pytest.raises(AppException) as error:
            renewer.ensure_held()
        assert error.value.code == "LEASE_LOST" and error.value.retryable
    finally:
        renewer.stop()


def test_unreachable_coordinator_is_tolerated_only_until_actual_expiry():
    client, _, renewer = _held(lock_ms=600, mode="unavailable")
    try:
        time.sleep(0.25)
        renewer.ensure_held()  # still inside the believed lease
        time.sleep(0.45)
        with pytest.raises(AppException):
            renewer.ensure_held()
        assert client.renew_calls >= 1
    finally:
        renewer.stop()


def test_disabled_renewal_expires_on_schedule():
    _, _, renewer = _held(enabled=False)
    assert renewer._thread is None
    renewer.ensure_held()
    time.sleep(0.35)
    with pytest.raises(AppException):
        renewer.ensure_held()
    renewer.stop()


def test_grpc_renew_transport():
    coordinator = StatefulLockCoordinator()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    lock_pb2_grpc.add_LockCoordinatorServicer_to_server(StatefulLockServicer(coordinator), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    client = GrpcLockCoordinatorClient(f"127.0.0.1:{port}", default_timeout_s=2.0)
    try:
        lease = client.acquire("P", "owner", "acq-g", 2000, 1000)
        expires_at = client.renew(lease.lock_id, "owner", 5000)
        assert expires_at and expires_at > lease.expires_at
        assert client.renew(lease.lock_id, "intruder", 5000) is None
    finally:
        server.stop(grace=0)
    with pytest.raises(AppException) as error:
        GrpcLockCoordinatorClient(f"127.0.0.1:{port}", default_timeout_s=0.5).renew(lease.lock_id, "owner", 5000)
    assert error.value.retryable


def _service(sessions, lock_client, beds, lease_ms, renewal=True):
    patients = FakePatientRegistryClient()
    patients.seed_admission("adm-1", "pat-1", "OPEN")
    triage = TriageDomainService(InMemoryTriageRepository(), MockFencingValidator(True))
    triage.set_triage("adm-1", 1, "POOL-A")
    beds.seed_bed("bed-1", "POOL-A", ward="WARD-NORTH")
    return BedAllocationDomainService(
        session_factory=sessions, patient_client=patients, triage_client=DirectTriageClient(triage),
        lock_client=lock_client, bed_client=beds, staff_client=FakeStaffSchedulerClient(initial_capacity=5),
        owner_id="replica-1", lock_lease_ms=lease_ms, lock_wait_timeout_ms=500, lock_renewal=renewal)


class SlowListingBeds(FakeBedRegistryClient):
    """A slow read inside the locked section (longer than the lease), counting mutations."""

    def __init__(self, delay_s):
        super().__init__()
        self.delay_s = delay_s
        self.allocate_calls = 0

    def list_available_beds(self, pool_id, request_id):
        time.sleep(self.delay_s)
        return super().list_available_beds(pool_id, request_id)

    def allocate_bed(self, context, bed_id, allocation_id):
        self.allocate_calls += 1
        return super().allocate_bed(context, bed_id, allocation_id)


def test_workflow_longer_than_the_lease_completes_with_renewal(pg_sessions):
    client = ScriptedRenewals(StatefulLockCoordinator())
    beds = SlowListingBeds(delay_s=1.2)
    service = _service(pg_sessions, client, beds, lease_ms=1000)
    threads_before = threading.active_count()
    result = service.create_allocation(CreateAllocationRequest(admission_id="adm-1", pool_id="POOL-A"), "r1", "k1")
    assert (result.status, result.bed_id) == ("ALLOCATED", "bed-1")
    assert client.renew_calls >= 1 and client.coordinator.acquisitions == 1
    assert service._renewers == {}
    time.sleep(0.1)
    assert threading.active_count() <= threads_before


def test_lost_lease_stops_new_mutations_and_recovery_finishes_with_stable_ids(pg_sessions):
    client = ScriptedRenewals(StatefulLockCoordinator())
    beds = SlowListingBeds(delay_s=1.2)
    service = _service(pg_sessions, client, beds, lease_ms=1000, renewal=False)
    first = service.create_allocation(CreateAllocationRequest(admission_id="adm-1", pool_id="POOL-A"), "r1", "k1")
    assert (first.status, first.pending_action) == ("RECOVERING", "RESERVE_BED")
    assert beds.allocate_calls == 0  # nothing was sent after ownership lapsed
    assert beds.beds["bed-1"].status == "AVAILABLE"

    beds.delay_s = 0
    with pg_sessions() as session:
        row = session.query(AllocationModel).filter_by(allocation_id=first.allocation_id).one()
        row.recovery_claim_until = None
        session.commit()
    service.recover_pending()
    settled = service.get_allocation(first.allocation_id)
    assert (settled.status, settled.bed_id, settled.pending_action) == ("ALLOCATED", "bed-1", None)
    assert beds.allocate_calls == 1 and beds.beds["bed-1"].current_allocation_id == first.allocation_id
    with pg_sessions() as session:  # the bed reservation kept its stable operation ID across the retry
        ops = session.query(OperationRecoveryModel).filter_by(operation_id=f"{first.allocation_id}:reserve-bed").all()
        assert [op.status for op in ops] == ["SUCCEEDED"]


def test_shared_lease_is_renewed_once_and_released_by_its_last_holder(pg_sessions):
    client = ScriptedRenewals(StatefulLockCoordinator())
    service = _service(pg_sessions, client, FakeBedRegistryClient(), lease_ms=1000)
    first = service._acquire_pool_lease("POOL-A", "acq-shared")
    second = service._acquire_pool_lease("POOL-A", "acq-shared")  # Coordinator returns the same lease
    assert first.lock_id == second.lock_id and service._renewers[first.lock_id].refs == 2
    assert service._release_pool_lease(first) is True
    assert client.coordinator.validate(first.lock_id, "replica-1", "POOL-A", first.fencing_token)[0]  # still held
    assert service._release_pool_lease(second) is True
    assert service._renewers == {}
    assert not client.coordinator.validate(first.lock_id, "replica-1", "POOL-A", first.fencing_token)[0]
