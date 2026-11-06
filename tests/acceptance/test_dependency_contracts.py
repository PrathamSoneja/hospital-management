"""Wire-contract checks for Lock Coordinator, Bed Registry, Staff Scheduler, and Patient Registry."""
import uuid

import grpc
import httpx
import pytest

from shared.generated import (bed_pb2, bed_pb2_grpc, common_pb2, lock_pb2, lock_pb2_grpc, staff_pb2,
                              staff_pb2_grpc)


def _lock(t):
    return lock_pb2_grpc.LockCoordinatorStub(grpc.insecure_channel(t["lock_addr"]))


def _ctx(lease, op):
    return common_pb2.MutationContext(operation_id=op, request_id=op, pool_id=lease.pool_id, lock_id=lease.lock_id,
                                      owner_id=lease.owner_id, fencing_token=lease.fencing_token)


def _acquire(lock, pool, owner, wait_ms=1000, lease_ms=5000):
    return lock.Acquire(lock_pb2.AcquireLockRequest(pool_id=pool, owner_id=owner, acquire_id=f"acc-{uuid.uuid4().hex}",
                                                    lease_ms=lease_ms, wait_timeout_ms=wait_ms), timeout=5)


def test_patient_admission_contract(targets):
    url = targets["patient_url"].rstrip("/")
    response = httpx.get(f"{url}/admissions/{targets['admission_id']}", headers={"X-Request-ID": "accept-1"}, timeout=5)
    assert response.status_code == 200
    body = response.json()
    assert body["admission_id"] == targets["admission_id"]
    assert isinstance(body["patient_id"], str) and body["patient_id"].strip()
    assert isinstance(body["status"], str) and body["status"].strip()
    missing = httpx.get(f"{url}/admissions/accept-missing-{uuid.uuid4().hex}", timeout=5)
    assert missing.status_code == 404


def test_bed_and_staff_read_contracts(targets):
    beds = bed_pb2_grpc.BedServiceStub(grpc.insecure_channel(targets["bed_addr"]))
    listed = beds.ListAvailableBeds(bed_pb2.ListAvailableBedsRequest(pool_id=targets["pool_id"], request_id="accept-2"),
                                    timeout=5)
    assert all(b.pool_id == targets["pool_id"] and b.bed_id and b.ward and b.status == "AVAILABLE" for b in listed.beds)
    found = beds.GetBed(bed_pb2.GetBedRequest(bed_id=targets["bed_id"], request_id="accept-3"), timeout=5)
    assert found.found and found.bed.bed_id == targets["bed_id"]
    staff = staff_pb2_grpc.StaffSchedulerStub(grpc.insecure_channel(targets["staff_addr"]))
    capacity = staff.CheckCapacity(staff_pb2.CheckCapacityRequest(pool_id=targets["pool_id"], request_id="accept-4"),
                                   timeout=5)
    assert capacity.has_capacity == (capacity.available_count > 0)


def test_lock_is_exclusive_fenced_and_releasable(mutating):
    lock, pool = _lock(mutating), mutating["pool_id"]
    first = _acquire(lock, pool, "accept-a")
    with pytest.raises(grpc.RpcError) as blocked:
        _acquire(lock, pool, "accept-b", wait_ms=200)
    assert blocked.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
    validate = lock_pb2.ValidateLockRequest(lock_id=first.lock_id, owner_id="accept-a", pool_id=pool,
                                            fencing_token=first.fencing_token)
    assert lock.Validate(validate, timeout=5).valid
    assert lock.Release(lock_pb2.ReleaseLockRequest(lock_id=first.lock_id, owner_id="accept-a"), timeout=5).success
    second = _acquire(lock, pool, "accept-b")
    assert second.fencing_token > first.fencing_token
    assert not lock.Validate(validate, timeout=5).valid
    lock.Release(lock_pb2.ReleaseLockRequest(lock_id=second.lock_id, owner_id="accept-b"), timeout=5)


def test_bed_and_staff_mutations_are_idempotent_by_operation_id(mutating):
    lock, pool, bed_id = _lock(mutating), mutating["pool_id"], mutating["bed_id"]
    beds = bed_pb2_grpc.BedServiceStub(grpc.insecure_channel(mutating["bed_addr"]))
    staff = staff_pb2_grpc.StaffSchedulerStub(grpc.insecure_channel(mutating["staff_addr"]))
    lease = _acquire(lock, pool, "accept-m", wait_ms=2000, lease_ms=10000)
    allocation, op = f"accept-alloc-{uuid.uuid4().hex[:8]}", f"accept-{uuid.uuid4().hex[:8]}"

    def allocate(operation, owner):
        return beds.AllocateBed(bed_pb2.AllocateBedRequest(context=_ctx(lease, operation), bed_id=bed_id,
                                                           allocation_id=owner), timeout=5)

    def assign(operation):
        return staff.AssignStaff(staff_pb2.AssignStaffRequest(context=_ctx(lease, operation), allocation_id=allocation,
                                                              bed_id=bed_id), timeout=5)

    def release_bed(operation, owner):
        return beds.ReleaseBed(bed_pb2.ReleaseBedRequest(context=_ctx(lease, operation), bed_id=bed_id,
                                                         allocation_id=owner), timeout=5)
    try:
        first = allocate(f"{op}:bed", allocation)
        assert first.success and allocate(f"{op}:bed", allocation) == first  # replay, same result
        assert not allocate(f"{op}:bed2", "someone-else").success           # ownership is exclusive
        assigned = assign(f"{op}:staff")
        assert assigned.success and assigned.staff_assignment_id and assign(f"{op}:staff") == assigned
        released = staff.ReleaseAssignment(staff_pb2.ReleaseAssignmentRequest(
            context=_ctx(lease, f"{op}:staff-rel"), staff_assignment_id=assigned.staff_assignment_id,
            allocation_id=allocation), timeout=5)
        assert released.success
        assert not release_bed(f"{op}:rel-x", "someone-else").success       # only the owner releases
        assert release_bed(f"{op}:rel", allocation).success
    finally:
        lock.Release(lock_pb2.ReleaseLockRequest(lock_id=lease.lock_id, owner_id="accept-m"), timeout=5)
