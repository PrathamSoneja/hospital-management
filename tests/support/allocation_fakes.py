"""
Injectable in-memory fake clients for teammates' dependencies.
Used explicitly for local unit/integration tests and demo mode.
NEVER used as a silent fallback when real services fail.
"""
import time
import uuid
from typing import Dict, List, Optional, Tuple
from services.bed_allocation.clients.base import (
    BedDto, BedRegistryClient, LockCoordinatorClient, LockLease,
    PatientAdmissionDto, PatientRegistryClient, StaffSchedulerClient,
    TriageClient, TriageDto, WaitlistEntryDto
)
from shared.observability.errors import AppException
from starlette import status

class FakePatientRegistryClient(PatientRegistryClient):
    def __init__(self):
        # Default seeded open admissions
        self.admissions: Dict[str, PatientAdmissionDto] = {}

    def seed_admission(self, admission_id: str, patient_id: str, status: str = "OPEN"):
        self.admissions[admission_id] = PatientAdmissionDto(
            admission_id=admission_id, patient_id=patient_id, status=status
        )

    def get_admission(self, admission_id: str, request_id: str) -> Optional[PatientAdmissionDto]:
        if admission_id in self.admissions:
            return self.admissions[admission_id]
        # Auto-seed OPEN admission for synthetic test IDs starting with "adm-"
        if admission_id.startswith("adm-"):
            dto = PatientAdmissionDto(admission_id=admission_id, patient_id=f"pat-{admission_id}", status="OPEN")
            self.admissions[admission_id] = dto
            return dto
        return None

class FakeLockCoordinatorClient(LockCoordinatorClient):
    def __init__(self):
        self._fencing_tokens: Dict[str, int] = {}
        self.active_locks: Dict[str, LockLease] = {}
        self.simulate_lock_timeout = False

    def acquire(self, pool_id: str, owner_id: str, acquire_id: str, lease_ms: int, wait_timeout_ms: int) -> LockLease:
        if self.simulate_lock_timeout:
            raise AppException(
                code="DEADLINE_EXCEEDED",
                message=f"Timed out waiting for lock on pool {pool_id}",
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                retryable=True
            )
        token = self._fencing_tokens.get(pool_id, 0) + 1
        self._fencing_tokens[pool_id] = token
        lock_id = f"lock-{pool_id}-{token}"
        lease = LockLease(
            lock_id=lock_id,
            pool_id=pool_id,
            owner_id=owner_id,
            fencing_token=token,
            expires_at=int((time.time() + (lease_ms / 1000.0)) * 1000),
            lock_wait_ms=5
        )
        self.active_locks[lock_id] = lease
        return lease

    def release(self, lock_id: str, owner_id: str) -> bool:
        if lock_id in self.active_locks:
            del self.active_locks[lock_id]
            return True
        return False

    def validate(self, lock_id: str, owner_id: str, pool_id: str, fencing_token: int) -> bool:
        lease = self.active_locks.get(lock_id)
        if not lease:
            return False
        return (lease.owner_id == owner_id and
                lease.pool_id == pool_id and
                lease.fencing_token == fencing_token)

class FakeBedRegistryClient(BedRegistryClient):
    def __init__(self):
        self.beds: Dict[str, BedDto] = {}
        self.operations: Dict[str, Tuple[str, str]] = {}
        self.simulate_allocate_failure = False
        self.simulate_release_failure = False
        self.simulate_compensation_failure = False

    def seed_bed(self, bed_id: str, pool_id: str, ward: str = "ICU-A"):
        self.beds[bed_id] = BedDto(
            bed_id=bed_id, pool_id=pool_id, ward=ward, status="AVAILABLE"
        )

    def list_available_beds(self, pool_id: str, request_id: str) -> List[BedDto]:
        return [b for b in self.beds.values() if b.pool_id == pool_id and b.status == "AVAILABLE"]

    def allocate_bed(self, context, bed_id: str, allocation_id: str) -> Tuple[bool, Optional[BedDto], str]:
        if context.operation_id in self.operations:
            result_bed_id, result_allocation_id = self.operations[context.operation_id]
            bed = self.beds.get(result_bed_id)
            if result_allocation_id == allocation_id and bed:
                return True, bed, ""
        if self.simulate_allocate_failure:
            return False, None, "Simulated bed allocation failure"
        bed = self.beds.get(bed_id)
        if not bed:
            return False, None, f"Bed {bed_id} not found"
        if bed.status != "AVAILABLE" and bed.current_allocation_id != allocation_id:
            return False, None, f"Bed {bed_id} already occupied by {bed.current_allocation_id}"

        bed.status = "OCCUPIED"
        bed.current_allocation_id = allocation_id
        self.operations[context.operation_id] = (bed_id, allocation_id)
        return True, bed, ""

    def release_bed(self, context, bed_id: str, allocation_id: str) -> Tuple[bool, str]:
        if context.operation_id in self.operations:
            return True, ""
        if self.simulate_release_failure or (self.simulate_compensation_failure and "prom-compensate-bed" in context.operation_id):
            return False, "Simulated bed release failure"
        bed = self.beds.get(bed_id)
        if not bed:
            return False, f"Bed {bed_id} not found"
        # Release only if still owned by the expected allocation (ownership safety)
        if bed.current_allocation_id != allocation_id:
            return False, f"Bed {bed_id} is owned by {bed.current_allocation_id}, not {allocation_id}"

        bed.status = "AVAILABLE"
        bed.current_allocation_id = None
        self.operations[context.operation_id] = (bed_id, allocation_id)
        return True, ""

class FakeStaffSchedulerClient(StaffSchedulerClient):
    def __init__(self, initial_capacity: int = 100):
        self.capacity: Dict[str, int] = {}
        self.default_capacity = initial_capacity
        self.assignments: Dict[str, Tuple[str, str]] = {} # assignment_id -> (allocation_id, bed_id)
        self.assignment_operations: Dict[str, Tuple[str, str, str]] = {}
        self.release_operations: set[str] = set()
        self.simulate_capacity_exhausted = False
        self.simulate_assignment_failure = False
        self.simulate_uncertain_outcome = False
        self.simulate_uncertain_release = False

    def set_capacity(self, pool_id: str, count: int):
        self.capacity[pool_id] = count

    def check_capacity(self, pool_id: str, request_id: str) -> Tuple[bool, int]:
        if self.simulate_capacity_exhausted:
            return False, 0
        cap = self.capacity.get(pool_id, self.default_capacity)
        return (cap > 0), cap

    def assign_staff(self, context, allocation_id: str, bed_id: str) -> Tuple[bool, Optional[str], Optional[str], str]:
        if context.operation_id in self.assignment_operations:
            assignment_id, staff_id, _ = self.assignment_operations[context.operation_id]
            return True, assignment_id, staff_id, ""
        if self.simulate_uncertain_outcome:
            raise AppException(
                code="DEADLINE_EXCEEDED",
                message="Downstream call to StaffScheduler timed out; outcome uncertain",
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                retryable=True
            )
        if self.simulate_assignment_failure:
            return False, None, None, "Simulated staff assignment failure"

        has_cap, cap = self.check_capacity(context.pool_id, context.request_id)
        if not has_cap:
            return False, None, None, "No staff capacity available"

        assignment_id = f"staff-asgn-{uuid.uuid4().hex[:8]}"
        staff_id = f"nurse-{uuid.uuid4().hex[:6]}"
        self.assignments[assignment_id] = (allocation_id, bed_id)
        self.assignment_operations[context.operation_id] = (assignment_id, staff_id, allocation_id)
        self.capacity[context.pool_id] = cap - 1
        return True, assignment_id, staff_id, ""

    def release_assignment(self, context, staff_assignment_id: str, allocation_id: str) -> Tuple[bool, str]:
        if context.operation_id in self.release_operations:
            return True, ""
        if self.simulate_uncertain_release:
            raise AppException(
                code="DEADLINE_EXCEEDED",
                message="Staff release outcome is uncertain",
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                retryable=True
            )
        if staff_assignment_id in self.assignments:
            expected_alloc, _ = self.assignments[staff_assignment_id]
            if expected_alloc == allocation_id:
                del self.assignments[staff_assignment_id]
                self.release_operations.add(context.operation_id)
                cap = self.capacity.get(context.pool_id, self.default_capacity)
                self.capacity[context.pool_id] = cap + 1
                return True, ""
            return False, f"Assignment owned by {expected_alloc}, not {allocation_id}"
        self.release_operations.add(context.operation_id)
        return True, "" # Idempotent release

class DirectTriageClient(TriageClient):
    """Direct adapter calling TriageDomainService in-process for tests and local demo mode."""
    def __init__(self, triage_domain_service):
        self.service = triage_domain_service

    def get_triage(self, admission_id: str, request_id: str) -> Optional[TriageDto]:
        try:
            r = self.service.get_triage(admission_id)
            return TriageDto(
                admission_id=r.admission_id,
                triage_level=r.triage_level,
                pool_id=r.pool_id,
                updated_at=r.updated_at
            )
        except Exception:
            return None

    def enqueue(self, context, allocation_id: str, admission_id: str) -> WaitlistEntryDto:
        e, _ = self.service.enqueue(context, allocation_id, admission_id)
        return WaitlistEntryDto(
            entry_id=e.entry_id,
            allocation_id=e.allocation_id,
            admission_id=e.admission_id,
            pool_id=e.pool_id,
            triage_level=e.triage_level,
            queued_at=e.queued_at
        )

    def peek_next(self, pool_id: str, request_id: str) -> Optional[WaitlistEntryDto]:
        e = self.service.peek_next(pool_id)
        if not e:
            return None
        return WaitlistEntryDto(
            entry_id=e.entry_id,
            allocation_id=e.allocation_id,
            admission_id=e.admission_id,
            pool_id=e.pool_id,
            triage_level=e.triage_level,
            queued_at=e.queued_at
        )

    def remove_entry(self, context, allocation_id: str, reason: str) -> bool:
        return self.service.remove_entry(context, allocation_id, reason)
