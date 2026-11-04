"""Thread-safe in-memory Staff Scheduler repository for tests and local demos."""

from datetime import datetime
import hashlib
import json
from threading import RLock

from services.staff_scheduler.models import (
    StaffAssignment,
    StaffCreate,
    StaffRecord,
    StaffShift,
    ShiftCreate,
)
from shared.observability.errors import AppException
from services.triage.repository.operations import fence_for_new_mutation
from starlette import status


class InMemoryStaffRepository:
    def __init__(self) -> None:
        self._lock = RLock()
        self._staff: dict[str, StaffRecord] = {}
        self._shifts: dict[str, dict[str, StaffShift]] = {}
        self._assignments: dict[str, StaffAssignment] = {}
        self._operations: dict[str, dict[str, object]] = {}
        self._pool_fences: dict[str, dict[str, object]] = {}
        self._idempotency: dict[tuple[str, str], tuple[str, dict[str, object]]] = {}

    def _replay(self, endpoint: str, key: str, request: dict[str, object]) -> dict[str, object] | None:
        fingerprint = hashlib.sha256(
            json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        previous = self._idempotency.get((endpoint, key))
        if previous is not None:
            if previous[0] != fingerprint:
                raise AppException("IDEMPOTENCY_MISMATCH", "Idempotency-Key was reused for a different request", status.HTTP_409_CONFLICT)
            return previous[1]
        self._idempotency[(endpoint, key)] = (fingerprint, {})
        return None

    def create_staff(self, staff: StaffCreate, idempotency_key: str) -> StaffRecord:
        with self._lock:
            request = staff.model_dump(mode="json")
            replay = self._replay("POST:/api/v1/staff", idempotency_key, request)
            if replay:
                return StaffRecord(**replay)
            if staff.staff_id in self._staff:
                self._idempotency.pop(("POST:/api/v1/staff", idempotency_key), None)
                raise AppException("STATE_CONFLICT", "Staff member already exists", status.HTTP_409_CONFLICT)
            record = StaffRecord(**staff.model_dump(), active_assignment_count=0)
            self._staff[record.staff_id] = record
            self._shifts[record.staff_id] = {}
            fingerprint = hashlib.sha256(
                json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            self._idempotency[("POST:/api/v1/staff", idempotency_key)] = (
                fingerprint,
                record.model_dump(mode="json"),
            )
            return record.model_copy(deep=True)

    def get_staff(self, staff_id: str) -> StaffRecord | None:
        with self._lock:
            record = self._staff.get(staff_id)
            return record.model_copy(deep=True) if record else None

    def create_shift(
        self,
        staff_id: str,
        shift_id: str,
        shift: ShiftCreate,
        idempotency_key: str,
    ) -> StaffShift:
        with self._lock:
            endpoint = f"PUT:/api/v1/staff/{staff_id}/shifts/{shift_id}"
            request = {
                "staff_id": staff_id,
                "shift_id": shift_id,
                **shift.model_dump(mode="json"),
            }
            replay = self._replay(endpoint, idempotency_key, request)
            if replay:
                return StaffShift(**replay)
            if staff_id not in self._staff:
                self._idempotency.pop((endpoint, idempotency_key), None)
                raise AppException("NOT_FOUND", "Staff member was not found", status.HTTP_404_NOT_FOUND)
            record = StaffShift(shift_id=shift_id, **shift.model_dump())
            self._shifts[staff_id][shift_id] = record
            fingerprint = hashlib.sha256(
                json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            self._idempotency[(endpoint, idempotency_key)] = (
                fingerprint,
                record.model_dump(mode="json"),
            )
            return record.model_copy(deep=True)

    def list_shifts(self, staff_id: str) -> list[StaffShift]:
        with self._lock:
            if staff_id not in self._staff:
                raise AppException("NOT_FOUND", "Staff member was not found", status.HTTP_404_NOT_FOUND)
            return [shift.model_copy(deep=True) for shift in self._shifts[staff_id].values()]

    def _available(self, staff_id: str, pool_id: str, at: datetime) -> bool:
        staff = self._staff[staff_id]
        if (
            not staff.active
            or staff.role.value != "NURSE"
            or pool_id not in staff.pool_ids
            or staff.active_assignment_count >= staff.max_concurrent_assignments
        ):
            return False
        return any(
            shift.active and shift.starts_at <= at < shift.ends_at
            for shift in self._shifts[staff_id].values()
        )

    def check_capacity(self, pool_id: str, at: datetime) -> int:
        with self._lock:
            return sum(
                self._staff[staff_id].max_concurrent_assignments
                - self._staff[staff_id].active_assignment_count
                for staff_id in self._staff
                if self._available(staff_id, pool_id, at)
            )

    def assign(
        self,
        context,
        allocation_id: str,
        bed_id: str,
        at: datetime,
    ) -> StaffAssignment | None:
        if not context.operation_id:
            raise AppException("INVALID_ARGUMENT", "operation_id is required", status.HTTP_400_BAD_REQUEST)
        if not context.pool_id or not context.lock_id or not context.owner_id:
            raise AppException("INVALID_ARGUMENT", "MutationContext lock identity is required", status.HTTP_400_BAD_REQUEST)
        with self._lock:
            previous_operation = self._operations.get(context.operation_id)
            if previous_operation is not None:
                if (
                    previous_operation["action"] != "ASSIGN"
                    or previous_operation["allocation_id"] != allocation_id
                    or previous_operation["bed_id"] != bed_id
                    or previous_operation["pool_id"] != context.pool_id
                ):
                    raise AppException("IDEMPOTENCY_MISMATCH", "operation_id was reused for a different assignment", status.HTTP_409_CONFLICT)
                assignment_id = previous_operation["assignment_id"]
                if assignment_id is None:
                    return None
                return self._assignments[str(assignment_id)].model_copy(deep=True)

            next_fence = fence_for_new_mutation(
                context,
                self._pool_fences.get(context.pool_id),
            )
            candidates = [
                staff_id
                for staff_id in sorted(self._staff)
                if self._available(staff_id, context.pool_id, at)
            ]
            if not candidates:
                self._operations[context.operation_id] = {
                    "action": "ASSIGN",
                    "allocation_id": allocation_id,
                    "bed_id": bed_id,
                    "pool_id": context.pool_id,
                    "assignment_id": None,
                }
                self._pool_fences[context.pool_id] = next_fence
                return None

            staff_id = candidates[0]
            assignment_id = f"staff-assignment-{context.operation_id}"
            assignment = StaffAssignment(
                assignment_id=assignment_id,
                staff_id=staff_id,
                allocation_id=allocation_id,
                pool_id=context.pool_id,
                bed_id=bed_id,
                operation_id=context.operation_id,
                status="ACTIVE",
                assigned_at=at,
            )
            self._staff[staff_id].active_assignment_count += 1
            self._assignments[assignment_id] = assignment
            self._operations[context.operation_id] = {
                "action": "ASSIGN",
                "allocation_id": allocation_id,
                "bed_id": bed_id,
                "pool_id": context.pool_id,
                "assignment_id": assignment_id,
            }
            self._pool_fences[context.pool_id] = next_fence
            return assignment.model_copy(deep=True)

    def release(
        self,
        context,
        assignment_id: str,
        allocation_id: str,
        at: datetime,
    ) -> bool:
        if not context.operation_id:
            raise AppException("INVALID_ARGUMENT", "operation_id is required", status.HTTP_400_BAD_REQUEST)
        with self._lock:
            previous_operation = self._operations.get(context.operation_id)
            if previous_operation is not None:
                if (
                    previous_operation["action"] != "RELEASE"
                    or previous_operation["allocation_id"] != allocation_id
                    or previous_operation["assignment_id"] != assignment_id
                ):
                    raise AppException("IDEMPOTENCY_MISMATCH", "operation_id was reused for a different release", status.HTTP_409_CONFLICT)
                return bool(previous_operation["success"])

            next_fence = fence_for_new_mutation(
                context,
                self._pool_fences.get(context.pool_id),
            )
            assignment = self._assignments.get(assignment_id)
            if (
                assignment is None
                or assignment.allocation_id != allocation_id
                or assignment.pool_id != context.pool_id
            ):
                self._operations[context.operation_id] = {
                    "action": "RELEASE",
                    "allocation_id": allocation_id,
                    "assignment_id": assignment_id,
                    "success": False,
                }
                self._pool_fences[context.pool_id] = next_fence
                return False
            if assignment.status == "ACTIVE":
                assignment.status = "RELEASED"
                assignment.released_at = at
                self._staff[assignment.staff_id].active_assignment_count -= 1
            self._operations[context.operation_id] = {
                "action": "RELEASE",
                "allocation_id": allocation_id,
                "assignment_id": assignment_id,
                "success": True,
            }
            self._pool_fences[context.pool_id] = next_fence
            return True

    def check_ready(self, timeout_s: float = 0.5) -> bool:
        return True
