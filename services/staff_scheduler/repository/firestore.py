"""Firestore-backed Staff Scheduler repository with atomic assignment counts."""

import hashlib
import json
import os
from datetime import datetime
from typing import Any

from google.api_core.exceptions import (
    AlreadyExists,
    DeadlineExceeded,
    GoogleAPICallError,
)
from google.cloud import firestore
from starlette import status

from services.staff_scheduler.models import (
    StaffAssignment,
    StaffCreate,
    StaffRecord,
    StaffShift,
    ShiftCreate,
)
from shared.observability.errors import AppException


def _fence(context: Any, previous: dict[str, Any] | None) -> dict[str, str]:
    token = int(context.fencing_token)
    if token < 0:
        raise AppException("FAILED_PRECONDITION", "Invalid fencing token", status.HTTP_409_CONFLICT)
    token_text = str(token)
    if previous:
        old_text = previous.get("fencing_token", "0")
        try:
            old_token = int(old_text)
        except (TypeError, ValueError) as error:
            raise AppException("FAILED_PRECONDITION", "Stored pool fence is invalid", status.HTTP_409_CONFLICT) from error
        if str(old_token) != old_text:
            raise AppException("FAILED_PRECONDITION", "Stored pool fence is not canonical", status.HTTP_409_CONFLICT)
        if token < old_token or (
            token == old_token
            and (context.lock_id != previous.get("lock_id") or context.owner_id != previous.get("owner_id"))
        ):
            raise AppException(
                "FAILED_PRECONDITION",
                "Mutation context fencing token is stale",
                status.HTTP_409_CONFLICT,
            )
    return {
        "pool_id": context.pool_id,
        "fencing_token": token_text,
        "lock_id": context.lock_id,
        "owner_id": context.owner_id,
    }


class FirestoreStaffRepository:
    def __init__(self, project_id: str | None = None) -> None:
        project = project_id or os.environ.get("FIRESTORE_PROJECT_ID")
        if not project:
            raise ValueError("FIRESTORE_PROJECT_ID is required for the Firestore backend")
        self.client = firestore.Client(project=project)
        self.staff = self.client.collection("staff")
        self.assignments = self.client.collection("staff_assignments")
        self.operations = self.client.collection("staff_operation_results")
        self.pool_fences = self.client.collection("staff_pool_fence")
        self.idempotency = self.client.collection("staff_idempotency")

    @staticmethod
    def _dependency_call(callable_, *args, **kwargs):
        try:
            return callable_(*args, **kwargs)
        except DeadlineExceeded as error:
            raise AppException("DEADLINE_EXCEEDED", "Firestore operation timed out", status.HTTP_504_GATEWAY_TIMEOUT, retryable=True) from error
        except AlreadyExists:
            raise
        except GoogleAPICallError as error:
            raise AppException("SERVICE_UNAVAILABLE", "Firestore is unavailable", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True) from error

    def check_ready(self, timeout_s: float = 0.5) -> bool:
        try:
            self.staff.document("_service_health").get(timeout=timeout_s)
            return True
        except (GoogleAPICallError, DeadlineExceeded):
            return False

    def create_staff(self, staff: StaffCreate, idempotency_key: str) -> StaffRecord:
        record = StaffRecord(**staff.model_dump(), active_assignment_count=0)
        ref = self.staff.document(staff.staff_id)
        endpoint = "POST:/api/v1/staff"
        request = staff.model_dump(mode="json")
        fingerprint = hashlib.sha256(
            json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        idempotency_ref = self.idempotency.document(
            hashlib.sha256(f"{endpoint}:{idempotency_key}".encode()).hexdigest()
        )

        @firestore.transactional
        def apply(transaction):
            idempotency_snapshot = idempotency_ref.get(transaction=transaction)
            if idempotency_snapshot.exists:
                stored = idempotency_snapshot.to_dict()
                if stored.get("request_hash") != fingerprint:
                    raise AppException("IDEMPOTENCY_MISMATCH", "Idempotency-Key was reused for a different request", status.HTTP_409_CONFLICT)
                return StaffRecord(**stored["response"])

            existing = ref.get(transaction=transaction)
            if existing.exists:
                raise AppException("STATE_CONFLICT", "Staff member already exists", status.HTTP_409_CONFLICT)
            transaction.create(ref, record.model_dump(mode="json"))
            transaction.create(idempotency_ref, {
                "request_hash": fingerprint,
                "response": record.model_dump(mode="json"),
            })
            return record

        return self._dependency_call(apply, self.client.transaction())

    def get_staff(self, staff_id: str) -> StaffRecord | None:
        snapshot = self._dependency_call(self.staff.document(staff_id).get, timeout=1.0)
        return StaffRecord(**snapshot.to_dict()) if snapshot.exists else None

    def create_shift(
        self,
        staff_id: str,
        shift_id: str,
        shift: ShiftCreate,
        idempotency_key: str,
    ) -> StaffShift:
        staff_ref = self.staff.document(staff_id)
        record = StaffShift(shift_id=shift_id, **shift.model_dump())
        endpoint = f"PUT:/api/v1/staff/{staff_id}/shifts/{shift_id}"
        request = {
            "staff_id": staff_id,
            "shift_id": shift_id,
            **shift.model_dump(mode="json"),
        }
        fingerprint = hashlib.sha256(
            json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        idempotency_ref = self.idempotency.document(
            hashlib.sha256(f"{endpoint}:{idempotency_key}".encode()).hexdigest()
        )
        shift_ref = staff_ref.collection("shifts").document(shift_id)

        @firestore.transactional
        def apply(transaction):
            idempotency_snapshot = idempotency_ref.get(transaction=transaction)
            if idempotency_snapshot.exists:
                stored = idempotency_snapshot.to_dict()
                if stored.get("request_hash") != fingerprint:
                    raise AppException("IDEMPOTENCY_MISMATCH", "Idempotency-Key was reused for a different request", status.HTTP_409_CONFLICT)
                return StaffShift(**stored["response"])
            staff_snapshot = staff_ref.get(transaction=transaction)
            if not staff_snapshot.exists:
                raise AppException("NOT_FOUND", "Staff member was not found", status.HTTP_404_NOT_FOUND)
            transaction.set(shift_ref, record.model_dump())
            transaction.create(idempotency_ref, {
                "request_hash": fingerprint,
                "response": record.model_dump(mode="json"),
            })
            return record

        return self._dependency_call(apply, self.client.transaction())

    def list_shifts(self, staff_id: str) -> list[StaffShift]:
        staff_ref = self.staff.document(staff_id)
        if not self._dependency_call(staff_ref.get, timeout=1.0).exists:
            raise AppException("NOT_FOUND", "Staff member was not found", status.HTTP_404_NOT_FOUND)
        snapshots = self._dependency_call(
            lambda: list(staff_ref.collection("shifts").stream(timeout=1.0))
        )
        return [StaffShift(**snapshot.to_dict()) for snapshot in snapshots]

    def _candidate_snapshots(self, pool_id: str, transaction=None):
        query = self.staff.where("pool_ids", "array_contains", pool_id)
        if transaction is None:
            return list(query.stream(timeout=1.0))
        return list(transaction.get(query))

    @staticmethod
    def _on_shift(staff_ref, at: datetime, transaction=None) -> bool:
        if transaction is None:
            shifts = list(staff_ref.collection("shifts").stream(timeout=1.0))
        else:
            shifts = list(transaction.get(staff_ref.collection("shifts")))
        for snapshot in shifts:
            shift = snapshot.to_dict()
            starts_at = shift.get("starts_at")
            ends_at = shift.get("ends_at")
            if shift.get("active") and starts_at and ends_at and starts_at <= at < ends_at:
                return True
        return False

    def _is_available(self, snapshot, pool_id: str, at: datetime) -> bool:
        staff = snapshot.to_dict()
        return (
            staff.get("active", False)
            and staff.get("role") == "NURSE"
            and pool_id in staff.get("pool_ids", [])
            and int(staff.get("active_assignment_count", 0))
            < int(staff.get("max_concurrent_assignments", 1))
            and self._on_shift(snapshot.reference, at)
        )

    def check_capacity(self, pool_id: str, at: datetime) -> int:
        candidates = self._dependency_call(lambda: self._candidate_snapshots(pool_id))
        count = 0
        for snapshot in candidates:
            if self._is_available(snapshot, pool_id, at):
                staff = snapshot.to_dict()
                count += int(staff.get("max_concurrent_assignments", 1)) - int(
                    staff.get("active_assignment_count", 0)
                )
        return count

    def assign(
        self,
        context,
        allocation_id: str,
        bed_id: str,
        at: datetime,
    ) -> StaffAssignment | None:
        if not context.operation_id or not context.lock_id or not context.owner_id or not context.pool_id:
            raise AppException("INVALID_ARGUMENT", "MutationContext identity fields are required", status.HTTP_400_BAD_REQUEST)
        operation_ref = self.operations.document(
            hashlib.sha256(context.operation_id.encode()).hexdigest()
        )
        fence_ref = self.pool_fences.document(context.pool_id)

        @firestore.transactional
        def apply(transaction):
            operation_snapshot = operation_ref.get(transaction=transaction)
            if operation_snapshot.exists:
                stored = operation_snapshot.to_dict()
                if (
                    stored.get("action") != "ASSIGN"
                    or stored.get("operation_id") != context.operation_id
                    or stored.get("allocation_id") != allocation_id
                    or stored.get("bed_id") != bed_id
                    or stored.get("pool_id") != context.pool_id
                ):
                    raise AppException("IDEMPOTENCY_MISMATCH", "operation_id was reused for another assignment", status.HTTP_409_CONFLICT)
                assignment_id = stored.get("assignment_id")
                if not assignment_id:
                    return None
                assignment_snapshot = self.assignments.document(assignment_id).get(transaction=transaction)
                if not assignment_snapshot.exists:
                    raise AppException("OUTCOME_UNKNOWN", "Assignment operation record refers to a missing assignment", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True)
                return StaffAssignment(**assignment_snapshot.to_dict())

            fence_snapshot = fence_ref.get(transaction=transaction)
            candidates = self._candidate_snapshots(context.pool_id, transaction)
            available: list[tuple[Any, dict[str, Any]]] = []
            for snapshot in sorted(candidates, key=lambda item: item.id):
                data = snapshot.to_dict()
                if (
                    data.get("active", False)
                    and data.get("role") == "NURSE"
                    and int(data.get("active_assignment_count", 0))
                    < int(data.get("max_concurrent_assignments", 1))
                    and self._on_shift(snapshot.reference, at, transaction)
                ):
                    available.append((snapshot, data))

            # Every required transaction read is complete before writing either record.
            next_fence = _fence(context, fence_snapshot.to_dict() if fence_snapshot.exists else None)
            if not available:
                transaction.set(operation_ref, {
                    "operation_id": context.operation_id,
                    "action": "ASSIGN",
                    "allocation_id": allocation_id,
                    "bed_id": bed_id,
                    "pool_id": context.pool_id,
                    "assignment_id": None,
                    "created_at": at,
                })
                transaction.set(fence_ref, next_fence)
                return None

            staff_snapshot, staff_data = available[0]
            assignment_id = f"staff-assignment-{hashlib.sha256(context.operation_id.encode()).hexdigest()[:32]}"
            assignment = StaffAssignment(
                assignment_id=assignment_id,
                staff_id=staff_snapshot.id,
                allocation_id=allocation_id,
                pool_id=context.pool_id,
                bed_id=bed_id,
                operation_id=context.operation_id,
                status="ACTIVE",
                assigned_at=at,
            )
            transaction.update(staff_snapshot.reference, {
                "active_assignment_count": int(staff_data.get("active_assignment_count", 0)) + 1,
            })
            transaction.create(
                self.assignments.document(assignment_id),
                assignment.model_dump(mode="json"),
            )
            transaction.set(operation_ref, {
                "operation_id": context.operation_id,
                "action": "ASSIGN",
                "allocation_id": allocation_id,
                "bed_id": bed_id,
                "pool_id": context.pool_id,
                "assignment_id": assignment_id,
                "created_at": at,
            })
            transaction.set(fence_ref, next_fence)
            return assignment

        return self._dependency_call(apply, self.client.transaction())

    def release(
        self,
        context,
        assignment_id: str,
        allocation_id: str,
        at: datetime,
    ) -> bool:
        if not context.operation_id or not context.lock_id or not context.owner_id or not context.pool_id:
            raise AppException("INVALID_ARGUMENT", "MutationContext identity fields are required", status.HTTP_400_BAD_REQUEST)
        operation_ref = self.operations.document(
            hashlib.sha256(context.operation_id.encode()).hexdigest()
        )
        assignment_ref = self.assignments.document(assignment_id)
        fence_ref = self.pool_fences.document(context.pool_id)

        @firestore.transactional
        def apply(transaction):
            operation_snapshot = operation_ref.get(transaction=transaction)
            if operation_snapshot.exists:
                stored = operation_snapshot.to_dict()
                if (
                    stored.get("action") != "RELEASE"
                    or stored.get("operation_id") != context.operation_id
                    or stored.get("allocation_id") != allocation_id
                    or stored.get("assignment_id") != assignment_id
                    or stored.get("pool_id") != context.pool_id
                ):
                    raise AppException("IDEMPOTENCY_MISMATCH", "operation_id was reused for another release", status.HTTP_409_CONFLICT)
                return bool(stored.get("success"))

            fence_snapshot = fence_ref.get(transaction=transaction)
            assignment_snapshot = assignment_ref.get(transaction=transaction)
            staff_ref = None
            staff_snapshot = None
            assignment_data = assignment_snapshot.to_dict() if assignment_snapshot.exists else None
            if assignment_data is not None:
                staff_ref = self.staff.document(assignment_data["staff_id"])
                staff_snapshot = staff_ref.get(transaction=transaction)

            # Complete transaction reads before writes, including lease fence validation.
            next_fence = _fence(context, fence_snapshot.to_dict() if fence_snapshot.exists else None)
            success = bool(
                assignment_data
                and assignment_data.get("allocation_id") == allocation_id
                and assignment_data.get("pool_id") == context.pool_id
                and assignment_data.get("status") == "ACTIVE"
                and staff_snapshot is not None
                and staff_snapshot.exists
            )
            if success:
                transaction.update(assignment_ref, {"status": "RELEASED", "released_at": at})
                current_count = int(staff_snapshot.to_dict().get("active_assignment_count", 0))
                if current_count < 1:
                    raise AppException("STATE_CONFLICT", "Staff assignment counter is inconsistent", status.HTTP_409_CONFLICT)
                transaction.update(staff_ref, {"active_assignment_count": current_count - 1})
            transaction.set(operation_ref, {
                "operation_id": context.operation_id,
                "action": "RELEASE",
                "allocation_id": allocation_id,
                "assignment_id": assignment_id,
                "pool_id": context.pool_id,
                "success": success,
                "created_at": at,
            })
            transaction.set(fence_ref, next_fence)
            return success

        return bool(self._dependency_call(apply, self.client.transaction()))
