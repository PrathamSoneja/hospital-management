"""Staff scheduling business logic shared by REST and gRPC transports."""

import logging
from datetime import datetime, timezone

from services.staff_scheduler.models import (
    StaffAssignment,
    StaffCreate,
    StaffRecord,
    StaffShift,
    ShiftCreate,
)
from services.staff_scheduler.repository.base import StaffRepository
from shared.observability.errors import AppException
from starlette import status

logger = logging.getLogger("staff-scheduler")


class StaffSchedulerService:
    def __init__(self, repository: StaffRepository) -> None:
        self.repository = repository

    def create_staff(self, staff: StaffCreate, idempotency_key: str) -> StaffRecord:
        if not idempotency_key.strip():
            raise AppException("INVALID_ARGUMENT", "Idempotency-Key is required", status.HTTP_400_BAD_REQUEST)
        return self.repository.create_staff(staff, idempotency_key.strip())

    def get_staff(self, staff_id: str) -> StaffRecord:
        staff = self.repository.get_staff(staff_id)
        if staff is None:
            raise AppException("NOT_FOUND", "Staff member was not found", status.HTTP_404_NOT_FOUND)
        return staff

    def create_shift(
        self,
        staff_id: str,
        shift_id: str,
        shift: ShiftCreate,
        idempotency_key: str,
    ) -> StaffShift:
        if not idempotency_key.strip():
            raise AppException("INVALID_ARGUMENT", "Idempotency-Key is required", status.HTTP_400_BAD_REQUEST)
        return self.repository.create_shift(
            staff_id,
            shift_id,
            shift,
            idempotency_key.strip(),
        )

    def list_shifts(self, staff_id: str) -> list[StaffShift]:
        return self.repository.list_shifts(staff_id)

    def check_capacity(self, pool_id: str) -> int:
        return self.repository.check_capacity(pool_id, datetime.now(timezone.utc))

    def assign(
        self,
        context,
        allocation_id: str,
        bed_id: str,
    ) -> StaffAssignment | None:
        if not allocation_id or not bed_id or not context.pool_id:
            raise AppException(
                "INVALID_ARGUMENT",
                "pool_id, allocation_id, and bed_id are required",
                status.HTTP_400_BAD_REQUEST,
            )
        assignment = self.repository.assign(
            context,
            allocation_id,
            bed_id,
            datetime.now(timezone.utc),
        )
        logger.info(
            "Staff assignment attempted",
            extra={
                "event": "staff_assignment_succeeded" if assignment else "staff_capacity_exhausted",
                "request_id": context.request_id,
                "pool_id": context.pool_id,
                "allocation_id": allocation_id,
                "operation_id": context.operation_id,
            },
        )
        return assignment

    def release(
        self,
        context,
        assignment_id: str,
        allocation_id: str,
    ) -> bool:
        if not assignment_id or not allocation_id or not context.pool_id:
            raise AppException(
                "INVALID_ARGUMENT",
                "pool_id, staff_assignment_id, and allocation_id are required",
                status.HTTP_400_BAD_REQUEST,
            )
        return self.repository.release(
            context,
            assignment_id,
            allocation_id,
            datetime.now(timezone.utc),
        )
