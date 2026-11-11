"""Repository contract for Staff Scheduler persistence."""

from datetime import datetime
from typing import Protocol

from services.staff_scheduler.models import (
    StaffAssignment,
    StaffCreate,
    StaffRecord,
    StaffShift,
    ShiftCreate,
)


class StaffRepository(Protocol):
    def create_staff(self, staff: StaffCreate, idempotency_key: str) -> StaffRecord: ...

    def get_staff(self, staff_id: str) -> StaffRecord | None: ...

    def create_shift(
        self,
        staff_id: str,
        shift_id: str,
        shift: ShiftCreate,
        idempotency_key: str,
    ) -> StaffShift: ...

    def list_shifts(self, staff_id: str) -> list[StaffShift]: ...

    def check_capacity(self, pool_id: str, at: datetime) -> int: ...

    def assign(
        self,
        context,
        allocation_id: str,
        bed_id: str,
        at: datetime,
    ) -> StaffAssignment | None: ...

    def release(
        self,
        context,
        assignment_id: str,
        allocation_id: str,
        at: datetime,
    ) -> bool: ...

    def check_ready(self, timeout_s: float = 0.5) -> bool: ...
