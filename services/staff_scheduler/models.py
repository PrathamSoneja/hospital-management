"""Typed Staff Scheduler records and REST payloads."""

from datetime import datetime, timezone
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StaffRole(StrEnum):
    NURSE = "NURSE"
    TECHNICIAN = "TECHNICIAN"


class StaffCreate(BaseModel):
    staff_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._-]+$")
    display_name: str = Field(min_length=1, max_length=100)
    role: StaffRole = StaffRole.NURSE
    pool_ids: list[str] = Field(min_length=1, max_length=100)
    active: bool = True
    max_concurrent_assignments: int = Field(default=1, ge=1, le=20)

    @field_validator("pool_ids")
    @classmethod
    def normalize_pools(cls, pool_ids: list[str]) -> list[str]:
        normalized = sorted({pool.strip() for pool in pool_ids if pool.strip()})
        if not normalized:
            raise ValueError("At least one non-empty pool_id is required")
        if len(normalized) != len(pool_ids):
            raise ValueError("pool_ids must not contain duplicates or blank values")
        return normalized


class StaffRecord(StaffCreate):
    model_config = ConfigDict(from_attributes=True)

    active_assignment_count: int = Field(default=0, ge=0)


class ShiftCreate(BaseModel):
    starts_at: datetime
    ends_at: datetime
    active: bool = True

    @model_validator(mode="after")
    def validate_shift(self) -> "ShiftCreate":
        if self.starts_at.tzinfo is None or self.ends_at.tzinfo is None:
            raise ValueError("Shift timestamps must include a timezone")
        if self.ends_at <= self.starts_at:
            raise ValueError("ends_at must be later than starts_at")
        self.starts_at = self.starts_at.astimezone(timezone.utc)
        self.ends_at = self.ends_at.astimezone(timezone.utc)
        return self


class StaffShift(ShiftCreate):
    model_config = ConfigDict(from_attributes=True)

    shift_id: str


class StaffAssignment(BaseModel):
    assignment_id: str
    staff_id: str
    allocation_id: str
    pool_id: str
    bed_id: str
    operation_id: str
    status: str
    assigned_at: datetime
    released_at: datetime | None = None
