"""REST API for staff roster administration and shift scheduling."""

import logging

from fastapi import APIRouter, Depends, Header, Request

from services.staff_scheduler.models import (
    StaffCreate,
    StaffRecord,
    StaffShift,
    ShiftCreate,
)
from services.staff_scheduler.service import StaffSchedulerService
from shared.observability.errors import AppException
from starlette import status

logger = logging.getLogger("staff-scheduler")
router = APIRouter(prefix="/api/v1")


def get_service(request: Request) -> StaffSchedulerService:
    service = getattr(request.app.state, "staff_scheduler", None)
    if service is None:
        raise AppException(
            "SERVICE_UNAVAILABLE",
            "Staff Scheduler is not initialized",
            status.HTTP_503_SERVICE_UNAVAILABLE,
            retryable=True,
        )
    return service


def _request_id(request: Request) -> str:
    return getattr(request.state, "request_id", "unknown")


@router.post("/staff", response_model=StaffRecord, status_code=status.HTTP_201_CREATED)
def create_staff_endpoint(
    payload: StaffCreate,
    request: Request,
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=1, max_length=128),
    service: StaffSchedulerService = Depends(get_service),
) -> StaffRecord:
    result = service.create_staff(payload, idempotency_key)
    logger.info(
        "Staff record created or replayed",
        extra={"event": "staff_record_written", "request_id": _request_id(request)},
    )
    return result


@router.get("/staff/{staff_id}", response_model=StaffRecord)
def get_staff_endpoint(
    staff_id: str,
    service: StaffSchedulerService = Depends(get_service),
) -> StaffRecord:
    return service.get_staff(staff_id)


@router.put("/staff/{staff_id}/shifts/{shift_id}", response_model=StaffShift)
def create_shift_endpoint(
    staff_id: str,
    shift_id: str,
    payload: ShiftCreate,
    request: Request,
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=1, max_length=128),
    service: StaffSchedulerService = Depends(get_service),
) -> StaffShift:
    result = service.create_shift(staff_id, shift_id, payload, idempotency_key)
    logger.info(
        "Staff shift created or updated",
        extra={"event": "staff_shift_written", "request_id": _request_id(request)},
    )
    return result


@router.get("/staff/{staff_id}/shifts", response_model=list[StaffShift])
def list_shifts_endpoint(
    staff_id: str,
    service: StaffSchedulerService = Depends(get_service),
) -> list[StaffShift]:
    return service.list_shifts(staff_id)
