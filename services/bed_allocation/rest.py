"""
FastAPI REST router for Bed Allocation Service.
Conforms strictly to contracts/openapi/allocation.openapi.yaml.
"""
from typing import Optional
from fastapi import APIRouter, Depends, Header, Query, Request, Response, status
from services.bed_allocation.models import (
    AllocationPage, AllocationResponse, CancelRequest,
    CreateAllocationRequest, ReleaseRequest, ReleaseResponse
)
from services.bed_allocation.service import BedAllocationDomainService

router = APIRouter(prefix="/api/v1")

_allocation_service: Optional[BedAllocationDomainService] = None

def get_service() -> BedAllocationDomainService:
    global _allocation_service
    if _allocation_service is None:
        raise RuntimeError("BedAllocationDomainService is not initialized")
    return _allocation_service

def set_service(service: BedAllocationDomainService):
    global _allocation_service
    _allocation_service = service

def _extract_request_id(request: Request, x_request_id: Optional[str]) -> str:
    if isinstance(x_request_id, str) and x_request_id:
        return x_request_id
    if hasattr(request, "state") and hasattr(request.state, "request_id"):
        return str(request.state.request_id)
    return "req-unknown"

@router.post("/allocations", response_model=AllocationResponse, status_code=status.HTTP_201_CREATED)
def create_allocation_endpoint(
    req: CreateAllocationRequest,
    request: Request,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    x_request_id: Optional[str] = Header(None, alias="X-Request-ID"),
    service: BedAllocationDomainService = Depends(get_service)
):
    req_id = _extract_request_id(request, x_request_id)
    return service.create_allocation(req=req, request_id=req_id, idempotency_key=idempotency_key)

@router.get("/allocations/{allocation_id}", response_model=AllocationResponse)
def get_allocation_endpoint(
    allocation_id: str,
    service: BedAllocationDomainService = Depends(get_service)
):
    return service.get_allocation(allocation_id)

@router.get("/allocations", response_model=AllocationPage)
def list_allocations_endpoint(
    pool_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    admission_id: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    cursor: Optional[str] = Query(None),
    service: BedAllocationDomainService = Depends(get_service)
):
    items, next_cursor = service.list_allocations(
        pool_id=pool_id, status_filter=status, admission_id=admission_id, limit=limit, cursor=cursor
    )
    return AllocationPage(items=items, next_cursor=next_cursor)

@router.post("/allocations/{allocation_id}/release", response_model=ReleaseResponse)
def release_allocation_endpoint(
    allocation_id: str,
    req: ReleaseRequest,
    request: Request,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    x_request_id: Optional[str] = Header(None, alias="X-Request-ID"),
    service: BedAllocationDomainService = Depends(get_service)
):
    req_id = _extract_request_id(request, x_request_id)
    return service.release_allocation(
        allocation_id=allocation_id, reason=req.reason, request_id=req_id, idempotency_key=idempotency_key
    )

@router.post("/allocations/{allocation_id}/cancel", response_model=AllocationResponse)
def cancel_allocation_endpoint(
    allocation_id: str,
    req: CancelRequest,
    request: Request,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    x_request_id: Optional[str] = Header(None, alias="X-Request-ID"),
    service: BedAllocationDomainService = Depends(get_service)
):
    req_id = _extract_request_id(request, x_request_id)
    return service.cancel_allocation(
        allocation_id=allocation_id, reason=req.reason, request_id=req_id, idempotency_key=idempotency_key
    )
