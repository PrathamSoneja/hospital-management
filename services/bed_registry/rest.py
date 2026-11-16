"""
REST API router for Bed Registry Service.
Owned by Member D (Jay Patel).
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, Path, Request, status
from services.bed_registry.models import (
    BedDto, CreateBedRequest, BedListResponse, FencingTokenResponse,
    AllocateBedRestRequest, ReleaseBedRestRequest, OperationResultResponse
)
from services.bed_registry.service import BedRegistryDomainService
from shared.observability.errors import AppException

router = APIRouter(prefix="/api/v1", tags=["Beds"])

_SERVICE: Optional[BedRegistryDomainService] = None

def set_service(service: BedRegistryDomainService):
    global _SERVICE
    _SERVICE = service

def get_service() -> BedRegistryDomainService:
    if _SERVICE is None:
        raise AppException(
            code="SERVICE_UNAVAILABLE",
            message="BedRegistryDomainService not initialized",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE
        )
    return _SERVICE

@router.get("/beds", response_model=BedListResponse)
def list_beds(
    pool_id: Optional[str] = Query(None, description="Filter by pool_id"),
    service: BedRegistryDomainService = Depends(get_service)
):
    if pool_id:
        beds = service.list_available_beds(pool_id)
    else:
        # If no pool specified, return all available beds from known pools or empty
        beds = []
    return BedListResponse(beds=beds, count=len(beds))

@router.post("/beds", response_model=BedDto, status_code=status.HTTP_201_CREATED)
def create_or_update_bed(
    payload: CreateBedRequest,
    service: BedRegistryDomainService = Depends(get_service)
):
    bed = service.create_or_update_bed(
        bed_id=payload.bed_id,
        pool_id=payload.pool_id,
        ward=payload.ward,
        status=payload.status or "AVAILABLE"
    )
    return bed

@router.get("/beds/{bed_id}", response_model=BedDto)
def get_bed(
    bed_id: str = Path(..., description="Bed identifier"),
    service: BedRegistryDomainService = Depends(get_service)
):
    bed = service.get_bed(bed_id)
    if not bed:
        raise AppException(
            code="NOT_FOUND",
            message=f"Bed '{bed_id}' not found",
            status_code=status.HTTP_404_NOT_FOUND
        )
    return bed

@router.get("/pools/{pool_id}/fencing-token", response_model=FencingTokenResponse)
def get_pool_fencing_token(
    pool_id: str = Path(..., description="Resource pool identifier"),
    service: BedRegistryDomainService = Depends(get_service)
):
    highest_token = service.get_highest_fencing_token(pool_id)
    return FencingTokenResponse(pool_id=pool_id, highest_fencing_token=highest_token)

@router.post("/beds/{bed_id}/allocate", response_model=OperationResultResponse)
def allocate_bed_rest(
    payload: AllocateBedRestRequest,
    bed_id: str = Path(...),
    request: Request = None,
    service: BedRegistryDomainService = Depends(get_service)
):
    req_id = getattr(request.state, "request_id", "unknown") if request else "unknown"
    bed = service.get_bed(bed_id)
    if not bed:
        raise AppException(
            code="NOT_FOUND",
            message=f"Bed '{bed_id}' not found",
            status_code=status.HTTP_404_NOT_FOUND
        )
    pool_id = payload.pool_id or bed.pool_id

    success, allocated_bed, err = service.allocate_bed(
        operation_id=payload.operation_id,
        pool_id=pool_id,
        bed_id=bed_id,
        allocation_id=payload.allocation_id,
        fencing_token=payload.fencing_token,
        request_id=req_id
    )

    if not success:
        if "Stale fencing token" in err:
            raise AppException(
                code="STALE_FENCING_TOKEN",
                message=err,
                status_code=status.HTTP_409_CONFLICT
            )
        raise AppException(
            code="STATE_CONFLICT",
            message=err,
            status_code=status.HTTP_409_CONFLICT
        )

    return OperationResultResponse(success=True, bed=allocated_bed, error_message="")

@router.post("/beds/{bed_id}/release", response_model=OperationResultResponse)
def release_bed_rest(
    payload: ReleaseBedRestRequest,
    bed_id: str = Path(...),
    request: Request = None,
    service: BedRegistryDomainService = Depends(get_service)
):
    req_id = getattr(request.state, "request_id", "unknown") if request else "unknown"
    bed = service.get_bed(bed_id)
    if not bed:
        raise AppException(
            code="NOT_FOUND",
            message=f"Bed '{bed_id}' not found",
            status_code=status.HTTP_404_NOT_FOUND
        )
    pool_id = payload.pool_id or bed.pool_id

    success, err = service.release_bed(
        operation_id=payload.operation_id,
        pool_id=pool_id,
        bed_id=bed_id,
        allocation_id=payload.allocation_id,
        fencing_token=payload.fencing_token,
        request_id=req_id
    )

    if not success:
        if "Stale fencing token" in err:
            raise AppException(
                code="STALE_FENCING_TOKEN",
                message=err,
                status_code=status.HTTP_409_CONFLICT
            )
        raise AppException(
            code="STATE_CONFLICT",
            message=err,
            status_code=status.HTTP_409_CONFLICT
        )

    return OperationResultResponse(success=True, bed=None, error_message="")

