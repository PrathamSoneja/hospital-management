"""
FastAPI REST router for Triage Service.
Conforms strictly to contracts/openapi/triage.openapi.yaml.
"""
from typing import Optional
from fastapi import APIRouter, Depends, Query
from services.triage.models import SetTriageRequest, TriageRecord, WaitlistPage
from services.triage.service import TriageDomainService

router = APIRouter(prefix="/api/v1")

# Global domain service instance set at startup or injected for testing
_triage_service: Optional[TriageDomainService] = None

def get_service() -> TriageDomainService:
    global _triage_service
    if _triage_service is None:
        raise RuntimeError("TriageDomainService is not initialized")
    return _triage_service

def set_service(service: TriageDomainService):
    global _triage_service
    _triage_service = service

@router.put("/triage/{admission_id}", response_model=TriageRecord)
def set_triage_endpoint(
    admission_id: str,
    body: SetTriageRequest,
    service: TriageDomainService = Depends(get_service)
):
    return service.set_triage(
        admission_id=admission_id,
        triage_level=body.triage_level,
        pool_id=body.pool_id
    )

@router.get("/triage/{admission_id}", response_model=TriageRecord)
def get_triage_endpoint(
    admission_id: str,
    service: TriageDomainService = Depends(get_service)
):
    return service.get_triage(admission_id)

@router.get("/waitlists/{pool_id}", response_model=WaitlistPage)
def get_waitlist_endpoint(
    pool_id: str,
    limit: int = Query(50, ge=1, le=200),
    cursor: Optional[str] = Query(None),
    service: TriageDomainService = Depends(get_service)
):
    items, next_cursor = service.list_waitlist(pool_id=pool_id, limit=limit, cursor=cursor)
    return WaitlistPage(items=items, next_cursor=next_cursor)
