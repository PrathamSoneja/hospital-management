"""
FastAPI REST router for Triage Service.
Conforms strictly to contracts/openapi/triage.openapi.yaml.
"""
from typing import Optional
import logging
from fastapi import APIRouter, Depends, Header, Query, Request
from services.triage.models import SetTriageRequest, TriageRecord, WaitlistPage
from services.triage.service import TriageDomainService

router = APIRouter(prefix="/api/v1")
logger = logging.getLogger("triage-service")

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
    request: Request,
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    service: TriageDomainService = Depends(get_service)
):
    request_id = getattr(request.state, "request_id", "unknown")
    record = service.put_triage(
        admission_id=admission_id,
        triage_level=body.triage_level,
        pool_id=body.pool_id,
        idempotency_key=idempotency_key or "",
        request_id=request_id,
    )
    logger.info("triage_assessment_committed", extra={
        "event": "triage_assessment_committed", "request_id": request_id,
        "pool_id": record.pool_id,
    })
    return record

@router.get("/triage/{admission_id}", response_model=TriageRecord)
def get_triage_endpoint(
    admission_id: str,
    request: Request,
    service: TriageDomainService = Depends(get_service)
):
    record = service.get_triage(admission_id)
    logger.info("triage_assessment_read", extra={"event": "triage_assessment_read", "request_id": getattr(request.state, "request_id", "unknown"), "pool_id": record.pool_id})
    return record

@router.get("/waitlists/{pool_id}", response_model=WaitlistPage)
def get_waitlist_endpoint(
    pool_id: str,
    request: Request,
    limit: int = Query(50, ge=1, le=200),
    cursor: Optional[str] = Query(None),
    service: TriageDomainService = Depends(get_service)
):
    items, next_cursor = service.list_waitlist(pool_id=pool_id, limit=limit, cursor=cursor)
    logger.info("triage_waitlist_read", extra={"event": "triage_waitlist_read", "request_id": getattr(request.state, "request_id", "unknown"), "pool_id": pool_id})
    return WaitlistPage(items=items, next_cursor=next_cursor)
