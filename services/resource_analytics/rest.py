"""
REST API router for Resource Analytics Service.
Owned by Member D (Jay Patel).
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, Path, status
from services.resource_analytics.models import (
    BedEvent, BedStats, PoolSummaryResponse, HospitalAnalyticsSummary
)
from services.resource_analytics.storage import BaseAnalyticsStorage
from services.resource_analytics.aggregator import StreamAggregatorEngine
from shared.observability.errors import AppException

router = APIRouter(prefix="/api/v1/analytics", tags=["Resource Analytics"])

_STORAGE: Optional[BaseAnalyticsStorage] = None
_AGGREGATOR: Optional[StreamAggregatorEngine] = None

def init_services(storage: BaseAnalyticsStorage, aggregator: StreamAggregatorEngine):
    global _STORAGE, _AGGREGATOR
    _STORAGE = storage
    _AGGREGATOR = aggregator

def get_storage() -> BaseAnalyticsStorage:
    if _STORAGE is None:
        raise AppException(
            code="SERVICE_UNAVAILABLE",
            message="Analytics storage not initialized",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE
        )
    return _STORAGE

def get_aggregator() -> StreamAggregatorEngine:
    if _AGGREGATOR is None:
        raise AppException(
            code="SERVICE_UNAVAILABLE",
            message="Stream aggregator not initialized",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE
        )
    return _AGGREGATOR

@router.get("/pools/{pool_id}/latest", response_model=BedStats)
def get_latest_pool_stats(
    pool_id: str = Path(..., description="Resource pool ID (e.g. WARDA-ICU)"),
    storage: BaseAnalyticsStorage = Depends(get_storage)
):
    stats = storage.get_latest_stats(pool_id)
    if not stats:
        raise AppException(
            code="NOT_FOUND",
            message=f"No analytics stats found for pool '{pool_id}'",
            status_code=status.HTTP_404_NOT_FOUND
        )
    return stats

@router.get("/pools/{pool_id}/history", response_model=List[BedStats])
def get_pool_history(
    pool_id: str = Path(...),
    limit: int = Query(60, ge=1, le=500),
    storage: BaseAnalyticsStorage = Depends(get_storage)
):
    return storage.get_stats_history(pool_id, limit=limit)

@router.get("/summary", response_model=HospitalAnalyticsSummary)
def get_hospital_summary(
    storage: BaseAnalyticsStorage = Depends(get_storage)
):
    all_latest = storage.get_all_pools_latest()
    pool_summaries = []
    total_req = 0
    total_alloc = 0
    total_wait = 0

    for s in all_latest:
        total_req += s.requests
        total_alloc += s.allocated
        total_wait += s.waitlisted
        pool_summaries.append(
            PoolSummaryResponse(
                pool_id=s.pool_id,
                current_occupancy=s.occupancy_at_window_end,
                total_requests=s.requests,
                total_allocated=s.allocated,
                total_waitlisted=s.waitlisted,
                average_contention_rate=s.contention_rate,
                latest_window_end=s.window_end
            )
        )

    sys_contention = (total_wait / total_req) if total_req > 0 else 0.0

    return HospitalAnalyticsSummary(
        active_pools=len(pool_summaries),
        total_requests=total_req,
        total_allocated=total_alloc,
        total_waitlisted=total_wait,
        system_contention_rate=round(sys_contention, 4),
        pools=pool_summaries
    )

@router.post("/ingest", response_model=List[BedStats])
def ingest_event(
    event: BedEvent,
    force_flush: bool = Query(False, description="Force flush current window for instant evaluation"),
    storage: BaseAnalyticsStorage = Depends(get_storage),
    aggregator: StreamAggregatorEngine = Depends(get_aggregator)
):
    """
    Ingests a raw BedEvent into the stream aggregator.
    Used for local testing, live demos, and event simulation.
    """
    closed_windows = aggregator.process_event(event)
    if force_flush:
        flushed = aggregator.flush_all_windows()
        closed_windows.extend(flushed)

    for stats in closed_windows:
        storage.save_window_stats(stats)

    return closed_windows

