"""
Event and Analytics data models conforming to project contracts.
Owned by Member D (Jay Patel).
"""
from datetime import datetime, timezone
from typing import List, Optional, Literal
from pydantic import BaseModel, Field

EventType = Literal[
    "ALLOCATION_REQUESTED",
    "BED_ALLOCATED",
    "ALLOCATION_WAITLISTED",
    "BED_RELEASED",
    "ALLOCATION_CONFLICT"
]

class BedEvent(BaseModel):
    """Event model matching contracts/events/bed-event.schema.json"""
    schema_version: str = "1.0.0"
    event_id: str
    stream_sequence: int = Field(..., ge=1)
    event_type: EventType
    event_time: str
    request_id: str
    allocation_id: str
    admission_id: str
    patient_id: str
    pool_id: str
    bed_id: Optional[str] = None
    lock_wait_ms: Optional[int] = None
    lock_contended: bool = False
    allocation_wait_ms: Optional[int] = None
    occupancy_delta: int = Field(..., ge=-1, le=1)

class BedStats(BaseModel):
    """Aggregated window output matching contracts/events/bed-stats.schema.json"""
    schema_version: str = "1.0.0"
    pool_id: str
    window_start: str
    window_end: str
    requests: int = Field(..., ge=0)
    allocated: int = Field(..., ge=0)
    waitlisted: int = Field(..., ge=0)
    conflicts: int = Field(..., ge=0)
    occupancy_at_window_end: int
    average_lock_wait_ms: Optional[float] = None
    average_allocation_wait_ms: Optional[float] = None
    contention_rate: float = Field(..., ge=0.0, le=1.0)

class PoolSummaryResponse(BaseModel):
    pool_id: str
    current_occupancy: int
    total_requests: int
    total_allocated: int
    total_waitlisted: int
    average_contention_rate: float
    latest_window_end: Optional[str] = None

class HospitalAnalyticsSummary(BaseModel):
    active_pools: int
    total_requests: int
    total_allocated: int
    total_waitlisted: int
    system_contention_rate: float
    pools: List[PoolSummaryResponse]

