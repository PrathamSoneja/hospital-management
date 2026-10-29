"""
Pydantic schemas and API contracts for Bed Allocation Service.
"""
from datetime import datetime, timezone
from typing import List, Optional
from pydantic import BaseModel, Field

class CreateAllocationRequest(BaseModel):
    admission_id: str = Field(..., min_length=1)
    pool_id: str = Field(..., min_length=1)

class AllocationResponse(BaseModel):
    allocation_id: str
    admission_id: str
    pool_id: str
    status: str # ALLOCATED, WAITLISTED, RELEASED, CANCELLED, RECOVERING
    bed_id: Optional[str] = None
    staff_assignment_id: Optional[str] = None
    waitlist_entry_id: Optional[str] = None
    pending_action: Optional[str] = None
    created_at: str

class ReleaseRequest(BaseModel):
    reason: str = Field(..., min_length=1)

class ReleaseResponse(BaseModel):
    allocation_id: str
    status: str = "RELEASED"
    promoted_allocation_id: Optional[str] = None
    pending_action: Optional[str] = None

class CancelRequest(BaseModel):
    reason: str = Field(..., min_length=1)

class AllocationPage(BaseModel):
    items: List[AllocationResponse]
    next_cursor: Optional[str] = None
