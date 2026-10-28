"""
Domain models and schema definitions for Triage and Waitlist entities.
"""
from datetime import datetime, timezone
from typing import List, Optional
from pydantic import BaseModel, Field, field_validator

class TriageRecord(BaseModel):
    admission_id: str
    triage_level: int = Field(..., ge=1, le=5, description="1 is most urgent, 5 is least urgent")
    pool_id: str
    updated_at: str

class SetTriageRequest(BaseModel):
    triage_level: int = Field(..., ge=1, le=5, description="1 is most urgent, 5 is least urgent")
    pool_id: str = Field(..., min_length=1)

class WaitlistEntry(BaseModel):
    entry_id: str
    allocation_id: str
    admission_id: str
    pool_id: str
    triage_level: int = Field(..., ge=1, le=5)
    queued_at: str

class WaitlistPage(BaseModel):
    items: List[WaitlistEntry]
    next_cursor: Optional[str] = None
