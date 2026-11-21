"""
Database models and API schemas for Bed Registry Service.
Owned by Member D (Jay Patel).
"""
from datetime import datetime, timezone
from typing import List, Optional
from pydantic import BaseModel, Field
from sqlalchemy import (
    Column, String, BigInteger, DateTime, Index
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class BedModel(Base):
    """
    Relational model representing hospital bed inventory.
    Stored in Cloud SQL PostgreSQL 16 (or local PostgreSQL / SQLite).
    """
    __tablename__ = "beds"

    bed_id = Column(String(64), primary_key=True)
    pool_id = Column(String(64), nullable=False, index=True)
    ward = Column(String(64), nullable=False)
    status = Column(String(32), nullable=False, default="AVAILABLE", index=True) # AVAILABLE, OCCUPIED, MAINTENANCE
    current_allocation_id = Column(String(64), nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False
    )

    __table_args__ = (
        Index("idx_bed_pool_status", "pool_id", "status"),
    )

class PoolFencingTokenModel(Base):
    """
    Tracks the highest seen fencing token (czxid from ZooKeeper) per resource pool.
    Used to fence off delayed / stale allocation writes.
    """
    __tablename__ = "pool_fencing_tokens"

    pool_id = Column(String(64), primary_key=True)
    highest_fencing_token = Column(BigInteger, nullable=False, default=0)
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False
    )

class BedOperationModel(Base):
    """
    Durable record of bed operations to ensure idempotent execution of operation_id.
    """
    __tablename__ = "bed_operations"

    operation_id = Column(String(128), primary_key=True)
    operation_type = Column(String(32), nullable=False) # ALLOCATE, RELEASE
    bed_id = Column(String(64), nullable=False, index=True)
    allocation_id = Column(String(64), nullable=False)
    fencing_token = Column(BigInteger, nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

# ============================================================================
# Pydantic Schemas for REST API & DTOs
# ============================================================================

class BedDto(BaseModel):
    bed_id: str
    pool_id: str
    ward: str
    status: str
    current_allocation_id: Optional[str] = None

class CreateBedRequest(BaseModel):
    bed_id: str = Field(..., min_length=1)
    pool_id: str = Field(..., min_length=1)
    ward: str = Field(..., min_length=1)
    status: Optional[str] = "AVAILABLE"

class BedListResponse(BaseModel):
    beds: List[BedDto]
    count: int

class FencingTokenResponse(BaseModel):
    pool_id: str
    highest_fencing_token: int
    updated_at: Optional[str] = None

class AllocateBedRestRequest(BaseModel):
    operation_id: str = Field(..., min_length=1)
    bed_id: str = Field(..., min_length=1)
    allocation_id: str = Field(..., min_length=1)
    fencing_token: int = Field(..., ge=0)
    pool_id: Optional[str] = None

class ReleaseBedRestRequest(BaseModel):
    operation_id: str = Field(..., min_length=1)
    bed_id: str = Field(..., min_length=1)
    allocation_id: str = Field(..., min_length=1)
    fencing_token: int = Field(..., ge=0)
    pool_id: Optional[str] = None

class OperationResultResponse(BaseModel):
    success: bool
    bed: Optional[BedDto] = None
    error_message: str = ""

