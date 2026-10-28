"""
SQLAlchemy database models and session factory for Bed Allocation Service.
Supports Cloud SQL PostgreSQL, local Docker PostgreSQL, and SQLite in tests.
"""
import os
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, Integer, DateTime, Text, Boolean, UniqueConstraint, Index, create_engine
)
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

class AllocationModel(Base):
    __tablename__ = "allocations"

    allocation_id = Column(String(64), primary_key=True)
    admission_id = Column(String(64), nullable=False, index=True)
    pool_id = Column(String(64), nullable=False, index=True)
    status = Column(String(32), nullable=False, index=True) # REQUESTED, ALLOCATED, WAITLISTED, RELEASED, CANCELLED, RECOVERING
    bed_id = Column(String(64), nullable=True)
    staff_assignment_id = Column(String(64), nullable=True)
    waitlist_entry_id = Column(String(64), nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    __table_args__ = (
        Index("idx_admission_status", "admission_id", "status"),
    )

class IdempotencyModel(Base):
    __tablename__ = "idempotency_keys"

    idempotency_key = Column(String(128), primary_key=True)
    endpoint = Column(String(128), nullable=False)
    request_hash = Column(String(64), nullable=False)
    response_status = Column(Integer, nullable=False)
    response_body = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

class OutboxEventModel(Base):
    __tablename__ = "outbox_events"

    event_id = Column(String(64), primary_key=True)
    destination = Column(String(32), nullable=False, index=True) # pubsub, kafka
    topic = Column(String(64), nullable=False)
    message_key = Column(String(64), nullable=True)
    payload = Column(Text, nullable=False)
    status = Column(String(32), default="PENDING", nullable=False, index=True) # PENDING, DELIVERED, FAILED
    retry_count = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

class OperationRecoveryModel(Base):
    __tablename__ = "operations"

    operation_id = Column(String(128), primary_key=True)
    allocation_id = Column(String(64), nullable=False, index=True)
    step = Column(String(64), nullable=False) # RESERVE_BED, ASSIGN_STAFF, RELEASE_BED
    status = Column(String(32), nullable=False) # PENDING, COMPLETED, FAILED, UNCERTAIN
    details = Column(Text, nullable=True)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

def get_engine(database_url: str = None):
    url = database_url or os.environ.get(
        "DATABASE_URL",
        "sqlite:///hospital_allocation.db"
    )
    if url.startswith("sqlite"):
        return create_engine(url, connect_args={"check_same_thread": False})
    return create_engine(url, pool_pre_ping=True, pool_size=10, max_overflow=20)

def init_db(engine):
    Base.metadata.create_all(bind=engine)
