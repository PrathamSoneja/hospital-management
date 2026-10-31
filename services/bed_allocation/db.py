"""
SQLAlchemy database models and session factory for Bed Allocation Service.
Supports Cloud SQL PostgreSQL, local Docker PostgreSQL, and SQLite in tests.
"""
import os
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, Integer, BigInteger, DateTime, Text, Boolean, UniqueConstraint, Index, create_engine, func, text
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
    pending_action = Column(String(64), nullable=True)
    workflow_data = Column(Text, nullable=True)
    recovery_claim_owner = Column(String(128), nullable=True)
    recovery_claim_until = Column(DateTime(timezone=True), nullable=True)
    recovery_attempts = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    __table_args__ = (
        Index("idx_admission_status", "admission_id", "status"),
        Index(
            "uq_allocations_active_admission",
            "admission_id",
            unique=True,
            postgresql_where=text("status NOT IN ('RELEASED', 'CANCELLED', 'FAILED')"),
            sqlite_where=text("status NOT IN ('RELEASED', 'CANCELLED', 'FAILED')"),
        ),
    )

class IdempotencyModel(Base):
    __tablename__ = "idempotency_keys"

    method = Column(String(8), primary_key=True)
    operation = Column(String(32), primary_key=True)
    target_id = Column(String(128), primary_key=True)
    idempotency_key = Column(String(128), primary_key=True)
    endpoint = Column(String(128), nullable=False)
    request_hash = Column(String(64), nullable=False)
    state = Column(String(16), nullable=False, default="IN_PROGRESS")
    allocation_id = Column(String(64), nullable=True, index=True)
    action_id = Column(String(128), nullable=True)
    response_status = Column(Integer, nullable=True)
    response_body = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

class OutboxEventModel(Base):
    __tablename__ = "outbox_events"

    event_id = Column(String(64), primary_key=True)
    destination = Column(String(32), nullable=False, index=True) # pubsub, kafka
    pool_id = Column(String(64), nullable=True)
    stream_sequence = Column(BigInteger, nullable=True)
    topic = Column(String(64), nullable=False)
    message_key = Column(String(64), nullable=True)
    payload = Column(Text, nullable=False)
    status = Column(String(32), default="PENDING", nullable=False, index=True) # PENDING, IN_FLIGHT, DELIVERED, POISONED
    retry_count = Column(Integer, default=0, nullable=False)
    attempt_count = Column(Integer, default=0, nullable=False)
    available_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), server_default=func.now(), nullable=False)
    claim_owner = Column(String(128), nullable=True)
    claim_token = Column(String(64), nullable=True)
    claim_until = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(String(512), nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    __table_args__ = (
        UniqueConstraint("destination", "pool_id", "stream_sequence", name="uq_outbox_stream_sequence"),
        Index("ix_outbox_available", "status", "available_at"),
    )


class OutboxStreamCounterModel(Base):
    __tablename__ = "outbox_stream_counters"

    destination = Column(String(32), primary_key=True)
    pool_id = Column(String(64), primary_key=True)
    last_sequence = Column(BigInteger, nullable=False, default=0)

class OperationRecoveryModel(Base):
    __tablename__ = "operations"

    operation_id = Column(String(128), primary_key=True)
    allocation_id = Column(String(64), nullable=False, index=True)
    step = Column(String(64), nullable=False) # RESERVE_BED, ASSIGN_STAFF, RELEASE_BED
    status = Column(String(32), nullable=False) # PENDING, COMPLETED, FAILED, UNCERTAIN
    details = Column(Text, nullable=True)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

def get_engine(database_url: str = None):
    from shared.config.runtime import secret_setting
    url = database_url or secret_setting("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL must be configured; SQLite is available only through explicit test engines")
    if url.startswith("sqlite"):
        if os.environ.get("ENVIRONMENT", "production").lower() != "test":
            raise RuntimeError("SQLite is test-only; use local Docker PostgreSQL or Cloud SQL")
        return create_engine(url, connect_args={"check_same_thread": False})
    # Bounded waits: connection attempts and pool checkout fail fast instead of hanging requests/probes.
    return create_engine(url, pool_pre_ping=True, pool_size=10, max_overflow=20, pool_timeout=5,
                         connect_args={"connect_timeout": 3} if url.startswith("postgresql") else {})

def init_db(engine):
    """Test-only metadata bootstrap. Production startup uses Alembic migrations."""
    Base.metadata.create_all(bind=engine)
