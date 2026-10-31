"""Database engine and session helpers for the Patient Registry service."""

import os
from collections.abc import Iterator
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

logger = logging.getLogger("patient-registry")


def get_engine(database_url: str | None = None) -> Engine:
    """Create a pooled SQLAlchemy engine for PostgreSQL or SQLite tests."""
    url = database_url or os.environ.get("PATIENT_REGISTRY_DATABASE_URL")
    if not url:
        raise RuntimeError("PATIENT_REGISTRY_DATABASE_URL must be configured")

    if url.startswith("sqlite"):
        return create_engine(
            url,
            connect_args={"check_same_thread": False},
            pool_pre_ping=True,
        )
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        pool_recycle=1800,
    )


def get_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Create a session factory with explicit transaction management."""
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def database_is_ready(engine: Engine | None) -> bool:
    """Check that the configured database accepts a lightweight query."""
    if engine is None:
        return False
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True
    except SQLAlchemyError:
        logger.exception("Patient Registry readiness database check failed")
        return False


def session_scope(factory: sessionmaker[Session]) -> Iterator[Session]:
    """Yield a session and ensure uncommitted work is rolled back on failure."""
    session = factory()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
