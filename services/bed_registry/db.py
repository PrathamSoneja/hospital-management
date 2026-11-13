"""
Database engine and session management for Bed Registry Service.
Owned by Member D (Jay Patel).
"""
import os
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from services.bed_registry.models import Base

logger = logging.getLogger("bed-registry-db")

_ENGINE = None
_SESSION_FACTORY = None

def get_database_url() -> str:
    db_url_file = os.environ.get("BED_REGISTRY_DATABASE_URL_FILE") or os.environ.get("DATABASE_URL_FILE")
    if db_url_file and os.path.exists(db_url_file):
        try:
            with open(db_url_file, "r") as f:
                content = f.read().strip()
                if content:
                    return content
        except Exception:
            pass
    return (
        os.environ.get("BED_REGISTRY_DATABASE_URL")
        or os.environ.get("DATABASE_URL")
        or "sqlite:///:memory:"
    )

def init_engine(db_url: str | None = None):
    global _ENGINE, _SESSION_FACTORY
    url = db_url or get_database_url()

    engine_kwargs = {"echo": False}
    if url.startswith("sqlite"):
        if ":memory:" in url:
            engine_kwargs["connect_args"] = {"check_same_thread": False}
            engine_kwargs["poolclass"] = StaticPool
        else:
            engine_kwargs["connect_args"] = {"check_same_thread": False}
    else:
        engine_kwargs["pool_pre_ping"] = True
        engine_kwargs["pool_size"] = int(os.environ.get("DB_POOL_SIZE", "10"))
        engine_kwargs["max_overflow"] = int(os.environ.get("DB_MAX_OVERFLOW", "20"))

    _ENGINE = create_engine(url, **engine_kwargs)
    _SESSION_FACTORY = sessionmaker(autocommit=False, autoflush=False, bind=_ENGINE)
    return _ENGINE

def get_engine():
    global _ENGINE
    if _ENGINE is None:
        init_engine()
    return _ENGINE

def get_session_factory():
    global _SESSION_FACTORY
    if _SESSION_FACTORY is None:
        init_engine()
    return _SESSION_FACTORY

@contextmanager
def get_db_session() -> Session:
    factory = get_session_factory()
    session = factory()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

def init_db(engine=None):
    """Initializes tables for Bed Registry."""
    eng = engine or get_engine()
    Base.metadata.create_all(bind=eng)
    logger.info("Bed Registry database tables verified/created successfully.")

