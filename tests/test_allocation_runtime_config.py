import pytest

from services.bed_allocation.db import get_engine


def test_allocation_requires_explicit_database_url(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(RuntimeError, match="DATABASE_URL must be configured"):
        get_engine()


def test_allocation_rejects_sqlite_outside_test_mode(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    with pytest.raises(RuntimeError, match="SQLite is test-only"):
        get_engine("sqlite://")


def test_allocation_allows_an_explicit_test_sqlite_engine(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "test")
    engine = get_engine("sqlite://")
    try:
        assert engine.dialect.name == "sqlite"
    finally:
        engine.dispose()
