"""
Shared fixtures for integration tests against disposable local services.

Set REQUIRE_INTEGRATION=1 (CI does) to turn every skipped test into a failure,
so a missing PostgreSQL or Firestore emulator cannot pass silently.
"""
import os
import uuid
from urllib.parse import urlsplit

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from services.bed_allocation.db import Base

REQUIRED_INTEGRATION_ENV = ("TEST_POSTGRES_URL", "FIRESTORE_EMULATOR_HOST")


def _integration_required() -> bool:
    return os.environ.get("REQUIRE_INTEGRATION", "").strip() == "1"


def pytest_sessionstart(session):
    if _integration_required():
        missing = [name for name in REQUIRED_INTEGRATION_ENV if not os.environ.get(name, "").strip()]
        if missing:
            raise pytest.UsageError(f"REQUIRE_INTEGRATION=1 but {', '.join(missing)} is not set")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if _integration_required() and report.skipped and not hasattr(report, "wasxfail"):
        report.outcome = "failed"
        report.longrepr = f"Skipped while REQUIRE_INTEGRATION=1: {report.longrepr}"


@pytest.fixture
def pg_sessions():
    """Session factory bound to a fresh temporary schema in the disposable TEST_POSTGRES_URL database."""
    database_url = os.environ.get("TEST_POSTGRES_URL")
    if not database_url:
        pytest.skip("Set TEST_POSTGRES_URL to an isolated local PostgreSQL database")
    if urlsplit(database_url).hostname not in {"localhost", "127.0.0.1", "::1", "postgres"}:
        pytest.fail("TEST_POSTGRES_URL must point to local PostgreSQL; cloud databases are not permitted")
    admin_engine = create_engine(database_url)
    schema = f"test_{uuid.uuid4().hex}"
    with admin_engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema}"},
                           pool_size=20, max_overflow=10)
    try:
        Base.metadata.create_all(bind=engine)
        yield sessionmaker(bind=engine, autoflush=False, autocommit=False)
    finally:
        engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin_engine.dispose()
