"""
Alembic paths on disposable databases (created on the TEST_POSTGRES_URL server, names end
in _test): empty bootstrap to head with no model drift, upgrade from 20261029_02 with legacy
outbox rows, and the supported 03 -> 02 -> head rollback/roll-forward path.
"""
import os
import subprocess
import sys

import pytest
from sqlalchemy import create_engine, text

from tests.sandbox.runtime import REPO_ROOT, assert_disposable_database_url, isolated_env, with_database


def _recreate(name):
    server = os.environ.get("TEST_POSTGRES_URL")
    if not server:
        pytest.skip("Set TEST_POSTGRES_URL to an isolated local PostgreSQL database")
    url = with_database(server, name)
    assert_disposable_database_url(url)
    admin = create_engine(with_database(server, "postgres"), isolation_level="AUTOCOMMIT")
    with admin.connect() as connection:
        connection.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        connection.execute(text(f'CREATE DATABASE "{name}"'))
    admin.dispose()
    return url


def _alembic(url, *args):
    result = subprocess.run([sys.executable, "-m", "alembic", *args], cwd=REPO_ROOT, capture_output=True, text=True,
                            env=isolated_env(REPO_ROOT / ".sandbox" / "migrations", DATABASE_URL=url), timeout=180)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout + result.stderr


def _scalar(url, sql):
    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            return connection.execute(text(sql)).all()
    finally:
        engine.dispose()


def test_empty_bootstrap_reaches_head_without_model_drift():
    url = _recreate("hospital_migration_empty_test")
    _alembic(url, "upgrade", "head")
    assert "20261031_04 (head)" in _alembic(url, "current")
    assert "No new upgrade operations detected" in _alembic(url, "check")


def test_upgrade_from_02_keeps_legacy_rows_unsequenced_and_rolls_back_cleanly():
    url = _recreate("hospital_migration_upgrade_test")
    _alembic(url, "upgrade", "20261029_02")
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.execute(text(
            "INSERT INTO outbox_events(event_id, destination, topic, message_key, payload, status, retry_count, created_at, updated_at) VALUES "
            "('legacy-pool', 'kafka', 'bed-events', 'P1', '{\"pool_id\": \"P1\"}', 'PENDING', 0, now(), now()),"
            "('legacy-nopool', 'pubsub', 'bed-notifications', NULL, 'not-json', 'FAILED', 5, now(), now())"))
    engine.dispose()
    output = _alembic(url, "upgrade", "head")
    assert "Legacy unsequenced outbox rows destination=kafka scope=pool count=1" in output
    rows = dict((r[0], r[1:]) for r in _scalar(url, "SELECT event_id, pool_id, stream_sequence, status FROM outbox_events"))
    assert rows == {"legacy-pool": ("P1", None, "PENDING"), "legacy-nopool": (None, None, "POISONED")}

    _alembic(url, "downgrade", "20261029_02")
    assert dict(_scalar(url, "SELECT event_id, status FROM outbox_events")) == {"legacy-pool": "PENDING", "legacy-nopool": "FAILED"}
    _alembic(url, "upgrade", "head")
    assert "No new upgrade operations detected" in _alembic(url, "check")
