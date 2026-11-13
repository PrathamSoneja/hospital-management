"""
Alembic paths on disposable databases (created on the TEST_POSTGRES_URL server, names end
in _test), prepared like Cloud SQL: migrations run as allocation_migrator, the service as
allocation_app. Covers empty bootstrap to head with no model drift, upgrade from 20261029_02
with legacy outbox rows, the supported head -> 02 -> head rollback/roll-forward path, and the
runtime role's privileges (DML allowed, DDL refused).
"""
import os
import subprocess
import sys

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

from tests.sandbox.runtime import REPO_ROOT, isolated_env, prepare_allocation_database

HEAD = "20261031_04"


def _recreate(name):
    server = os.environ.get("TEST_POSTGRES_URL")
    if not server:
        pytest.skip("Set TEST_POSTGRES_URL to an isolated local PostgreSQL database")
    return prepare_allocation_database(server, name)  # (migrator_url, app_url)


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


def _denied(url, sql):
    """True when the statement fails with insufficient privilege (and changes nothing)."""
    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            try:
                connection.execute(text(sql))
            except ProgrammingError as error:
                connection.rollback()
                return getattr(error.orig, "pgcode", None) in {"42501"}  # insufficient_privilege / must be owner
            connection.rollback()
            return False
    finally:
        engine.dispose()


def test_empty_bootstrap_reaches_head_without_model_drift():
    migrator, _ = _recreate("hospital_migration_empty_test")
    _alembic(migrator, "upgrade", "head")
    assert f"{HEAD} (head)" in _alembic(migrator, "current")
    assert "No new upgrade operations detected" in _alembic(migrator, "check")


def test_upgrade_from_02_keeps_legacy_rows_unsequenced_and_rolls_back_cleanly():
    migrator, _ = _recreate("hospital_migration_upgrade_test")
    _alembic(migrator, "upgrade", "20261029_02")
    engine = create_engine(migrator)
    with engine.begin() as connection:
        connection.execute(text(
            "INSERT INTO outbox_events(event_id, destination, topic, message_key, payload, status, retry_count, created_at, updated_at) VALUES "
            "('legacy-pool', 'kafka', 'bed-events', 'P1', '{\"pool_id\": \"P1\"}', 'PENDING', 0, now(), now()),"
            "('legacy-nopool', 'pubsub', 'bed-notifications', NULL, 'not-json', 'FAILED', 5, now(), now())"))
    engine.dispose()
    output = _alembic(migrator, "upgrade", "head")
    assert "Legacy unsequenced outbox rows destination=kafka scope=pool count=1" in output
    rows = dict((r[0], r[1:]) for r in _scalar(migrator, "SELECT event_id, pool_id, stream_sequence, status FROM outbox_events"))
    assert rows == {"legacy-pool": ("P1", None, "PENDING"), "legacy-nopool": (None, None, "POISONED")}

    _alembic(migrator, "downgrade", "20261029_02")
    assert dict(_scalar(migrator, "SELECT event_id, status FROM outbox_events")) == {"legacy-pool": "PENDING", "legacy-nopool": "FAILED"}
    _alembic(migrator, "upgrade", "head")
    assert f"{HEAD} (head)" in _alembic(migrator, "current")
    assert "No new upgrade operations detected" in _alembic(migrator, "check")


def test_runtime_role_has_dml_but_no_ddl_and_grants_follow_the_revision():
    migrator, app = _recreate("hospital_migration_privileges_test")
    _alembic(migrator, "upgrade", "head")

    engine = create_engine(app)
    with engine.begin() as connection:  # the DML the service needs
        connection.execute(text("INSERT INTO outbox_stream_counters(destination, pool_id, last_sequence) VALUES ('kafka', 'P', 0)"))
        connection.execute(text("UPDATE outbox_stream_counters SET last_sequence = last_sequence + 1 WHERE pool_id = 'P'"))
        assert connection.execute(text("SELECT last_sequence FROM outbox_stream_counters WHERE pool_id = 'P'")).scalar() == 1
        connection.execute(text("DELETE FROM outbox_stream_counters WHERE pool_id = 'P'"))
        assert connection.execute(text("SELECT count(*) FROM allocations")).scalar() == 0
    engine.dispose()

    for ddl in ("CREATE TABLE app_owned_probe (id int)",
                "ALTER TABLE allocations ADD COLUMN probe int",
                "DROP TABLE outbox_events",
                "TRUNCATE allocations",
                "CREATE INDEX probe_ix ON allocations (pool_id)"):
        assert _denied(app, ddl), ddl

    # Default privileges from 20261031_04 cover tables a later migration creates as the migrator.
    with create_engine(migrator).begin() as connection:
        connection.execute(text("CREATE TABLE future_migration_probe (id int)"))
    with create_engine(app).begin() as connection:
        connection.execute(text("INSERT INTO future_migration_probe VALUES (1)"))
    with create_engine(migrator).begin() as connection:
        connection.execute(text("DROP TABLE future_migration_probe"))

    _alembic(migrator, "downgrade", "20261031_03")  # supported rollback revokes runtime DML
    assert _denied(app, "SELECT count(*) FROM allocations")
    _alembic(migrator, "upgrade", "head")
    assert _scalar(app, "SELECT count(*) FROM allocations") == [(0,)]
