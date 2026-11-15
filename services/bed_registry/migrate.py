"""
Bed Registry schema migration, run by the migration identity (bed_registry_migrator) as a Job.

Creates the tables (idempotent create_all; the schema has no Alembic history yet) and grants the runtime
role DML only. The runtime role name comes from BED_REGISTRY_RUNTIME_ROLE (default bed_registry). The
database URL is read from BED_REGISTRY_DATABASE_URL_FILE / BED_REGISTRY_DATABASE_URL and never logged.
"""
import logging
import os
import re
import sys

from sqlalchemy import text

from services.bed_registry.db import get_database_url, init_db, init_engine

logger = logging.getLogger("bed-registry-migrate")
ROLE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")

GRANTS = (
    "GRANT USAGE ON SCHEMA public TO {role}",
    "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {role}",
    "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO {role}",
    "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {role}",
    "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {role}",
)


def migrate(runtime_role: str) -> list:
    if not ROLE.match(runtime_role):
        raise ValueError("BED_REGISTRY_RUNTIME_ROLE must be a plain lowercase PostgreSQL role name")
    engine = init_engine(get_database_url())
    init_db(engine)
    tables = sorted(t for t in __import__("services.bed_registry.models", fromlist=["Base"]).Base.metadata.tables)
    if engine.dialect.name == "postgresql":
        with engine.begin() as connection:
            for statement in GRANTS:
                connection.execute(text(statement.format(role=runtime_role)))
    return tables


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    role = os.environ.get("BED_REGISTRY_RUNTIME_ROLE", "bed_registry").strip()
    tables = migrate(role)
    logger.info("bed registry schema ready: tables=%s runtime_role=%s", ",".join(tables), role)
    return 0


if __name__ == "__main__":
    sys.exit(main())
