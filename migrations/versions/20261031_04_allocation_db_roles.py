"""Grant Allocation runtime privileges on migration-owned database objects."""

from alembic import op

revision = "20261031_04"
down_revision = "20261031_03"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public "
        "TO allocation_app"
    )
    op.execute(
        "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public "
        "TO allocation_app"
    )
    op.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE allocation_migrator IN SCHEMA public "
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO allocation_app"
    )
    op.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE allocation_migrator IN SCHEMA public "
        "GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO allocation_app"
    )


def downgrade() -> None:
    op.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE allocation_migrator IN SCHEMA public "
        "REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM allocation_app"
    )
    op.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE allocation_migrator IN SCHEMA public "
        "REVOKE USAGE, SELECT, UPDATE ON SEQUENCES FROM allocation_app"
    )
    op.execute("REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM allocation_app")
    op.execute("REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM allocation_app")
