"""Grant runtime access to objects created by the migration role.

Revision ID: 20261112_02
Revises: 20261031_01
"""

from alembic import op

revision = "20261112_02"
down_revision = "20261031_01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE patient_registry_migrator IN SCHEMA public "
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO patient_registry"
    )
    op.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE patient_registry_migrator IN SCHEMA public "
        "GRANT USAGE, SELECT ON SEQUENCES TO patient_registry"
    )


def downgrade() -> None:
    op.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE patient_registry_migrator IN SCHEMA public "
        "REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM patient_registry"
    )
    op.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE patient_registry_migrator IN SCHEMA public "
        "REVOKE USAGE, SELECT ON SEQUENCES FROM patient_registry"
    )
