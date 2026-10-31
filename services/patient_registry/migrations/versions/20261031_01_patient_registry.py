"""Create Patient Registry-owned tables.

Revision ID: 20261031_01
Revises:
"""

from alembic import op
import sqlalchemy as sa

revision = "20261031_01"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "patients",
        sa.Column("patient_id", sa.String(length=64), nullable=False),
        sa.Column("first_name", sa.String(length=100), nullable=False),
        sa.Column("last_name", sa.String(length=100), nullable=False),
        sa.Column("date_of_birth", sa.Date(), nullable=False),
        sa.Column("phone", sa.String(length=32), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("patient_id"),
    )
    op.create_table(
        "admissions",
        sa.Column("admission_id", sa.String(length=64), nullable=False),
        sa.Column("patient_id", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("admitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("discharged_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["patient_id"], ["patients.patient_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("admission_id"),
    )
    op.create_index("ix_admissions_patient_id", "admissions", ["patient_id"])
    op.create_index(
        "idx_admissions_patient_status",
        "admissions",
        ["patient_id", "status"],
    )
    op.create_table(
        "patient_documents",
        sa.Column("document_id", sa.String(length=64), nullable=False),
        sa.Column("patient_id", sa.String(length=64), nullable=False),
        sa.Column("document_type", sa.String(length=32), nullable=False),
        sa.Column("storage_object", sa.String(length=256), nullable=False),
        sa.Column("content_type", sa.String(length=128), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("uploaded_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["patient_id"], ["patients.patient_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("document_id"),
        sa.UniqueConstraint("storage_object"),
    )
    op.create_index(
        "idx_patient_documents_patient",
        "patient_documents",
        ["patient_id", "uploaded_at"],
    )
    op.create_table(
        "idempotency_keys",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("endpoint", sa.String(length=128), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("response_status", sa.Integer(), nullable=False),
        sa.Column("response_body", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "endpoint",
            "idempotency_key",
            name="uq_patient_idempotency_endpoint_key",
        ),
    )
    op.execute("GRANT CONNECT ON DATABASE patients TO patient_registry")
    op.execute("GRANT USAGE ON SCHEMA public TO patient_registry")
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public "
        "TO patient_registry"
    )
    op.execute(
        "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO patient_registry"
    )
    op.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public "
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO patient_registry"
    )
    op.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public "
        "GRANT USAGE, SELECT ON SEQUENCES TO patient_registry"
    )


def downgrade() -> None:
    op.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public "
        "REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM patient_registry"
    )
    op.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public "
        "REVOKE USAGE, SELECT ON SEQUENCES FROM patient_registry"
    )
    op.drop_table("idempotency_keys")
    op.drop_index("idx_patient_documents_patient", table_name="patient_documents")
    op.drop_table("patient_documents")
    op.drop_index("idx_admissions_patient_status", table_name="admissions")
    op.drop_index("ix_admissions_patient_id", table_name="admissions")
    op.drop_table("admissions")
    op.drop_table("patients")
    op.execute(
        "REVOKE SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public "
        "FROM patient_registry"
    )
    op.execute(
        "REVOKE USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public FROM patient_registry"
    )
    op.execute("REVOKE USAGE ON SCHEMA public FROM patient_registry")
    op.execute("REVOKE CONNECT ON DATABASE patients FROM patient_registry")
