"""Create the Allocation service's initial PostgreSQL schema.

Revision ID: 20261029_00
Revises:
"""
from alembic import op
import sqlalchemy as sa

revision = "20261029_00"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "allocations",
        sa.Column("allocation_id", sa.String(length=64), nullable=False),
        sa.Column("admission_id", sa.String(length=64), nullable=False),
        sa.Column("pool_id", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("bed_id", sa.String(length=64), nullable=True),
        sa.Column("staff_assignment_id", sa.String(length=64), nullable=True),
        sa.Column("waitlist_entry_id", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("allocation_id"),
    )
    op.create_index("idx_admission_status", "allocations", ["admission_id", "status"])
    op.create_index("ix_allocations_admission_id", "allocations", ["admission_id"])
    op.create_index("ix_allocations_pool_id", "allocations", ["pool_id"])
    op.create_index("ix_allocations_status", "allocations", ["status"])

    op.create_table(
        "idempotency_keys",
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("endpoint", sa.String(length=128), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("response_status", sa.Integer(), nullable=False),
        sa.Column("response_body", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("idempotency_key"),
    )

    op.create_table(
        "operations",
        sa.Column("operation_id", sa.String(length=128), nullable=False),
        sa.Column("allocation_id", sa.String(length=64), nullable=False),
        sa.Column("step", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("details", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("operation_id"),
    )
    op.create_index("ix_operations_allocation_id", "operations", ["allocation_id"])

    op.create_table(
        "outbox_events",
        sa.Column("event_id", sa.String(length=64), nullable=False),
        sa.Column("destination", sa.String(length=32), nullable=False),
        sa.Column("topic", sa.String(length=64), nullable=False),
        sa.Column("message_key", sa.String(length=64), nullable=True),
        sa.Column("payload", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("event_id"),
    )
    op.create_index("ix_outbox_events_destination", "outbox_events", ["destination"])
    op.create_index("ix_outbox_events_status", "outbox_events", ["status"])


def downgrade():
    op.drop_index("ix_outbox_events_status", table_name="outbox_events")
    op.drop_index("ix_outbox_events_destination", table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_index("ix_operations_allocation_id", table_name="operations")
    op.drop_table("operations")
    op.drop_table("idempotency_keys")
    op.drop_index("ix_allocations_status", table_name="allocations")
    op.drop_index("ix_allocations_pool_id", table_name="allocations")
    op.drop_index("ix_allocations_admission_id", table_name="allocations")
    op.drop_index("idx_admission_status", table_name="allocations")
    op.drop_table("allocations")
