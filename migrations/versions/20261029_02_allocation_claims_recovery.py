"""Add scoped idempotency claims and resumable Allocation workflow state.

Revision ID: 20261029_02
Revises: 20261029_01
"""
from alembic import op
import sqlalchemy as sa

revision = "20261029_02"
down_revision = "20261029_01"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("allocations", sa.Column("pending_action", sa.String(length=64), nullable=True))
    op.add_column("allocations", sa.Column("workflow_data", sa.Text(), nullable=True))
    op.add_column("allocations", sa.Column("recovery_claim_owner", sa.String(length=128), nullable=True))
    op.add_column("allocations", sa.Column("recovery_claim_until", sa.DateTime(timezone=True), nullable=True))
    op.add_column(
        "allocations",
        sa.Column("recovery_attempts", sa.Integer(), server_default="0", nullable=False),
    )

    op.add_column("idempotency_keys", sa.Column("method", sa.String(length=8), nullable=True))
    op.add_column("idempotency_keys", sa.Column("operation", sa.String(length=32), nullable=True))
    op.add_column("idempotency_keys", sa.Column("target_id", sa.String(length=128), nullable=True))
    op.add_column("idempotency_keys", sa.Column("state", sa.String(length=16), nullable=True))
    op.add_column("idempotency_keys", sa.Column("allocation_id", sa.String(length=64), nullable=True))
    op.add_column("idempotency_keys", sa.Column("action_id", sa.String(length=128), nullable=True))
    op.add_column("idempotency_keys", sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True))

    # Preserve completed claims from the previous schema. The old endpoint strings
    # and cached responses contain the target and allocation identifiers.
    op.execute(sa.text(
        """
        UPDATE idempotency_keys
        SET method = split_part(endpoint, ' ', 1),
            operation = CASE
                WHEN endpoint = 'POST /api/v1/allocations' THEN 'CREATE'
                WHEN endpoint LIKE '%/release' THEN 'RELEASE'
                WHEN endpoint LIKE '%/cancel' THEN 'CANCEL'
                ELSE 'LEGACY'
            END,
            target_id = CASE
                WHEN endpoint = 'POST /api/v1/allocations'
                    THEN COALESCE(response_body::json->>'admission_id', 'legacy:' || idempotency_key)
                ELSE split_part(split_part(endpoint, '/allocations/', 2), '/', 1)
            END,
            state = 'COMPLETED',
            allocation_id = response_body::json->>'allocation_id',
            action_id = COALESCE(response_body::json->>'allocation_id', 'legacy:' || idempotency_key),
            updated_at = created_at
        """
    ))
    op.alter_column("idempotency_keys", "method", existing_type=sa.String(length=8), nullable=False)
    op.alter_column("idempotency_keys", "operation", existing_type=sa.String(length=32), nullable=False)
    op.alter_column("idempotency_keys", "target_id", existing_type=sa.String(length=128), nullable=False)
    op.alter_column("idempotency_keys", "state", existing_type=sa.String(length=16), nullable=False)
    op.alter_column("idempotency_keys", "updated_at", existing_type=sa.DateTime(timezone=True), nullable=False)
    op.alter_column("idempotency_keys", "response_status", existing_type=sa.Integer(), nullable=True)
    op.alter_column("idempotency_keys", "response_body", existing_type=sa.Text(), nullable=True)

    op.drop_constraint("idempotency_keys_pkey", "idempotency_keys", type_="primary")
    op.create_primary_key(
        "idempotency_keys_pkey",
        "idempotency_keys",
        ["method", "operation", "target_id", "idempotency_key"],
    )
    op.create_index("ix_idempotency_keys_allocation_id", "idempotency_keys", ["allocation_id"])


def downgrade():
    op.execute(sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM idempotency_keys GROUP BY idempotency_key HAVING COUNT(*) > 1
            ) THEN
                RAISE EXCEPTION 'Cannot restore globally unique idempotency keys while scoped duplicates exist';
            END IF;
        END $$
        """
    ))
    op.drop_index("ix_idempotency_keys_allocation_id", table_name="idempotency_keys")
    op.drop_constraint("idempotency_keys_pkey", "idempotency_keys", type_="primary")
    op.create_primary_key("idempotency_keys_pkey", "idempotency_keys", ["idempotency_key"])
    op.alter_column("idempotency_keys", "response_body", existing_type=sa.Text(), nullable=False)
    op.alter_column("idempotency_keys", "response_status", existing_type=sa.Integer(), nullable=False)
    op.drop_column("idempotency_keys", "updated_at")
    op.drop_column("idempotency_keys", "action_id")
    op.drop_column("idempotency_keys", "allocation_id")
    op.drop_column("idempotency_keys", "state")
    op.drop_column("idempotency_keys", "target_id")
    op.drop_column("idempotency_keys", "operation")
    op.drop_column("idempotency_keys", "method")
    op.drop_column("allocations", "recovery_attempts")
    op.drop_column("allocations", "recovery_claim_until")
    op.drop_column("allocations", "recovery_claim_owner")
    op.drop_column("allocations", "workflow_data")
    op.drop_column("allocations", "pending_action")
