"""Add per-stream ordering and leased outbox delivery state.

Revision ID: 20261031_03
Revises: 20261029_02
"""
import json
import logging

from alembic import op
import sqlalchemy as sa

revision = "20261031_03"
down_revision = "20261029_02"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "outbox_stream_counters",
        sa.Column("destination", sa.String(length=32), nullable=False),
        sa.Column("pool_id", sa.String(length=64), nullable=False),
        sa.Column("last_sequence", sa.BigInteger(), server_default="0", nullable=False),
        sa.PrimaryKeyConstraint("destination", "pool_id", name="pk_outbox_stream_counters"),
    )
    op.add_column("outbox_events", sa.Column("pool_id", sa.String(length=64), nullable=True))
    op.add_column("outbox_events", sa.Column("stream_sequence", sa.BigInteger(), nullable=True))
    op.add_column("outbox_events", sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False))
    op.add_column("outbox_events", sa.Column("available_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("outbox_events", sa.Column("claim_owner", sa.String(length=128), nullable=True))
    op.add_column("outbox_events", sa.Column("claim_token", sa.String(length=64), nullable=True))
    op.add_column("outbox_events", sa.Column("claim_until", sa.DateTime(timezone=True), nullable=True))
    op.add_column("outbox_events", sa.Column("last_error", sa.String(length=512), nullable=True))

    bind = op.get_bind()
    rows = bind.execute(sa.text("SELECT event_id, payload, created_at, retry_count FROM outbox_events")).mappings().all()
    for row in rows:
        pool_id = None
        try:
            # Copy only the exact stream identity already present in the stored payload.
            pool_id = json.loads(row["payload"]).get("pool_id")
            if not isinstance(pool_id, str) or not pool_id.strip():
                pool_id = None
        except (TypeError, ValueError):
            pass
        bind.execute(
            sa.text("UPDATE outbox_events SET pool_id=:pool, available_at=:created, attempt_count=:attempts WHERE event_id=:id"),
            {"pool": pool_id, "created": row["created_at"], "attempts": row["retry_count"], "id": row["event_id"]},
        )

    op.alter_column(
        "outbox_events", "available_at", existing_type=sa.DateTime(timezone=True),
        nullable=False, server_default=sa.func.now(),
    )
    op.execute(sa.text("UPDATE outbox_events SET status='POISONED' WHERE status='FAILED'"))
    op.create_index("ix_outbox_available", "outbox_events", ["status", "available_at"])
    op.create_unique_constraint(
        "uq_outbox_stream_sequence", "outbox_events", ["destination", "pool_id", "stream_sequence"]
    )

    # Existing rows keep a NULL stream_sequence: no order is guessed for them. Unfinished
    # ones block their stream until an operator resolves them (docs/handoff.md procedure).
    counts = bind.execute(sa.text(
        "SELECT destination, (pool_id IS NULL) AS no_pool, count(*) FROM outbox_events "
        "WHERE status <> 'DELIVERED' GROUP BY destination, (pool_id IS NULL) ORDER BY 1, 2"
    )).all()
    for destination, no_pool, count in counts:
        logging.getLogger("alembic.runtime.migration").warning(
            "Legacy unsequenced outbox rows destination=%s scope=%s count=%s",
            destination, "destination" if no_pool else "pool", count,
        )


def downgrade():
    # Map new states to ones the previous dispatcher understands; RESOLVED rows stay inert.
    op.execute(sa.text("UPDATE outbox_events SET status='PENDING' WHERE status='IN_FLIGHT'"))
    op.execute(sa.text("UPDATE outbox_events SET status='FAILED' WHERE status='POISONED'"))
    op.drop_constraint("uq_outbox_stream_sequence", "outbox_events", type_="unique")
    op.drop_index("ix_outbox_available", table_name="outbox_events")
    for column in ("last_error", "claim_until", "claim_token", "claim_owner", "available_at", "attempt_count", "stream_sequence", "pool_id"):
        op.drop_column("outbox_events", column)
    op.drop_table("outbox_stream_counters")
