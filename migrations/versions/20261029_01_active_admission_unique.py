from alembic import op
import sqlalchemy as sa

revision = "20261029_01"
down_revision = None
branch_labels = None
depends_on = None

INDEX_NAME = "uq_allocations_active_admission"


def upgrade():
    op.execute(sa.text(
        """
        DO $$
        DECLARE duplicate_report TEXT;
        BEGIN
            SELECT string_agg(
                format('%s: %s', admission_id, allocations),
                '; ' ORDER BY admission_id
            )
            INTO duplicate_report
            FROM (
                SELECT admission_id,
                       string_agg(allocation_id || ':' || status, ', ' ORDER BY allocation_id) AS allocations
                FROM allocations
                WHERE status NOT IN ('RELEASED', 'CANCELLED', 'FAILED')
                GROUP BY admission_id
                HAVING COUNT(*) > 1
            ) AS duplicate_admissions;

            IF duplicate_report IS NOT NULL THEN
                RAISE EXCEPTION
                    'Cannot add active-admission uniqueness; resolve these duplicate active allocations explicitly, without automatic deletion: %',
                    duplicate_report;
            END IF;
        END $$
        """
    ))

    op.execute(sa.text(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_allocations_active_admission "
        "ON allocations (admission_id) "
        "WHERE status NOT IN ('RELEASED', 'CANCELLED', 'FAILED')"
    ))


def downgrade():
    op.execute(sa.text(f"DROP INDEX IF EXISTS {INDEX_NAME}"))