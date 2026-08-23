"""Add execution archive catalog [1dfbad4fc7c1].

Revision ID: 1dfbad4fc7c1
Revises: 0.96.3
Create Date: 2026-08-23 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1dfbad4fc7c1"
down_revision = "0.96.3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    op.create_table(
        "execution_archive",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created", sa.DateTime(), nullable=False),
        sa.Column("updated", sa.DateTime(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("root_run_id", sa.Uuid(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("policy_version", sa.Integer(), nullable=False),
        sa.Column("eligible_at", sa.DateTime(), nullable=False),
        sa.Column("source_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("bucket", sa.String(length=63), nullable=True),
        sa.Column("manifest_key", sa.TEXT(), nullable=True),
        sa.Column("manifest_version_id", sa.TEXT(), nullable=True),
        sa.Column("manifest_sha256", sa.String(length=64), nullable=True),
        sa.Column("canonical_bytes", sa.BigInteger(), nullable=True),
        sa.Column("stored_bytes", sa.BigInteger(), nullable=True),
        sa.Column("checkpoint", sa.TEXT(), nullable=True),
        sa.Column("legal_hold", sa.Boolean(), nullable=False),
        sa.Column("tombstoned_at", sa.DateTime(), nullable=True),
        sa.Column("tombstoned_by", sa.Uuid(), nullable=True),
        sa.Column("committed_at", sa.DateTime(), nullable=True),
        sa.Column("compacted_at", sa.DateTime(), nullable=True),
        sa.Column("last_error", sa.TEXT(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "root_run_id",
            "generation",
            name="unique_execution_archive_root_generation",
        ),
    )
    op.create_index(
        "ix_execution_archive_project_id_state_eligible_at",
        "execution_archive",
        ["project_id", "state", "eligible_at"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    op.drop_index(
        "ix_execution_archive_project_id_state_eligible_at",
        table_name="execution_archive",
    )
    op.drop_table("execution_archive")
