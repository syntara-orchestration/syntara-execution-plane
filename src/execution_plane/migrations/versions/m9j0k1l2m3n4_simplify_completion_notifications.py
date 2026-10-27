"""Remove notifier ownership and clean up legacy completion identity.

Revision ID: m9j0k1l2m3n4
Revises: l8i9j0k1l2m3
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "m9j0k1l2m3n4"
down_revision: str | Sequence[str] | None = "l8i9j0k1l2m3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EP = "execution_plane"


def upgrade() -> None:
    """Retain delivery progress while removing redundant callback UUIDs."""
    op.drop_column("work_items", "completion_notification_claim_owner_id", schema=EP)
    # Earlier installations of l8i9j0k1l2m3 included this column.
    op.execute("ALTER TABLE execution_plane.work_items DROP COLUMN IF EXISTS completion_command_id")


def downgrade() -> None:
    """Restore notifier ownership; expired leases can be reclaimed by older code."""
    op.add_column(
        "work_items", sa.Column("completion_notification_claim_owner_id", sa.UUID(), nullable=True), schema=EP
    )
