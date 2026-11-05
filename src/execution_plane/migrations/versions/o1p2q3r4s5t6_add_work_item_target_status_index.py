"""Index work items by execution target and status.

Revision ID: o1p2q3r4s5t6
Revises: n0k1l2m3n4o5
Create Date: 2026-11-04
"""

from collections.abc import Sequence

from alembic import op

revision: str = "o1p2q3r4s5t6"
down_revision: str | Sequence[str] | None = "n0k1l2m3n4o5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EP = "execution_plane"


def upgrade() -> None:
    """Support target drain and usage queries without scanning work history."""
    op.create_index(
        "ix_work_items_execution_target_status",
        "work_items",
        ["execution_target_id", "status"],
        schema=EP,
    )


def downgrade() -> None:
    """Remove the target and status index."""
    op.drop_index("ix_work_items_execution_target_status", "work_items", schema=EP)
