"""Store queryable work item runtime context.

Revision ID: 21fd6bda6f1d
Revises: o1p2q3r4s5t6
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "21fd6bda6f1d"
down_revision = "o1p2q3r4s5t6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add the runtime context column and index."""
    op.add_column(
        "work_items",
        sa.Column("runtime_context", JSONB(), server_default="{}", nullable=False),
        schema="execution_plane",
    )
    op.create_index(
        "ix_work_items_runtime_context",
        "work_items",
        ["runtime_context"],
        schema="execution_plane",
        postgresql_using="gin",
    )


def downgrade() -> None:
    """Remove the runtime context column and index."""
    op.drop_index("ix_work_items_runtime_context", "work_items", schema="execution_plane")
    op.drop_column("work_items", "runtime_context", schema="execution_plane")
