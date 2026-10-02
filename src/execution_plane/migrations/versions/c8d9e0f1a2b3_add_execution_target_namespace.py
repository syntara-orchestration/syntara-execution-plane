"""Add Kubernetes namespace to execution targets.

Revision ID: c8d9e0f1a2b3
Revises: b7c8d9e0f1a2
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c8d9e0f1a2b3"
down_revision: str | Sequence[str] | None = "b7c8d9e0f1a2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EP = "execution_plane"


def upgrade() -> None:
    """Add a required Kubernetes namespace to each execution target."""
    op.add_column(
        "execution_targets",
        sa.Column("namespace", sa.String(), nullable=False, server_default="default"),
        schema=EP,
    )
    op.alter_column("execution_targets", "namespace", server_default=None, schema=EP)


def downgrade() -> None:
    """Remove Kubernetes namespace from execution targets."""
    op.drop_column("execution_targets", "namespace", schema=EP)
