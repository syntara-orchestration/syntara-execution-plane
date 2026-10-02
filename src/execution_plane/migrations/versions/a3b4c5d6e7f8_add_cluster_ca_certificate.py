"""Store per-cluster API trust roots encrypted at rest.

Revision ID: a3b4c5d6e7f8
Revises: f2b3c4d5e6f7
Create Date: 2026-10-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a3b4c5d6e7f8"
down_revision: str | Sequence[str] | None = "f2b3c4d5e6f7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EP = "execution_plane"


def upgrade() -> None:
    """Add encrypted optional certificate fields to desired and observed cluster state."""
    op.add_column("clusters", sa.Column("ca_certificate", sa.String(), nullable=True), schema=EP)
    op.add_column("cluster_bindings", sa.Column("ca_certificate", sa.String(), nullable=True), schema=EP)


def downgrade() -> None:
    """Remove the optional per-cluster trust-root fields."""
    op.drop_column("cluster_bindings", "ca_certificate", schema=EP)
    op.drop_column("clusters", "ca_certificate", schema=EP)
