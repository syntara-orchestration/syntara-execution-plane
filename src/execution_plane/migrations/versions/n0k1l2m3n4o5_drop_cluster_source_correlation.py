"""Drop cluster source-correlation and project scope columns.

Revision ID: n0k1l2m3n4o5
Revises: l8h9i0j1k2l3
Create Date: 2026-10-29

The cluster's primary key is now the caller-supplied physical UUID (shared with
the owning AO Integration), so the separate source_integration_id correlation key
and its uniqueness constraint are gone, along with the source_revision
configuration counter and the unused project_ids scope. Pre-release hard cutover:
the constraint is dropped before the columns (required DDL order); a rolling
deploy would instead split the nullable step from the drop.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "n0k1l2m3n4o5"
down_revision: str | Sequence[str] | None = "l8h9i0j1k2l3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EP = "execution_plane"


def upgrade() -> None:
    """Remove the correlation key, its constraint, the revision counter, and project scope."""
    op.drop_constraint("uq_clusters_source_integration", "clusters", schema=EP)
    op.drop_column("clusters", "source_integration_id", schema=EP)
    op.drop_column("clusters", "source_revision", schema=EP)
    op.drop_column("clusters", "project_ids", schema=EP)


def downgrade() -> None:
    """Restore the correlation key, revision counter, project scope, and constraint."""
    op.add_column("clusters", sa.Column("source_integration_id", sa.UUID(), nullable=True), schema=EP)
    op.add_column(
        "clusters",
        sa.Column("source_revision", sa.Integer(), server_default="0", nullable=False),
        schema=EP,
    )
    op.add_column("clusters", sa.Column("project_ids", JSONB(), nullable=True), schema=EP)
    op.create_unique_constraint(
        "uq_clusters_source_integration",
        "clusters",
        ["source_client_id", "source_integration_id"],
        schema=EP,
    )
