"""Drop the cluster_bindings table after direct provisioning replaces binding reconciliation.

Revision ID: l8h9i0j1k2l3
Revises: k7g8h9i0j1k2
Create Date: 2026-10-22

All active clusters were already tracked in the clusters table (bound via
source_client_id / source_integration_id). Rows that never reached 'ready'
represent unfinished installs and must be re-run after upgrade. No credentials
or source identity are discarded: those live on the clusters table.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "l8h9i0j1k2l3"
down_revision: str | Sequence[str] | None = "k7g8h9i0j1k2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EP = "execution_plane"


def upgrade() -> None:
    """Drop the binding table now that provisioning is synchronous."""
    op.drop_index("ix_cluster_bindings_reconcile", table_name="cluster_bindings", schema=EP)
    op.drop_table("cluster_bindings", schema=EP)


def downgrade() -> None:
    """Recreate the cluster_bindings table with all columns added across prior migrations."""
    op.create_table(
        "cluster_bindings",
        sa.Column("client_id", sa.String(length=128), nullable=False),
        sa.Column("source_integration_id", sa.UUID(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("endpoint", sa.String(length=2048), nullable=False),
        sa.Column("namespace", sa.String(length=63), nullable=False),
        sa.Column("credential", sa.String(), nullable=False),
        sa.Column("ca_certificate", sa.String(), nullable=True),
        sa.Column("project_ids", JSONB(), nullable=True),
        sa.Column("labels", JSONB(), server_default="{}", nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default=sa.true(), nullable=False),
        sa.Column("is_default", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("cluster_id", sa.UUID(), nullable=True),
        sa.Column("observed_revision", sa.Integer(), server_default="0", nullable=False),
        sa.Column("status", sa.String(length=32), server_default="pending", nullable=False),
        sa.Column("status_message", sa.String(length=1000), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("client_id", "source_integration_id"),
        schema=EP,
    )
    op.create_index("ix_cluster_bindings_reconcile", "cluster_bindings", ["status", "updated_at"], schema=EP)
