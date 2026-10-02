"""Add versioned client-owned cluster bindings and credential encryption.

Revision ID: f2b3c4d5e6f7
Revises: e1a2b3c4d5e6
Create Date: 2026-10-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "f2b3c4d5e6f7"
down_revision: str | Sequence[str] | None = "e1a2b3c4d5e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EP = "execution_plane"


def upgrade() -> None:
    """Persist opaque client identities, project grants, and desired cluster state."""
    op.drop_constraint("clusters_name_key", "clusters", schema=EP, type_="unique")
    op.drop_constraint("clusters_endpoint_key", "clusters", schema=EP, type_="unique")
    op.add_column("clusters", sa.Column("source_client_id", sa.String(length=128), nullable=True), schema=EP)
    op.add_column("clusters", sa.Column("source_integration_id", sa.UUID(), nullable=True), schema=EP)
    op.add_column("clusters", sa.Column("source_revision", sa.Integer(), server_default="0", nullable=False), schema=EP)
    op.add_column("clusters", sa.Column("project_ids", JSONB(), nullable=True), schema=EP)
    op.create_unique_constraint(
        "uq_clusters_source_integration",
        "clusters",
        ["source_client_id", "source_integration_id"],
        schema=EP,
    )
    op.create_table(
        "cluster_bindings",
        sa.Column("client_id", sa.String(length=128), nullable=False),
        sa.Column("source_integration_id", sa.UUID(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("endpoint", sa.String(length=2048), nullable=False),
        sa.Column("namespace", sa.String(length=63), nullable=False),
        sa.Column("credential", sa.String(), nullable=False),
        sa.Column("project_ids", JSONB(), nullable=True),
        sa.Column("labels", JSONB(), server_default="{}", nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default=sa.true(), nullable=False),
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


def downgrade() -> None:
    """Remove desired-state records and project ownership metadata."""
    op.drop_index("ix_cluster_bindings_reconcile", table_name="cluster_bindings", schema=EP)
    op.drop_table("cluster_bindings", schema=EP)
    op.drop_constraint("uq_clusters_source_integration", "clusters", schema=EP, type_="unique")
    op.drop_column("clusters", "project_ids", schema=EP)
    op.drop_column("clusters", "source_revision", schema=EP)
    op.drop_column("clusters", "source_integration_id", schema=EP)
    op.drop_column("clusters", "source_client_id", schema=EP)
    op.create_unique_constraint("clusters_name_key", "clusters", ["name"], schema=EP)
    op.create_unique_constraint("clusters_endpoint_key", "clusters", ["endpoint"], schema=EP)
