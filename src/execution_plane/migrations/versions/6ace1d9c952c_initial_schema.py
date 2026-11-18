"""Initial execution_plane schema.

Revision ID: 6ace1d9c952c
Revises:
Create Date: 2026-11-18

Squashed pre-release baseline. Existing development databases must be recreated.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "6ace1d9c952c"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the current tables, indexes, constraints, and runtime-role grants."""
    op.execute("CREATE SCHEMA IF NOT EXISTS execution_plane")

    op.create_table(
        "clusters",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("endpoint", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("status_message", sa.String(), nullable=True),
        sa.Column("labels", JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("credential_ref", sa.UUID(), nullable=False),
        sa.Column("created_by", sa.UUID(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_by", sa.UUID(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("cluster_type", sa.String(), nullable=False),
        sa.Column("source_client_id", sa.String(length=128), nullable=True),
        sa.Column("ca_certificate", sa.String(), nullable=True),
        sa.Column("is_default", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.PrimaryKeyConstraint("id", name="clusters_pkey"),
        schema="execution_plane",
    )
    op.create_index(
        "uq_clusters_default",
        "clusters",
        ["is_default"],
        unique=True,
        schema="execution_plane",
        postgresql_where=sa.text("is_default = true"),
    )
    op.create_table(
        "execution_targets",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("backend_type", sa.String(), nullable=False),
        sa.Column("endpoint", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("labels", JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_ran_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cluster_id", sa.UUID(), nullable=False),
        sa.Column("is_default", sa.Boolean(), nullable=False),
        sa.Column("credential_ref", sa.UUID(), nullable=False),
        sa.Column("status_message", sa.String(), nullable=True),
        sa.Column("created_by", sa.UUID(), nullable=False),
        sa.Column("updated_by", sa.UUID(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("placement", JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["cluster_id"], ["execution_plane.clusters.id"], name="execution_targets_cluster_id_fkey"
        ),
        sa.PrimaryKeyConstraint("id", name="execution_targets_pkey"),
        sa.UniqueConstraint("cluster_id", "name", name="execution_targets_cluster_name_key"),
        schema="execution_plane",
    )
    op.create_index("ix_execution_targets_name", "execution_targets", ["name"], schema="execution_plane")
    op.create_index(
        "uq_execution_targets_default_cluster",
        "execution_targets",
        ["cluster_id"],
        unique=True,
        schema="execution_plane",
        postgresql_where=sa.text("is_default = true"),
    )
    op.create_table(
        "work_items",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("execution_target_id", sa.UUID(), nullable=True),
        sa.Column("payload", JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("result", JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("client_id", sa.String(length=128), nullable=False),
        sa.Column("claim_owner_id", sa.UUID(), nullable=True),
        sa.Column("claim_generation", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("backend_resource_name", sa.String(length=253), nullable=True),
        sa.Column("backend_resource_uid", sa.String(length=64), nullable=True),
        sa.Column(
            "resource_cleanup_status",
            sa.String(length=32),
            server_default=sa.text("'not_required'::character varying"),
            nullable=False,
        ),
        sa.Column("resource_cleanup_error", sa.String(length=1000), nullable=True),
        sa.Column("completion_notification_state", sa.String(length=10), nullable=True),
        sa.Column("completion_notification_attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("completion_notification_next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completion_notification_last_error", sa.String(length=1000), nullable=True),
        sa.Column("completion_notification_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completion_notification_claim_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("runtime_context", JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("cancellation_requested", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("deletion_requested", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.CheckConstraint(
            "NOT deletion_requested OR cancellation_requested", name="ck_work_items_deletion_requires_cancellation"
        ),
        sa.ForeignKeyConstraint(
            ["execution_target_id"],
            ["execution_plane.execution_targets.id"],
            name="work_items_execution_target_id_fkey",
        ),
        sa.PrimaryKeyConstraint("id", name="work_items_pkey"),
        schema="execution_plane",
    )
    op.create_index(
        "ix_work_items_completion_delivery",
        "work_items",
        ["completion_notification_next_attempt_at", "completion_notification_claim_expires_at", "completed_at"],
        schema="execution_plane",
        postgresql_where=sa.text("completion_notification_state IN ('pending', 'delivering')"),
    )
    op.create_index(
        "ix_work_items_execution_target_status",
        "work_items",
        ["execution_target_id", "status"],
        schema="execution_plane",
    )
    op.create_index(
        "ix_work_items_pending",
        "work_items",
        ["created_at"],
        schema="execution_plane",
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        "ix_work_items_runtime_context",
        "work_items",
        ["runtime_context"],
        schema="execution_plane",
        postgresql_using="gin",
    )
    op.create_index("ix_work_items_status", "work_items", ["status"], schema="execution_plane")

    # Runtime roles are optional in deployments using the database owner.
    op.execute("REVOKE CONNECT ON DATABASE execution_plane FROM PUBLIC")
    op.execute(
        sa.text(
            """DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'execution_plane_runtime') THEN
                GRANT CONNECT ON DATABASE execution_plane TO execution_plane_runtime;
            END IF;
            END $$;"""
        )
    )
    op.execute("REVOKE ALL ON SCHEMA execution_plane FROM PUBLIC")
    op.execute(
        sa.text(
            """DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'execution_plane_runtime') THEN
                GRANT USAGE ON SCHEMA execution_plane TO execution_plane_runtime;
                GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA execution_plane TO execution_plane_runtime;
                ALTER DEFAULT PRIVILEGES IN SCHEMA execution_plane
                    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO execution_plane_runtime;
            END IF;
            END $$;"""
        )
    )


def downgrade() -> None:
    """Remove application tables in dependency order."""
    op.drop_table("work_items", schema="execution_plane")
    op.drop_table("execution_targets", schema="execution_plane")
    op.drop_table("clusters", schema="execution_plane")
    # Keep the schema and version table so Alembic can record the downgrade
    # and a later upgrade can recreate the application tables.
