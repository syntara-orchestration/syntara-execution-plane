"""Move work ownership to the standalone Execution Plane service.

Revision ID: e1a2b3c4d5e6
Revises: d9e0f1a2b3c4
Create Date: 2026-10-01

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "e1a2b3c4d5e6"
down_revision: str | Sequence[str] | None = "d9e0f1a2b3c4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EP = "execution_plane"


def upgrade() -> None:
    """Add scoped idempotency and durable events, removing Temporal tokens."""
    op.execute("REVOKE CONNECT ON DATABASE execution_plane FROM PUBLIC")
    op.execute("GRANT CONNECT ON DATABASE execution_plane TO execution_plane_runtime")
    connection = op.get_bind()
    legacy_count = connection.execute(
        sa.text("SELECT count(*) FROM execution_plane.work_items WHERE activity_handle IS NOT NULL")
    ).scalar_one()
    if legacy_count:
        msg = (
            f"Refusing to remove {legacy_count} legacy Temporal activity token(s) from execution_plane.work_items. "
            "Drain and reconcile legacy work, then archive or remove the old rows before upgrading."
        )
        raise RuntimeError(msg)

    op.add_column("work_items", sa.Column("client_id", sa.String(length=128), nullable=True), schema=EP)
    op.add_column("work_items", sa.Column("project_id", sa.UUID(), nullable=True), schema=EP)
    op.add_column("work_items", sa.Column("request_id", sa.String(length=200), nullable=True), schema=EP)
    op.add_column("work_items", sa.Column("request_hash", sa.String(length=64), nullable=True), schema=EP)
    op.execute(
        sa.text(
            """UPDATE execution_plane.work_items
            SET client_id = 'legacy-unscoped',
                project_id = '00000000-0000-0000-0000-000000000000',
                request_id = 'legacy:' || id::text,
                request_hash = md5(payload::text)
            WHERE client_id IS NULL"""
        )
    )
    op.alter_column("work_items", "client_id", nullable=False, schema=EP)
    op.alter_column("work_items", "project_id", nullable=False, schema=EP)
    op.alter_column("work_items", "request_id", nullable=False, schema=EP)
    op.alter_column("work_items", "request_hash", nullable=False, schema=EP)
    op.create_unique_constraint(
        "uq_work_items_request_scope",
        "work_items",
        ["client_id", "project_id", "request_id"],
        schema=EP,
    )
    op.drop_column("work_items", "activity_handle", schema=EP)
    op.drop_column("work_items", "signaled_at", schema=EP)

    op.create_table(
        "completion_events",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("work_item_id", sa.UUID(), nullable=False),
        sa.Column("client_id", sa.String(length=128), nullable=False),
        sa.Column("project_id", sa.UUID(), nullable=False),
        sa.Column("request_id", sa.String(length=200), nullable=False),
        sa.Column("state_revision", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("result", JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_error", sa.String(length=1000), nullable=True),
        sa.ForeignKeyConstraint(["work_item_id"], [f"{EP}.work_items.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("work_item_id", name="uq_completion_events_work_item"),
        schema=EP,
    )
    op.create_index(
        "ix_completion_events_delivery",
        "completion_events",
        ["delivered_at", "next_attempt_at", "created_at"],
        schema=EP,
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
    """Remove service tables and restore nullable legacy token columns."""
    op.drop_index("ix_completion_events_delivery", table_name="completion_events", schema=EP)
    op.drop_table("completion_events", schema=EP)
    op.add_column("work_items", sa.Column("activity_handle", sa.Text(), nullable=True), schema=EP)
    op.add_column("work_items", sa.Column("signaled_at", sa.DateTime(timezone=True), nullable=True), schema=EP)
    op.drop_constraint("uq_work_items_request_scope", "work_items", schema=EP, type_="unique")
    op.drop_column("work_items", "request_hash", schema=EP)
    op.drop_column("work_items", "request_id", schema=EP)
    op.drop_column("work_items", "project_id", schema=EP)
    op.drop_column("work_items", "client_id", schema=EP)
