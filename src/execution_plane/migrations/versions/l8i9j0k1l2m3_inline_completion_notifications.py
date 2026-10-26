"""Inline completion notifications on work items.

Revision ID: l8i9j0k1l2m3
Revises: k7g8h9i0j1k2
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "l8i9j0k1l2m3"
down_revision: str | Sequence[str] | None = "k7g8h9i0j1k2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EP = "execution_plane"


def upgrade() -> None:
    """Move green-field completion delivery state onto work items."""
    op.add_column("work_items", sa.Column("completion_command_id", sa.UUID(), nullable=True), schema=EP)
    op.add_column(
        "work_items",
        sa.Column(
            "notification_state",
            sa.Enum("pending", "delivering", "delivered", name="notificationstate", native_enum=False),
            nullable=True,
        ),
        schema=EP,
    )
    op.add_column(
        "work_items",
        sa.Column("notification_attempts", sa.Integer(), server_default="0", nullable=False),
        schema=EP,
    )
    op.add_column(
        "work_items",
        sa.Column("notification_next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        schema=EP,
    )
    op.add_column(
        "work_items",
        sa.Column("notification_last_error", sa.String(length=1000), nullable=True),
        schema=EP,
    )
    op.add_column("work_items", sa.Column("notified_at", sa.DateTime(timezone=True), nullable=True), schema=EP)
    op.add_column(
        "work_items",
        sa.Column("notification_claim_owner_id", sa.UUID(), nullable=True),
        schema=EP,
    )
    op.add_column(
        "work_items",
        sa.Column("notification_claim_expires_at", sa.DateTime(timezone=True), nullable=True),
        schema=EP,
    )
    op.create_index(
        "ix_work_items_completion_delivery",
        "work_items",
        ["notification_next_attempt_at", "notification_claim_expires_at", "completed_at"],
        schema=EP,
        postgresql_where=sa.text("notification_state IN ('pending', 'delivering')"),
    )
    op.drop_index("ix_completion_events_delivery", table_name="completion_events", schema=EP)
    op.drop_table("completion_events", schema=EP)


def downgrade() -> None:
    """Restore the former completion-event outbox table."""
    op.drop_index("ix_work_items_completion_delivery", table_name="work_items", schema=EP)
    op.drop_column("work_items", "notification_claim_expires_at", schema=EP)
    op.drop_column("work_items", "notification_claim_owner_id", schema=EP)
    op.drop_column("work_items", "notified_at", schema=EP)
    op.drop_column("work_items", "notification_last_error", schema=EP)
    op.drop_column("work_items", "notification_next_attempt_at", schema=EP)
    op.drop_column("work_items", "notification_attempts", schema=EP)
    op.drop_column("work_items", "notification_state", schema=EP)
    op.drop_column("work_items", "completion_command_id", schema=EP)
    op.create_table(
        "completion_events",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("work_item_id", sa.UUID(), nullable=False),
        sa.Column("client_id", sa.String(length=128), nullable=False),
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
