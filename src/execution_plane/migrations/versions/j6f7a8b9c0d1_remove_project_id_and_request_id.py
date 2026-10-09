"""Remove project_id and request_id from work_items and completion_events."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "j6f7a8b9c0d1"
down_revision: str | Sequence[str] | None = "i5e6f7a8b9c0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Drop project/request identity columns now carried by the client-supplied UUID PK."""
    op.drop_constraint("uq_work_items_request_scope", "work_items", schema="execution_plane")
    op.drop_index("ix_work_items_work_correlation_id", table_name="work_items", schema="execution_plane")
    op.drop_column("work_items", "project_id", schema="execution_plane")
    op.drop_column("work_items", "request_id", schema="execution_plane")
    op.drop_column("work_items", "request_hash", schema="execution_plane")
    op.drop_column("work_items", "work_correlation_id", schema="execution_plane")

    op.drop_column("completion_events", "project_id", schema="execution_plane")
    op.drop_column("completion_events", "request_id", schema="execution_plane")


def downgrade() -> None:
    """Restore project/request identity columns with nullable placeholders."""
    op.add_column(
        "completion_events",
        sa.Column("request_id", sa.String(200), nullable=True),
        schema="execution_plane",
    )
    op.add_column(
        "completion_events",
        sa.Column("project_id", sa.Uuid(), nullable=True),
        schema="execution_plane",
    )

    op.add_column(
        "work_items",
        sa.Column("work_correlation_id", sa.Uuid(), nullable=True),
        schema="execution_plane",
    )
    op.add_column(
        "work_items",
        sa.Column("request_hash", sa.String(64), nullable=True),
        schema="execution_plane",
    )
    op.add_column(
        "work_items",
        sa.Column("request_id", sa.String(200), nullable=True),
        schema="execution_plane",
    )
    op.add_column(
        "work_items",
        sa.Column("project_id", sa.Uuid(), nullable=True),
        schema="execution_plane",
    )
    op.create_index(
        "ix_work_items_work_correlation_id",
        "work_items",
        ["work_correlation_id"],
        schema="execution_plane",
    )
    op.create_unique_constraint(
        "uq_work_items_request_scope",
        "work_items",
        ["client_id", "project_id", "request_id"],
        schema="execution_plane",
    )
