"""Replace execution-target namespace with typed placement JSONB."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "e0f1a2b3c4d5"
down_revision: str | Sequence[str] | None = "a3b4c5d6e7f8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EP = "execution_plane"


def upgrade() -> None:
    """Replace an empty pre-release target table's namespace with typed placement JSONB."""
    target_count = op.get_bind().execute(sa.text("SELECT count(*) FROM execution_plane.execution_targets")).scalar_one()
    if target_count:
        msg = (
            "Refusing to replace execution-target namespace values in a populated table. "
            "Backfill typed placement before upgrading this database."
        )
        raise RuntimeError(msg)
    op.drop_column("execution_targets", "namespace", schema=EP)
    op.add_column("execution_targets", sa.Column("placement", JSONB(), nullable=False), schema=EP)


def downgrade() -> None:
    """Restore the pre-release namespace column and remove placement JSONB."""
    target_count = op.get_bind().execute(sa.text("SELECT count(*) FROM execution_plane.execution_targets")).scalar_one()
    if target_count:
        msg = "Refusing to downgrade a populated execution-target table to the namespace-only model."
        raise RuntimeError(msg)
    op.drop_column("execution_targets", "placement", schema=EP)
    op.add_column("execution_targets", sa.Column("namespace", sa.String(), nullable=False), schema=EP)
