"""Migration engine ownership and centralized creation."""

from __future__ import annotations

import runpy
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from alembic import context
from alembic.config import Config

from execution_plane.database import engine as engine_module


@pytest.mark.parametrize("failure", [None, "connect", "execute"])
def test_migrations_use_and_dispose_unpooled_engine(failure: str | None) -> None:
    config = Config()
    url = "postgresql+asyncpg://localhost/migration_test"
    config.set_main_option("sqlalchemy.url", url)
    engine = MagicMock()
    engine.dispose = AsyncMock()
    connection = AsyncMock()
    engine.connect.return_value.__aenter__.return_value = connection
    if failure == "connect":
        engine.connect.return_value.__aenter__.side_effect = RuntimeError("migration failure")
    elif failure == "execute":
        connection.execute.side_effect = RuntimeError("migration failure")

    path = Path(__file__).parents[1] / "src/execution_plane/migrations/env.py"
    with (
        patch.object(context, "config", config, create=True),
        patch.object(context, "is_offline_mode", return_value=False),
        patch.object(engine_module, "create_unpooled_engine", return_value=engine) as create_engine,
    ):
        if failure is None:
            runpy.run_path(str(path))
            connection.commit.assert_awaited_once()
            connection.run_sync.assert_awaited_once()
        else:
            with pytest.raises(RuntimeError, match="migration failure"):
                runpy.run_path(str(path))
        create_engine.assert_called_once_with(url)
    engine.dispose.assert_awaited_once()
