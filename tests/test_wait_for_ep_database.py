"""Tests for the compose bootstrap wait script."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

if TYPE_CHECKING:
    from types import ModuleType

_WAIT_SCRIPT = Path(__file__).resolve().parents[1] / "tools" / "wait_for_ep_database.py"


def _load_wait_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("wait_for_ep_database", _WAIT_SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def wait_mod() -> ModuleType:
    return _load_wait_module()


def test_expected_heads_match_alembic_ini(wait_mod: ModuleType) -> None:
    expected = frozenset(ScriptDirectory.from_config(Config("alembic.ini")).get_heads())
    assert wait_mod._expected_heads() == expected
    assert expected


def test_alembic_heads_from_env_are_optional(wait_mod: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WAIT_FOR_ALEMBIC_HEAD", raising=False)
    assert wait_mod._alembic_heads_from_env() is None
    monkeypatch.setenv("WAIT_FOR_ALEMBIC_HEAD", "1")
    monkeypatch.setattr(wait_mod, "_expected_heads", lambda: frozenset({"abc123"}))
    assert wait_mod._alembic_heads_from_env() == frozenset({"abc123"})


@pytest.fixture
def readiness_engine(wait_mod: ModuleType, monkeypatch: pytest.MonkeyPatch) -> tuple[MagicMock, AsyncMock]:
    engine = MagicMock()
    engine.dispose = AsyncMock()
    conn = AsyncMock()
    engine.connect.return_value.__aenter__.return_value = conn
    monkeypatch.setattr(wait_mod, "create_unpooled_engine", MagicMock(return_value=engine))
    return engine, conn


@pytest.mark.parametrize("applied", [["oldhead"], ["newhead", "otherhead"], ["newhead"]])
async def test_ready_checks_alembic_heads(wait_mod, readiness_engine, applied):
    engine, conn = readiness_engine
    result = MagicMock()
    result.scalars.return_value.all.return_value = applied
    conn.exec_driver_sql.return_value = result
    url = "postgresql+asyncpg://unused"
    if applied == ["newhead"]:
        await wait_mod._ready(url, sql=None, heads=frozenset({"newhead"}))
    else:
        with pytest.raises(RuntimeError, match="alembic heads mismatch"):
            await wait_mod._ready(url, sql=None, heads=frozenset({"newhead"}))
    wait_mod.create_unpooled_engine.assert_called_once_with(url)
    conn.exec_driver_sql.assert_awaited_once_with(wait_mod._ALEMBIC_VERSION_SQL)
    engine.connect.return_value.__aexit__.assert_awaited_once()
    engine.dispose.assert_awaited_once()


@pytest.mark.parametrize("value", [None, 1, 0])
async def test_ready_checks_optional_sql(wait_mod, readiness_engine, value):
    engine, conn = readiness_engine
    result = MagicMock()
    result.scalar.return_value = value
    conn.exec_driver_sql.return_value = result
    if value is None:
        with pytest.raises(RuntimeError, match="query returned no row"):
            await wait_mod._ready("postgresql+asyncpg://unused", sql="SELECT 1", heads=None)
    else:
        await wait_mod._ready("postgresql+asyncpg://unused", sql="SELECT 1", heads=None)
    conn.exec_driver_sql.assert_awaited_once_with("SELECT 1")
    engine.connect.return_value.__aexit__.assert_awaited_once()
    engine.dispose.assert_awaited_once()


@pytest.mark.parametrize("failure", ["connect", "query"])
async def test_ready_disposes_engine_on_database_failure(wait_mod, readiness_engine, failure):
    engine, conn = readiness_engine
    if failure == "connect":
        engine.connect.return_value.__aenter__.side_effect = RuntimeError("database unavailable")
    else:
        conn.exec_driver_sql.side_effect = RuntimeError("database unavailable")
    with pytest.raises(RuntimeError, match="database unavailable"):
        await wait_mod._ready("postgresql+asyncpg://unused", sql="SELECT 1", heads=None)
    engine.dispose.assert_awaited_once()


@pytest.mark.parametrize("scheme", ["postgresql", "postgresql+asyncpg", "postgresql+psycopg"])
def test_database_url_supports_migration_only_environment(wait_mod, monkeypatch, scheme):
    monkeypatch.delenv("EP_DATABASE_URL", raising=False)
    monkeypatch.setenv("DATABASE_URL", f"{scheme}://user:p%40ss@localhost/ep")
    assert wait_mod._database_url() == "postgresql+asyncpg://user:p%40ss@localhost/ep"


async def test_wait_for_database_uses_alembic_heads(wait_mod: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EP_DATABASE_URL", "postgresql+asyncpg://user:password@localhost/execution_plane")
    monkeypatch.setenv("WAIT_FOR_ALEMBIC_HEAD", "1")
    monkeypatch.delenv("WAIT_FOR_SQL", raising=False)
    monkeypatch.setattr(wait_mod, "_expected_heads", lambda: frozenset({"headrev"}))

    seen: dict[str, object] = {}

    async def _ready(dsn: str, sql: str | None, heads: frozenset[str] | None) -> None:
        seen["dsn"] = dsn
        seen["sql"] = sql
        seen["heads"] = heads

    monkeypatch.setattr(wait_mod, "_ready", _ready)
    await wait_mod.wait_for_database()
    assert seen["sql"] is None
    assert seen["heads"] == frozenset({"headrev"})
    assert "execution_plane" in str(seen["dsn"])
