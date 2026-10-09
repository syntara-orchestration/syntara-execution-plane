"""Tests for the compose bootstrap wait script."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, patch

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

if TYPE_CHECKING:
    from types import ModuleType

_WAIT_SCRIPT = Path(__file__).resolve().parents[2] / "tools" / "wait_for_ep_database.py"


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


async def test_ready_rejects_missing_alembic_heads(wait_mod: ModuleType) -> None:
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=[{"version_num": "oldhead"}])
    with (
        patch.object(wait_mod.asyncpg, "connect", AsyncMock(return_value=conn)),
        pytest.raises(RuntimeError, match="alembic heads mismatch"),
    ):
        await wait_mod._ready("postgresql://unused", sql=None, heads=frozenset({"newhead"}))
    conn.fetch.assert_awaited_once_with(wait_mod._ALEMBIC_VERSION_SQL)
    conn.close.assert_awaited_once()


async def test_ready_rejects_unexpected_alembic_heads(wait_mod: ModuleType) -> None:
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=[{"version_num": "newhead"}, {"version_num": "otherhead"}])
    with (
        patch.object(wait_mod.asyncpg, "connect", AsyncMock(return_value=conn)),
        pytest.raises(RuntimeError, match="alembic heads mismatch"),
    ):
        await wait_mod._ready("postgresql://unused", sql=None, heads=frozenset({"newhead"}))
    conn.fetch.assert_awaited_once_with(wait_mod._ALEMBIC_VERSION_SQL)
    conn.close.assert_awaited_once()


async def test_ready_accepts_applied_alembic_heads(wait_mod: ModuleType) -> None:
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=[{"version_num": "newhead"}])
    with patch.object(wait_mod.asyncpg, "connect", AsyncMock(return_value=conn)):
        await wait_mod._ready("postgresql://unused", sql=None, heads=frozenset({"newhead"}))
    conn.fetch.assert_awaited_once_with(wait_mod._ALEMBIC_VERSION_SQL)
    conn.close.assert_awaited_once()


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
