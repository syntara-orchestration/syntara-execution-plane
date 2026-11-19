"""Explicit pooled and unpooled engine policies."""

from unittest.mock import Mock

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from execution_plane.database import engine as engine_module


@pytest.mark.parametrize("configured", [False, True])
def test_engine_applies_pool_bounds_and_ssl(monkeypatch: pytest.MonkeyPatch, *, configured: bool) -> None:
    if configured:
        monkeypatch.setenv("EP_DB_POOL_SIZE", "3")
        monkeypatch.setenv("EP_DB_POOL_MAX_OVERFLOW", "2")
        monkeypatch.setenv("EP_DB_POOL_TIMEOUT", "7.5")
    monkeypatch.setenv("EP_DB_SSL_MODE", "require")
    create_engine = Mock(spec=create_async_engine)
    monkeypatch.setattr(engine_module, "create_async_engine", create_engine)
    url = "postgresql+asyncpg://localhost/test?sslrootcert=/ca.pem"

    engine_module.create_pooled_engine(url)

    create_engine.assert_called_once_with(
        "postgresql+asyncpg://localhost/test",
        connect_args={"ssl": "require"},
        pool_size=3 if configured else 5,
        max_overflow=2 if configured else 0,
        pool_timeout=7.5 if configured else 30.0,
        pool_pre_ping=True,
    )


async def test_unpooled_engine_uses_null_pool_without_service_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EP_DATABASE_URL", raising=False)
    engine = engine_module.create_unpooled_engine("postgresql+asyncpg://localhost/test")
    try:
        assert isinstance(engine.pool, NullPool)
    finally:
        await engine.dispose()


def test_unpooled_engine_applies_tls_without_runtime_pool_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EP_DB_POOL_SIZE", "0")
    monkeypatch.setenv("EP_DB_SSL_MODE", "require")
    create_engine = Mock(spec=create_async_engine)
    monkeypatch.setattr(engine_module, "create_async_engine", create_engine)

    engine_module.create_unpooled_engine("postgresql+asyncpg://localhost/test?sslrootcert=/ca.pem")

    create_engine.assert_called_once_with(
        "postgresql+asyncpg://localhost/test", poolclass=NullPool, connect_args={"ssl": "require"}
    )
