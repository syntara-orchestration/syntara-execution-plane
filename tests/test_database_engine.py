"""Runtime pool configuration and explicit pool compatibility."""

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

    engine_module.create_ep_engine(url)

    create_engine.assert_called_once_with(
        "postgresql+asyncpg://localhost/test",
        connect_args={"ssl": "require"},
        pool_size=3 if configured else 5,
        max_overflow=2 if configured else 0,
        pool_timeout=7.5 if configured else 30.0,
        pool_pre_ping=True,
    )


async def test_explicit_null_pool_does_not_receive_queue_pool_options() -> None:
    engine = engine_module.create_ep_engine("postgresql+asyncpg://localhost/test", poolclass=NullPool)
    try:
        assert isinstance(engine.pool, NullPool)
    finally:
        await engine.dispose()
