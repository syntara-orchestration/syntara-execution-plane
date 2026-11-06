"""Explicit pooled and unpooled engine creation with EP database TLS."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from execution_plane.config import get_ep_settings
from execution_plane.database.ssl import ssl_connect_args_from_env, strip_unsupported_asyncpg_ssl_query


def create_pooled_engine(database_url: str) -> AsyncEngine:
    """Create a bounded, reusable connection pool with checkout health checks.

    Ordinary worker database logic must use the single pooled engine created at
    worker startup, borrowing it through stores rather than creating more engines
    or unpooled connections. The dedicated LISTEN connection is managed separately.
    The API also creates one pooled engine for its application lifetime.

    The caller owns disposal. EP_DB_POOL_* settings govern connection capacity
    and acquisition waiting, independently of WorkItem runtime.
    """
    settings = get_ep_settings()
    return create_async_engine(
        strip_unsupported_asyncpg_ssl_query(database_url),
        connect_args=ssl_connect_args_from_env(),
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_pool_max_overflow,
        pool_timeout=settings.db_pool_timeout,
        pool_pre_ping=True,
    )


def create_unpooled_engine(database_url: str) -> AsyncEngine:
    """Create an engine that opens and closes a physical connection per acquisition.

    Intended for short-lived tooling and independent test observers. Ordinary
    worker database logic must use the worker's shared pooled engine instead.
    EP_DB_POOL_* settings do not apply; the caller owns engine disposal.
    """
    url = strip_unsupported_asyncpg_ssl_query(database_url)
    connect_args = ssl_connect_args_from_env()
    return create_async_engine(url, poolclass=NullPool, connect_args=connect_args)
