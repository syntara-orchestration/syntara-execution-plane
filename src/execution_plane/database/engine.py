"""SQLAlchemy async engine factory with EP database TLS via connect_args."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from execution_plane.config import get_ep_settings
from execution_plane.database.ssl import ssl_connect_args_from_env, strip_unsupported_asyncpg_ssl_query

if TYPE_CHECKING:
    from sqlalchemy.pool import Pool


def create_ep_engine(database_url: str, poolclass: type[Pool] | None = None) -> AsyncEngine:
    """Create a bounded runtime pool with checkout health checks and EP database TLS.

    Explicit pool classes retain their own sizing behavior (for example NullPool
    in tests); queue-pool settings only apply to the default runtime pool.
    """
    url = strip_unsupported_asyncpg_ssl_query(database_url)
    connect_args = ssl_connect_args_from_env()
    if poolclass is None:
        settings = get_ep_settings()
        return create_async_engine(
            url,
            connect_args=connect_args,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_pool_max_overflow,
            pool_timeout=settings.db_pool_timeout,
            pool_pre_ping=True,
        )
    return create_async_engine(url, poolclass=poolclass, connect_args=connect_args, pool_pre_ping=True)
