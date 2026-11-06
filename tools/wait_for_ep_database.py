"""Block until the Execution Plane database accepts connections.

podman-compose starts dependent containers as soon as a required container exists,
so compose healthchecks cannot order bootstrap. Callers wait here instead.
"""

from __future__ import annotations

import asyncio
import os
import time

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.engine import make_url

from execution_plane.database.engine import create_unpooled_engine

_DEFAULT_TIMEOUT_SECONDS = 180.0
_RETRY_PAUSE_SECONDS = 2.0
_ALEMBIC_VERSION_SQL = "SELECT version_num FROM execution_plane.alembic_version"


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL") or os.environ.get("EP_DATABASE_URL")
    if not url:
        msg = "DATABASE_URL or EP_DATABASE_URL is required"
        raise SystemExit(msg)
    return make_url(url).set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


def _expected_heads() -> frozenset[str]:
    config_path = os.environ.get("ALEMBIC_CONFIG", "alembic.ini")
    heads = ScriptDirectory.from_config(Config(config_path)).get_heads()
    if not heads:
        msg = f"no alembic heads in {config_path}"
        raise RuntimeError(msg)
    return frozenset(heads)


def _alembic_heads_from_env() -> frozenset[str] | None:
    if not os.environ.get("WAIT_FOR_ALEMBIC_HEAD"):
        return None
    return _expected_heads()


async def _ready(database_url: str, sql: str | None, heads: frozenset[str] | None) -> None:
    engine = create_unpooled_engine(database_url)
    try:
        async with engine.connect() as conn:
            if sql:
                result = await conn.exec_driver_sql(sql)
                if result.scalar() is None:
                    msg = f"query returned no row: {sql}"
                    raise RuntimeError(msg)
            if heads:
                result = await conn.exec_driver_sql(_ALEMBIC_VERSION_SQL)
                applied = set(result.scalars().all())
                if applied != heads:
                    msg = f"alembic heads mismatch: expected={sorted(heads)} applied={sorted(applied)}"
                    raise RuntimeError(msg)
    finally:
        await engine.dispose()


async def wait_for_database() -> None:
    """Retry readiness checks with a fresh, explicitly unpooled engine per attempt."""
    database_url = _database_url()
    sql = os.environ.get("WAIT_FOR_SQL") or None
    heads = _alembic_heads_from_env()
    timeout = float(os.environ.get("WAIT_FOR_DATABASE_SECONDS", _DEFAULT_TIMEOUT_SECONDS))
    deadline = time.monotonic() + timeout
    last_error = "not attempted"
    while time.monotonic() < deadline:
        try:
            await _ready(database_url, sql, heads)
            return
        except Exception as exc:  # noqa: BLE001 — bootstrap races any connect failure
            last_error = str(exc)
            print(f"Waiting for database: {last_error}", flush=True)
            await asyncio.sleep(_RETRY_PAUSE_SECONDS)
    msg = f"Timed out waiting for database: {last_error}"
    raise SystemExit(msg)


if __name__ == "__main__":
    asyncio.run(wait_for_database())
