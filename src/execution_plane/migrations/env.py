"""Alembic environment for the execution_plane schema.

Runs migrations scoped to the `execution_plane` PostgreSQL schema only.
The Syntara API's migrations (in backend/src/syntara/core/database/migrations/)
manage the `public` schema independently — these two migration chains never touch
each other's tables.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING

from alembic import context
from sqlalchemy import Table, text
from sqlmodel import SQLModel

from execution_plane.database.engine import create_unpooled_engine
from execution_plane.database.ssl import strip_unsupported_asyncpg_ssl_query
from execution_plane.logging.logging import build_json_formatter

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection
    from sqlalchemy.schema import SchemaItem

# Import all execution_plane models so they are registered in SQLModel.metadata.
import execution_plane.models.cluster
import execution_plane.models.execution_target
import execution_plane.models.work_item  # noqa: F401

EP_SCHEMA = "execution_plane"

config = context.config


def configure_structured_migration_logging() -> None:
    """Route Alembic and SQLAlchemy logs through EP's structured formatter."""
    handler = logging.StreamHandler()
    handler.setFormatter(build_json_formatter())
    root_logger = logging.getLogger()
    root_logger.handlers[:] = [handler]
    root_logger.setLevel(logging.INFO)
    logging.getLogger("alembic").setLevel(logging.INFO)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


if config.config_file_name is not None:
    configure_structured_migration_logging()

target_metadata = SQLModel.metadata

database_url = config.get_main_option("sqlalchemy.url") or os.environ.get("DATABASE_URL")
if not database_url:
    msg = "DATABASE_URL must be set or sqlalchemy.url provided in alembic.ini"
    raise RuntimeError(msg)
migration_database_url = strip_unsupported_asyncpg_ssl_query(database_url)
config.set_main_option("sqlalchemy.url", migration_database_url)


def include_object(
    obj: SchemaItem,
    _name: str | None,
    type_: str,
    _reflected: bool,  # noqa: FBT001 — Alembic passes this callback argument positionally.
    _compare_to: SchemaItem | None,
) -> bool:
    """Restrict autogenerate to the execution_plane schema only."""
    if type_ == "table":
        return isinstance(obj, Table) and obj.schema == EP_SCHEMA
    return True


def run_migrations_offline() -> None:
    """Generate migration SQL without opening a database connection."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        include_object=include_object,
        compare_type=True,
        compare_server_default=True,
        version_table_schema=EP_SCHEMA,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Run schema-scoped migrations on the supplied connection."""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
        include_object=include_object,
        compare_type=True,
        compare_server_default=True,
        version_table_schema=EP_SCHEMA,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Prepare the schema using an explicitly unpooled migration engine."""
    connectable = create_unpooled_engine(migration_database_url)
    try:
        async with connectable.connect() as connection:
            await connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {EP_SCHEMA}"))
            await connection.commit()
            await connection.run_sync(do_run_migrations)
    finally:
        await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations against a live database."""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
