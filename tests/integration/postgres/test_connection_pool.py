"""Physical connection reuse, bounded acquisition, recovery, and disposal."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.database.engine import create_pooled_engine, create_unpooled_engine
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.work_item.work_item_store import WorkStore

pytestmark = pytest.mark.integration


async def test_stores_reuse_connections_and_owner_closes_them(migrated_database, monkeypatch):
    monkeypatch.setenv("EP_DB_POOL_SIZE", "1")
    engine = create_pooled_engine(migrated_database)
    observer = create_unpooled_engine(migrated_database)
    connections_created = []
    event.listen(engine.sync_engine, "connect", lambda *_: connections_created.append(True))
    try:
        async with engine.connect() as connection:
            pid = await connection.scalar(text("SELECT pg_backend_pid()"))
        async with (
            WorkStore.from_engine(engine) as work,
            ClusterStore.from_engine(engine) as clusters,
            ExecutionTargetStore.from_engine(engine) as targets,
        ):
            for _ in range(10):
                assert await work.get(uuid4(), client_id="pool-test") is None
                assert await clusters.get(uuid4()) is None
                assert await targets.get(uuid4()) is None
        # Closing borrowers leaves the shared pool usable, with the same backend.
        async with engine.connect() as connection:
            assert await connection.scalar(text("SELECT pg_backend_pid()")) == pid
        assert len(connections_created) == 1
        await engine.dispose()
        async with observer.connect() as connection:
            async with asyncio.timeout(5):
                while await connection.scalar(
                    text("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = :pid)"), {"pid": pid}
                ):
                    await connection.rollback()
                    await asyncio.sleep(0.01)
    finally:
        await engine.dispose()
        await observer.dispose()


async def test_pool_waits_without_opening_overflow_and_times_out(integration_database_url, monkeypatch):
    monkeypatch.setenv("EP_DB_POOL_SIZE", "1")
    monkeypatch.setenv("EP_DB_POOL_TIMEOUT", "0.1")
    engine = create_pooled_engine(integration_database_url)
    waiting = asyncio.Event()

    async def borrow() -> int:
        waiting.set()
        async with engine.connect() as connection:
            return await connection.scalar(text("SELECT pg_backend_pid()"))

    borrower = None
    try:
        async with engine.connect() as connection:
            pid = await connection.scalar(text("SELECT pg_backend_pid()"))
            with pytest.raises(PoolTimeoutError):
                async with engine.connect():
                    pytest.fail("An exhausted pool must not create an overflow connection")
            borrower = asyncio.create_task(borrow())
            await waiting.wait()
            assert not borrower.done()
        assert await asyncio.wait_for(borrower, timeout=5) == pid
    finally:
        if borrower is not None:
            borrower.cancel()
            await asyncio.gather(borrower, return_exceptions=True)
        await engine.dispose()


async def test_pre_ping_replaces_a_terminated_pooled_connection(integration_database_url, monkeypatch):
    monkeypatch.setenv("EP_DB_POOL_SIZE", "1")
    engine = create_pooled_engine(integration_database_url)
    observer = create_unpooled_engine(integration_database_url)
    try:
        async with engine.connect() as connection:
            pid = await connection.scalar(text("SELECT pg_backend_pid()"))
        async with observer.connect() as connection:
            assert await connection.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
            # Wait until termination is visible before testing checkout recovery.
            async with asyncio.timeout(5):
                while await connection.scalar(
                    text("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = :pid)"), {"pid": pid}
                ):
                    await connection.rollback()
                    await asyncio.sleep(0.01)
        async with engine.connect() as connection:
            assert await connection.scalar(text("SELECT pg_backend_pid()")) != pid
    finally:
        await engine.dispose()
        await observer.dispose()
