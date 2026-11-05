"""Tests for the shared database-store lifecycle base class."""

from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.store_base import StoreBase
from execution_plane.work_item.work_item_store import WorkStore


def test_database_stores_share_the_store_lifecycle_base() -> None:
    """All database-backed stores use the common resource lifecycle implementation."""
    assert issubclass(WorkStore, StoreBase)
    assert issubclass(ClusterStore, StoreBase)
    assert issubclass(ExecutionTargetStore, StoreBase)


async def test_borrowed_transaction_flushes_without_committing_or_rolling_back():
    """A compound domain handler owns its transaction boundary."""
    from unittest.mock import AsyncMock

    from execution_plane.store_base import StoreBase

    session = AsyncMock()
    store = StoreBase.from_transaction(session)
    await store._commit(session)
    await store._rollback(session)
    session.flush.assert_awaited_once()
    session.commit.assert_not_awaited()
    session.rollback.assert_not_awaited()


async def test_closing_borrowed_stores_does_not_dispose_shared_engine():
    """Only the runtime owner can dispose a pool used by multiple stores."""
    from unittest.mock import AsyncMock

    from sqlalchemy.ext.asyncio import AsyncEngine

    engine = AsyncMock(spec=AsyncEngine)
    async with (
        WorkStore.from_engine(engine),
        ClusterStore.from_engine(engine),
        ExecutionTargetStore.from_engine(engine),
    ):
        pass
    engine.dispose.assert_not_awaited()
