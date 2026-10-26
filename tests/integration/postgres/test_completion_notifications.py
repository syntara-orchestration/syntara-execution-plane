"""PostgreSQL coverage for inline completion notification delivery."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from execution_plane.models.work_item import NotificationState, WorkItem, WorkItemStatus
from execution_plane.work_item.work_item_store import WorkStore

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_concurrent_notifiers_claim_one_work_item_once(migrated_database: str) -> None:
    """Two notifier instances cannot claim or deliver the same completion together."""
    engine = create_async_engine(migrated_database)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    item_id = uuid4()
    try:
        async with WorkStore.from_engine(engine) as store:
            await store.dispatch("completion-concurrency-test", item_id, {"invocation": {"version": 1}})
            await store.set_result(item_id, {"output": "ok"}, WorkItemStatus.COMPLETED)

        first = WorkStore.from_engine(engine)
        second = WorkStore.from_engine(engine)
        try:
            first_claimed, second_claimed = await asyncio.gather(
                first.claim_due_completion_notifications(),
                second.claim_due_completion_notifications(),
            )
            first_ids = {item.id for item in first_claimed}
            second_ids = {item.id for item in second_claimed}
            assert first_ids.isdisjoint(second_ids)
            all_claimed = [*first_claimed, *second_claimed]
            claimed = next(item for item in all_claimed if item.id == item_id)
            assert sum(item.id == item_id for item in all_claimed) == 1
            assert claimed.notification_state is NotificationState.DELIVERING
            assert claimed.notification_claim_owner_id is not None

            claiming_store = first if any(item.id == item_id for item in first_claimed) else second
            delivered = await claiming_store.record_completion_notification_attempt(
                item_id,
                claim_owner_id=claimed.notification_claim_owner_id,
                delivered=True,
            )
            assert delivered is True
        finally:
            await first.close()
            await second.close()

        async with sessions() as session:
            item = await session.get(WorkItem, item_id)
            assert item is not None
            assert item.notification_state is NotificationState.DELIVERED
            assert item.notified_at is not None
    finally:
        async with sessions() as session:
            item = await session.get(WorkItem, item_id)
            if item is not None:
                await session.delete(item)
            await session.commit()
        await engine.dispose()
