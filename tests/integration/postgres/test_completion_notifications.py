"""PostgreSQL coverage for inline completion notification delivery."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from execution_plane.models.work_item import CompletionNotificationState, WorkItem, WorkItemStatus
from execution_plane.work_item.work_item_store import WorkStore

pytestmark = pytest.mark.integration


@pytest.fixture
async def completion_work(migrated_database):
    """Create an isolated completion and clean up its database resources."""
    engine = create_async_engine(migrated_database)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    item_id = uuid4()
    try:
        async with WorkStore.from_engine(engine) as store:
            await store.dispatch("completion-concurrency-test", item_id, {"invocation": {"version": 1}})
            await store.set_result(item_id, {"output": "ok"}, WorkItemStatus.COMPLETED)
            yield store, sessions, item_id
    finally:
        async with sessions() as session:
            item = await session.get(WorkItem, item_id)
            if item is not None:
                await session.delete(item)
            await session.commit()
        await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_notifiers_claim_one_work_item_once(completion_work) -> None:
    """Two notifier instances cannot initially claim the same completion together."""
    store, sessions, item_id = completion_work
    async with WorkStore.from_engine(store._engine) as second:
        first_claimed, second_claimed = await asyncio.gather(
            store.claim_work_items_that_need_completion_notification(),
            second.claim_work_items_that_need_completion_notification(),
        )
        first_ids = {item.id for item in first_claimed}
        second_ids = {item.id for item in second_claimed}
        assert first_ids.isdisjoint(second_ids)
        all_claimed = [*first_claimed, *second_claimed]
        claimed = next(item for item in all_claimed if item.id == item_id)
        assert sum(item.id == item_id for item in all_claimed) == 1
        assert claimed.completion_notification_state is CompletionNotificationState.DELIVERING

        assert await store.record_completion_notification_delivery(item_id) is True

    async with sessions() as session:
        item = await session.get(WorkItem, item_id)
        assert item.completion_notification_state is CompletionNotificationState.DELIVERED
        assert item.completion_notification_delivered_at is not None


@pytest.mark.asyncio
async def test_concurrent_acknowledgements_transition_once(completion_work) -> None:
    """Competing acknowledgements serialize and apply DELIVERING -> DELIVERED once."""
    store, sessions, item_id = completion_work
    await store.claim_work_items_that_need_completion_notification()

    changed = await asyncio.gather(
        store.record_completion_notification_delivery(item_id),
        store.record_completion_notification_delivery(item_id),
    )

    assert sorted(changed) == [False, True]
    async with sessions() as session:
        item = await session.get(WorkItem, item_id)
        assert item.completion_notification_state is CompletionNotificationState.DELIVERED
        assert item.completion_notification_attempts == 1
        assert item.completion_notification_claim_expires_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("first_succeeds", [True, False])
async def test_completion_responses_wait_for_row_lock_and_recheck_state(completion_work, first_succeeds) -> None:
    """A waiting response observes the committed transition instead of overwriting it."""
    store, sessions, item_id = completion_work
    await store.claim_work_items_that_need_completion_notification()
    # Each store has its own transaction; the first holds the row lock until commit.
    async with sessions() as first_session, sessions() as second_session:
        first = WorkStore.from_session(first_session)
        second = WorkStore.from_session(second_session)
        cached_item = await second_session.get(WorkItem, item_id)
        assert cached_item.completion_notification_state is CompletionNotificationState.DELIVERING
        await first_session.get(WorkItem, item_id, with_for_update=True)
        if first_succeeds:
            response = second.record_completion_notification_attempt(item_id, error="late timeout")
        else:
            response = second.record_completion_notification_delivery(item_id)
        waiting = asyncio.create_task(response)
        try:
            done, _ = await asyncio.wait({waiting}, timeout=0.1)
            assert not done
            if first_succeeds:
                assert await first.record_completion_notification_delivery(item_id) is True
            else:
                assert (
                    await first.record_completion_notification_attempt(item_id, error="timeout", retry_after_seconds=0)
                    is True
                )
            assert await asyncio.wait_for(waiting, timeout=5) is False
            expected = CompletionNotificationState.DELIVERED if first_succeeds else CompletionNotificationState.PENDING
            assert cached_item.completion_notification_state is expected
        finally:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)

    if not first_succeeds:
        # Success after another attempt scheduled a retry is harmless; that retry
        # obtains an acknowledgement from the idempotent receiver.
        claimed = await store.claim_work_items_that_need_completion_notification()
        assert item_id in {item.id for item in claimed}
        assert await store.record_completion_notification_delivery(item_id) is True

    async with sessions() as session:
        item = await session.get(WorkItem, item_id)
        assert item.completion_notification_state is CompletionNotificationState.DELIVERED
        assert item.completion_notification_last_error is None
        assert item.completion_notification_delivered_at is not None


@pytest.mark.asyncio
async def test_expired_completion_lease_can_be_reclaimed(completion_work) -> None:
    """A notifier crash leaves a recoverable lease and does not lose the completion."""
    store, sessions, item_id = completion_work
    await store.claim_work_items_that_need_completion_notification()
    async with sessions() as session:
        item = await session.get(WorkItem, item_id, with_for_update=True)
        item.completion_notification_claim_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()

    claimed = await store.claim_work_items_that_need_completion_notification()
    assert item_id in {item.id for item in claimed}
    assert await store.record_completion_notification_delivery(item_id) is True

    # Responses from any other in-flight attempt cannot undo the acknowledgement.
    assert await store.record_completion_notification_attempt(item_id, error="late timeout") is False
    assert await store.record_completion_notification_delivery(item_id) is False
    async with sessions() as session:
        item = await session.get(WorkItem, item_id)
        assert item.completion_notification_state is CompletionNotificationState.DELIVERED
        assert item.completion_notification_attempts == 1
