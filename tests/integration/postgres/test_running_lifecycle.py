"""Real transactions for RUNNING claims, cancellation, recovery, and draining."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.models.cluster import Cluster
from execution_plane.models.completion_event import CompletionEvent
from execution_plane.models.execution_target import ExecutionTarget, TargetStatus
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.store_errors import StaleClaimError
from execution_plane.work_item.work_item_store import WorkStore
from tests.integration.postgres.test_work_dispatch import _seed_claimed_item

pytestmark = pytest.mark.integration


@pytest.fixture
async def attempt(migrated_database):
    engine = create_async_engine(migrated_database)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = WorkStore.from_engine(engine)
    item_id = await _seed_claimed_item(sessions, payload={})
    async with sessions() as session:
        item = await session.get(WorkItem, item_id)
        target = await session.get(ExecutionTarget, item.execution_target_id)
        cluster_id = target.cluster_id
    fence = {"claim_owner_id": item.claim_owner_id, "claim_generation": item.claim_generation}
    try:
        assert await store.mark_dispatched(item_id, **fence)
        yield SimpleNamespace(store=store, sessions=sessions, engine=engine, item=item, fence=fence)
    finally:
        async with sessions.begin() as session:
            await session.execute(delete(CompletionEvent).where(CompletionEvent.work_item_id == item_id))
            await session.execute(delete(WorkItem).where(WorkItem.id == item_id))
            await session.execute(delete(ExecutionTarget).where(ExecutionTarget.id == item.execution_target_id))
            await session.execute(delete(Cluster).where(Cluster.id == cluster_id))
        await engine.dispose()


async def test_running_checks_both_claim_fences_and_renews_the_lease(attempt):
    item = attempt.item
    with pytest.raises(StaleClaimError):
        await attempt.store.mark_running(item.id, claim_owner_id=uuid4(), claim_generation=item.claim_generation)
    with pytest.raises(StaleClaimError):
        await attempt.store.mark_running(
            item.id, claim_owner_id=item.claim_owner_id, claim_generation=item.claim_generation + 1
        )
    before = datetime.now(UTC)
    assert await attempt.store.mark_running(item.id, **attempt.fence)
    restored = await attempt.store.get(item.id, client_id=item.client_id)
    assert restored.status is WorkItemStatus.RUNNING
    assert restored.claimed_at >= before
    assert await attempt.store.refresh_claim(item.id, **attempt.fence) is False


async def test_cancellation_racing_start_never_gets_overwritten(attempt):
    # Each store operation obtains its own session and locks the same row.
    await asyncio.gather(
        attempt.store.mark_running(attempt.item.id, **attempt.fence),
        attempt.store.request_cancel(attempt.item.id, client_id=attempt.item.client_id),
    )
    item = await attempt.store.get(attempt.item.id, client_id=attempt.item.client_id)
    assert item.status is WorkItemStatus.CANCEL_REQUESTED
    assert not await attempt.store.mark_running(item.id, **attempt.fence)
    await attempt.store.requeue_pre_execute_failure(item.id, **attempt.fence)
    item = await attempt.store.get(item.id, client_id=item.client_id)
    assert item.status is WorkItemStatus.CANCEL_REQUESTED
    assert item.result is None
    assert await attempt.store.get_completion_event(item.id) is None


async def test_terminal_result_wins_cancellation_race(attempt):
    assert await attempt.store.mark_running(attempt.item.id, **attempt.fence)
    await asyncio.gather(
        attempt.store.set_result(attempt.item.id, {"output": {"ok": True}}, WorkItemStatus.COMPLETED, **attempt.fence),
        attempt.store.request_cancel(attempt.item.id, client_id=attempt.item.client_id),
    )
    item = await attempt.store.get(attempt.item.id, client_id=attempt.item.client_id)
    assert item.status is WorkItemStatus.COMPLETED
    event = await attempt.store.get_completion_event(item.id)
    assert event.status == "completed"


async def test_expired_running_lease_is_reconciled_without_a_new_attempt(attempt):
    item = attempt.item
    assert await attempt.store.mark_running(item.id, **attempt.fence)
    async with attempt.sessions.begin() as session:
        await session.execute(
            update(WorkItem).where(WorkItem.id == item.id).values(claimed_at=datetime.now(UTC) - timedelta(seconds=120))
        )
    # Protect unrelated integration fixtures from the global reservation scan.
    async with attempt.sessions.begin() as session:
        await session.execute(select(WorkItem).where(WorkItem.id != item.id).with_for_update())
        assert await attempt.store.reserve_one() is None
    restored = await attempt.store.get(item.id, client_id=item.client_id)
    assert restored.status is WorkItemStatus.RECONCILIATION_REQUIRED
    assert restored.claim_owner_id is None
    assert restored.claim_generation == item.claim_generation
    event = await attempt.store.get_completion_event(item.id)
    assert event.status == "reconciliation_required"


async def test_running_work_blocks_both_drain_checks_and_target_finalization(attempt):
    item = attempt.item
    assert await attempt.store.mark_running(item.id, **attempt.fence)
    async with attempt.sessions.begin() as session:
        await session.execute(
            update(ExecutionTarget)
            .where(ExecutionTarget.id == item.execution_target_id)
            .values(enabled=False, status=TargetStatus.DRAINING)
        )
    assert not await attempt.store.is_target_drained(item.execution_target_id)
    targets = ExecutionTargetStore.from_engine(attempt.engine)
    await targets.finalize_delete(item.execution_target_id)
    assert await targets.get(item.execution_target_id) is not None
