"""Persist intent before returning, and delete only after background cleanup."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.work_item.work_item_store import WorkStore

pytestmark = pytest.mark.integration


@pytest.fixture
async def controls(migrated_database):
    engine = create_async_engine(migrated_database)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner = str(uuid4())
    try:
        yield WorkStore.from_engine(engine), sessions, owner
    finally:
        async with sessions.begin() as session:
            await session.execute(delete(WorkItem).where(WorkItem.client_id == owner))
        await engine.dispose()


async def test_cancellation_survives_a_new_store_and_cannot_dispatch(controls):
    store, sessions, owner = controls
    item_id = uuid4()
    await store.dispatch(owner, item_id, {})
    await store.request_cancel(item_id, client_id=owner)
    async with sessions() as session:
        item = await session.get(WorkItem, item_id)
        assert item.cancellation_requested is True
        assert item.status is WorkItemStatus.PENDING
        assert item.result is None
    assert await store.mark_dispatched(item_id) is False
    async with sessions() as session:
        await WorkStore.from_session(session).reconcile_control_requests()
    item = await store.get(item_id, client_id=owner)
    assert item.status is WorkItemStatus.CANCELLED
    assert item.result == {"cancelled": True, "execution_started": False}


async def test_pending_deletion_is_async_idempotent_and_client_scoped(controls):
    store, sessions, owner = controls
    item_id = uuid4()
    await store.dispatch(owner, item_id, {})
    await store.request_delete(item_id, client_id="foreign")
    assert (await store.get(item_id, client_id=owner)).deletion_requested is False
    await store.request_delete(item_id, client_id=owner)
    await store.request_delete(item_id, client_id=owner)
    item = await store.get(item_id, client_id=owner)
    assert item.deletion_requested
    assert item.cancellation_requested
    assert item.status is WorkItemStatus.PENDING
    async with sessions() as session:
        await WorkStore.from_session(session).reconcile_control_requests()
    assert await store.get(item_id, client_id=owner) is None
    await store.request_delete(item_id, client_id=owner)


async def test_running_deletion_retains_work_until_resource_cleanup_is_confirmed(controls):
    store, sessions, owner = controls
    item_id, claim_owner = uuid4(), uuid4()
    async with sessions.begin() as session:
        session.add(
            WorkItem(
                id=item_id,
                client_id=owner,
                status=WorkItemStatus.RUNNING,
                created_at=datetime.now(UTC),
                claimed_at=datetime.now(UTC),
                claim_owner_id=claim_owner,
                claim_generation=1,
                backend_resource_name="owned-job",
                backend_resource_uid="owned-uid",
                resource_cleanup_status="pending",
            )
        )
    await store.request_delete(item_id, client_id=owner)
    assert (await store.get(item_id, client_id=owner)).status is WorkItemStatus.RUNNING
    await store.reconcile_control_requests()
    assert (await store.get(item_id, client_id=owner)).status is WorkItemStatus.CANCEL_REQUESTED
    assert await store.refresh_claim(item_id, claim_owner_id=claim_owner, claim_generation=1) is True
    await store.set_result(
        item_id, {"cancelled": True}, WorkItemStatus.CANCELLED, claim_owner_id=claim_owner, claim_generation=1
    )
    await store.reconcile_control_requests()
    assert await store.get(item_id, client_id=owner) is not None
    assert item_id in [item.id for item in await store.deletions_awaiting_cleanup()]
    await store.update_backend_resource(
        item_id,
        resource_name="owned-job",
        resource_uid="owned-uid",
        cleanup_status="complete",
        claim_owner_id=None,
        claim_generation=1,
    )
    await store.reconcile_control_requests()
    assert await store.get(item_id, client_id=owner) is None


async def test_deletion_suppresses_new_completion_delivery(controls):
    store, _sessions, owner = controls
    item_id = uuid4()
    await store.dispatch(owner, item_id, {})
    await store.set_result(item_id, {}, WorkItemStatus.COMPLETED)
    await store.request_delete(item_id, client_id=owner)
    assert item_id not in [item.id for item in await store.claim_work_items_that_need_completion_notification()]
