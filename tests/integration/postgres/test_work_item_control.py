"""Persist intent before returning, and delete only after background cleanup."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import delete, update
from sqlalchemy.exc import IntegrityError
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
    assert (await store.get(item_id, client_id=owner)).status is WorkItemStatus.RUNNING
    assert await store.refresh_claim(item_id, claim_owner_id=claim_owner, claim_generation=1) is True
    await store.set_result(
        item_id, {"cancelled": True}, WorkItemStatus.CANCELLED, claim_owner_id=claim_owner, claim_generation=1
    )
    await store.reconcile_control_requests()
    assert await store.get(item_id, client_id=owner) is not None
    assert item_id in [item.id for item in await store.work_awaiting_requested_cleanup()]
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


async def test_database_rejects_deletion_without_cancellation_on_insert_and_update(controls):
    store, sessions, owner = controls
    async with sessions() as session:
        session.add(
            WorkItem(
                id=uuid4(),
                client_id=owner,
                deletion_requested=True,
                cancellation_requested=False,
                created_at=datetime.now(UTC),
            )
        )
        with pytest.raises(IntegrityError, match="ck_work_items_deletion_requires_cancellation"):
            await session.commit()
        await session.rollback()
    item_id = uuid4()
    await store.dispatch(owner, item_id, {})
    async with sessions() as session:
        with pytest.raises(IntegrityError, match="ck_work_items_deletion_requires_cancellation"):
            await session.execute(update(WorkItem).where(WorkItem.id == item_id).values(deletion_requested=True))
        await session.rollback()
    await store.request_delete(item_id, client_id=owner)
    async with sessions() as session:
        with pytest.raises(IntegrityError, match="ck_work_items_deletion_requires_cancellation"):
            await session.execute(update(WorkItem).where(WorkItem.id == item_id).values(cancellation_requested=False))
        await session.rollback()


@pytest.mark.parametrize("status", [WorkItemStatus.DISPATCHED, WorkItemStatus.RUNNING, WorkItemStatus.COMPLETED])
async def test_cancellation_intent_preserves_lifecycle_and_claim_timestamp(controls, status):
    store, sessions, owner = controls
    item_id = uuid4()
    claimed_at = datetime.now(UTC)
    async with sessions.begin() as session:
        session.add(WorkItem(id=item_id, client_id=owner, status=status, created_at=claimed_at, claimed_at=claimed_at))
    await store.request_cancel(item_id, client_id=owner)
    await store.reconcile_control_requests()
    item = await store.get(item_id, client_id=owner)
    assert item.cancellation_requested is True
    assert item.deletion_requested is False
    assert item.status is status
    assert item.claimed_at == claimed_at


@pytest.mark.parametrize("status", list(WorkItemStatus))
@pytest.mark.parametrize("operation", ["cancel", "delete"])
async def test_request_records_intent_unconditionally_and_changes_no_outcome(controls, status, operation):
    store, sessions, owner = controls
    item_id = uuid4()
    now = datetime.now(UTC)
    async with sessions.begin() as session:
        session.add(
            WorkItem(
                id=item_id,
                client_id=owner,
                status=status,
                created_at=now,
                claimed_at=now,
                completed_at=now,
                result={"existing": "outcome"},
            )
        )
    before = (await store.get(item_id, client_id=owner)).model_dump()
    for _ in range(2):
        if operation == "cancel":
            await store.request_cancel(item_id, client_id=owner)
        else:
            await store.request_delete(item_id, client_id=owner)
        after = (await store.get(item_id, client_id=owner)).model_dump()
        assert after == {**before, "cancellation_requested": True, "deletion_requested": operation == "delete"}


@pytest.mark.parametrize("dispatch_first", [False, True])
async def test_queued_cancellation_fences_dispatch_but_never_cancels_allocated_work(controls, dispatch_first):
    from execution_plane.store_errors import StaleClaimError

    store, sessions, owner = controls
    item_id, claim_owner = uuid4(), uuid4()
    async with sessions.begin() as session:
        session.add(
            WorkItem(
                id=item_id,
                client_id=owner,
                status=WorkItemStatus.CLAIMED,
                created_at=datetime.now(UTC),
                claim_owner_id=claim_owner,
                claim_generation=1,
            )
        )
    if dispatch_first:
        assert await store.mark_dispatched(item_id, claim_owner_id=claim_owner, claim_generation=1)
    await store.request_cancel(item_id, client_id=owner)
    await store.reconcile_control_requests()
    item = await store.get(item_id, client_id=owner)
    if dispatch_first:
        assert item.status is WorkItemStatus.DISPATCHED
        assert item.result is None
        assert item.completion_notification_state is None
        assert item.claim_owner_id == claim_owner
    else:
        assert item.status is WorkItemStatus.CANCELLED
        assert item.result == {"cancelled": True, "execution_started": False}
        with pytest.raises(StaleClaimError):
            await store.mark_dispatched(item_id, claim_owner_id=claim_owner, claim_generation=1)


async def test_unconfirmed_pre_execute_cleanup_retains_uncertainty_and_cancel_intent(controls):
    store, sessions, owner = controls
    item_id, claim_owner = uuid4(), uuid4()
    async with sessions.begin() as session:
        session.add(
            WorkItem(
                id=item_id,
                client_id=owner,
                status=WorkItemStatus.DISPATCHED,
                created_at=datetime.now(UTC),
                claim_owner_id=claim_owner,
                claim_generation=1,
                backend_resource_name="owned-job",
                resource_cleanup_status="failed",
            )
        )
    await store.request_cancel(item_id, client_id=owner)
    await store.requeue_pre_execute_failure(item_id, claim_owner_id=claim_owner, claim_generation=1)
    item = await store.get(item_id, client_id=owner)
    assert item.status is WorkItemStatus.RECONCILIATION_REQUIRED
    assert item.cancellation_requested is True
    assert item.deletion_requested is False
    assert item_id in [row.id for row in await store.work_awaiting_requested_cleanup()]
    await store.update_backend_resource(
        item_id,
        resource_name="owned-job",
        resource_uid=None,
        cleanup_status="complete",
        claim_owner_id=None,
        claim_generation=1,
    )
    await store.reconcile_control_requests()
    after = await store.get(item_id, client_id=owner)
    assert after.status is WorkItemStatus.RECONCILIATION_REQUIRED
    assert after.result == item.result
    assert item_id not in [row.id for row in await store.work_awaiting_requested_cleanup()]


async def test_queued_state_with_unconfirmed_resources_is_not_declared_cancelled(controls):
    store, sessions, owner = controls
    item_id = uuid4()
    async with sessions.begin() as session:
        session.add(
            WorkItem(
                id=item_id,
                client_id=owner,
                status=WorkItemStatus.PENDING,
                created_at=datetime.now(UTC),
                backend_resource_name="unconfirmed-job",
                resource_cleanup_status="failed",
            )
        )
    await store.request_cancel(item_id, client_id=owner)
    await store.reconcile_control_requests()
    item = await store.get(item_id, client_id=owner)
    assert item.status is WorkItemStatus.PENDING
    assert item.result is None
    assert item.completion_notification_state is None
