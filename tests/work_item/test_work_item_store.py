"""Tests for database ownership and lifecycle in WorkStore."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, Self
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import NullPool

from execution_plane.models.cluster import Cluster, ClusterType
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane.models.work_item import CompletionNotificationState, WorkItem, WorkItemStatus
from execution_plane.store_errors import StaleClaimError
from execution_plane.work_item.errors import DuplicateWorkItemError, WorkItemNotFoundError
from execution_plane.work_item.work_item_store import WorkStore


class _Session:
    def __init__(self, item: WorkItem | None = None) -> None:
        self.item = item
        self.items: dict[type[object], Any] = {}
        self.added: list[Any] = []
        self.commits = 0
        self.rollbacks = 0
        self.execute = AsyncMock()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def add(self, item: object) -> None:
        self.added.append(item)

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1

    async def get(
        self,
        model: object,
        _item_id: uuid.UUID,
        *,
        with_for_update: bool = False,
        populate_existing: bool = False,
    ) -> object:
        del with_for_update, populate_existing
        if model in self.items:
            return self.items[model]
        return self.item


class _SessionFactory:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def __call__(self) -> _Session:
        return self.session


class _ScalarResult:
    def __init__(self, value: uuid.UUID | None) -> None:
        self.value = value

    def scalar_one_or_none(self) -> uuid.UUID | None:
        return self.value

    def scalars(self) -> Self:
        return self

    def first(self) -> uuid.UUID | None:
        return self.value


class _ClaimResult:
    def __init__(self, item: object | None) -> None:
        self.item = item

    def scalars(self) -> Self:
        return self

    def all(self) -> list[WorkItem]:
        return [self.item] if self.item is not None else []

    def first(self) -> WorkItem | None:
        return self.item

    def scalar_one_or_none(self) -> object | None:
        return self.item


def _work_item(**overrides: object) -> WorkItem:
    defaults: dict[str, object] = {
        "id": uuid.uuid4(),
        "client_id": "test-client",
        "status": WorkItemStatus.PENDING,
        "created_at": datetime.now(UTC),
    }
    defaults.update(overrides)
    return WorkItem(**defaults)


def _cluster() -> Cluster:
    return Cluster(
        id=uuid.uuid4(),
        name="test-cluster",
        created_by=uuid.uuid4(),
        created_at=datetime.now(UTC),
        updated_by=uuid.uuid4(),
        updated_at=datetime.now(UTC),
        cluster_type=ClusterType.OPENSHIFT,
    )


def _target(target_id: uuid.UUID, cluster: Cluster) -> ExecutionTarget:
    return ExecutionTarget(
        id=target_id,
        cluster_id=cluster.id,
        name="worker-target",
        backend_type=BackendType.VANILLA_K8S,
        endpoint="https://example.test",
        placement={"type": "static"},
        status=TargetStatus.ACTIVE,
        credential_ref=uuid.uuid4(),
        created_by=uuid.uuid4(),
        created_at=datetime.now(UTC),
        updated_by=uuid.uuid4(),
        updated_at=datetime.now(UTC),
    )


def _store() -> WorkStore:
    return WorkStore("postgresql+asyncpg://localhost/syntara")


@pytest.mark.asyncio
async def test_dispatch_owns_session_and_commits_database_notification() -> None:
    """Dispatch uses an internally managed session and commits the database notification."""
    store = _store()
    session = _Session()
    session.execute.return_value = _ClaimResult(None)
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    item = await store.dispatch(
        "test-client",
        uuid.uuid4(),
        {"input_config": {}},
    )

    assert item.status == WorkItemStatus.PENDING
    assert session.added == [item]
    assert session.commits == 1
    session.execute.assert_awaited()
    await store.close()


@pytest.mark.asyncio
async def test_set_result_reloads_item_by_id() -> None:
    """Result persistence does not require a session-bound WorkItem."""
    store = _store()
    item = WorkItem(
        id=uuid.uuid4(),
        client_id="test-client",
        created_at=datetime.now(UTC),
    )
    session = _Session(item)
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    await store.set_result(item.id, {"output": "ok"}, WorkItemStatus.COMPLETED)

    assert item.status == WorkItemStatus.COMPLETED
    assert item.result == {"output": "ok"}
    assert item.completion_notification_state is CompletionNotificationState.PENDING
    assert item.completion_notification_attempts == 0
    assert item.completion_notification_next_attempt_at == item.completed_at
    assert item.completion_notification_last_error is None
    assert item.completion_notification_delivered_at is None
    assert session.commits == 1
    await store.close()


@pytest.mark.asyncio
async def test_is_target_drained_checks_claimed_and_dispatched_work() -> None:
    """A target is drained only when no active WorkItems reference it."""
    store = _store()
    session = _Session()
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]
    target_id = uuid.uuid4()

    session.execute.return_value = _ScalarResult(None)
    assert await store.is_target_drained(target_id) is True
    active_item = WorkItem(
        id=uuid.uuid4(),
        activity_handle="handle",
        execution_target_id=target_id,
        status=WorkItemStatus.CLAIMED,
        created_at=datetime.now(UTC),
    )
    session.execute.return_value = _ScalarResult(active_item.id)
    assert await store.is_target_drained(target_id) is False
    await store.close()


@pytest.mark.asyncio
async def test_reserve_one_claims_pending_work_without_choosing_target() -> None:
    """A reserved item stays visible to draining while worker-side placement runs."""
    store = _store()
    item = WorkItem(
        id=uuid.uuid4(),
        activity_handle="handle",
        status=WorkItemStatus.PENDING,
        created_at=datetime.now(UTC),
    )
    session = _Session()
    session.execute.side_effect = [_ClaimResult(None), _ClaimResult(item)]
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    claimed = await store.reserve_one()

    assert claimed is item
    assert claimed.execution_target_id is None
    assert claimed.status is WorkItemStatus.CLAIMED
    assert claimed.claimed_at is not None
    assert claimed.claim_owner_id is not None
    assert claimed.claim_generation == 1
    assert session.commits == 2
    await store.close()


@pytest.mark.asyncio
async def test_uncertain_dispatch_records_reconciliation_completion_notification() -> None:
    """An unknown node result remains visible and reaches AO through inline state."""
    owner_id = uuid.uuid4()
    item = WorkItem(
        id=uuid.uuid4(),
        client_id="test-client",
        status=WorkItemStatus.DISPATCHED,
        claim_owner_id=owner_id,
        claim_generation=3,
        created_at=datetime.now(UTC),
    )
    session = _Session(item)
    store = _store()
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    await store.mark_reconciliation_required(
        item.id,
        "gRPC outcome was uncertain",
        claim_owner_id=owner_id,
        claim_generation=3,
    )

    assert item.status is WorkItemStatus.RECONCILIATION_REQUIRED
    assert item.completed_at is not None
    assert item.result["error_type"] == "WorkloadOutcomeUnknownError"
    assert item.completion_notification_state is CompletionNotificationState.PENDING
    assert item.completion_notification_next_attempt_at == item.completed_at
    assert session.commits == 1
    await store.close()


@pytest.mark.asyncio
async def test_accepts_engine_options() -> None:
    """Store callers can select a pool appropriate for their lifecycle."""
    store = WorkStore("postgresql+asyncpg://localhost/syntara", poolclass=NullPool)

    assert store._engine is not None
    assert isinstance(store._engine.pool, NullPool)
    await store.close()


@pytest.mark.asyncio
async def test_dispatch_raises_on_duplicate_id() -> None:
    """Submitting the same work item ID twice raises DuplicateWorkItemError."""
    store = _store()
    item_id = uuid.uuid4()
    session = _Session()
    session.commit = AsyncMock(side_effect=IntegrityError(None, None, Exception()))
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    with pytest.raises(DuplicateWorkItemError):
        await store.dispatch("test-client", item_id, {"input_config": {}})

    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_read_queries_are_client_scoped_and_bounded() -> None:
    """Read helpers return the first item and a bounded list."""
    store = _store()
    item = _work_item()
    session = _Session()
    session.execute.side_effect = [_ClaimResult(item), _ClaimResult(item)]
    store._session_factory = _SessionFactory(session)

    assert await store.get(item.id, client_id=item.client_id) is item
    assert await store.list_for_client(client_id=item.client_id, limit=10) == [item]
    await store.close()


@pytest.mark.asyncio
async def test_reserve_one_requeues_expired_uncertain_dispatch() -> None:
    """Expired uncertain dispatches become reconciliation work before a new claim is made."""
    owner_id = uuid.uuid4()
    item = _work_item(
        status=WorkItemStatus.CANCEL_REQUESTED,
        claim_owner_id=owner_id,
        claim_generation=2,
        claimed_at=datetime.now(UTC),
    )
    store = _store()
    session = _Session()
    session.execute.side_effect = [_ClaimResult(item), _ClaimResult(None)]
    store._session_factory = _SessionFactory(session)

    assert await store.reserve_one() is None
    assert item.status is WorkItemStatus.RECONCILIATION_REQUIRED
    assert item.claim_owner_id is None
    assert item.result["cancellation_requested"] is True
    assert item.completion_notification_state is CompletionNotificationState.PENDING
    assert session.commits == 1
    await store.close()


@pytest.mark.asyncio
async def test_reserve_one_reclaims_expired_claim() -> None:
    """A stale claimed item is retried with a new claim owner and generation."""
    item = _work_item(
        status=WorkItemStatus.CLAIMED,
        claim_generation=4,
        claimed_at=datetime(2020, 1, 1, tzinfo=UTC),
    )
    store = _store()
    session = _Session()
    session.execute.side_effect = [_ClaimResult(None), _ClaimResult(item)]
    store._session_factory = _SessionFactory(session)

    claimed = await store.reserve_one()

    assert claimed is item
    assert item.claim_owner_id is not None
    assert item.claim_generation == 5
    assert session.commits == 2
    await store.close()


@pytest.mark.asyncio
async def test_reserve_one_rolls_back_database_failure() -> None:
    """Reservation failures do not leave a partial transaction."""
    store = _store()
    session = _Session()
    session.execute.side_effect = RuntimeError("database unavailable")
    store._session_factory = _SessionFactory(session)

    with pytest.raises(RuntimeError, match="database unavailable"):
        await store.reserve_one()

    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_assign_target_persists_eligible_target() -> None:
    """A valid claim can be finalized with a still-eligible execution target."""
    owner_id = uuid.uuid4()
    target_id = uuid.uuid4()
    cluster = _cluster()
    target = _target(target_id, cluster)
    item = _work_item(
        status=WorkItemStatus.CLAIMED,
        claim_owner_id=owner_id,
        claim_generation=3,
    )
    store = _store()
    session = _Session()
    session.items[WorkItem] = item
    session.execute.return_value = _ClaimResult((target, cluster))
    store._session_factory = _SessionFactory(session)
    eligibility = AsyncMock(return_value=True)

    assigned = await store.assign_target(
        item.id,
        target_id,
        claim_owner_id=owner_id,
        claim_generation=3,
        eligibility=eligibility,
    )

    assert assigned is item
    assert item.execution_target_id == target_id
    eligibility.assert_awaited_once_with(cluster, target)
    assert session.commits == 1
    await store.close()


@pytest.mark.asyncio
async def test_assign_target_rejects_missing_item() -> None:
    """Missing work cannot be assigned to a target."""
    store = _store()
    session = _Session()
    store._session_factory = _SessionFactory(session)
    item_id = uuid.uuid4()

    with pytest.raises(WorkItemNotFoundError):
        await store.assign_target(
            item_id,
            uuid.uuid4(),
            claim_owner_id=uuid.uuid4(),
            claim_generation=1,
            eligibility=AsyncMock(return_value=True),
        )

    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_assign_target_rejects_stale_claim() -> None:
    """A stale worker cannot choose a target for a newer execution attempt."""
    owner_id = uuid.uuid4()
    item = _work_item(status=WorkItemStatus.CLAIMED, claim_owner_id=owner_id, claim_generation=2)
    store = _store()
    session = _Session()
    session.items[WorkItem] = item
    store._session_factory = _SessionFactory(session)

    with pytest.raises(StaleClaimError):
        await store.assign_target(
            item.id,
            uuid.uuid4(),
            claim_owner_id=owner_id,
            claim_generation=1,
            eligibility=AsyncMock(return_value=True),
        )

    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_assign_target_rejects_ineligible_or_unavailable_target() -> None:
    """Neither a missing target nor an eligibility failure changes the claim."""
    owner_id = uuid.uuid4()
    item = _work_item(
        status=WorkItemStatus.CLAIMED,
        claim_owner_id=owner_id,
        claim_generation=1,
        execution_target_id=None,
    )
    store = _store()
    session = _Session()
    session.items[WorkItem] = item
    store._session_factory = _SessionFactory(session)

    session.execute.return_value = _ClaimResult(None)
    assert (
        await store.assign_target(
            item.id, uuid.uuid4(), claim_owner_id=owner_id, claim_generation=1, eligibility=AsyncMock()
        )
        is None
    )

    cluster = _cluster()
    target = _target(uuid.uuid4(), cluster)
    session.execute.return_value = _ClaimResult((target, cluster))
    assert (
        await store.assign_target(
            item.id,
            target.id,
            claim_owner_id=owner_id,
            claim_generation=1,
            eligibility=AsyncMock(return_value=False),
        )
        is None
    )
    assert item.execution_target_id is None
    assert session.commits == 0
    await store.close()


@pytest.mark.asyncio
async def test_release_claim_returns_unassigned_reservation_to_pending() -> None:
    """A still-current, unassigned reservation can be released safely."""
    owner_id = uuid.uuid4()
    item = _work_item(
        status=WorkItemStatus.CLAIMED,
        claim_owner_id=owner_id,
        claim_generation=2,
        claimed_at=datetime.now(UTC),
    )
    store = _store()
    session = _Session()
    session.items[WorkItem] = item
    store._session_factory = _SessionFactory(session)

    assert await store.release_claim(item.id, claim_owner_id=owner_id, claim_generation=2) is True
    assert item.status is WorkItemStatus.PENDING
    assert item.claimed_at is None
    assert item.claim_owner_id is None
    assert session.commits == 1
    await store.close()


@pytest.mark.asyncio
async def test_release_claim_rejects_assigned_or_stale_reservations() -> None:
    """Assigned or stale reservations are not returned to the queue."""
    owner_id = uuid.uuid4()
    target_id = uuid.uuid4()
    assigned = _work_item(
        status=WorkItemStatus.CLAIMED,
        claim_owner_id=owner_id,
        claim_generation=1,
        execution_target_id=target_id,
    )
    stale = _work_item(status=WorkItemStatus.CLAIMED, claim_owner_id=owner_id, claim_generation=3)
    store = _store()
    session = _Session()
    store._session_factory = _SessionFactory(session)

    session.items[WorkItem] = assigned
    assert await store.release_claim(assigned.id, claim_owner_id=owner_id, claim_generation=1) is False

    session.items[WorkItem] = stale
    with pytest.raises(StaleClaimError):
        await store.release_claim(stale.id, claim_owner_id=owner_id, claim_generation=2)
    assert session.commits == 0
    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_mark_dispatched_transitions_claimed_work_once() -> None:
    """Only a current claimed item crosses the external dispatch boundary."""
    owner_id = uuid.uuid4()
    item = _work_item(
        status=WorkItemStatus.CLAIMED,
        claim_owner_id=owner_id,
        claim_generation=1,
        claimed_at=datetime.now(UTC),
    )
    store = _store()
    session = _Session()
    session.items[WorkItem] = item
    store._session_factory = _SessionFactory(session)

    assert await store.mark_dispatched(item.id, claim_owner_id=owner_id, claim_generation=1) is True
    assert item.status is WorkItemStatus.DISPATCHED
    assert await store.mark_dispatched(item.id, claim_owner_id=owner_id, claim_generation=1) is True
    assert session.commits == 1
    await store.close()


@pytest.mark.asyncio
async def test_mark_dispatched_rejects_cancelled_or_terminal_work() -> None:
    """Cancellation and terminal states win over a late dispatch confirmation."""
    owner_id = uuid.uuid4()
    statuses = [
        WorkItemStatus.CANCEL_REQUESTED,
        WorkItemStatus.COMPLETED,
        WorkItemStatus.PENDING,
    ]
    store = _store()
    session = _Session()
    store._session_factory = _SessionFactory(session)

    for status in statuses:
        item = _work_item(
            status=status,
            claim_owner_id=owner_id if status is not WorkItemStatus.PENDING else None,
            claim_generation=1,
        )
        session.items[WorkItem] = item
        claim_owner_id = item.claim_owner_id
        assert await store.mark_dispatched(item.id, claim_owner_id=claim_owner_id, claim_generation=1) is False
        assert item.status is status
    assert session.commits == 0
    await store.close()


@pytest.mark.asyncio
async def test_refresh_claim_renews_lease_and_reports_cancellation() -> None:
    """Lease refresh is idempotent for active states and reports cancel requests."""
    owner_id = uuid.uuid4()
    claimed = _work_item(status=WorkItemStatus.CLAIMED, claim_owner_id=owner_id, claim_generation=1)
    cancel_requested = _work_item(
        status=WorkItemStatus.CANCEL_REQUESTED,
        claim_owner_id=owner_id,
        claim_generation=1,
        claimed_at=datetime(2020, 1, 1, tzinfo=UTC),
    )
    store = _store()
    session = _Session()
    store._session_factory = _SessionFactory(session)

    session.items[WorkItem] = claimed
    assert await store.refresh_claim(claimed.id, claim_owner_id=owner_id, claim_generation=1) is False
    assert claimed.claimed_at is not None

    session.items[WorkItem] = cancel_requested
    assert await store.refresh_claim(cancel_requested.id, claim_owner_id=owner_id, claim_generation=1) is True
    assert session.commits == 2
    await store.close()


@pytest.mark.asyncio
async def test_refresh_claim_returns_none_for_missing_or_stale_claims() -> None:
    """Missing and stale claims do not renew leases."""
    owner_id = uuid.uuid4()
    stale = _work_item(status=WorkItemStatus.CLAIMED, claim_owner_id=owner_id, claim_generation=3)
    store = _store()
    session = _Session()
    store._session_factory = _SessionFactory(session)

    assert await store.refresh_claim(uuid.uuid4(), claim_owner_id=owner_id, claim_generation=1) is None
    session.items[WorkItem] = stale
    assert await store.refresh_claim(stale.id, claim_owner_id=owner_id, claim_generation=2) is None
    assert session.commits == 0
    await store.close()


@pytest.mark.asyncio
async def test_update_backend_resource_records_pending_cleanup() -> None:
    """Pending backend cleanup remains fenced to the owning worker claim."""
    owner_id = uuid.uuid4()
    item = _work_item(
        status=WorkItemStatus.CLAIMED,
        claim_owner_id=owner_id,
        claim_generation=2,
    )
    store = _store()
    session = _Session()
    session.items[WorkItem] = item
    store._session_factory = _SessionFactory(session)

    await store.update_backend_resource(
        item.id,
        resource_name="worker-job",
        resource_uid="job-uid",
        cleanup_status="pending",
        claim_owner_id=owner_id,
        claim_generation=2,
    )

    assert item.backend_resource_name == "worker-job"
    assert item.backend_resource_uid == "job-uid"
    assert item.resource_cleanup_status == "pending"
    assert session.commits == 1
    await store.close()


@pytest.mark.asyncio
async def test_update_backend_resource_records_completed_cleanup() -> None:
    """Completed cleanup keeps the allocated resource identity and records its status."""
    owner_id = uuid.uuid4()
    item = _work_item(
        status=WorkItemStatus.CLAIMED,
        claim_owner_id=owner_id,
        claim_generation=1,
        backend_resource_name="old-job",
        backend_resource_uid="old-uid",
    )
    store = _store()
    session = _Session()
    session.items[WorkItem] = item
    store._session_factory = _SessionFactory(session)

    await store.update_backend_resource(
        item.id,
        resource_name="old-job",
        resource_uid="new-uid",
        cleanup_status="completed",
        cleanup_error=None,
        claim_owner_id=owner_id,
        claim_generation=1,
    )

    assert item.backend_resource_name == "old-job"
    assert item.backend_resource_uid == "new-uid"
    assert item.resource_cleanup_status == "completed"
    await store.close()


@pytest.mark.asyncio
async def test_update_backend_resource_rejects_stale_and_mismatched_updates() -> None:
    """Stale generations and unrelated resource names cannot overwrite allocation state."""
    owner_id = uuid.uuid4()
    stale = _work_item(status=WorkItemStatus.CLAIMED, claim_owner_id=owner_id, claim_generation=2)
    mismatch = _work_item(
        status=WorkItemStatus.CLAIMED,
        claim_owner_id=owner_id,
        claim_generation=1,
        backend_resource_name="worker-job",
    )
    store = _store()
    session = _Session()
    store._session_factory = _SessionFactory(session)

    session.items[WorkItem] = stale
    with pytest.raises(StaleClaimError):
        await store.update_backend_resource(
            stale.id,
            resource_name="worker-job",
            resource_uid=None,
            cleanup_status="pending",
            claim_owner_id=owner_id,
            claim_generation=1,
        )

    session.items[WorkItem] = mismatch
    with pytest.raises(StaleClaimError):
        await store.update_backend_resource(
            mismatch.id,
            resource_name="other-job",
            resource_uid=None,
            cleanup_status="failed",
            claim_owner_id=owner_id,
            claim_generation=1,
        )
    assert session.rollbacks == 2
    await store.close()


@pytest.mark.asyncio
async def test_set_result_rejects_non_terminal_status() -> None:
    """Only terminal outcomes can create a completion event."""
    store = _store()
    session = _Session()
    store._session_factory = _SessionFactory(session)

    with pytest.raises(ValueError, match="terminal status"):
        await store.set_result(uuid.uuid4(), {"output": "ok"}, WorkItemStatus.DISPATCHED)

    assert session.commits == 0
    assert session.rollbacks == 0
    await store.close()


@pytest.mark.asyncio
async def test_set_result_is_idempotent_for_terminal_work() -> None:
    """Terminal work retains its original result and completion notification."""
    item = _work_item(status=WorkItemStatus.COMPLETED, result={"output": "old"})
    store = _store()
    session = _Session()
    session.items[WorkItem] = item
    store._session_factory = _SessionFactory(session)

    result = await store.set_result(item.id, {"output": "new"}, WorkItemStatus.FAILED)

    assert result is item
    assert item.status is WorkItemStatus.COMPLETED
    assert item.result == {"output": "old"}
    assert session.commits == 0
    await store.close()


@pytest.mark.asyncio
async def test_set_result_rejects_missing_and_stale_work() -> None:
    """Result persistence is fenced by a current claim and fails for missing work."""
    owner_id = uuid.uuid4()
    stale = _work_item(
        status=WorkItemStatus.CLAIMED,
        claim_owner_id=owner_id,
        claim_generation=3,
    )
    store = _store()
    session = _Session()
    store._session_factory = _SessionFactory(session)

    with pytest.raises(WorkItemNotFoundError):
        await store.set_result(uuid.uuid4(), {"output": "ok"}, WorkItemStatus.COMPLETED)

    session.items[WorkItem] = stale
    with pytest.raises(StaleClaimError):
        await store.set_result(
            stale.id,
            {"output": "ok"},
            WorkItemStatus.COMPLETED,
            claim_owner_id=owner_id,
            claim_generation=2,
        )
    assert session.rollbacks == 2
    await store.close()


@pytest.mark.asyncio
async def test_request_cancel_cancels_unclaimed_work() -> None:
    """Work that has not started is cancelled immediately with a completion notification."""
    item = _work_item(status=WorkItemStatus.PENDING)
    store = _store()
    session = _Session()
    session.execute.return_value = _ClaimResult(item)
    store._session_factory = _SessionFactory(session)

    cancelled = await store.request_cancel(item.id, client_id=item.client_id)

    assert cancelled is item
    assert item.status is WorkItemStatus.CANCELLED
    assert item.result == {"cancelled": True, "execution_started": False}
    assert item.completion_notification_state is CompletionNotificationState.PENDING
    assert session.commits == 1
    await store.close()


@pytest.mark.asyncio
async def test_request_cancel_requests_termination_after_dispatch() -> None:
    """Dispatched work remains visible while cancellation is requested from the worker."""
    item = _work_item(status=WorkItemStatus.DISPATCHED)
    store = _store()
    session = _Session()
    session.execute.return_value = _ClaimResult(item)
    store._session_factory = _SessionFactory(session)

    requested = await store.request_cancel(item.id, client_id=item.client_id)

    assert requested is item
    assert item.status is WorkItemStatus.CANCEL_REQUESTED
    assert item.claimed_at is not None
    assert session.added == []
    assert session.commits == 1
    assert session.execute.await_count == 2
    await store.close()


@pytest.mark.asyncio
async def test_request_cancel_is_noop_for_terminal_work() -> None:
    """Terminal work cannot be cancelled again."""
    item = _work_item(status=WorkItemStatus.CANCELLED, result={"cancelled": True})
    store = _store()
    session = _Session()
    session.execute.return_value = _ClaimResult(item)
    store._session_factory = _SessionFactory(session)

    result = await store.request_cancel(item.id, client_id=item.client_id)

    assert result is item
    assert item.status is WorkItemStatus.CANCELLED
    assert session.added == []
    assert session.commits == 1
    await store.close()


@pytest.mark.asyncio
async def test_request_cancel_rejects_missing_or_foreign_work() -> None:
    """Cancellation is scoped to an item owned by the requesting client."""
    store = _store()
    session = _Session()
    session.execute.return_value = _ClaimResult(None)
    store._session_factory = _SessionFactory(session)
    item_id = uuid.uuid4()

    with pytest.raises(WorkItemNotFoundError):
        await store.request_cancel(item_id, client_id="other-client")

    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_reconciliation_reason_is_bounded_before_persistence() -> None:
    """Uncertain outcomes retain a bounded explanation without a service facade."""
    item = _work_item(status=WorkItemStatus.DISPATCHED)
    store = _store()
    session = _Session()
    session.items[WorkItem] = item
    store._session_factory = _SessionFactory(session)

    result = await store.mark_reconciliation_required(item.id, "x" * 1500)

    assert result is item
    assert item.result is not None
    assert len(item.result["error"]) == 1000
    assert session.commits == 1
    await store.close()


@pytest.mark.asyncio
async def test_mark_reconciliation_required_is_idempotent_for_terminal_work() -> None:
    """Terminal work is not moved back into reconciliation."""
    item = _work_item(status=WorkItemStatus.FAILED, result={"error": "old"})
    store = _store()
    session = _Session()
    session.items[WorkItem] = item
    store._session_factory = _SessionFactory(session)

    result = await store.mark_reconciliation_required(item.id, "new failure")

    assert result is item
    assert item.status is WorkItemStatus.FAILED
    assert item.result == {"error": "old"}
    assert session.added == []
    assert session.commits == 0
    await store.close()


@pytest.mark.asyncio
async def test_requeue_pre_execute_failure_cancels_requested_work() -> None:
    """A pre-execute cancellation request becomes a durable cancellation."""
    owner_id = uuid.uuid4()
    item = _work_item(
        status=WorkItemStatus.CANCEL_REQUESTED,
        claim_owner_id=owner_id,
        claim_generation=1,
    )
    store = _store()
    session = _Session()
    session.items[WorkItem] = item
    store._session_factory = _SessionFactory(session)

    result = await store.requeue_pre_execute_failure(
        item.id,
        claim_owner_id=owner_id,
        claim_generation=1,
    )

    assert result is item
    assert item.status is WorkItemStatus.CANCELLED
    assert item.result == {"cancelled": True, "execution_started": False}
    assert item.completion_notification_state is CompletionNotificationState.PENDING
    await store.close()


@pytest.mark.asyncio
async def test_requeue_pre_execute_failure_returns_dispatched_work() -> None:
    """A proven pre-execute failure returns the original work item to the queue."""
    owner_id = uuid.uuid4()
    target_id = uuid.uuid4()
    item = _work_item(
        status=WorkItemStatus.DISPATCHED,
        claim_owner_id=owner_id,
        claim_generation=1,
        execution_target_id=target_id,
        claimed_at=datetime.now(UTC),
    )
    store = _store()
    session = _Session()
    session.items[WorkItem] = item
    store._session_factory = _SessionFactory(session)

    result = await store.requeue_pre_execute_failure(
        item.id,
        claim_owner_id=owner_id,
        claim_generation=1,
    )

    assert result is item
    assert item.status is WorkItemStatus.PENDING
    assert item.execution_target_id is None
    assert item.claim_owner_id is None
    assert item.claimed_at is None
    await store.close()


@pytest.mark.asyncio
async def test_completion_retry_rejects_negative_delay() -> None:
    """The Work store rejects retry schedules before opening a session."""
    store = _store()
    session = _Session()
    store._session_factory = _SessionFactory(session)

    with pytest.raises(ValueError, match="non-negative"):
        await store.record_completion_notification_attempt(
            uuid.uuid4(),
            error="client unavailable",
            retry_after_seconds=-1,
        )

    assert session.commits == 0
    assert session.rollbacks == 0
    await store.close()


@pytest.mark.asyncio
async def test_completion_notification_delivery_and_retry() -> None:
    """Inline completion notification state records successful delivery and failed retries."""
    delivered = _work_item(
        status=WorkItemStatus.COMPLETED,
        completion_notification_state=CompletionNotificationState.DELIVERING,
    )
    retryable = _work_item(
        status=WorkItemStatus.FAILED,
        completion_notification_state=CompletionNotificationState.DELIVERING,
    )
    store = _store()
    session = _Session()
    session.items[WorkItem] = delivered
    store._session_factory = _SessionFactory(session)

    await store.record_completion_notification_delivery(delivered.id)
    assert delivered.completion_notification_state is CompletionNotificationState.DELIVERED
    assert delivered.completion_notification_delivered_at is not None
    assert delivered.completion_notification_last_error is None
    assert delivered.completion_notification_attempts == 1

    session.items[WorkItem] = retryable
    await store.record_completion_notification_attempt(
        retryable.id,
        error="client unavailable",
        retry_after_seconds=30,
    )
    assert retryable.completion_notification_state is CompletionNotificationState.PENDING
    assert retryable.completion_notification_delivered_at is None
    assert retryable.completion_notification_last_error == "client unavailable"
    assert retryable.completion_notification_next_attempt_at is not None
    assert retryable.completion_notification_attempts == 1
    assert session.commits == 2
    await store.close()


@pytest.mark.asyncio
async def test_completion_notification_attempt_ignores_missing_work() -> None:
    """A missing work item does not create a completion notification retry."""
    store = _store()
    session = _Session()
    store._session_factory = _SessionFactory(session)

    assert (
        await store.record_completion_notification_attempt(
            uuid.uuid4(),
            error="client unavailable",
            retry_after_seconds=10,
        )
        is False
    )

    assert session.commits == 0
    await store.close()


@pytest.mark.asyncio
async def test_due_completion_notifications_are_claimed_in_batches() -> None:
    """Only due, undelivered work items are atomically claimed for delivery."""
    item = _work_item(
        status=WorkItemStatus.COMPLETED,
        completion_notification_state=CompletionNotificationState.PENDING,
    )
    store = _store()
    session = _Session()
    session.execute.return_value = _ClaimResult(item)
    store._session_factory = _SessionFactory(session)

    assert await store.claim_work_items_that_need_completion_notification(limit=25) == [item]
    assert item.completion_notification_state is CompletionNotificationState.DELIVERING
    assert item.completion_notification_claim_expires_at is not None
    await store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [None, CompletionNotificationState.PENDING, CompletionNotificationState.DELIVERED])
@pytest.mark.parametrize("succeeded", [True, False])
async def test_completion_notification_updates_only_delivering_work(state, succeeded) -> None:
    """Late and duplicate responses cannot change a notification outside DELIVERING."""
    now = datetime.now(UTC)
    item = _work_item(
        status=WorkItemStatus.COMPLETED,
        completion_notification_state=state,
        completion_notification_attempts=2,
        completion_notification_delivered_at=now if state is CompletionNotificationState.DELIVERED else None,
    )
    before = item.model_dump()
    session = _Session(item)
    store = _store()
    store._session_factory = _SessionFactory(session)

    if succeeded:
        changed = await store.record_completion_notification_delivery(item.id)
    else:
        changed = await store.record_completion_notification_attempt(item.id, error="late failure")

    assert changed is False
    assert item.model_dump() == before
    assert session.commits == 0
    await store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", list(CompletionNotificationState))
async def test_reconciliation_preserves_initialized_completion_notification(state) -> None:
    """Repeated lifecycle updates retain notification progress without a separate identity marker."""
    now = datetime.now(UTC)
    item = _work_item(
        status=WorkItemStatus.RECONCILIATION_REQUIRED,
        completion_notification_state=state,
        completion_notification_attempts=3,
        completion_notification_next_attempt_at=now,
        completion_notification_last_error="client unavailable",
        completion_notification_delivered_at=now if state is CompletionNotificationState.DELIVERED else None,
        completion_notification_claim_expires_at=now if state is CompletionNotificationState.DELIVERING else None,
    )
    before = {key: value for key, value in item.model_dump().items() if key.startswith("completion_notification_")}
    store = _store()
    session = _Session(item)
    store._session_factory = _SessionFactory(session)

    await store.mark_reconciliation_required(item.id, "still uncertain")

    after = {key: value for key, value in item.model_dump().items() if key.startswith("completion_notification_")}
    assert after == before
    await store.close()
