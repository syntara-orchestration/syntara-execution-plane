"""WorkStore — the only path for mutating WorkItem state.

Nothing outside this module should construct, modify, or commit WorkItem
objects. The public methods express the intended lifecycle transitions;
any state not reachable through them is not a valid transition.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import and_, case, func, or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlmodel import col

from execution_plane.models.cluster import Cluster
from execution_plane.models.execution_target import ExecutionTarget
from execution_plane.models.work_item import CompletionNotificationState, WorkItem, WorkItemStatus
from execution_plane.query_filters import apply_list_filters
from execution_plane.query_sorting import apply_cursor_sort
from execution_plane.store_base import StoreBase
from execution_plane.store_errors import StaleClaimError
from execution_plane.work_item.errors import DuplicateWorkItemError, WorkItemNotFoundError
from execution_plane.work_item.policy import (
    DRAIN_BLOCKING_STATUSES,
    REFRESHABLE_CLAIM_STATUSES,
    TERMINAL_RESULT_STATUSES,
    bounded_reconciliation_reason,
    require_terminal_result_status,
    validate_completion_notification_retry_delay,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlalchemy.ext.asyncio import AsyncSession

_NOTIFY_CHANNEL = "execution_plane_work_items"
_COMPLETION_NOTIFICATION_CLAIM_SECONDS = 30


class WorkStore(StoreBase):
    """Persist work item lifecycle transitions and own database resources."""

    async def dispatch(
        self,
        client_id: str,
        item_id: uuid.UUID,
        payload: dict[str, Any],
    ) -> WorkItem:
        """Insert work with a client-supplied UUID; raises DuplicateWorkItemError on collision."""
        item = WorkItem(
            id=item_id,
            client_id=client_id,
            status=WorkItemStatus.PENDING,
            payload=payload,
            created_at=datetime.now(UTC),
        )
        async with self._session_context() as session:
            try:
                session.add(item)
                # pg_notify is transactional — delivered only after this commit.
                await session.execute(text(f"SELECT pg_notify('{_NOTIFY_CHANNEL}', '')"))
                await session.commit()
            except IntegrityError as exc:
                await session.rollback()
                raise DuplicateWorkItemError(item_id) from exc
            except Exception:
                await session.rollback()
                raise
        return item

    async def get(self, item_id: uuid.UUID, *, client_id: str) -> WorkItem | None:
        """Return one work item scoped to the authenticated client."""
        async with self._session_context() as session:
            result = await session.execute(
                select(WorkItem).where(col(WorkItem.id) == item_id).where(col(WorkItem.client_id) == client_id)
            )
            return result.scalars().first()

    async def list_for_client(
        self,
        *,
        client_id: str,
        limit: int,
        cursor: dict[str, Any] | None = None,
        sort_field: str = "created_at",
        sort_direction: str = "desc",
        filters: dict[str, str] | None = None,
    ) -> list[WorkItem]:
        """Adapt syntara.core.services.base.BaseService._build_list_query for owned work."""
        statement = select(WorkItem).where(col(WorkItem.client_id) == client_id)
        statement = apply_list_filters(statement, filters, WorkItem)
        statement = apply_cursor_sort(statement, WorkItem, cursor, sort_field=sort_field, sort_direction=sort_direction)
        async with self._session_context() as session:
            result = await session.execute(statement.limit(limit))
            return list(result.scalars().all())

    async def count_for_client(self, *, client_id: str, filters: dict[str, str] | None = None) -> int:
        """Adapt syntara.core.services.base.BaseService._get_total_count with client scoping."""
        statement = select(func.count()).select_from(WorkItem).where(col(WorkItem.client_id) == client_id)
        statement = apply_list_filters(statement, filters, WorkItem)
        async with self._session_context() as session:
            result = await session.execute(statement)
            return result.scalar_one()

    async def reserve_one(self) -> WorkItem | None:
        """Reserve queued work for worker-side placement resolution."""
        lease_expiry = datetime.now(UTC) - timedelta(seconds=60)
        async with self._session_context() as session:
            try:
                uncertain_result = await session.execute(
                    select(WorkItem)
                    .where(col(WorkItem.status).in_([WorkItemStatus.DISPATCHED, WorkItemStatus.CANCEL_REQUESTED]))
                    .where(col(WorkItem.claimed_at) < lease_expiry)
                    .with_for_update(skip_locked=True)
                )
                for uncertain in uncertain_result.scalars().all():
                    cancellation_requested = uncertain.status is WorkItemStatus.CANCEL_REQUESTED
                    uncertain.status = WorkItemStatus.RECONCILIATION_REQUIRED
                    now = datetime.now(UTC)
                    uncertain.result = {
                        "error": "Worker lease expired after node dispatch may have started",
                        "error_type": "WorkloadOutcomeUnknownError",
                        "cancellation_requested": cancellation_requested,
                    }
                    uncertain.claim_owner_id = None
                    uncertain.completed_at = now
                    self._initialize_completion_notification_fields(uncertain, now)
                await session.commit()

                result = await session.execute(
                    select(WorkItem)
                    .where(
                        or_(
                            col(WorkItem.status) == WorkItemStatus.PENDING,
                            and_(
                                col(WorkItem.status) == WorkItemStatus.CLAIMED,
                                col(WorkItem.claimed_at) < lease_expiry,
                            ),
                        )
                    )
                    .order_by(
                        case((col(WorkItem.status) == WorkItemStatus.PENDING, 0), else_=1),
                        col(WorkItem.created_at),
                    )
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                item = result.scalars().first()
                if item is None:
                    return None
                if item.status is WorkItemStatus.PENDING:
                    item.status = WorkItemStatus.CLAIMED
                item.claimed_at = datetime.now(UTC)
                item.claim_owner_id = uuid.uuid4()
                item.claim_generation += 1
                await session.commit()
                return item
            except Exception:
                await session.rollback()
                raise

    async def assign_target(
        self,
        item_id: uuid.UUID,
        target_id: uuid.UUID,
        *,
        claim_owner_id: uuid.UUID,
        claim_generation: int,
        eligibility: Callable[[Cluster, ExecutionTarget], Awaitable[bool]],
    ) -> WorkItem | None:
        """Finalize a reserved claim with a still-eligible target."""
        async with self._session_context() as session:
            try:
                item = await session.get(WorkItem, item_id, with_for_update=True)
                if item is None:
                    raise WorkItemNotFoundError(item_id)  # noqa: TRY301
                self._validate_claim(item, claim_owner_id, claim_generation)
                if item.status is not WorkItemStatus.CLAIMED or item.execution_target_id is not None:
                    return None
                target_result = await session.execute(
                    select(ExecutionTarget, Cluster)
                    .join(Cluster, col(Cluster.id) == col(ExecutionTarget.cluster_id))
                    .where(col(ExecutionTarget.id) == target_id)
                    .with_for_update(skip_locked=True, of=(ExecutionTarget, Cluster))
                )
                assignment = target_result.first()
                if assignment is None:
                    return None
                target, cluster = assignment
                if not await eligibility(cluster, target):
                    return None
                item.execution_target_id = target.id
                await session.commit()
                return item
            except Exception:
                await session.rollback()
                raise

    async def release_claim(
        self,
        item_id: uuid.UUID,
        *,
        claim_owner_id: uuid.UUID,
        claim_generation: int,
    ) -> bool:
        """Return an unassigned reservation to the pending queue."""
        async with self._session_context() as session:
            try:
                item = await session.get(WorkItem, item_id, with_for_update=True)
                if item is None:
                    raise WorkItemNotFoundError(item_id)  # noqa: TRY301
                self._validate_claim(item, claim_owner_id, claim_generation)
                if item.status is not WorkItemStatus.CLAIMED or item.execution_target_id is not None:
                    return False
                item.status = WorkItemStatus.PENDING
                item.claimed_at = None
                item.claim_owner_id = None
                await session.commit()
                return True
            except Exception:
                await session.rollback()
                raise

    async def mark_dispatched(
        self,
        item_id: uuid.UUID,
        *,
        claim_owner_id: uuid.UUID | None = None,
        claim_generation: int | None = None,
    ) -> bool:
        """Persist the external-dispatch boundary unless cancellation already won."""
        async with self._session_context() as session:
            try:
                item = await session.get(WorkItem, item_id, with_for_update=True)
                if item is None:
                    raise WorkItemNotFoundError(item_id)  # noqa: TRY301
                self._validate_claim(item, claim_owner_id, claim_generation)
                if item.status in {
                    WorkItemStatus.CANCEL_REQUESTED,
                    WorkItemStatus.COMPLETED,
                    WorkItemStatus.FAILED,
                    WorkItemStatus.CANCELLED,
                }:
                    return False
                if item.status is WorkItemStatus.DISPATCHED:
                    return True
                if item.status is not WorkItemStatus.CLAIMED:
                    return False
                item.status = WorkItemStatus.DISPATCHED
                item.claimed_at = datetime.now(UTC)
                await session.commit()
                return True
            except Exception:
                await session.rollback()
                raise

    async def refresh_claim(
        self,
        item_id: uuid.UUID,
        *,
        claim_owner_id: uuid.UUID | None = None,
        claim_generation: int | None = None,
    ) -> bool | None:
        """Renew the controller lease and report whether cancellation was requested."""
        async with self._session_context() as session:
            item = await session.get(WorkItem, item_id, with_for_update=True)
            if item is None:
                return None
            try:
                self._validate_claim(item, claim_owner_id, claim_generation)
            except StaleClaimError:
                return None
            if item.status in REFRESHABLE_CLAIM_STATUSES:
                item.claimed_at = datetime.now(UTC)
                await session.commit()
            return item.status is WorkItemStatus.CANCEL_REQUESTED

    async def update_backend_resource(
        self,
        item_id: uuid.UUID,
        *,
        resource_name: str,
        resource_uid: str | None,
        cleanup_status: str,
        cleanup_error: str | None = None,
        claim_owner_id: uuid.UUID | None,
        claim_generation: int,
    ) -> None:
        """Persist cold-start allocation identity and its independent cleanup state."""
        bounded_cleanup_error = bounded_reconciliation_reason(cleanup_error) if cleanup_error is not None else None
        async with self._session_context() as session:
            try:
                item = await session.get(WorkItem, item_id, with_for_update=True)
                if item is None:
                    raise WorkItemNotFoundError(item_id)  # noqa: TRY301
                if item.claim_generation != claim_generation:
                    raise StaleClaimError("Backend resource update belongs to a stale execution attempt")  # noqa: EM101, TRY003, TRY301
                if cleanup_status == "pending":
                    self._validate_claim(item, claim_owner_id, claim_generation)
                elif item.backend_resource_name not in (None, resource_name):
                    raise StaleClaimError("Backend resource update does not match the recorded allocation")  # noqa: EM101, TRY003, TRY301
                if item.backend_resource_name != resource_name:
                    item.backend_resource_uid = None
                item.backend_resource_name = resource_name
                if resource_uid is not None:
                    item.backend_resource_uid = resource_uid
                item.resource_cleanup_status = cleanup_status
                item.resource_cleanup_error = bounded_cleanup_error
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def set_result(
        self,
        item_id: uuid.UUID,
        result: dict[str, Any],
        status: WorkItemStatus,
        *,
        claim_owner_id: uuid.UUID | None = None,
        claim_generation: int | None = None,
    ) -> WorkItem:
        """Atomically persist a terminal result and its completion outbox event."""
        require_terminal_result_status(status)
        async with self._session_context() as session:
            try:
                item = await session.get(WorkItem, item_id, with_for_update=True)
                if item is None:
                    raise WorkItemNotFoundError(item_id)  # noqa: TRY301
                self._validate_claim(item, claim_owner_id, claim_generation)
                if item.status in TERMINAL_RESULT_STATUSES:
                    return item
                now = datetime.now(UTC)
                item.result = result
                item.status = status
                item.completed_at = now
                item.claim_owner_id = None
                self._initialize_completion_notification_fields(item, now)
                await session.commit()
                return item
            except Exception:
                await session.rollback()
                raise

    async def request_cancel(
        self,
        item_id: uuid.UUID,
        *,
        client_id: str,
    ) -> WorkItem:
        """Cancel queued work or durably request termination of a dispatched job."""
        async with self._session_context() as session:
            try:
                result = await session.execute(
                    select(WorkItem)
                    .where(col(WorkItem.id) == item_id)
                    .where(col(WorkItem.client_id) == client_id)
                    .with_for_update()
                )
                item = result.scalars().first()
                if item is None:
                    raise WorkItemNotFoundError(item_id)  # noqa: TRY301
                await self._request_cancel_locked(session, item)
                await session.commit()
                return item
            except Exception:
                await session.rollback()
                raise

    async def mark_reconciliation_required(
        self,
        item_id: uuid.UUID,
        reason: str,
        *,
        claim_owner_id: uuid.UUID | None = None,
        claim_generation: int | None = None,
    ) -> WorkItem:
        """Persist an uncertain external outcome without implying failure or rerunning it."""
        bounded_reason = bounded_reconciliation_reason(reason)
        async with self._session_context() as session:
            try:
                item = await session.get(WorkItem, item_id, with_for_update=True)
                if item is None:
                    raise WorkItemNotFoundError(item_id)  # noqa: TRY301
                self._validate_claim(item, claim_owner_id, claim_generation)
                if item.status in TERMINAL_RESULT_STATUSES:
                    return item
                cancel_requested = item.status is WorkItemStatus.CANCEL_REQUESTED
                item.status = WorkItemStatus.RECONCILIATION_REQUIRED
                now = datetime.now(UTC)
                item.result = {
                    "error": bounded_reason,
                    "error_type": "WorkloadOutcomeUnknownError",
                    "cancellation_requested": cancel_requested,
                }
                item.claimed_at = None
                item.claim_owner_id = None
                item.completed_at = now
                self._initialize_completion_notification_fields(item, now)
                await session.commit()
                return item
            except Exception:
                await session.rollback()
                raise

    async def requeue_pre_execute_failure(
        self,
        item_id: uuid.UUID,
        *,
        claim_owner_id: uuid.UUID,
        claim_generation: int,
    ) -> WorkItem:
        """Return a proven pre-Execute failure to PENDING without changing request identity."""
        async with self._session_context() as session:
            try:
                item = await session.get(WorkItem, item_id, with_for_update=True)
                if item is None:
                    raise WorkItemNotFoundError(item_id)  # noqa: TRY301
                self._validate_claim(item, claim_owner_id, claim_generation)
                if item.status is WorkItemStatus.CANCEL_REQUESTED:
                    now = datetime.now(UTC)
                    result = {"cancelled": True, "execution_started": False}
                    item.result = result
                    item.status = WorkItemStatus.CANCELLED
                    item.completed_at = now
                    item.claimed_at = None
                    item.claim_owner_id = None
                    self._initialize_completion_notification_fields(item, now)
                elif item.status is WorkItemStatus.DISPATCHED:
                    item.status = WorkItemStatus.PENDING
                    item.execution_target_id = None
                    item.claimed_at = None
                    item.claim_owner_id = None
                await session.commit()
                return item
            except Exception:
                await session.rollback()
                raise

    @staticmethod
    def _initialize_completion_notification_fields(item: WorkItem, now: datetime) -> None:
        """Initialize completion delivery once, preserving retries and acknowledgements."""
        if item.completion_notification_state is not None:
            return
        item.completion_notification_state = CompletionNotificationState.PENDING
        item.completion_notification_attempts = 0
        item.completion_notification_next_attempt_at = now
        item.completion_notification_last_error = None
        item.completion_notification_delivered_at = None
        item.completion_notification_claim_expires_at = None

    @staticmethod
    def _validate_claim(
        item: WorkItem,
        claim_owner_id: uuid.UUID | None,
        claim_generation: int | None,
    ) -> None:
        """Fence a state change when the caller supplies its claim generation."""
        if claim_owner_id is None and claim_generation is None:
            return
        if item.claim_owner_id != claim_owner_id or item.claim_generation != claim_generation:
            raise StaleClaimError("Worker claim is no longer current")  # noqa: EM101, TRY003

    async def _request_cancel_locked(self, session: AsyncSession, item: WorkItem) -> None:
        """Apply a cancellation transition while the work-item row is locked."""
        if item.status in {
            WorkItemStatus.COMPLETED,
            WorkItemStatus.FAILED,
            WorkItemStatus.CANCELLED,
            WorkItemStatus.CANCEL_REQUESTED,
            WorkItemStatus.RECONCILIATION_REQUIRED,
        }:
            return
        now = datetime.now(UTC)
        if item.status in {WorkItemStatus.DISPATCHED, WorkItemStatus.RECONCILIATION_REQUIRED}:
            item.status = WorkItemStatus.CANCEL_REQUESTED
            item.claimed_at = now
            await session.execute(text(f"SELECT pg_notify('{_NOTIFY_CHANNEL}', '')"))
            return
        # A claim is not yet allowed to create a Job. Locking the row makes this
        # transition atomic with mark_dispatched.
        item.status = WorkItemStatus.CANCELLED
        item.result = {"cancelled": True, "execution_started": False}
        item.completed_at = now
        item.claimed_at = None
        item.claim_owner_id = None
        self._initialize_completion_notification_fields(item, now)

    async def claim_work_items_that_need_completion_notification(self, *, limit: int = 100) -> list[WorkItem]:
        """Atomically claim a bounded batch of due work-item completion notifications."""
        now = datetime.now(UTC)
        async with self._session_context() as session:
            try:
                result = await session.execute(
                    select(WorkItem)
                    .where(
                        col(WorkItem.status).in_(
                            {
                                WorkItemStatus.COMPLETED,
                                WorkItemStatus.FAILED,
                                WorkItemStatus.CANCELLED,
                                WorkItemStatus.RECONCILIATION_REQUIRED,
                            }
                        )
                    )
                    .where(
                        or_(
                            and_(
                                col(WorkItem.completion_notification_state) == CompletionNotificationState.PENDING,
                                col(WorkItem.completion_notification_next_attempt_at) <= now,
                            ),
                            and_(
                                col(WorkItem.completion_notification_state) == CompletionNotificationState.DELIVERING,
                                col(WorkItem.completion_notification_claim_expires_at) <= now,
                            ),
                        )
                    )
                    .order_by(col(WorkItem.completed_at))
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
                items = list(result.scalars().all())
                claim_expires_at = now + timedelta(seconds=_COMPLETION_NOTIFICATION_CLAIM_SECONDS)
                for item in items:
                    item.completion_notification_state = CompletionNotificationState.DELIVERING
                    item.completion_notification_claim_expires_at = claim_expires_at
                await session.commit()
                return items
            except Exception:
                await session.rollback()
                raise

    async def record_completion_notification_delivery(self, item_id: uuid.UUID) -> bool:
        """Transition DELIVERING to DELIVERED while holding the work-item row lock."""
        async with self._session_context() as session:
            try:
                item = await session.get(WorkItem, item_id, with_for_update=True, populate_existing=True)
                if item is None or item.completion_notification_state is not CompletionNotificationState.DELIVERING:
                    return False
                item.completion_notification_attempts += 1
                item.completion_notification_last_error = None
                item.completion_notification_state = CompletionNotificationState.DELIVERED
                item.completion_notification_delivered_at = datetime.now(UTC)
                item.completion_notification_claim_expires_at = None
                await session.commit()
                return True
            except Exception:
                await session.rollback()
                raise

    async def record_completion_notification_attempt(
        self,
        item_id: uuid.UUID,
        *,
        error: str,
        retry_after_seconds: int = 10,
    ) -> bool:
        """Transition DELIVERING to PENDING under the row lock; delivered work stays delivered."""
        validate_completion_notification_retry_delay(retry_after_seconds)
        async with self._session_context() as session:
            try:
                item = await session.get(WorkItem, item_id, with_for_update=True, populate_existing=True)
                if item is None or item.completion_notification_state is not CompletionNotificationState.DELIVERING:
                    return False
                item.completion_notification_attempts += 1
                item.completion_notification_last_error = error[:1000]
                item.completion_notification_state = CompletionNotificationState.PENDING
                item.completion_notification_next_attempt_at = datetime.now(UTC) + timedelta(
                    seconds=retry_after_seconds
                )
                item.completion_notification_claim_expires_at = None
                await session.commit()
                return True
            except Exception:
                await session.rollback()
                raise

    async def is_target_drained(self, target_id: uuid.UUID) -> bool:
        """Return whether no owned or uncertain work remains on a target."""
        async with self._session_context() as session:
            result = await session.execute(
                select(col(WorkItem.id))
                .where(col(WorkItem.execution_target_id) == target_id)
                .where(col(WorkItem.status).in_(DRAIN_BLOCKING_STATUSES))
                .limit(1)
            )
            return result.scalar_one_or_none() is None
