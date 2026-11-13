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
from execution_plane.models.work_item import CompletionNotificationState, WorkItem
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
from execution_plane_client.models.work_item import WorkItemStatus

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

_NOTIFY_CHANNEL = "execution_plane_work_items"
_COMPLETION_NOTIFICATION_CLAIM_SECONDS = 30


class WorkStore(StoreBase):
    """Persist work item lifecycle transitions and own database resources."""

    async def check_ready(self) -> None:
        """Verify database connectivity and work_items readability.

        Raises on any database error. Used by the execution-plane-healthz
        readiness probe (AAP-93327); deliberately schema-aware through the
        ORM model so it works regardless of the connection's search_path.
        """
        async with self._session_context() as session:
            await session.execute(select(col(WorkItem.id)).limit(0))

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
        """Finish never-started cancellations while reserving the next runnable item."""
        lease_expiry = datetime.now(UTC) - timedelta(seconds=60)
        async with self._session_context() as session:
            try:
                uncertain_result = await session.execute(
                    select(WorkItem)
                    .where(
                        col(WorkItem.status).in_(
                            [
                                WorkItemStatus.DISPATCHED,
                                WorkItemStatus.RUNNING,
                            ]
                        )
                    )
                    .where(col(WorkItem.claimed_at) < lease_expiry)
                    .with_for_update(skip_locked=True)
                )
                for uncertain in uncertain_result.scalars().all():
                    cancellation_requested = uncertain.cancellation_requested
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

                while True:
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
                        .where(
                            or_(
                                col(WorkItem.cancellation_requested).is_(False),
                                col(WorkItem.resource_cleanup_status) == "complete",
                                and_(
                                    col(WorkItem.backend_resource_name).is_(None),
                                    col(WorkItem.resource_cleanup_status) == "not_required",
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
                    if item.cancellation_requested:
                        self._mark_cancelled(item, execution_started=False)
                        await session.commit()
                        continue
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
        """Finalize never-started cancellation or persist the external-dispatch boundary."""
        async with self._session_context() as session:
            try:
                item = await session.get(WorkItem, item_id, with_for_update=True)
                if item is None:
                    raise WorkItemNotFoundError(item_id)  # noqa: TRY301
                self._validate_claim(item, claim_owner_id, claim_generation)
                if (
                    item.cancellation_requested
                    and item.status in {WorkItemStatus.PENDING, WorkItemStatus.CLAIMED}
                    and (
                        item.resource_cleanup_status == "complete"
                        or (item.backend_resource_name is None and item.resource_cleanup_status == "not_required")
                    )
                ):
                    self._mark_cancelled(item, execution_started=False)
                    await session.commit()
                    return False
                if item.cancellation_requested or item.status in {
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
            return item.cancellation_requested

    async def mark_running(
        self,
        item_id: uuid.UUID,
        *,
        claim_owner_id: uuid.UUID,
        claim_generation: int,
    ) -> bool:
        """Persist that the backend submitted Execute for the current claim."""
        async with self._session_context() as session:
            try:
                item = await session.get(WorkItem, item_id, with_for_update=True)
                if item is None:
                    raise WorkItemNotFoundError(item_id)  # noqa: TRY301
                self._validate_claim(item, claim_owner_id, claim_generation)
                if item.status is WorkItemStatus.RUNNING:
                    return True
                if item.cancellation_requested or item.status is not WorkItemStatus.DISPATCHED:
                    return False
                item.status = WorkItemStatus.RUNNING
                item.claimed_at = datetime.now(UTC)
                await session.commit()
                return True
            except Exception:
                await session.rollback()
                raise

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
        """Record the caller's request for any owned item, including terminal work."""
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
                item.cancellation_requested = True
                await session.execute(text(f"SELECT pg_notify('{_NOTIFY_CHANNEL}', '')"))
                await session.commit()
                return item
            except Exception:
                await session.rollback()
                raise

    async def request_delete(self, item_id: uuid.UUID, *, client_id: str) -> None:
        """Durably request removal after the worker confirms resource cleanup.

        Absent and foreign work are indistinguishable no-ops. No other client's
        work is modified and repeated requests can safely recover a lost receipt.
        """
        async with self._session_context() as session, session.begin():
            result = await session.execute(
                select(WorkItem)
                .where(col(WorkItem.id) == item_id, col(WorkItem.client_id) == client_id)
                .with_for_update()
            )
            item = result.scalars().first()
            if item is not None:
                item.deletion_requested = True
                item.cancellation_requested = True
                await session.execute(text(f"SELECT pg_notify('{_NOTIFY_CHANNEL}', '')"))

    async def purge_deleted_work(self, *, limit: int = 100) -> None:
        """Delete at most limit deletion-requested rows with no owner and confirmed cleanup.

        Eligible rows are terminal or RECONCILIATION_REQUIRED and have cleanup
        status complete/not_required. Lock and delete them in one transaction,
        skipping locked rows. This does not change execution outcomes or contact
        Kubernetes; queued cancellation is handled by the normal start path.
        """
        async with self._session_context() as session, session.begin():
            result = await session.execute(
                select(WorkItem)
                .where(
                    col(WorkItem.deletion_requested).is_(True),
                    col(WorkItem.status).in_([*TERMINAL_RESULT_STATUSES, WorkItemStatus.RECONCILIATION_REQUIRED]),
                    col(WorkItem.claim_owner_id).is_(None),
                    col(WorkItem.resource_cleanup_status).in_(["not_required", "complete"]),
                )
                .order_by(col(WorkItem.created_at))
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            for item in result.scalars().all():
                await session.delete(item)

    async def work_awaiting_requested_cleanup(
        self, *, after_id: uuid.UUID | None = None, limit: int = 100
    ) -> list[WorkItem]:
        """Find deletions and abandoned cancellations needing backend cleanup."""
        statement = select(WorkItem).where(
            or_(
                col(WorkItem.deletion_requested).is_(True),
                and_(
                    col(WorkItem.cancellation_requested).is_(True),
                    col(WorkItem.status) == WorkItemStatus.RECONCILIATION_REQUIRED,
                ),
            ),
            col(WorkItem.status).in_([*TERMINAL_RESULT_STATUSES, WorkItemStatus.RECONCILIATION_REQUIRED]),
            col(WorkItem.claim_owner_id).is_(None),
            col(WorkItem.backend_resource_name).is_not(None),
            col(WorkItem.resource_cleanup_status).not_in(["not_required", "complete"]),
        )
        if after_id is not None:
            statement = statement.where(col(WorkItem.id) > after_id)
        async with self._session_context() as session:
            result = await session.execute(statement.order_by(col(WorkItem.id)).limit(limit))
            return list(result.scalars().all())

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
                cancel_requested = item.cancellation_requested
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
        """Handle pre-Execute failure without mistaking cancellation intent for confirmation."""
        async with self._session_context() as session:
            try:
                item = await session.get(WorkItem, item_id, with_for_update=True)
                if item is None:
                    raise WorkItemNotFoundError(item_id)  # noqa: TRY301
                self._validate_claim(item, claim_owner_id, claim_generation)
                if item.status is not WorkItemStatus.DISPATCHED:
                    return item
                if (
                    item.cancellation_requested
                    and item.backend_resource_name is not None
                    and item.resource_cleanup_status != "complete"
                ):
                    # The backend failed before Execute, but its allocation may still exist.
                    # Keep the request for the existing resource cleanup loop to process.
                    now = datetime.now(UTC)
                    item.status = WorkItemStatus.RECONCILIATION_REQUIRED
                    item.result = {
                        "error": "Pre-execution failure left backend resource cleanup unconfirmed",
                        "error_type": "WorkloadOutcomeUnknownError",
                        "cancellation_requested": item.cancellation_requested,
                    }
                    item.claimed_at = None
                    item.claim_owner_id = None
                    item.completed_at = now
                    self._initialize_completion_notification_fields(item, now)
                elif item.cancellation_requested:
                    self._mark_cancelled(item, execution_started=False)
                else:
                    item.status = WorkItemStatus.PENDING
                    item.execution_target_id = None
                    item.claimed_at = None
                    item.claim_owner_id = None
                await session.commit()
                return item
            except Exception:
                await session.rollback()
                raise

    def _mark_cancelled(self, item: WorkItem, *, execution_started: bool) -> None:
        """Set the cancelled outcome, clear the claim, and initialize completion delivery.

        The caller holds the row lock and has established that cancellation is
        safe to record. Eligibility checks and transaction commit belong to it.
        """
        now = datetime.now(UTC)
        item.status = WorkItemStatus.CANCELLED
        item.result = {"cancelled": True, "execution_started": execution_started}
        item.completed_at = now
        item.claimed_at = None
        item.claim_owner_id = None
        self._initialize_completion_notification_fields(item, now)

    @staticmethod
    def _initialize_completion_notification_fields(item: WorkItem, now: datetime) -> None:
        """Initialize completion delivery once, preserving retries and acknowledgements."""
        if item.deletion_requested or item.completion_notification_state is not None:
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
                    .where(col(WorkItem.deletion_requested).is_(False))
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
