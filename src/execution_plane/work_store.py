"""WorkStore — the only path for mutating WorkItem state.

Nothing outside this module should construct, modify, or commit WorkItem
objects. The public methods express the intended lifecycle transitions;
any state not reachable through them is not a valid transition.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import and_, case, or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlmodel import col

from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.completion_event import CompletionEvent
from execution_plane.models.execution_target import ExecutionTarget, TargetStatus
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.store_base import StoreBase

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

_NOTIFY_CHANNEL = "execution_plane_work_items"


class WorkItemNotFoundError(LookupError):
    """Raised when a lifecycle transition targets an unknown work item."""

    def __init__(self, item_id: uuid.UUID) -> None:
        """Identify the missing work item."""
        super().__init__(f"Work item {item_id} does not exist")


class IdempotencyConflictError(ValueError):
    """Raised when a caller reuses a request ID with different input."""

    def __init__(self) -> None:
        """Describe the idempotency-key conflict."""
        super().__init__("request_id is already associated with a different execution request")


class WorkStore(StoreBase):
    """Persist work item lifecycle transitions and own database resources."""

    async def dispatch(
        self,
        client_id: str,
        project_id: uuid.UUID,
        request_id: str,
        work_correlation_id: uuid.UUID,
        payload: dict[str, Any],
    ) -> WorkItem:
        """Idempotently insert scoped work and wake the EP worker via pg_notify."""
        request_hash = hashlib.sha256(
            json.dumps(
                {"work_correlation_id": str(work_correlation_id), "payload": payload},
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        ).hexdigest()
        key_filter = (
            (col(WorkItem.client_id) == client_id)
            & (col(WorkItem.project_id) == project_id)
            & (col(WorkItem.request_id) == request_id)
        )
        item = WorkItem(
            id=uuid.uuid4(),
            client_id=client_id,
            project_id=project_id,
            request_id=request_id,
            request_hash=request_hash,
            work_correlation_id=work_correlation_id,
            status=WorkItemStatus.PENDING,
            payload=payload,
            created_at=datetime.now(UTC),
        )
        async with self._session_context() as session:
            try:
                result = await session.execute(select(WorkItem).where(key_filter))
                existing = result.scalars().first()
                if existing is not None:
                    self._validate_same_request(existing, request_hash)
                    return existing
                session.add(item)
                # pg_notify is transactional — delivered only after this commit.
                await session.execute(text(f"SELECT pg_notify('{_NOTIFY_CHANNEL}', '')"))
                await session.commit()
            except IntegrityError:
                await session.rollback()
                result = await session.execute(select(WorkItem).where(key_filter))
                existing = result.scalars().first()
                if existing is None:
                    raise
                self._validate_same_request(existing, request_hash)
                return existing
            except Exception:
                await session.rollback()
                raise
        return item

    @staticmethod
    def _validate_same_request(item: WorkItem, request_hash: str) -> None:
        if item.request_hash != request_hash:
            raise IdempotencyConflictError

    async def get(self, item_id: uuid.UUID, *, client_id: str, project_id: uuid.UUID) -> WorkItem | None:
        """Return one work item only inside the authenticated client/project scope."""
        async with self._session_context() as session:
            result = await session.execute(
                select(WorkItem)
                .where(col(WorkItem.id) == item_id)
                .where(col(WorkItem.client_id) == client_id)
                .where(col(WorkItem.project_id) == project_id)
            )
            return result.scalars().first()

    async def get_by_request_id(self, request_id: str, *, client_id: str, project_id: uuid.UUID) -> WorkItem | None:
        """Resolve an idempotency key without exposing other callers' records."""
        async with self._session_context() as session:
            result = await session.execute(
                select(WorkItem)
                .where(col(WorkItem.request_id) == request_id)
                .where(col(WorkItem.client_id) == client_id)
                .where(col(WorkItem.project_id) == project_id)
            )
            return result.scalars().first()

    async def get_completion_event(self, work_item_id: uuid.UUID) -> CompletionEvent | None:
        """Return the durable result event associated with a terminal work item."""
        async with self._session_context() as session:
            result = await session.execute(
                select(CompletionEvent).where(col(CompletionEvent.work_item_id) == work_item_id)
            )
            return result.scalar_one_or_none()

    async def list_scoped(self, *, client_id: str, project_id: uuid.UUID, limit: int) -> list[WorkItem]:
        """Return a bounded list inside the authenticated client/project scope."""
        async with self._session_context() as session:
            result = await session.execute(
                select(WorkItem)
                .where(col(WorkItem.client_id) == client_id)
                .where(col(WorkItem.project_id) == project_id)
                .order_by(col(WorkItem.created_at).desc())
                .limit(limit)
            )
            return list(result.scalars().all())

    async def list_for_client(self, *, client_id: str, limit: int) -> list[WorkItem]:
        """Return bounded work for a client whose signed grant covers all projects."""
        async with self._session_context() as session:
            result = await session.execute(
                select(WorkItem)
                .where(col(WorkItem.client_id) == client_id)
                .where(col(WorkItem.project_id) != uuid.UUID(int=0))
                .order_by(col(WorkItem.created_at).desc())
                .limit(limit)
            )
            return list(result.scalars().all())

    async def claim_one(self) -> WorkItem | None:
        """Claim queued work or recover a claim whose controller lease expired."""
        lease_expiry = datetime.now(UTC) - timedelta(seconds=60)
        async with self._session_context() as session:
            try:
                result = await session.execute(
                    select(WorkItem)
                    .where(
                        or_(
                            col(WorkItem.status) == WorkItemStatus.PENDING,
                            and_(
                                col(WorkItem.status).in_(
                                    [
                                        WorkItemStatus.CLAIMED,
                                        WorkItemStatus.DISPATCHED,
                                        WorkItemStatus.CANCEL_REQUESTED,
                                        WorkItemStatus.RECONCILIATION_REQUIRED,
                                    ]
                                ),
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
                # Keep an existing assignment while recovering a leased item. New
                # work currently uses the first active project-eligible target; the
                # placement resolver is not yet wired into this queue selector.
                if item.execution_target_id is None:
                    target_result = await session.execute(
                        select(col(ExecutionTarget.id))
                        .join(Cluster, col(Cluster.id) == col(ExecutionTarget.cluster_id))
                        .where(col(ExecutionTarget.enabled).is_(True))
                        .where(col(ExecutionTarget.status) == TargetStatus.ACTIVE)
                        .where(col(Cluster.enabled).is_(True))
                        .where(col(Cluster.status) == ClusterStatus.ACTIVE)
                        .where(
                            or_(
                                col(Cluster.project_ids).is_(None),
                                col(Cluster.project_ids).contains([str(item.project_id)]),
                            )
                        )
                        .order_by(col(ExecutionTarget.is_default).desc(), col(ExecutionTarget.id))
                        .limit(1)
                        .with_for_update(skip_locked=True, of=ExecutionTarget)
                    )
                    target_id = target_result.scalar_one_or_none()
                    if target_id is None:
                        return None
                    item.execution_target_id = target_id
                if item.status is WorkItemStatus.PENDING:
                    item.status = WorkItemStatus.CLAIMED
                item.claimed_at = datetime.now(UTC)
                await session.commit()
                return item
            except Exception:
                await session.rollback()
                raise

    async def mark_dispatched(self, item_id: uuid.UUID) -> bool:
        """Persist the external-dispatch boundary unless cancellation already won."""
        async with self._session_context() as session:
            try:
                item = await session.get(WorkItem, item_id, with_for_update=True)
                if item is None:
                    raise WorkItemNotFoundError(item_id)  # noqa: TRY301
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

    async def refresh_claim(self, item_id: uuid.UUID) -> bool:
        """Renew the controller lease and report whether cancellation was requested."""
        async with self._session_context() as session:
            item = await session.get(WorkItem, item_id, with_for_update=True)
            if item is None:
                return False
            if item.status in {
                WorkItemStatus.CLAIMED,
                WorkItemStatus.DISPATCHED,
                WorkItemStatus.CANCEL_REQUESTED,
                WorkItemStatus.RECONCILIATION_REQUIRED,
            }:
                item.claimed_at = datetime.now(UTC)
                await session.commit()
            return item.status is WorkItemStatus.CANCEL_REQUESTED

    async def set_result(
        self,
        item_id: uuid.UUID,
        result: dict[str, Any],
        status: WorkItemStatus,
    ) -> WorkItem:
        """Atomically persist a terminal result and its completion outbox event."""
        async with self._session_context() as session:
            try:
                item = await session.get(WorkItem, item_id, with_for_update=True)
                if item is None:
                    raise WorkItemNotFoundError(item_id)  # noqa: TRY301
                if item.status in {WorkItemStatus.COMPLETED, WorkItemStatus.FAILED, WorkItemStatus.CANCELLED}:
                    return item
                now = datetime.now(UTC)
                item.result = result
                item.status = status
                item.completed_at = now
                session.add(
                    CompletionEvent(
                        id=uuid.uuid4(),
                        work_item_id=item.id,
                        client_id=item.client_id,
                        project_id=item.project_id,
                        request_id=item.request_id,
                        state_revision=1,
                        status=status.value,
                        result=result,
                        created_at=now,
                        next_attempt_at=now,
                    )
                )
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
        project_id: uuid.UUID,
    ) -> WorkItem:
        """Cancel queued work or durably request termination of a dispatched job."""
        async with self._session_context() as session:
            try:
                result = await session.execute(
                    select(WorkItem)
                    .where(col(WorkItem.id) == item_id)
                    .where(col(WorkItem.client_id) == client_id)
                    .where(col(WorkItem.project_id) == project_id)
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

    async def mark_reconciliation_required(self, item_id: uuid.UUID, reason: str) -> WorkItem:
        """Persist an uncertain external outcome without implying failure or rerunning it."""
        async with self._session_context() as session:
            try:
                item = await session.get(WorkItem, item_id, with_for_update=True)
                if item is None:
                    raise WorkItemNotFoundError(item_id)  # noqa: TRY301
                if item.status in {WorkItemStatus.COMPLETED, WorkItemStatus.FAILED, WorkItemStatus.CANCELLED}:
                    return item
                cancel_requested = item.status is WorkItemStatus.CANCEL_REQUESTED
                if not cancel_requested:
                    item.status = WorkItemStatus.RECONCILIATION_REQUIRED
                item.result = {
                    "error": reason[:1000],
                    "error_type": "WorkloadOutcomeUnknownError",
                    "cancellation_requested": cancel_requested,
                }
                item.claimed_at = datetime.now(UTC)
                await session.commit()
                return item
            except Exception:
                await session.rollback()
                raise

    async def request_cancel_by_request_id(
        self,
        request_id: str,
        *,
        client_id: str,
        project_id: uuid.UUID,
        work_correlation_id: uuid.UUID,
    ) -> WorkItem:
        """Cancel by stable key, inserting a tombstone if submission has not arrived."""
        key_filter = (
            (col(WorkItem.client_id) == client_id)
            & (col(WorkItem.project_id) == project_id)
            & (col(WorkItem.request_id) == request_id)
        )
        request_hash = hashlib.sha256(
            f"cancelled-before-submit:{client_id}:{project_id}:{request_id}".encode()
        ).hexdigest()
        async with self._session_context() as session:
            try:
                result = await session.execute(select(WorkItem).where(key_filter).with_for_update())
                item = result.scalars().first()
                if item is None:
                    now = datetime.now(UTC)
                    item = WorkItem(
                        id=uuid.uuid4(),
                        client_id=client_id,
                        project_id=project_id,
                        request_id=request_id,
                        request_hash=request_hash,
                        work_correlation_id=work_correlation_id,
                        status=WorkItemStatus.CANCELLED,
                        payload={},
                        result={"cancelled": True, "execution_started": False},
                        created_at=now,
                        completed_at=now,
                    )
                    session.add(item)
                else:
                    await self._request_cancel_locked(session, item)
                await session.commit()
                return item
            except IntegrityError:
                await session.rollback()
                existing = await self.get_by_request_id(request_id, client_id=client_id, project_id=project_id)
                if existing is None:
                    raise
                return await self.request_cancel(
                    existing.id,
                    client_id=client_id,
                    project_id=project_id,
                )
            except Exception:
                await session.rollback()
                raise

    async def _request_cancel_locked(self, session: AsyncSession, item: WorkItem) -> None:
        """Apply a cancellation transition while the work-item row is locked."""
        if item.status in {
            WorkItemStatus.COMPLETED,
            WorkItemStatus.FAILED,
            WorkItemStatus.CANCELLED,
            WorkItemStatus.CANCEL_REQUESTED,
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
        session.add(
            CompletionEvent(
                id=uuid.uuid4(),
                work_item_id=item.id,
                client_id=item.client_id,
                project_id=item.project_id,
                request_id=item.request_id,
                state_revision=1,
                status=WorkItemStatus.CANCELLED.value,
                result=item.result,
                created_at=now,
                next_attempt_at=now,
            )
        )

    async def get_pending_completion_events(self, *, limit: int = 100) -> list[CompletionEvent]:
        """Load a bounded batch of due completion-event outbox rows."""
        now = datetime.now(UTC)
        async with self._session_context() as session:
            result = await session.execute(
                select(CompletionEvent)
                .where(col(CompletionEvent.delivered_at).is_(None))
                .where(col(CompletionEvent.next_attempt_at) <= now)
                .order_by(col(CompletionEvent.created_at))
                .limit(limit)
            )
            return list(result.scalars().all())

    async def record_completion_event_attempt(
        self,
        event_id: uuid.UUID,
        *,
        delivered: bool,
        error: str | None = None,
        retry_after_seconds: int = 10,
    ) -> None:
        """Record delivery or schedule a retry for a completion event."""
        async with self._session_context() as session:
            try:
                event = await session.get(CompletionEvent, event_id, with_for_update=True)
                if event is None:
                    return
                event.attempts += 1
                event.last_error = None if delivered else (error or "delivery failed")[:1000]
                if delivered:
                    event.delivered_at = datetime.now(UTC)
                else:
                    event.next_attempt_at = datetime.now(UTC) + timedelta(seconds=retry_after_seconds)
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def is_target_drained(self, target_id: uuid.UUID) -> bool:
        """Return whether no owned or uncertain work remains on a target."""
        async with self._session_context() as session:
            result = await session.execute(
                select(col(WorkItem.id))
                .where(col(WorkItem.execution_target_id) == target_id)
                .where(
                    col(WorkItem.status).in_(
                        [
                            WorkItemStatus.CLAIMED,
                            WorkItemStatus.DISPATCHED,
                            WorkItemStatus.CANCEL_REQUESTED,
                            WorkItemStatus.RECONCILIATION_REQUIRED,
                        ]
                    )
                )
                .limit(1)
            )
            return result.scalar_one_or_none() is None
