"""Persistence operations for execution-target lifecycle transitions."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlmodel import col

from execution_plane.labels import labels_with_execution_target_id, merge_user_labels
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.query_filters import apply_list_filters
from execution_plane.query_sorting import apply_cursor_sort
from execution_plane.store_base import StoreBase

if TYPE_CHECKING:
    from collections.abc import Sequence

    from execution_plane.models.execution_target_placement import (
        ExecutionTargetPlacement,
        ExecutionTargetPlacementTypes,
    )


class ExecutionTargetNotFoundError(LookupError):
    """Raised when a lifecycle transition targets an unknown execution target."""

    def __init__(self, target_id: uuid.UUID) -> None:
        """Identify the missing target."""
        super().__init__(f"Execution target {target_id} does not exist")


class DefaultExecutionTargetError(ValueError):
    """Raised when an operation would violate default-target protection."""


class TargetNotActivatableError(ValueError):
    """Raised when a target is no longer in its registration state."""


class TargetNotEligibleForDefaultError(ValueError):
    """Raised when an execution target cannot become the Cluster default."""

    def __init__(self, target_id: uuid.UUID) -> None:
        """Identify the ineligible target."""
        super().__init__(f"Execution target {target_id} is not eligible to become the default")


class ClusterNotAvailableError(ValueError):
    """Raised when a target is created for a Cluster that cannot accept targets."""


class ExecutionTargetStore(StoreBase):
    """Persist execution targets and own their database resources."""

    @staticmethod
    def _merge_placement(
        current: ExecutionTargetPlacementTypes,
        patch: ExecutionTargetPlacementTypes,
    ) -> ExecutionTargetPlacementTypes:
        """Apply explicitly supplied placement fields without dropping stored values."""
        if current.type != patch.type:
            return patch
        updates = patch.model_dump(exclude={"type"}, exclude_unset=True)
        return current.model_copy(update=updates)

    @staticmethod
    def _can_add_execution_target(cluster: Cluster | None) -> bool:
        """Return whether a Cluster can accept another target."""
        if cluster is None:
            return False
        if not cluster.enabled:
            return False
        return cluster.status in [ClusterStatus.ACTIVE, ClusterStatus.REGISTERING]

    async def create(
        self,
        cluster_id: uuid.UUID,
        name: str,
        backend_type: BackendType,
        endpoint: str,
        credential_ref: uuid.UUID,
        is_default: bool,  # noqa: FBT001
        created_by: uuid.UUID,
        placement: ExecutionTargetPlacement,
        labels: dict[str, str] | None = None,
        *,
        target_id: uuid.UUID | None = None,
    ) -> ExecutionTarget:
        """Create a target, rejecting a second default in the same cluster."""
        now = datetime.now(UTC)
        target = ExecutionTarget(
            cluster_id=cluster_id,
            name=name,
            backend_type=backend_type,
            endpoint=endpoint,
            placement=placement,
            credential_ref=credential_ref,
            is_default=is_default,
            labels={},
            created_by=created_by,
            created_at=now,
            updated_by=created_by,
            updated_at=now,
        )
        if target_id is not None:
            target.id = target_id
        target.labels = labels_with_execution_target_id(labels, target.id)

        async with self._session_context() as session:
            try:
                cluster = await session.get(Cluster, cluster_id, with_for_update=True)
                if not self._can_add_execution_target(cluster):
                    raise ClusterNotAvailableError(cluster_id)  # noqa: TRY301
                if is_default:
                    result = await session.execute(
                        select(ExecutionTarget)
                        .where(col(ExecutionTarget.cluster_id) == cluster_id)
                        .where(col(ExecutionTarget.is_default).is_(True))
                    )
                    if result.scalar_one_or_none() is not None:
                        raise DefaultExecutionTargetError  # noqa: TRY301
                session.add(target)
                await self._commit(session)
                return self._without_secret(target)
            except IntegrityError as exc:
                await self._rollback(session)
                if is_default:
                    raise DefaultExecutionTargetError from exc
                raise
            except Exception:
                await self._rollback(session)
                raise

    async def get(self, target_id: uuid.UUID) -> ExecutionTarget | None:
        """Return a target by ID."""
        async with self._session_context() as session:
            target = await session.get(ExecutionTarget, target_id)
            return None if target is None else self._without_secret(target)

    async def get_with_credentials(self, target_id: uuid.UUID) -> ExecutionTarget | None:
        """Return a target by ID with its credential reference intact."""
        async with self._session_context() as session:
            return await session.get(ExecutionTarget, target_id)

    async def list(
        self,
        cluster_id: uuid.UUID | None = None,
        eligible_only: bool = False,  # noqa: FBT001, FBT002
        status: TargetStatus | None = None,
        limit: int | None = None,
    ) -> Sequence[ExecutionTarget]:
        """List targets, optionally restricting results to work-eligible targets."""
        statement = select(ExecutionTarget)
        if cluster_id is not None:
            statement = statement.where(col(ExecutionTarget.cluster_id) == cluster_id)
        if status is not None:
            statement = statement.where(col(ExecutionTarget.status) == status)
        if eligible_only:
            statement = (
                statement.join(Cluster)
                .where(col(ExecutionTarget.enabled).is_(True))
                .where(col(ExecutionTarget.status) == TargetStatus.ACTIVE)
                .where(col(Cluster.enabled).is_(True))
                .where(col(Cluster.status) == ClusterStatus.ACTIVE)
            )
        if limit is not None:
            statement = statement.limit(limit)
        async with self._session_context() as session:
            result = await session.execute(statement)
            targets = list(result.scalars().all())
            return [self._without_secret(target) for target in targets]

    async def list_for_client(
        self,
        client_id: str,
        *,
        limit: int,
        target_id: uuid.UUID | None = None,
        cursor: dict[str, Any] | None = None,
        sort_field: str = "created_at",
        sort_direction: str = "asc",
        filters: dict[str, str] | None = None,
    ) -> Sequence[ExecutionTarget]:
        """Adapt syntara.core.services.base.BaseService._build_list_query with client scoping."""
        statement = select(ExecutionTarget).join(Cluster).where(col(Cluster.source_client_id) == client_id)
        if target_id is not None:
            statement = statement.where(col(ExecutionTarget.id) == target_id)
        statement = apply_list_filters(statement, filters, ExecutionTarget)
        statement = apply_cursor_sort(
            statement, ExecutionTarget, cursor, sort_field=sort_field, sort_direction=sort_direction
        )
        async with self._session_context() as session:
            result = await session.execute(statement.limit(limit))
            return [self._without_secret(target) for target in result.scalars().all()]

    async def count_for_client(self, client_id: str, filters: dict[str, str] | None = None) -> int:
        """Adapt syntara.core.services.base.BaseService._get_total_count using the same filters."""
        statement = (
            select(func.count())
            .select_from(ExecutionTarget)
            .join(Cluster)
            .where(col(Cluster.source_client_id) == client_id)
        )
        statement = apply_list_filters(statement, filters, ExecutionTarget)
        async with self._session_context() as session:
            result = await session.execute(statement)
            return result.scalar_one()

    async def request_delete(self, target_id: uuid.UUID, updated_by: uuid.UUID) -> ExecutionTarget:
        """Disable a non-default target and move it to DRAINING."""
        async with self._session_context() as session:
            try:
                target = await session.get(ExecutionTarget, target_id, with_for_update=True)
                if target is None:
                    raise ExecutionTargetNotFoundError(target_id)  # noqa: TRY301
                if target.is_default:
                    raise DefaultExecutionTargetError  # noqa: TRY301
                target.enabled = False
                target.status = TargetStatus.DRAINING
                target.updated_by = updated_by
                target.updated_at = datetime.now(UTC)
                await self._commit(session)
                return self._without_secret(target)
            except Exception:
                await self._rollback(session)
                raise

    async def activate(self, target_id: uuid.UUID, updated_by: uuid.UUID) -> ExecutionTarget:
        """Mark a successfully registered target eligible for work."""
        async with self._session_context() as session:
            try:
                target = await session.get(ExecutionTarget, target_id, with_for_update=True)
                if target is None:
                    raise ExecutionTargetNotFoundError(target_id)  # noqa: TRY301
                if target.status is not TargetStatus.REGISTERING:
                    raise TargetNotActivatableError(target_id)  # noqa: TRY301
                target.enabled = True
                target.status = TargetStatus.ACTIVE
                target.updated_by = updated_by
                target.updated_at = datetime.now(UTC)
                await self._commit(session)
                return self._without_secret(target)
            except Exception:
                await self._rollback(session)
                raise

    async def set_default(self, target_id: uuid.UUID, updated_by: uuid.UUID) -> ExecutionTarget:
        """Atomically promote one target and demote the Cluster's current default."""
        async with self._session_context() as session:
            try:
                target = await session.get(ExecutionTarget, target_id)
                if target is None:
                    raise ExecutionTargetNotFoundError(target_id)  # noqa: TRY301
                cluster = await session.get(Cluster, target.cluster_id, with_for_update=True)
                if cluster is None or not cluster.enabled or cluster.status is not ClusterStatus.ACTIVE:
                    raise ClusterNotAvailableError(target.cluster_id)  # noqa: TRY301
                target = await session.get(ExecutionTarget, target_id, with_for_update=True)
                if target is None:
                    raise ExecutionTargetNotFoundError(target_id)  # noqa: TRY301
                if not target.enabled or target.status is not TargetStatus.ACTIVE:
                    raise TargetNotEligibleForDefaultError(target_id)  # noqa: TRY301

                result = await session.execute(
                    select(ExecutionTarget)
                    .where(col(ExecutionTarget.cluster_id) == target.cluster_id)
                    .where(col(ExecutionTarget.is_default).is_(True))
                    .with_for_update()
                )
                previous_default = result.scalar_one_or_none()
                now = datetime.now(UTC)
                if previous_default is not None and previous_default.id != target.id:
                    previous_default.is_default = False
                    previous_default.updated_by = updated_by
                    previous_default.updated_at = now
                    await session.flush()
                if not target.is_default:
                    target.is_default = True
                    target.updated_by = updated_by
                    target.updated_at = now
                await self._commit(session)
                return self._without_secret(target)
            except IntegrityError as exc:
                await self._rollback(session)
                raise DefaultExecutionTargetError from exc
            except Exception:
                await self._rollback(session)
                raise

    async def update(
        self,
        target_id: uuid.UUID,
        *,
        updated_by: uuid.UUID,
        name: str | None = None,
        endpoint: str | None = None,
        placement: ExecutionTargetPlacement | None = None,
        labels: dict[str, str] | None = None,
        status_message: str | None = None,
        credential_ref: uuid.UUID | None = None,
        enabled: bool | None = None,
    ) -> ExecutionTarget:
        """Update mutable target metadata without changing ownership or default status."""
        async with self._session_context() as session:
            try:
                target = await session.get(ExecutionTarget, target_id, with_for_update=True)
                if target is None:
                    raise ExecutionTargetNotFoundError(target_id)  # noqa: TRY301
                if name is not None:
                    target.name = name
                if endpoint is not None:
                    target.endpoint = endpoint
                if placement is not None:
                    target.placement = self._merge_placement(target.placement, placement)
                if labels is not None:
                    target.labels = merge_user_labels(target.labels, labels)
                if status_message is not None:
                    target.status_message = status_message
                if credential_ref is not None:
                    target.credential_ref = credential_ref
                if enabled is not None:
                    target.enabled = enabled
                target.updated_by = updated_by
                target.updated_at = datetime.now(UTC)
                await self._commit(session)
                return self._without_secret(target)
            except Exception:
                await self._rollback(session)
                raise

    async def reactivate(
        self,
        target_id: uuid.UUID,
        *,
        updated_by: uuid.UUID,
    ) -> ExecutionTarget:
        """Re-enable a DRAINING target and transition it back to ACTIVE."""
        async with self._session_context() as session:
            try:
                target = await session.get(ExecutionTarget, target_id, with_for_update=True)
                if target is None:
                    raise ExecutionTargetNotFoundError(target_id)  # noqa: TRY301
                target.enabled = True
                target.status = TargetStatus.ACTIVE
                target.status_message = None
                target.updated_by = updated_by
                target.updated_at = datetime.now(UTC)
                await self._commit(session)
                return self._without_secret(target)
            except Exception:
                await self._rollback(session)
                raise

    async def record_health(
        self,
        target_id: uuid.UUID,
        *,
        healthy: bool,
        message: str | None,
        updated_by: uuid.UUID,
    ) -> ExecutionTarget:
        """Persist ACTIVE ↔ DEGRADED from a health probe. Other states are left unchanged."""
        async with self._session_context() as session:
            try:
                target = await session.get(ExecutionTarget, target_id, with_for_update=True)
                if target is None:
                    raise ExecutionTargetNotFoundError(target_id)  # noqa: TRY301
                if target.status not in {TargetStatus.ACTIVE, TargetStatus.DEGRADED}:
                    return self._without_secret(target)
                target.status = TargetStatus.ACTIVE if healthy else TargetStatus.DEGRADED
                target.status_message = None if healthy else message
                target.updated_by = updated_by
                target.updated_at = datetime.now(UTC)
                await self._commit(session)
                return self._without_secret(target)
            except Exception:
                await self._rollback(session)
                raise

    async def mark_failed(self, target_id: uuid.UUID, updated_by: uuid.UUID) -> ExecutionTarget:
        """Record an irrecoverable drain failure without exposing backend details."""
        async with self._session_context() as session:
            try:
                target = await session.get(ExecutionTarget, target_id)
                if target is None:
                    raise ExecutionTargetNotFoundError(target_id)  # noqa: TRY301
                target.enabled = False
                target.status = TargetStatus.FAILED
                target.status_message = "target drain failed"
                target.updated_by = updated_by
                target.updated_at = datetime.now(UTC)
                await self._commit(session)
                return self._without_secret(target)
            except Exception:
                await self._rollback(session)
                raise

    async def finalize_delete(self, target_id: uuid.UUID) -> None:
        """Physically delete a drained target when finalization is safe."""
        async with self._session_context() as session:
            try:
                target = await session.get(ExecutionTarget, target_id, with_for_update=True)
                if target is None:
                    return
                if target.enabled or target.status != TargetStatus.DRAINING:
                    return
                active_work = await session.execute(
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
                if active_work.scalar_one_or_none() is not None:
                    return
                await session.execute(
                    update(WorkItem)
                    .where(col(WorkItem.execution_target_id) == target_id)
                    .values(execution_target_id=None)
                )
                await session.delete(target)
                await self._commit(session)
            except Exception:
                await self._rollback(session)
                raise

    @staticmethod
    def _without_secret(target: ExecutionTarget) -> ExecutionTarget:
        """Return a detached target representation without its credential reference."""
        public = ExecutionTarget.model_validate(target.model_dump())
        public.credential_ref = uuid.UUID(int=0)
        return public
