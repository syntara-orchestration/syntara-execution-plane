"""Persistence operations for Cluster lifecycle transitions."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlmodel import col

from execution_plane.labels import labels_with_cluster_id, merge_user_labels
from execution_plane.models.cluster import Cluster, ClusterStatus, ClusterType
from execution_plane.models.execution_target import ExecutionTarget, TargetStatus
from execution_plane.query_filters import apply_list_filters
from execution_plane.query_sorting import apply_cursor_sort
from execution_plane.store_base import StoreBase

if TYPE_CHECKING:
    import builtins
    from collections.abc import Sequence


class ClusterNotFoundError(LookupError):
    """Raised when a lifecycle operation targets an unknown Cluster."""

    def __init__(self, cluster_id: uuid.UUID) -> None:
        """Identify the missing Cluster."""
        super().__init__(f"Cluster {cluster_id} does not exist")


class DefaultClusterError(ValueError):
    """Raised when an operation would violate global default-Cluster protection."""


class ClusterNotEligibleForDefaultError(ValueError):
    """Raised when a Cluster cannot become the global default."""

    def __init__(self, cluster_id: uuid.UUID) -> None:
        """Identify the ineligible Cluster."""
        super().__init__(f"Cluster {cluster_id} is not eligible to become the default")


DEFAULT_ADVISORY_LOCK = 703571


class ClusterStore(StoreBase):
    """Persist Cluster state and own the database resources it uses."""

    @staticmethod
    def _can_record_discovery_state(cluster: Cluster) -> bool:
        """Return whether a Cluster can have its discovery state set."""
        if not cluster.enabled:
            return False
        return cluster.status is ClusterStatus.REGISTERING

    async def create(
        self,
        cluster_id: uuid.UUID,
        name: str,
        endpoint: str,
        credential_ref: uuid.UUID,
        created_by: uuid.UUID,
        labels: dict[str, str] | None = None,
        *,
        cluster_type: ClusterType = ClusterType.OPENSHIFT,
        source_client_id: str | None = None,
        ca_certificate: str | None = None,
    ) -> Cluster:
        """Persist a new Cluster in REGISTERING state under a caller-supplied id."""
        now = datetime.now(UTC)
        cluster = Cluster(
            id=cluster_id,
            name=name,
            endpoint=endpoint,
            credential_ref=credential_ref,
            ca_certificate=ca_certificate,
            cluster_type=cluster_type,
            source_client_id=source_client_id,
            labels={},
            status=ClusterStatus.REGISTERING,
            created_by=created_by,
            created_at=now,
            updated_by=created_by,
            updated_at=now,
        )
        cluster.labels = labels_with_cluster_id(labels, cluster.id)
        async with self._session_context() as session:
            try:
                session.add(cluster)
                await session.commit()
                return self._without_secret(cluster)
            except Exception:
                await session.rollback()
                raise

    async def get(self, cluster_id: uuid.UUID) -> Cluster | None:
        """Return a Cluster with its CA certificate redacted."""
        async with self._session_context() as session:
            cluster = await session.get(Cluster, cluster_id)
            return None if cluster is None else self._without_secret(cluster)

    async def get_with_credentials(self, cluster_id: uuid.UUID) -> Cluster | None:
        """Return a Cluster with its CA certificate intact."""
        async with self._session_context() as session:
            return await session.get(Cluster, cluster_id)

    async def get_by_name(self, name: str) -> Cluster | None:
        """Return a Cluster by name without its CA certificate."""
        async with self._session_context() as session:
            result = await session.execute(select(Cluster).where(col(Cluster.name) == name).limit(1))
            cluster = result.scalar_one_or_none()
            return None if cluster is None else self._without_secret(cluster)

    async def list(self, *, status: ClusterStatus | None = None, enabled: bool | None = None) -> builtins.list[Cluster]:
        """List Clusters for administrative or recovery workflows."""
        statement = select(Cluster)
        if status is not None:
            statement = statement.where(col(Cluster.status) == status)
        if enabled is not None:
            statement = statement.where(col(Cluster.enabled).is_(enabled))
        async with self._session_context() as session:
            result = await session.execute(statement)
            return [self._without_secret(cluster) for cluster in result.scalars().all()]

    async def list_for_client(
        self,
        client_id: str,
        *,
        limit: int,
        cursor: dict[str, Any] | None = None,
        sort_field: str = "created_at",
        sort_direction: str = "asc",
        filters: dict[str, str] | None = None,
    ) -> Sequence[Cluster]:
        """List clusters owned by the given client, with pagination and filtering."""
        statement = select(Cluster).where(col(Cluster.source_client_id) == client_id)
        statement = apply_list_filters(statement, filters, Cluster)
        statement = apply_cursor_sort(statement, Cluster, cursor, sort_field=sort_field, sort_direction=sort_direction)
        async with self._session_context() as session:
            result = await session.execute(statement.limit(limit))
            return [self._without_secret(c) for c in result.scalars().all()]

    async def count_for_client(self, client_id: str, filters: dict[str, str] | None = None) -> int:
        """Return the number of clusters owned by the given client."""
        statement = select(func.count()).select_from(Cluster).where(col(Cluster.source_client_id) == client_id)
        statement = apply_list_filters(statement, filters, Cluster)
        async with self._session_context() as session:
            result = await session.execute(statement)
            return result.scalar_one()

    async def get_for_client(self, client_id: str, cluster_id: uuid.UUID) -> Cluster | None:
        """Return one cluster owned by the given client, identified by cluster.id."""
        async with self._session_context() as session:
            cluster = await session.get(Cluster, cluster_id)
            if cluster is None or cluster.source_client_id != client_id:
                return None
            return self._without_secret(cluster)

    async def update(
        self,
        cluster_id: uuid.UUID,
        *,
        updated_by: uuid.UUID,
        name: str | None = None,
        endpoint: str | None = None,
        credential_ref: uuid.UUID | None = None,
        ca_certificate: str | None = None,
        enabled: bool | None = None,
        labels: dict[str, str] | None = None,
    ) -> Cluster:
        """Update mutable cluster fields."""
        async with self._session_context() as session:
            try:
                cluster = await session.get(Cluster, cluster_id, with_for_update=True)
                if cluster is None:
                    raise ClusterNotFoundError(cluster_id)  # noqa: TRY301
                if name is not None:
                    cluster.name = name
                if endpoint is not None:
                    cluster.endpoint = endpoint
                if credential_ref is not None:
                    cluster.credential_ref = credential_ref
                if ca_certificate is not None:
                    cluster.ca_certificate = ca_certificate
                if enabled is not None:
                    cluster.enabled = enabled
                if labels is not None:
                    cluster.labels = merge_user_labels(cluster.labels, labels)
                cluster.updated_by = updated_by
                cluster.updated_at = datetime.now(UTC)
                await session.commit()
                return self._without_secret(cluster)
            except Exception:
                await session.rollback()
                raise

    async def set_default(self, cluster_id: uuid.UUID, updated_by: uuid.UUID) -> Cluster:
        """Atomically promote one Cluster and demote the previous global default."""
        async with self._session_context() as session:
            try:
                await session.execute(select(func.pg_advisory_xact_lock(DEFAULT_ADVISORY_LOCK)))
                cluster = await session.get(Cluster, cluster_id, with_for_update=True)
                if cluster is None:
                    raise ClusterNotFoundError(cluster_id)  # noqa: TRY301
                if not cluster.enabled or cluster.status is not ClusterStatus.ACTIVE:
                    raise ClusterNotEligibleForDefaultError(cluster_id)  # noqa: TRY301
                result = await session.execute(
                    select(Cluster).where(col(Cluster.is_default).is_(True)).with_for_update()
                )
                previous_default = result.scalar_one_or_none()
                now = datetime.now(UTC)
                if previous_default is not None and previous_default.id != cluster.id:
                    previous_default.is_default = False
                    previous_default.updated_by = updated_by
                    previous_default.updated_at = now
                    await session.flush()
                if not cluster.is_default:
                    cluster.is_default = True
                    cluster.updated_by = updated_by
                    cluster.updated_at = now
                await session.commit()
                return self._without_secret(cluster)
            except IntegrityError as exc:
                await session.rollback()
                raise DefaultClusterError from exc
            except Exception:
                await session.rollback()
                raise

    async def ensure_default(self, cluster_id: uuid.UUID, updated_by: uuid.UUID) -> Cluster:
        """Promote the supplied active Cluster when no global default exists."""
        async with self._session_context() as session:
            try:
                await session.execute(select(func.pg_advisory_xact_lock(DEFAULT_ADVISORY_LOCK)))
                cluster = await session.get(Cluster, cluster_id, with_for_update=True)
                if cluster is None:
                    raise ClusterNotFoundError(cluster_id)  # noqa: TRY301
                result = await session.execute(
                    select(Cluster).where(col(Cluster.is_default).is_(True)).with_for_update()
                )
                if result.scalar_one_or_none() is None:
                    if not cluster.enabled or cluster.status is not ClusterStatus.ACTIVE:
                        raise ClusterNotEligibleForDefaultError(cluster_id)  # noqa: TRY301
                    cluster.is_default = True
                    cluster.updated_by = updated_by
                    cluster.updated_at = datetime.now(UTC)
                await session.commit()
                return self._without_secret(cluster)
            except Exception:
                await session.rollback()
                raise

    async def reactivate(
        self,
        cluster_id: uuid.UUID,
        *,
        updated_by: uuid.UUID,
        endpoint: str | None = None,
        credential_ref: uuid.UUID | None = None,
        ca_certificate: str | None = None,
        labels: dict[str, str] | None = None,
    ) -> Cluster:
        """Re-enable a DRAINING cluster and transition it back to ACTIVE."""
        async with self._session_context() as session:
            try:
                cluster = await session.get(Cluster, cluster_id, with_for_update=True)
                if cluster is None:
                    raise ClusterNotFoundError(cluster_id)  # noqa: TRY301
                cluster.enabled = True
                cluster.status = ClusterStatus.ACTIVE
                cluster.status_message = None
                if endpoint is not None:
                    cluster.endpoint = endpoint
                if credential_ref is not None:
                    cluster.credential_ref = credential_ref
                if ca_certificate is not None:
                    cluster.ca_certificate = ca_certificate
                if labels is not None:
                    cluster.labels = merge_user_labels(cluster.labels, labels)
                cluster.updated_by = updated_by
                cluster.updated_at = datetime.now(UTC)
                await session.commit()
                return self._without_secret(cluster)
            except Exception:
                await session.rollback()
                raise

    async def record_discovery_state(
        self,
        cluster_id: uuid.UUID,
        status: ClusterStatus,
        status_message: str | None,
        updated_by: uuid.UUID,
    ) -> Cluster:
        """Persist registration/discovery state on the existing Cluster."""
        async with self._session_context() as session:
            try:
                cluster = await session.get(Cluster, cluster_id, with_for_update=True)
                if cluster is None:
                    raise ClusterNotFoundError(cluster_id)  # noqa: TRY301
                if not self._can_record_discovery_state(cluster):
                    return self._without_secret(cluster)
                cluster.status = status
                cluster.enabled = status is not ClusterStatus.ERROR
                cluster.status_message = status_message
                cluster.updated_by = updated_by
                cluster.updated_at = datetime.now(UTC)
                await session.commit()
                return self._without_secret(cluster)
            except Exception:
                await session.rollback()
                raise

    async def mark_drain_failed(
        self,
        cluster_id: uuid.UUID,
        status_message: str,
        updated_by: uuid.UUID,
    ) -> Cluster:
        """Persist a failed drain on a Cluster that is no longer available."""
        async with self._session_context() as session:
            try:
                cluster = await session.get(Cluster, cluster_id, with_for_update=True)
                if cluster is None:
                    raise ClusterNotFoundError(cluster_id)  # noqa: TRY301
                cluster.enabled = False
                cluster.status = ClusterStatus.ERROR
                cluster.status_message = status_message
                cluster.updated_by = updated_by
                cluster.updated_at = datetime.now(UTC)
                await session.commit()
                return self._without_secret(cluster)
            except Exception:
                await session.rollback()
                raise

    async def request_delete(self, cluster_id: uuid.UUID, updated_by: uuid.UUID) -> Cluster:
        """Disable a Cluster and all targets before asynchronous draining."""
        async with self._session_context() as session:
            try:
                cluster = await session.get(Cluster, cluster_id, with_for_update=True)
                if cluster is None:
                    raise ClusterNotFoundError(cluster_id)  # noqa: TRY301
                if cluster.is_default:
                    raise DefaultClusterError  # noqa: TRY301
                now = datetime.now(UTC)
                cluster.enabled = False
                cluster.status = ClusterStatus.DRAINING
                cluster.updated_by = updated_by
                cluster.updated_at = now
                result = await session.execute(
                    select(ExecutionTarget).where(col(ExecutionTarget.cluster_id) == cluster_id)
                )
                for target in result.scalars().all():
                    target.enabled = False
                    target.status = TargetStatus.DRAINING
                    target.updated_by = updated_by
                    target.updated_at = now
                await session.commit()
                return self._without_secret(cluster)
            except Exception:
                await session.rollback()
                raise

    async def finalize_delete(self, cluster_id: uuid.UUID) -> None:
        """Delete a Cluster when its persisted state makes finalization safe."""
        async with self._session_context() as session:
            try:
                cluster = await session.get(Cluster, cluster_id, with_for_update=True)
                if cluster is None:
                    return
                if cluster.enabled or cluster.status is not ClusterStatus.DRAINING:
                    return
                result = await session.execute(
                    select(ExecutionTarget).where(col(ExecutionTarget.cluster_id) == cluster_id)
                )
                if result.scalars().all():
                    return
                await session.delete(cluster)
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    @staticmethod
    def _without_secret(cluster: Cluster) -> Cluster:
        """Return a detached Cluster without its credential reference or CA certificate."""
        public = Cluster.model_validate(cluster.model_dump())
        public.credential_ref = uuid.UUID(int=0)
        public.ca_certificate = None
        return public
