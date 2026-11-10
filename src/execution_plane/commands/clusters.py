"""Cluster commands register and manage clusters, addressed by a caller-supplied id."""

import hashlib
import uuid
from typing import TypedDict

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from execution_plane.cluster.cluster_store import DEFAULT_ADVISORY_LOCK, ClusterStore, DefaultClusterError
from execution_plane.commands.constants import SYSTEM_ACTOR_ID
from execution_plane.commands.context import EPCommandContext
from execution_plane.labels import user_labels
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.commands.clusters import (
    ClusterDelete,
    ClusterRegister,
    ClusterUpdate,
)
from execution_plane_client.errors import ProtocolConflict, UnsupportedCommand
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import MessageEnvelope, ResourceReference


class _ClusterUpdateFields(TypedDict, total=False):
    """Optional keyword arguments accepted by ClusterStore.update."""

    name: str
    endpoint: str
    credential_ref: uuid.UUID
    ca_certificate: str
    enabled: bool
    labels: dict[str, str]


class ResourceNotFoundError(LookupError):
    """A resource is absent or belongs to another client."""


def _validated_payload[T: CommandPayload](payload: CommandPayload, expected: type[T]) -> T:
    """Reject payloads that do not match the handler's installed command."""
    if type(payload) is not expected:
        raise UnsupportedCommand
    return payload


async def _advisory_lock(session: AsyncSession, resource_id: uuid.UUID) -> None:
    """Acquire a per-cluster advisory lock for the duration of the transaction."""
    digest = hashlib.sha256(f"cluster:{resource_id}".encode()).digest()
    await session.execute(select(func.pg_advisory_xact_lock(int.from_bytes(digest[:8], signed=True))))


def _cluster_result(cluster: Cluster) -> HandlingResult:
    return HandlingResult(ResourceReference(kind="cluster", resource_id=cluster.id))


class ClusterRegisterCommandHandler:
    """Register a Cluster independently of its ExecutionTargets."""

    async def handle(
        self, message: MessageEnvelope, payload: CommandPayload, context: EPCommandContext
    ) -> HandlingResult:
        """Atomically upsert caller-owned cluster configuration without creating targets."""
        del message
        command = _validated_payload(payload, ClusterRegister)
        sessions = async_sessionmaker(context.engine, expire_on_commit=False)
        async with sessions.begin() as session:
            await _advisory_lock(session, command.resource_id)
            # Default selection also locks Cluster rows. Take its lock before any
            # row lock so registration follows the same order as set_default.
            await session.execute(select(func.pg_advisory_xact_lock(DEFAULT_ADVISORY_LOCK)))
            existing = await session.get(Cluster, command.resource_id, with_for_update=True)
            if existing is not None and existing.source_client_id != context.client_id:
                raise ProtocolConflict
            store = ClusterStore.from_transaction(session)
            if existing is None:
                await store.create(
                    command.resource_id,
                    command.name,
                    command.endpoint,
                    command.credential_ref,
                    SYSTEM_ACTOR_ID,
                    user_labels(command.labels),
                    source_client_id=context.client_id,
                    ca_certificate=command.ca_certificate,
                )
                await store.activate(command.resource_id, SYSTEM_ACTOR_ID)
            else:
                if existing.is_default and not command.enabled:
                    raise DefaultClusterError
                if existing.status is not ClusterStatus.ACTIVE:
                    await store.reactivate(command.resource_id, updated_by=SYSTEM_ACTOR_ID)
            cluster = await store.update(
                command.resource_id,
                updated_by=SYSTEM_ACTOR_ID,
                name=command.name,
                endpoint=command.endpoint,
                credential_ref=command.credential_ref,
                ca_certificate=command.ca_certificate,
                enabled=command.enabled,
                labels=user_labels(command.labels),
            )
            if command.is_default:
                cluster = await store.set_default(cluster.id, SYSTEM_ACTOR_ID)
            elif command.enabled:
                cluster = await store.ensure_default(cluster.id, SYSTEM_ACTOR_ID)
        return _cluster_result(cluster)


def _cluster_update_fields(command: ClusterUpdate) -> _ClusterUpdateFields:
    """Build the field-level kwargs for a ClusterStore.update call."""
    kwargs: _ClusterUpdateFields = {}
    if command.name is not None:
        kwargs["name"] = command.name
    if command.endpoint is not None:
        kwargs["endpoint"] = command.endpoint
    if command.credential_ref is not None:
        kwargs["credential_ref"] = command.credential_ref
    if command.ca_certificate is not None:
        kwargs["ca_certificate"] = command.ca_certificate
    if command.enabled is not None:
        kwargs["enabled"] = command.enabled
    if command.labels is not None:
        kwargs["labels"] = user_labels(command.labels)
    return kwargs


class ClusterUpdateCommandHandler:
    """Apply a partial update to an existing Cluster addressed by id."""

    async def handle(
        self, message: MessageEnvelope, payload: CommandPayload, context: EPCommandContext
    ) -> HandlingResult:
        """Merge the patch with current cluster state and apply it atomically."""
        del message
        command = _validated_payload(payload, ClusterUpdate)
        cluster_store = ClusterStore.from_engine(context.engine)

        existing = await cluster_store.get_for_client(context.client_id, command.resource_id)
        if existing is None:
            raise ResourceNotFoundError
        if (command.enabled is False or command.is_default is False) and existing.is_default:
            raise DefaultClusterError

        cluster = await cluster_store.update(
            existing.id,
            updated_by=SYSTEM_ACTOR_ID,
            **_cluster_update_fields(command),
        )

        if command.is_default is True:
            cluster = await cluster_store.set_default(existing.id, SYSTEM_ACTOR_ID)
        elif command.is_default is None:
            cluster = await cluster_store.ensure_default(existing.id, SYSTEM_ACTOR_ID)

        return _cluster_result(cluster)


class ClusterDeleteCommandHandler:
    """Disable and drain an existing Cluster addressed by id."""

    async def handle(
        self, message: MessageEnvelope, payload: CommandPayload, context: EPCommandContext
    ) -> HandlingResult:
        """Initiate cluster deletion, rejecting attempts to delete the global default."""
        del message
        command = _validated_payload(payload, ClusterDelete)
        cluster_store = ClusterStore.from_engine(context.engine)

        existing = await cluster_store.get_for_client(context.client_id, command.resource_id)
        if existing is None:
            raise ResourceNotFoundError

        cluster = await cluster_store.request_delete(existing.id, SYSTEM_ACTOR_ID)
        return _cluster_result(cluster)
