"""Cluster commands provision and manage clusters using domain lifecycle operations."""

import hashlib
import uuid
from typing import TypedDict

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from execution_plane.cluster.cluster_registrar import ClusterRegistrar, NoopDiscoveryMechanism
from execution_plane.cluster.cluster_store import ClusterStore, DefaultClusterError
from execution_plane.commands.context import EPCommandContext
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.labels import user_labels
from execution_plane.models.cluster import Cluster
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.commands.clusters import (
    ClusterDelete,
    ClusterProvision,
    ClusterRegister,
    ClusterUpdate,
)
from execution_plane_client.errors import ProtocolConflict, UnsupportedCommand
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import MessageEnvelope, ResourceReference

SYSTEM_ACTOR_ID = uuid.UUID(int=0)


class _ClusterUpdateFields(TypedDict, total=False):
    """Optional keyword arguments accepted by ClusterStore.update."""

    name: str
    endpoint: str
    api_key: str
    ca_certificate: str
    labels: dict[str, str]
    project_ids: list[uuid.UUID] | None


class ResourceNotFoundError(LookupError):
    """A resource is absent or belongs to another client."""


def _validated_payload[T: CommandPayload](payload: CommandPayload, expected: type[T]) -> T:
    """Reject payloads that do not match the handler's installed command."""
    if type(payload) is not expected:
        raise UnsupportedCommand
    return payload


async def _advisory_lock(session: AsyncSession, client_id: str, resource_key: object) -> None:
    """Acquire a per-integration advisory lock for the duration of the transaction."""
    digest = hashlib.sha256(f"cluster:{client_id}:{resource_key}".encode()).digest()
    await session.execute(select(func.pg_advisory_xact_lock(int.from_bytes(digest[:8], signed=True))))


def _cluster_result(cluster: Cluster, revision: int) -> HandlingResult:
    return HandlingResult(ResourceReference(kind="cluster", resource_id=cluster.id, revision=revision))


def _build_stores(context: EPCommandContext) -> tuple[ClusterStore, ExecutionTargetStore, ClusterRegistrar]:
    cluster_store = ClusterStore.from_engine(context.engine)
    target_store = ExecutionTargetStore.from_engine(context.engine)
    registrar = ClusterRegistrar(cluster_store, target_store, NoopDiscoveryMechanism())
    return cluster_store, target_store, registrar


class ClusterRegisterCommandHandler:
    """Provision or idempotently update a Cluster directly from install-time desired state."""

    async def handle(
        self, message: MessageEnvelope, payload: CommandPayload, context: EPCommandContext
    ) -> HandlingResult:
        """Provision the cluster and default target synchronously."""
        del message
        command = _validated_payload(payload, ClusterRegister)
        return await _provision_cluster(command, context)


class ClusterProvisionCommandHandler:
    """Provision a Cluster and its default target synchronously."""

    async def handle(
        self, message: MessageEnvelope, payload: CommandPayload, context: EPCommandContext
    ) -> HandlingResult:
        """Provision the cluster and default target synchronously."""
        del message
        command = _validated_payload(payload, ClusterProvision)
        return await _provision_cluster(command, context)


async def _provision_cluster(command: ClusterRegister, context: EPCommandContext) -> HandlingResult:
    """Shared provision path for register and provision commands."""
    desired_labels = user_labels(command.labels)
    cluster_store, _, registrar = _build_stores(context)
    sessions = async_sessionmaker(context.engine, expire_on_commit=False)

    # Check revision under advisory lock to serialise concurrent requests.
    async with sessions.begin() as session:
        await _advisory_lock(session, context.client_id, command.resource_key)
        existing = await cluster_store.get_by_source(context.client_id, command.resource_key)
        if existing is not None:
            if command.desired_revision < existing.source_revision:
                raise ProtocolConflict
            if command.desired_revision == existing.source_revision:
                return _cluster_result(existing, command.desired_revision)
    # Provision (create or update) outside the advisory-lock transaction.
    # Concurrent creates for the same integration are prevented by the unique
    # constraint on (source_client_id, source_integration_id).
    cluster = await registrar.provision(
        command.name,
        command.endpoint,
        command.credential,
        KubernetesPlacement(namespace=command.namespace),
        SYSTEM_ACTOR_ID,
        desired_labels,
        source_client_id=context.client_id,
        source_integration_id=command.resource_key,
        source_revision=command.desired_revision,
        project_ids=command.project_ids,
        is_default=command.is_default,
        ca_certificate=command.ca_certificate,
    )
    return _cluster_result(cluster, command.desired_revision)


def _cluster_update_fields(command: ClusterUpdate) -> _ClusterUpdateFields:
    """Build the field-level kwargs for a ClusterStore.update call."""
    kwargs: _ClusterUpdateFields = {}
    if command.name is not None:
        kwargs["name"] = command.name
    if command.endpoint is not None:
        kwargs["endpoint"] = command.endpoint
    if command.credential is not None:
        kwargs["api_key"] = command.credential
    if command.ca_certificate is not None:
        kwargs["ca_certificate"] = command.ca_certificate
    if command.labels is not None:
        kwargs["labels"] = user_labels(command.labels)
    if "project_ids" in command.model_fields_set:
        kwargs["project_ids"] = command.project_ids
    return kwargs


class ClusterUpdateCommandHandler:
    """Apply a partial update to an existing Cluster."""

    async def handle(
        self, message: MessageEnvelope, payload: CommandPayload, context: EPCommandContext
    ) -> HandlingResult:
        """Merge the patch with current cluster state and apply it atomically."""
        del message
        command = _validated_payload(payload, ClusterUpdate)
        cluster_store, _, _ = _build_stores(context)
        sessions = async_sessionmaker(context.engine, expire_on_commit=False)

        async with sessions.begin() as session:
            await _advisory_lock(session, context.client_id, command.resource_key)
            existing = await cluster_store.get_by_source(context.client_id, command.resource_key)
            if existing is None:
                raise ResourceNotFoundError
            if command.desired_revision <= existing.source_revision:
                if command.desired_revision == existing.source_revision:
                    return _cluster_result(existing, command.desired_revision)
                raise ProtocolConflict
            if (command.enabled is False or command.is_default is False) and existing.is_default:
                raise DefaultClusterError

        if command.enabled is False:
            cluster = await cluster_store.request_delete(existing.id, SYSTEM_ACTOR_ID)
            await cluster_store.update(
                existing.id, updated_by=SYSTEM_ACTOR_ID, source_revision=command.desired_revision
            )
            return _cluster_result(cluster, command.desired_revision)

        cluster = await cluster_store.update(
            existing.id,
            updated_by=SYSTEM_ACTOR_ID,
            source_revision=command.desired_revision,
            **_cluster_update_fields(command),
        )

        if command.is_default is True:
            cluster = await cluster_store.set_default(existing.id, SYSTEM_ACTOR_ID)
        elif command.is_default is None:
            cluster = await cluster_store.ensure_default(existing.id, SYSTEM_ACTOR_ID)

        return _cluster_result(cluster, command.desired_revision)


class ClusterDeleteCommandHandler:
    """Disable and drain an existing Cluster."""

    async def handle(
        self, message: MessageEnvelope, payload: CommandPayload, context: EPCommandContext
    ) -> HandlingResult:
        """Initiate cluster deletion, rejecting attempts to delete the global default."""
        del message
        command = _validated_payload(payload, ClusterDelete)
        cluster_store, _, _ = _build_stores(context)
        sessions = async_sessionmaker(context.engine, expire_on_commit=False)

        async with sessions.begin() as session:
            await _advisory_lock(session, context.client_id, command.resource_key)
            existing = await cluster_store.get_by_source(context.client_id, command.resource_key)
            if existing is None:
                raise ResourceNotFoundError
            if command.desired_revision < existing.source_revision:
                raise ProtocolConflict
            if command.desired_revision == existing.source_revision:
                # Idempotent: already at this revision, just return current state.
                return _cluster_result(existing, command.desired_revision)

        cluster = await cluster_store.request_delete(existing.id, SYSTEM_ACTOR_ID)
        await cluster_store.update(existing.id, updated_by=SYSTEM_ACTOR_ID, source_revision=command.desired_revision)
        return _cluster_result(cluster, command.desired_revision)
