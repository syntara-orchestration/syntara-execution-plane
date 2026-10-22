"""Execution-target commands use the existing target lifecycle."""

from uuid import UUID

from pydantic import TypeAdapter
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from execution_plane.commands.clusters import ResourceNotFoundError
from execution_plane.commands.constants import SYSTEM_ACTOR_ID
from execution_plane.commands.context import EPCommandContext
from execution_plane.execution_target.execution_target_store import (
    DefaultExecutionTargetError,
    ExecutionTargetStore,
)
from execution_plane.models.cluster import Cluster
from execution_plane.models.execution_target import BackendType, ExecutionTarget
from execution_plane.models.execution_target_placement import ExecutionTargetPlacement
from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.commands.targets import TargetDelete, TargetRegister, TargetUpdate
from execution_plane_client.errors import ProtocolConflict, UnsupportedCommand
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import MessageEnvelope, ResourceReference


def _validated_payload[T: CommandPayload](payload: CommandPayload, expected: type[T]) -> T:
    if type(payload) is not expected:
        raise UnsupportedCommand
    return payload


async def _owned_cluster(session: AsyncSession, cluster_id: UUID, client_id: str) -> Cluster:
    cluster = await session.get(Cluster, cluster_id, with_for_update=True)
    if cluster is None or cluster.source_client_id != client_id:
        raise ResourceNotFoundError
    return cluster


async def _owned_target(session: AsyncSession, target_id: UUID, client_id: str) -> ExecutionTarget:
    target = await session.get(ExecutionTarget, target_id)
    if target is None:
        raise ResourceNotFoundError
    await _owned_cluster(session, target.cluster_id, client_id)
    target = await session.get(ExecutionTarget, target_id, with_for_update=True)
    if target is None:
        raise ResourceNotFoundError
    return target


def _target_result(target_id: UUID) -> HandlingResult:
    return HandlingResult(ResourceReference(kind="execution_target", resource_id=target_id))


class TargetRegisterCommandHandler:
    """Register one independently identified ExecutionTarget."""

    async def handle(
        self, message: MessageEnvelope, payload: CommandPayload, context: EPCommandContext
    ) -> HandlingResult:
        """Create and activate a caller-owned target."""
        del message
        command = _validated_payload(payload, TargetRegister)
        sessions = async_sessionmaker(context.engine, expire_on_commit=False)
        async with sessions.begin() as session:
            await _owned_cluster(session, command.cluster_id, context.client_id)
            if await session.get(ExecutionTarget, command.resource_id) is not None:
                raise ProtocolConflict
            target_store = ExecutionTargetStore.from_transaction(session)
            await target_store.create(
                cluster_id=command.cluster_id,
                name=command.name,
                backend_type=BackendType(command.backend_type),
                endpoint=command.endpoint,
                credential_ref=command.credential_ref,
                is_default=False,
                created_by=SYSTEM_ACTOR_ID,
                placement=TypeAdapter(ExecutionTargetPlacement).validate_python(command.placement.model_dump()),
                labels=command.labels,
                target_id=command.resource_id,
            )
            await target_store.activate(command.resource_id, SYSTEM_ACTOR_ID)
            if command.is_default:
                await target_store.set_default(command.resource_id, SYSTEM_ACTOR_ID)
        return _target_result(command.resource_id)


class TargetUpdateCommandHandler:
    """Patch one independently managed ExecutionTarget."""

    async def handle(
        self, message: MessageEnvelope, payload: CommandPayload, context: EPCommandContext
    ) -> HandlingResult:
        """Apply authorized metadata and default-promotion changes."""
        del message
        command = _validated_payload(payload, TargetUpdate)
        sessions = async_sessionmaker(context.engine, expire_on_commit=False)
        async with sessions.begin() as session:
            target = await _owned_target(session, command.resource_id, context.client_id)
            if any(getattr(command, key) is None for key in command.model_fields_set):
                raise ProtocolConflict
            if command.is_default is False and target.is_default:
                raise DefaultExecutionTargetError
            if command.placement is not None and command.placement.type != target.placement.type:
                raise ProtocolConflict
            target_store = ExecutionTargetStore.from_transaction(session)
            await target_store.update(
                command.resource_id,
                updated_by=SYSTEM_ACTOR_ID,
                name=command.name,
                endpoint=command.endpoint,
                credential_ref=command.credential_ref,
                labels=command.labels,
                placement=None
                if command.placement is None
                else TypeAdapter(ExecutionTargetPlacement).validate_python(
                    command.placement.model_dump(exclude_unset=True)
                ),
            )
            if command.is_default:
                await target_store.set_default(command.resource_id, SYSTEM_ACTOR_ID)
        return _target_result(command.resource_id)


class TargetDeleteCommandHandler:
    """Request draining one independently managed ExecutionTarget."""

    async def handle(
        self, message: MessageEnvelope, payload: CommandPayload, context: EPCommandContext
    ) -> HandlingResult:
        """Drain a non-default caller-owned target."""
        del message
        command = _validated_payload(payload, TargetDelete)
        sessions = async_sessionmaker(context.engine, expire_on_commit=False)
        async with sessions.begin() as session:
            target = await _owned_target(session, command.resource_id, context.client_id)
            if target.is_default:
                raise DefaultExecutionTargetError
            target_store = ExecutionTargetStore.from_transaction(session)
            await target_store.request_delete(command.resource_id, SYSTEM_ACTOR_ID)
        return _target_result(command.resource_id)
