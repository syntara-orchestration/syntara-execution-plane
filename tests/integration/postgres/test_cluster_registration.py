"""Cluster-only registration is atomic, independent, and safe with bounded pools."""

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.commands.clusters import ClusterRegisterCommandHandler
from execution_plane.commands.context import EPCommandContext
from execution_plane.commands.targets import TargetRegisterCommandHandler
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import ExecutionTarget, TargetStatus
from execution_plane_client.commands.clusters import ClusterRegister
from execution_plane_client.commands.targets import KubernetesPlacement, TargetRegister
from execution_plane_client.errors import ProtocolConflict

pytestmark = pytest.mark.integration


@pytest.fixture(params=[1, 4], ids=["one-connection", "concurrent-connections"])
async def registration(request, migrated_database):
    engine = create_async_engine(migrated_database, pool_size=request.param, max_overflow=0, pool_timeout=1)
    client_id = f"registration-{uuid4()}"
    context = EPCommandContext(client_id=client_id, engine=engine)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    command = ClusterRegister(
        resource_id=uuid4(),
        name="cluster",
        endpoint="https://cluster.example",
        credential_ref=uuid4(),
    )
    try:
        yield context, sessions, command
    finally:
        async with sessions.begin() as session:
            ids = select(Cluster.id).where(Cluster.source_client_id.in_([client_id, f"{client_id}-other"]))
            await session.execute(delete(ExecutionTarget).where(ExecutionTarget.cluster_id.in_(ids)))
            await session.execute(delete(Cluster).where(Cluster.id.in_(ids)))
        await engine.dispose()


async def _register(context, command) -> object:
    message = command.to_message(
        source_id=context.client_id, destination_id="execution-plane", client_id=context.client_id
    )
    return await ClusterRegisterCommandHandler().handle(message, command, context)


async def test_concurrent_registration_reuses_one_transaction_and_creates_no_targets(registration):
    context, sessions, command = registration
    receipts = await asyncio.wait_for(asyncio.gather(*(_register(context, command) for _ in range(8))), timeout=10)
    assert all(receipt.resource.resource_id == command.resource_id for receipt in receipts)
    async with sessions() as session:
        cluster = await session.get(Cluster, command.resource_id)
        assert cluster.status is ClusterStatus.ACTIVE
        assert cluster.source_client_id == context.client_id
        assert (
            list(await session.scalars(select(ExecutionTarget).where(ExecutionTarget.cluster_id == cluster.id))) == []
        )


async def test_concurrent_registration_cannot_adopt_another_clients_cluster(registration):
    context, sessions, command = registration
    other = EPCommandContext(client_id=f"{context.client_id}-other", engine=context.engine)
    results = await asyncio.wait_for(
        asyncio.gather(_register(context, command), _register(other, command), return_exceptions=True),
        timeout=10,
    )
    assert sum(isinstance(result, ProtocolConflict) for result in results) == 1
    assert sum(not isinstance(result, Exception) for result in results) == 1
    async with sessions() as session:
        cluster = await session.get(Cluster, command.resource_id)
        assert cluster.source_client_id in {context.client_id, other.client_id}


async def test_registration_failure_rolls_back_all_cluster_changes(registration, monkeypatch):
    context, sessions, command = registration

    async def fail_default(*_args: object) -> None:
        message = "default selection failed"
        raise RuntimeError(message)

    monkeypatch.setattr(ClusterStore, "ensure_default", fail_default)
    with pytest.raises(RuntimeError, match="default selection failed"):
        await _register(context, command)
    async with sessions() as session:
        assert await session.get(Cluster, command.resource_id) is None


async def test_explicit_target_registration_and_cluster_reactivation_are_independent(registration):
    context, sessions, command = registration
    await _register(context, command)
    target_command = TargetRegister(
        resource_id=uuid4(),
        cluster_id=command.resource_id,
        name="explicit-target",
        backend_type="kubernetes",
        endpoint=command.endpoint,
        credential_ref=uuid4(),
        placement=KubernetesPlacement(namespace="chosen"),
        is_default=True,
    )
    message = target_command.to_message(
        source_id=context.client_id,
        destination_id="execution-plane",
        client_id=context.client_id,
    )
    await TargetRegisterCommandHandler().handle(message, target_command, context)
    async with sessions.begin() as session:
        cluster = await session.get(Cluster, command.resource_id)
        cluster.status = ClusterStatus.DRAINING
        cluster.enabled = False
        target = await session.get(ExecutionTarget, target_command.resource_id)
        target.status = TargetStatus.DRAINING
        target.enabled = False
    async with sessions() as session:
        baseline = (await session.get(ExecutionTarget, target_command.resource_id)).model_dump()
    updated = command.model_copy(update={"name": "renamed", "credential_ref": uuid4(), "labels": {"team": "new"}})
    await _register(context, updated)
    async with sessions() as session:
        cluster = await session.get(Cluster, command.resource_id)
        assert cluster.status is ClusterStatus.ACTIVE
        assert cluster.enabled
        assert cluster.name == "renamed"
        assert cluster.credential_ref == updated.credential_ref
        assert (await session.get(ExecutionTarget, target_command.resource_id)).model_dump() == baseline


async def test_disabled_registration_preserves_enabled_flag(registration):
    context, sessions, command = registration
    await _register(context, command.model_copy(update={"enabled": False}))
    async with sessions() as session:
        cluster = await session.get(Cluster, command.resource_id)
        assert cluster.status is ClusterStatus.ACTIVE
        assert not cluster.enabled
        assert not cluster.is_default
