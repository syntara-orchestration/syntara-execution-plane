"""Cluster commands keep provisioning on one connection under one resource lock."""

import asyncio
import hashlib
from typing import Never
from unittest.mock import Mock
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel import col

from execution_plane.cluster.cluster_store import DEFAULT_ADVISORY_LOCK, ClusterStore
from execution_plane.commands.clusters import ClusterProvisionCommandHandler, ClusterRegisterCommandHandler
from execution_plane.commands.context import EPCommandContext
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import ExecutionTarget
from execution_plane_client.commands.clusters import ClusterProvision, ClusterRegister
from execution_plane_client.errors import ProtocolConflict
from execution_plane_client.protocol import MessageEnvelope

pytestmark = pytest.mark.integration


@pytest.fixture(params=[1, 2], ids=["one-connection", "two-connections"])
async def provisioning_context(migrated_database, request):
    engine = create_async_engine(migrated_database, pool_size=request.param, max_overflow=0, pool_timeout=1)
    try:
        yield EPCommandContext(client_id=f"provision-{uuid4()}", engine=engine)
    finally:
        await engine.dispose()


def _command(command_type=ClusterRegister) -> ClusterRegister:
    return command_type(
        resource_id=uuid4(),
        name="provision-transaction",
        endpoint="https://cluster.test",
        credential_ref=uuid4(),
    )


@pytest.mark.parametrize(
    ("handler_type", "command_type"),
    [(ClusterRegisterCommandHandler, ClusterRegister), (ClusterProvisionCommandHandler, ClusterProvision)],
)
async def test_concurrent_provisioning_reuses_one_transaction(provisioning_context, handler_type, command_type):
    context = provisioning_context
    command = _command(command_type)
    handler = handler_type()
    async with asyncio.timeout(10):
        results = await asyncio.gather(
            *(handler.handle(Mock(spec=MessageEnvelope), command, context) for _ in range(3))
        )
    assert all(result.resource.resource_id == command.resource_id for result in results)
    sessions = async_sessionmaker(context.engine)
    async with sessions() as session:
        cluster = await session.get(Cluster, command.resource_id)
        assert cluster.status is ClusterStatus.ACTIVE
        targets = (
            await session.scalars(select(ExecutionTarget).where(col(ExecutionTarget.cluster_id) == cluster.id))
        ).all()
        assert len(targets) == 1
        assert targets[0].is_default


async def test_resource_lock_covers_target_creation(provisioning_context, migrated_database, monkeypatch):
    context = provisioning_context
    command = _command()
    digest = hashlib.sha256(f"cluster:{command.resource_id}".encode()).digest()
    lock_key = int.from_bytes(digest[:8], signed=True)
    observer = create_async_engine(migrated_database)
    create = ExecutionTargetStore.create

    async def assert_locked_then_create(store, **kwargs: object) -> ExecutionTarget:
        async with observer.connect() as connection:
            assert await connection.scalar(select(func.pg_try_advisory_xact_lock(lock_key))) is False
        return await create(store, **kwargs)

    monkeypatch.setattr(ExecutionTargetStore, "create", assert_locked_then_create)
    try:
        await ClusterRegisterCommandHandler().handle(Mock(spec=MessageEnvelope), command, context)
        async with observer.connect() as connection:
            assert await connection.scalar(select(func.pg_try_advisory_xact_lock(lock_key))) is True
    finally:
        await observer.dispose()


async def test_provisioning_rejects_a_different_owner(provisioning_context):
    context = provisioning_context
    command = _command()
    handler = ClusterRegisterCommandHandler()
    await handler.handle(Mock(spec=MessageEnvelope), command, context)
    other = EPCommandContext(client_id=f"other-{uuid4()}", engine=context.engine)
    with pytest.raises(ProtocolConflict):
        await handler.handle(Mock(spec=MessageEnvelope), command, other)


async def test_default_lock_precedes_cluster_update(provisioning_context, migrated_database, monkeypatch):
    context = provisioning_context
    command = _command()
    handler = ClusterRegisterCommandHandler()
    await handler.handle(Mock(spec=MessageEnvelope), command, context)
    observer = create_async_engine(migrated_database)
    update = ClusterStore.update

    async def assert_default_locked_then_update(store, *args: object, **kwargs: object) -> Cluster:
        async with observer.connect() as connection:
            assert await connection.scalar(select(func.pg_try_advisory_xact_lock(DEFAULT_ADVISORY_LOCK))) is False
        return await update(store, *args, **kwargs)

    monkeypatch.setattr(ClusterStore, "update", assert_default_locked_then_update)
    try:
        await handler.handle(Mock(spec=MessageEnvelope), command, context)
    finally:
        await observer.dispose()


@pytest.mark.parametrize("database_error", [False, True], ids=["activation-failure", "database-failure"])
async def test_target_failure_commits_error_state_without_partial_target(
    provisioning_context, monkeypatch, *, database_error
):
    context = provisioning_context
    command = _command()

    async def fail_activation(store, *_args: object) -> Never:
        if database_error:
            await store._session.execute(text("SELECT 1 / 0"))
        message = "activation failed"
        raise RuntimeError(message)

    monkeypatch.setattr(ExecutionTargetStore, "activate", fail_activation)
    with pytest.raises(DBAPIError if database_error else RuntimeError):
        await ClusterRegisterCommandHandler().handle(Mock(spec=MessageEnvelope), command, context)
    sessions = async_sessionmaker(context.engine)
    async with sessions() as session:
        cluster = await session.get(Cluster, command.resource_id)
        assert cluster.status is ClusterStatus.ERROR
        assert cluster.status_message == "default target creation failed"
        assert (
            await session.scalar(select(ExecutionTarget).where(col(ExecutionTarget.cluster_id) == cluster.id)) is None
        )


async def test_unexpected_provisioning_failure_rolls_back_changes(provisioning_context, monkeypatch):
    context = provisioning_context
    command = _command()

    async def fail_default(*_args: object) -> Never:
        message = "default promotion failed"
        raise RuntimeError(message)

    monkeypatch.setattr(ClusterStore, "ensure_default", fail_default)
    with pytest.raises(RuntimeError, match="default promotion failed"):
        await ClusterRegisterCommandHandler().handle(Mock(spec=MessageEnvelope), command, context)
    sessions = async_sessionmaker(context.engine)
    async with sessions() as session:
        assert await session.get(Cluster, command.resource_id) is None
        assert (
            await session.scalar(select(ExecutionTarget).where(col(ExecutionTarget.cluster_id) == command.resource_id))
            is None
        )
