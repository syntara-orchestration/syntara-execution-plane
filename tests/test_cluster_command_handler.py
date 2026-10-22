"""Cluster command handlers provision clusters directly using domain lifecycle operations."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Self
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from execution_plane.cluster.cluster_store import DefaultClusterError
from execution_plane.commands.clusters import (
    ClusterDeleteCommandHandler,
    ClusterProvisionCommandHandler,
    ClusterRegisterCommandHandler,
)
from execution_plane.commands.context import EPCommandContext
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane_client.commands.clusters import ClusterDelete, ClusterProvision, ClusterRegister
from execution_plane_client.errors import ProtocolConflict
from execution_plane_client.protocol import MessageEnvelope


def _active_cluster(*, is_default: bool = False) -> Cluster:
    now = datetime.now(UTC)
    return Cluster(
        id=uuid.uuid4(),
        name="cluster-a",
        endpoint="https://cluster.example",
        api_key="",
        status=ClusterStatus.ACTIVE,
        source_client_id="ao",
        source_integration_id=uuid.uuid4(),
        source_revision=1,
        is_default=is_default,
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


class _Session:
    """Minimal session stub for advisory lock and get operations."""

    def __init__(self, cluster: Cluster | None = None) -> None:
        self.cluster = cluster

    async def execute(self, _statement: object) -> None:
        return None

    async def get(self, model: object, _key: object, **_kwargs: object) -> Cluster | None:
        if model is Cluster:
            return self.cluster
        return None


class _Transaction:
    def __init__(self, session: _Session) -> None:
        self.session = session

    async def __aenter__(self) -> _Session:
        return self.session

    async def __aexit__(self, *_: object) -> None:
        return None


class _SessionMaker:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def __call__(self, *_: object, **_kwargs: object) -> Self:
        return self

    def begin(self) -> _Transaction:
        return _Transaction(self.session)


def _context(engine: AsyncEngine) -> EPCommandContext:
    return EPCommandContext(client_id="ao", engine=engine)


def _mock_context(monkeypatch: pytest.MonkeyPatch, session: _Session) -> EPCommandContext:
    engine = Mock(spec=AsyncEngine)
    monkeypatch.setattr(
        "execution_plane.commands.clusters.async_sessionmaker",
        lambda *_args, **_kwargs: _SessionMaker(session),
    )
    return _context(engine)


@pytest.mark.asyncio
async def test_cluster_register_cleanses_internal_labels(monkeypatch: pytest.MonkeyPatch) -> None:
    session = _Session()
    provisioned = _active_cluster()
    mock_registrar = AsyncMock()
    mock_registrar.provision = AsyncMock(return_value=provisioned)
    mock_store = AsyncMock()
    mock_store.get_by_source = AsyncMock(return_value=None)
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterStore.from_engine", lambda _e: mock_store)
    monkeypatch.setattr("execution_plane.commands.clusters.ExecutionTargetStore.from_engine", lambda _e: AsyncMock())
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterRegistrar", lambda *_a, **_k: mock_registrar)

    payload = ClusterRegister(
        resource_key=uuid.uuid4(),
        desired_revision=1,
        name="cluster-a",
        endpoint="https://cluster.example",
        namespace="ep",
        credential_ref=uuid.uuid4(),
        labels={"team": "platform", "_cluster_id": "spoofed", "_internal": "spoofed"},
    )

    context = _mock_context(monkeypatch, session)
    await ClusterRegisterCommandHandler().handle(Mock(spec=MessageEnvelope), payload, context)

    call_labels = mock_registrar.provision.call_args.args[5]
    assert "_cluster_id" not in call_labels
    assert "_internal" not in call_labels
    assert call_labels == {"team": "platform"}


@pytest.mark.asyncio
async def test_cluster_register_returns_cluster_id_in_receipt(monkeypatch: pytest.MonkeyPatch) -> None:
    session = _Session()
    cluster = _active_cluster()
    mock_registrar = AsyncMock()
    mock_registrar.provision = AsyncMock(return_value=cluster)
    mock_store = AsyncMock()
    mock_store.get_by_source = AsyncMock(return_value=None)
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterStore.from_engine", lambda _e: mock_store)
    monkeypatch.setattr("execution_plane.commands.clusters.ExecutionTargetStore.from_engine", lambda _e: AsyncMock())
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterRegistrar", lambda *_a, **_k: mock_registrar)

    payload = ClusterRegister(
        resource_key=uuid.uuid4(),
        desired_revision=1,
        name="cluster-a",
        endpoint="https://cluster.example",
        namespace="ep",
        credential_ref=uuid.uuid4(),
    )

    context = _mock_context(monkeypatch, session)
    result = await ClusterRegisterCommandHandler().handle(Mock(spec=MessageEnvelope), payload, context)

    assert result.resource is not None
    assert result.resource.kind == "cluster"
    assert result.resource.resource_id == cluster.id


@pytest.mark.asyncio
async def test_cluster_register_is_idempotent_for_same_revision(monkeypatch: pytest.MonkeyPatch) -> None:
    key = uuid.uuid4()
    cluster = _active_cluster()
    cluster.source_integration_id = key
    cluster.source_revision = 1
    session = _Session(cluster)
    mock_store = AsyncMock()
    mock_store.get_by_source = AsyncMock(return_value=cluster)
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterStore.from_engine", lambda _e: mock_store)
    monkeypatch.setattr("execution_plane.commands.clusters.ExecutionTargetStore.from_engine", lambda _e: AsyncMock())
    mock_registrar = AsyncMock()
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterRegistrar", lambda *_a, **_k: mock_registrar)

    payload = ClusterRegister(
        resource_key=key,
        desired_revision=1,
        name="cluster-a",
        endpoint="https://cluster.example",
        namespace="ep",
        credential_ref=uuid.uuid4(),
    )
    context = _mock_context(monkeypatch, session)
    result = await ClusterRegisterCommandHandler().handle(Mock(spec=MessageEnvelope), payload, context)

    mock_registrar.provision.assert_not_called()
    assert result.resource.resource_id == cluster.id


@pytest.mark.asyncio
async def test_cluster_register_rejects_stale_revision(monkeypatch: pytest.MonkeyPatch) -> None:
    key = uuid.uuid4()
    cluster = _active_cluster()
    cluster.source_integration_id = key
    cluster.source_revision = 3
    session = _Session(cluster)
    mock_store = AsyncMock()
    mock_store.get_by_source = AsyncMock(return_value=cluster)
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterStore.from_engine", lambda _e: mock_store)
    monkeypatch.setattr("execution_plane.commands.clusters.ExecutionTargetStore.from_engine", lambda _e: AsyncMock())
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterRegistrar", lambda *_a, **_k: AsyncMock())

    payload = ClusterRegister(
        resource_key=key,
        desired_revision=1,
        name="cluster-a",
        endpoint="https://cluster.example",
        namespace="ep",
        credential_ref=uuid.uuid4(),
    )
    context = _mock_context(monkeypatch, session)
    handler = ClusterRegisterCommandHandler()
    message = Mock(spec=MessageEnvelope)
    with pytest.raises(ProtocolConflict):
        await handler.handle(message, payload, context)


@pytest.mark.parametrize(
    ("payload_class", "handler_class"),
    [
        (ClusterRegister, ClusterRegisterCommandHandler),
        (ClusterProvision, ClusterProvisionCommandHandler),
    ],
)
@pytest.mark.asyncio
async def test_cluster_provision_passes_is_default_to_registrar(
    monkeypatch: pytest.MonkeyPatch,
    payload_class: type[ClusterRegister],
    handler_class: type[ClusterRegisterCommandHandler | ClusterProvisionCommandHandler],
) -> None:
    session = _Session()
    cluster = _active_cluster(is_default=True)
    mock_registrar = AsyncMock()
    mock_registrar.provision = AsyncMock(return_value=cluster)
    mock_store = AsyncMock()
    mock_store.get_by_source = AsyncMock(return_value=None)
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterStore.from_engine", lambda _e: mock_store)
    monkeypatch.setattr("execution_plane.commands.clusters.ExecutionTargetStore.from_engine", lambda _e: AsyncMock())
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterRegistrar", lambda *_a, **_k: mock_registrar)

    payload = payload_class(
        resource_key=uuid.uuid4(),
        desired_revision=1,
        name="cluster-a",
        endpoint="https://cluster.example",
        namespace="ep",
        credential_ref=uuid.uuid4(),
        is_default=True,
    )

    context = _mock_context(monkeypatch, session)
    await handler_class().handle(Mock(spec=MessageEnvelope), payload, context)

    assert mock_registrar.provision.call_args.kwargs["is_default"] is True


@pytest.mark.asyncio
async def test_cluster_delete_rejects_the_current_default(monkeypatch: pytest.MonkeyPatch) -> None:
    key = uuid.uuid4()
    cluster = _active_cluster(is_default=True)
    cluster.source_integration_id = key
    cluster.source_revision = 1
    session = _Session(cluster)
    mock_store = AsyncMock()
    mock_store.get_by_source = AsyncMock(return_value=cluster)
    mock_store.request_delete = AsyncMock(side_effect=DefaultClusterError)
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterStore.from_engine", lambda _e: mock_store)
    monkeypatch.setattr("execution_plane.commands.clusters.ExecutionTargetStore.from_engine", lambda _e: AsyncMock())
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterRegistrar", lambda *_a, **_k: AsyncMock())

    payload = ClusterDelete(resource_key=key, desired_revision=2)
    context = _mock_context(monkeypatch, session)

    handler = ClusterDeleteCommandHandler()
    message = Mock(spec=MessageEnvelope)
    with pytest.raises(DefaultClusterError):
        await handler.handle(message, payload, context)


@pytest.mark.asyncio
async def test_cluster_delete_raises_not_found_when_cluster_is_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    from execution_plane.commands.clusters import ResourceNotFoundError

    session = _Session()
    mock_store = AsyncMock()
    mock_store.get_by_source = AsyncMock(return_value=None)
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterStore.from_engine", lambda _e: mock_store)
    monkeypatch.setattr("execution_plane.commands.clusters.ExecutionTargetStore.from_engine", lambda _e: AsyncMock())
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterRegistrar", lambda *_a, **_k: AsyncMock())

    payload = ClusterDelete(resource_key=uuid.uuid4(), desired_revision=1)
    context = _mock_context(monkeypatch, session)

    handler = ClusterDeleteCommandHandler()
    message = Mock(spec=MessageEnvelope)
    with pytest.raises(ResourceNotFoundError):
        await handler.handle(message, payload, context)


@pytest.mark.asyncio
async def test_cluster_delete_rejects_stale_revision(monkeypatch: pytest.MonkeyPatch) -> None:
    key = uuid.uuid4()
    cluster = _active_cluster()
    cluster.source_integration_id = key
    cluster.source_revision = 5
    session = _Session(cluster)
    mock_store = AsyncMock()
    mock_store.get_by_source = AsyncMock(return_value=cluster)
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterStore.from_engine", lambda _e: mock_store)
    monkeypatch.setattr("execution_plane.commands.clusters.ExecutionTargetStore.from_engine", lambda _e: AsyncMock())
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterRegistrar", lambda *_a, **_k: AsyncMock())

    payload = ClusterDelete(resource_key=key, desired_revision=3)
    context = _mock_context(monkeypatch, session)

    handler = ClusterDeleteCommandHandler()
    message = Mock(spec=MessageEnvelope)
    with pytest.raises(ProtocolConflict):
        await handler.handle(message, payload, context)


@pytest.mark.asyncio
async def test_cluster_delete_is_idempotent_for_same_revision(monkeypatch: pytest.MonkeyPatch) -> None:
    key = uuid.uuid4()
    cluster = _active_cluster()
    cluster.source_integration_id = key
    cluster.source_revision = 2
    session = _Session(cluster)
    mock_store = AsyncMock()
    mock_store.get_by_source = AsyncMock(return_value=cluster)
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterStore.from_engine", lambda _e: mock_store)
    monkeypatch.setattr("execution_plane.commands.clusters.ExecutionTargetStore.from_engine", lambda _e: AsyncMock())
    monkeypatch.setattr("execution_plane.commands.clusters.ClusterRegistrar", lambda *_a, **_k: AsyncMock())

    payload = ClusterDelete(resource_key=key, desired_revision=2)
    context = _mock_context(monkeypatch, session)

    result = await ClusterDeleteCommandHandler().handle(Mock(spec=MessageEnvelope), payload, context)

    mock_store.request_delete.assert_not_called()
    assert result.resource.resource_id == cluster.id
