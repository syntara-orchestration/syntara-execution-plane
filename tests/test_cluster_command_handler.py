"""Cluster registration owns one transaction and never manages targets."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from execution_plane.cluster.cluster_store import ClusterStore, DefaultClusterError
from execution_plane.commands.clusters import (
    ClusterDeleteCommandHandler,
    ClusterRegisterCommandHandler,
    ResourceNotFoundError,
)
from execution_plane.commands.context import EPCommandContext
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane_client.commands.clusters import ClusterDelete, ClusterRegister
from execution_plane_client.errors import ProtocolConflict
from execution_plane_client.protocol import MessageEnvelope


def _cluster(*, status: ClusterStatus, is_default: bool = False) -> Cluster:
    now = datetime.now(UTC)
    return Cluster(
        id=uuid4(),
        name="cluster-a",
        endpoint="https://cluster.example",
        credential_ref=uuid4(),
        source_client_id="ao",
        created_by=uuid4(),
        created_at=now,
        updated_by=uuid4(),
        updated_at=now,
        status=status,
        is_default=is_default,
    )


def _registration(monkeypatch, existing=None) -> tuple[EPCommandContext, AsyncMock, AsyncMock, Mock]:
    session = AsyncMock()
    session.get.return_value = existing
    transaction = AsyncMock()
    transaction.__aenter__.return_value = session
    sessions = Mock()
    sessions.begin.return_value = transaction
    monkeypatch.setattr("execution_plane.commands.clusters.async_sessionmaker", Mock(return_value=sessions))
    store = AsyncMock(spec=ClusterStore)
    cluster = existing or _cluster(status=ClusterStatus.ACTIVE)
    store.update.return_value = cluster
    store.ensure_default.return_value = cluster
    store.set_default.return_value = cluster
    borrowed = Mock(return_value=store)
    monkeypatch.setattr(ClusterStore, "from_transaction", borrowed)
    # Registration must not acquire a second session via an engine-backed store.
    monkeypatch.setattr(ClusterStore, "from_engine", Mock(side_effect=AssertionError("nested acquisition")))
    context = EPCommandContext(client_id="ao", engine=Mock(spec=AsyncEngine))
    return context, store, session, borrowed


async def test_register_borrows_transaction_and_cleanses_labels(monkeypatch):
    context, store, session, borrowed = _registration(monkeypatch)
    command = ClusterRegister(
        resource_id=uuid4(),
        name="cluster-a",
        endpoint="https://cluster.example",
        credential_ref=uuid4(),
        labels={"team": "platform", "_cluster_id": "spoofed", "_internal": "spoofed"},
    )
    result = await ClusterRegisterCommandHandler().handle(Mock(spec=MessageEnvelope), command, context)
    borrowed.assert_called_once_with(session)
    assert store.create.call_args.args[5] == {"team": "platform"}
    assert store.update.call_args.kwargs["labels"] == {"team": "platform"}
    store.activate.assert_awaited_once()
    assert result.resource.kind == "cluster"
    assert result.resource.resource_id == store.update.return_value.id


@pytest.mark.parametrize("status", [ClusterStatus.ACTIVE, ClusterStatus.DRAINING, ClusterStatus.ERROR])
async def test_register_updates_owned_cluster_and_reactivates_when_needed(monkeypatch, status):
    cluster = _cluster(status=status)
    context, store, _, _ = _registration(monkeypatch, cluster)
    command = ClusterRegister(
        resource_id=cluster.id,
        name="renamed",
        endpoint=cluster.endpoint,
        credential_ref=uuid4(),
    )
    await ClusterRegisterCommandHandler().handle(Mock(spec=MessageEnvelope), command, context)
    store.create.assert_not_awaited()
    assert store.update.call_args.kwargs["name"] == "renamed"
    assert store.reactivate.await_count == (status is not ClusterStatus.ACTIVE)


async def test_register_rejects_another_clients_cluster(monkeypatch):
    cluster = _cluster(status=ClusterStatus.ACTIVE)
    cluster.source_client_id = "other-client"
    context, store, _, borrowed = _registration(monkeypatch, cluster)
    command = ClusterRegister(
        resource_id=cluster.id,
        name=cluster.name,
        endpoint=cluster.endpoint,
        credential_ref=uuid4(),
    )
    with pytest.raises(ProtocolConflict):
        await ClusterRegisterCommandHandler().handle(Mock(spec=MessageEnvelope), command, context)
    borrowed.assert_not_called()
    store.update.assert_not_awaited()


@pytest.mark.parametrize("is_default", [False, True])
async def test_register_selects_default_explicitly_or_bootstraps_it(monkeypatch, is_default):
    context, store, _, _ = _registration(monkeypatch)
    command = ClusterRegister(
        resource_id=uuid4(),
        name="cluster",
        endpoint="https://cluster.test",
        credential_ref=uuid4(),
        is_default=is_default,
    )
    await ClusterRegisterCommandHandler().handle(Mock(spec=MessageEnvelope), command, context)
    assert store.set_default.await_count == is_default
    assert store.ensure_default.await_count == (not is_default)


async def test_register_disabled_cluster_does_not_make_it_default(monkeypatch):
    context, store, _, _ = _registration(monkeypatch)
    command = ClusterRegister(
        resource_id=uuid4(),
        name="cluster",
        endpoint="https://cluster.test",
        credential_ref=uuid4(),
        enabled=False,
    )
    await ClusterRegisterCommandHandler().handle(Mock(spec=MessageEnvelope), command, context)
    assert store.update.call_args.kwargs["enabled"] is False
    store.ensure_default.assert_not_awaited()


async def test_register_cannot_disable_current_default(monkeypatch):
    cluster = _cluster(status=ClusterStatus.ACTIVE, is_default=True)
    context, store, _, _ = _registration(monkeypatch, cluster)
    command = ClusterRegister(
        resource_id=cluster.id,
        name=cluster.name,
        endpoint=cluster.endpoint,
        credential_ref=uuid4(),
        enabled=False,
    )
    with pytest.raises(DefaultClusterError):
        await ClusterRegisterCommandHandler().handle(Mock(spec=MessageEnvelope), command, context)
    store.update.assert_not_awaited()


@pytest.mark.parametrize("outcome", ["missing", "default", "owned"])
async def test_cluster_delete_checks_ownership_and_default(monkeypatch, outcome):
    cluster = _cluster(status=ClusterStatus.ACTIVE, is_default=outcome == "default")
    store = AsyncMock(spec=ClusterStore)
    store.get_for_client.return_value = None if outcome == "missing" else cluster
    store.request_delete.return_value = cluster
    if outcome == "default":
        store.request_delete.side_effect = DefaultClusterError
    monkeypatch.setattr(ClusterStore, "from_engine", Mock(return_value=store))
    context = EPCommandContext(client_id="ao", engine=Mock(spec=AsyncEngine))
    handler = ClusterDeleteCommandHandler()
    command = ClusterDelete(resource_id=cluster.id)
    if outcome == "owned":
        result = await handler.handle(Mock(spec=MessageEnvelope), command, context)
        assert result.resource.resource_id == cluster.id
    else:
        with pytest.raises(ResourceNotFoundError if outcome == "missing" else DefaultClusterError):
            await handler.handle(Mock(spec=MessageEnvelope), command, context)
