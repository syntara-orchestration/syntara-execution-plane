"""Execution-target command orchestration for default reassignment."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Self
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from execution_plane.commands.context import EPCommandContext
from execution_plane.commands.targets import (
    TargetDeleteCommandHandler,
    TargetRegisterCommandHandler,
    TargetUpdateCommandHandler,
)
from execution_plane.execution_target.execution_target_store import (
    DefaultExecutionTargetError,
    ExecutionTargetStore,
)
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane.models.execution_target_placement import ExecutionTargetPlacement, KubernetesPlacement
from execution_plane_client.commands.targets import (
    KubernetesPlacement as ClientKubernetesPlacement,
)
from execution_plane_client.commands.targets import (
    TargetDelete,
    TargetRegister,
    TargetUpdate,
)
from execution_plane_client.protocol import MessageEnvelope


def _cluster() -> Cluster:
    now = datetime.now(UTC)
    return Cluster(
        id=uuid4(),
        name="cluster-a",
        endpoint="https://cluster.example",
        credential_ref=uuid4(),
        status=ClusterStatus.ACTIVE,
        enabled=True,
        source_client_id="ao",
        created_by=uuid4(),
        updated_by=uuid4(),
        created_at=now,
        updated_at=now,
    )


def _target(*, cluster_id: UUID, is_default: bool = False) -> ExecutionTarget:
    now = datetime.now(UTC)
    return ExecutionTarget(
        id=uuid4(),
        cluster_id=cluster_id,
        name="target-a",
        backend_type=BackendType.VANILLA_K8S,
        endpoint="https://target.example",
        placement=KubernetesPlacement(namespace="execution"),
        credential_ref=uuid4(),
        is_default=is_default,
        status=TargetStatus.ACTIVE,
        enabled=True,
        created_by=uuid4(),
        updated_by=uuid4(),
        created_at=now,
        updated_at=now,
    )


class _FakeStore:
    def __init__(self, target: ExecutionTarget | None = None) -> None:
        self.target = target
        self.created: dict[str, object] | None = None
        self.activated: list[UUID] = []
        self.updated: list[dict[str, object]] = []
        self.defaults_set: list[UUID] = []
        self.delete_requested: list[UUID] = []

    async def create(
        self,
        *,
        cluster_id: UUID,
        name: str,
        backend_type: BackendType,
        endpoint: str,
        credential_ref: UUID,
        is_default: bool,
        created_by: UUID,
        placement: ExecutionTargetPlacement,
        labels: dict[str, str],
        target_id: UUID,
    ) -> ExecutionTarget:
        self.created = {
            "cluster_id": cluster_id,
            "name": name,
            "backend_type": backend_type,
            "endpoint": endpoint,
            "credential_ref": credential_ref,
            "is_default": is_default,
            "created_by": created_by,
            "placement": placement,
            "labels": labels,
            "target_id": target_id,
        }
        created_target = self.target or _target(cluster_id=cluster_id)
        self.target = created_target
        return created_target

    async def activate(self, target_id: UUID, updated_by: UUID) -> ExecutionTarget:
        self.activated.append(target_id)
        return self.target or _target(cluster_id=uuid4())

    async def update(self, target_id: UUID, **kwargs: object) -> ExecutionTarget:
        self.updated.append({"target_id": target_id, **kwargs})
        assert self.target is not None
        return self.target

    async def set_default(self, target_id: UUID, updated_by: UUID) -> ExecutionTarget:
        self.defaults_set.append(target_id)
        assert self.target is not None
        return self.target

    async def request_delete(self, target_id: UUID, updated_by: UUID) -> ExecutionTarget:
        self.delete_requested.append(target_id)
        assert self.target is not None
        return self.target


class _Session:
    def __init__(self, *, cluster: Cluster, target: ExecutionTarget | None = None) -> None:
        self.cluster = cluster
        self.target = target

    async def get(
        self,
        model: type[object],
        resource_id: UUID,
        **_kwargs: object,
    ) -> Cluster | ExecutionTarget | None:
        del resource_id
        if model is Cluster:
            return self.cluster
        return self.target


class _Transaction:
    def __init__(self, session: _Session) -> None:
        self.session = session

    async def __aenter__(self) -> _Session:
        return self.session

    async def __aexit__(self, *_exc_info: object) -> None:
        return None


class _SessionMaker:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def __call__(self, *_args: object, **_kwargs: object) -> Self:
        return self

    def begin(self) -> _Transaction:
        return _Transaction(self.session)


def _context(monkeypatch: pytest.MonkeyPatch, session: _Session, store: _FakeStore) -> EPCommandContext:
    monkeypatch.setattr(
        "execution_plane.commands.targets.async_sessionmaker",
        lambda *_args, **_kwargs: _SessionMaker(session),
    )
    monkeypatch.setattr(
        ExecutionTargetStore,
        "from_transaction",
        classmethod(lambda _cls, _session: store),
    )
    return EPCommandContext(client_id="ao", engine=Mock(spec=AsyncEngine))


@pytest.mark.asyncio
async def test_target_register_promotes_the_new_target_as_default(monkeypatch: pytest.MonkeyPatch) -> None:
    cluster = _cluster()
    target_id = uuid4()
    session = _Session(cluster=cluster)
    store = _FakeStore()
    payload = TargetRegister(
        resource_id=target_id,
        cluster_id=cluster.id,
        name="workers",
        backend_type="vanilla_k8s",
        endpoint="https://target.example",
        credential_ref=uuid4(),
        placement=ClientKubernetesPlacement(namespace="execution"),
        is_default=True,
    )

    await TargetRegisterCommandHandler().handle(
        Mock(spec=MessageEnvelope),
        payload,
        _context(monkeypatch, session, store),
    )

    assert store.created is not None
    assert store.created["is_default"] is False
    assert store.activated == [target_id]
    assert store.defaults_set == [target_id]


@pytest.mark.asyncio
async def test_target_update_promotes_the_target_as_default(monkeypatch: pytest.MonkeyPatch) -> None:
    cluster = _cluster()
    target = _target(cluster_id=cluster.id)
    session = _Session(cluster=cluster, target=target)
    store = _FakeStore(target=target)
    payload = TargetUpdate(resource_id=target.id, name="workers", is_default=True)

    await TargetUpdateCommandHandler().handle(
        Mock(spec=MessageEnvelope),
        payload,
        _context(monkeypatch, session, store),
    )

    assert store.updated[0]["name"] == "workers"
    assert store.defaults_set == [target.id]


@pytest.mark.asyncio
async def test_target_update_allows_metadata_changes_to_the_current_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cluster = _cluster()
    target = _target(cluster_id=cluster.id, is_default=True)
    session = _Session(cluster=cluster, target=target)
    store = _FakeStore(target=target)

    await TargetUpdateCommandHandler().handle(
        Mock(spec=MessageEnvelope),
        TargetUpdate(resource_id=target.id, name="renamed"),
        _context(monkeypatch, session, store),
    )

    assert store.updated[0]["name"] == "renamed"
    assert store.defaults_set == []


@pytest.mark.asyncio
async def test_target_update_passes_enabled_changes_to_the_store(monkeypatch: pytest.MonkeyPatch) -> None:
    cluster = _cluster()
    target = _target(cluster_id=cluster.id)
    session = _Session(cluster=cluster, target=target)
    store = _FakeStore(target=target)

    await TargetUpdateCommandHandler().handle(
        Mock(spec=MessageEnvelope),
        TargetUpdate(resource_id=target.id, enabled=False),
        _context(monkeypatch, session, store),
    )

    assert store.updated[0]["enabled"] is False


@pytest.mark.asyncio
async def test_target_update_rejects_demoting_the_current_default(monkeypatch: pytest.MonkeyPatch) -> None:
    cluster = _cluster()
    target = _target(cluster_id=cluster.id, is_default=True)
    session = _Session(cluster=cluster, target=target)
    store = _FakeStore(target=target)

    with pytest.raises(DefaultExecutionTargetError):
        await TargetUpdateCommandHandler().handle(
            Mock(spec=MessageEnvelope),
            TargetUpdate(resource_id=target.id, is_default=False),
            _context(monkeypatch, session, store),
        )

    assert store.updated == []
    assert store.defaults_set == []


@pytest.mark.asyncio
async def test_target_delete_rejects_the_current_default(monkeypatch: pytest.MonkeyPatch) -> None:
    cluster = _cluster()
    target = _target(cluster_id=cluster.id, is_default=True)
    session = _Session(cluster=cluster, target=target)
    store = _FakeStore(target=target)

    with pytest.raises(DefaultExecutionTargetError):
        await TargetDeleteCommandHandler().handle(
            Mock(spec=MessageEnvelope),
            TargetDelete(resource_id=target.id),
            _context(monkeypatch, session, store),
        )

    assert store.delete_requested == []
