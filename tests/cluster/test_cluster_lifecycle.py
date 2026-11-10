"""Tests for Cluster persistence and registration orchestration."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Self

import pytest

from execution_plane.models.cluster import Cluster, ClusterStatus, ClusterType
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane_client.models.execution_target import KubernetesPlacement

_DATABASE_UNAVAILABLE = "database unavailable"


def _cluster(*, status: ClusterStatus = ClusterStatus.REGISTERING, is_default: bool = False) -> Cluster:
    now = datetime.now(UTC)
    return Cluster(
        id=uuid.uuid4(),
        name="cluster-a",
        endpoint="https://cluster.example",
        credential_ref=uuid.uuid4(),
        status=status,
        is_default=is_default,
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


def _target(*, cluster_id: uuid.UUID | None = None, is_default: bool = False) -> ExecutionTarget:
    now = datetime.now(UTC)
    return ExecutionTarget(
        id=uuid.uuid4(),
        cluster_id=cluster_id or uuid.uuid4(),
        name="target-a",
        endpoint="https://target.example",
        credential_ref=uuid.uuid4(),
        backend_type=BackendType.KUBERNETES,
        is_default=is_default,
        status=TargetStatus.ACTIVE,
        placement=KubernetesPlacement(
            namespace="execution",
            node_selectors=["kubernetes.io/os=linux"],
            tolerations=["dedicated=execution:NoSchedule"],
        ),
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


class _Result:
    def __init__(self, cluster: Cluster | None = None, targets: list[ExecutionTarget] | None = None) -> None:
        self.cluster = cluster
        self.targets = targets or []

    def scalar_one_or_none(self) -> Cluster | ExecutionTarget | None:
        if self.cluster is not None:
            return self.cluster
        return self.targets[0] if self.targets else None

    def scalars(self) -> Self:
        return self

    def all(self) -> list[ExecutionTarget]:
        return self.targets


class _Session:
    def __init__(
        self, *, cluster: Cluster | None = None, targets: list[ExecutionTarget] | None = None, fail_commit: bool = False
    ) -> None:
        self.cluster = cluster
        self.targets = targets or []
        self.fail_commit = fail_commit
        self.added: Cluster | None = None
        self.deleted: Cluster | None = None
        self.commits = 0
        self.rollbacks = 0

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def add(self, cluster: Cluster) -> None:
        self.added = cluster

    async def get(self, _model: object, _cluster_id: uuid.UUID, **_: object) -> Cluster | None:
        return self.cluster if self.cluster is not None else self.added

    async def execute(self, _statement: object) -> _Result:
        return _Result(self.cluster, self.targets)

    async def commit(self) -> None:
        self.commits += 1
        if self.fail_commit:
            raise RuntimeError(_DATABASE_UNAVAILABLE)

    async def rollback(self) -> None:
        self.rollbacks += 1

    async def delete(self, cluster: Cluster) -> None:
        self.deleted = cluster


class _SessionFactory:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def __call__(self) -> _Session:
        return self.session


@pytest.mark.asyncio
async def test_store_creates_a_registering_cluster_and_owns_commit() -> None:
    from execution_plane.cluster.cluster_store import ClusterStore

    store = ClusterStore("postgresql+asyncpg://localhost/syntara")
    session = _Session()
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]
    actor_id = uuid.uuid4()
    credential_ref = uuid.uuid4()

    cluster = await store.create(
        uuid.uuid4(), "cluster-a", "https://cluster.example", credential_ref, actor_id, {"region": "eu-west"}
    )

    assert cluster.status is ClusterStatus.REGISTERING
    assert cluster.enabled is True
    assert cluster.cluster_type is ClusterType.OPENSHIFT
    assert cluster.credential_ref == uuid.UUID(int=0)
    assert session.added is not None
    assert session.added.credential_ref == credential_ref
    assert session.added.cluster_type is ClusterType.OPENSHIFT
    assert session.added.created_by == actor_id
    assert session.commits == 1
    await store.close()


@pytest.mark.asyncio
async def test_store_persists_an_explicit_cluster_type() -> None:
    from execution_plane.cluster.cluster_store import ClusterStore

    store = ClusterStore("postgresql+asyncpg://localhost/syntara")
    session = _Session()
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    cluster = await store.create(
        uuid.uuid4(),
        "cluster-a",
        "https://cluster.example",
        uuid.uuid4(),
        uuid.uuid4(),
        cluster_type=ClusterType.RHEL,
    )

    assert cluster.cluster_type is ClusterType.RHEL
    assert session.added is not None
    assert session.added.cluster_type is ClusterType.RHEL
    await store.close()


@pytest.mark.asyncio
async def test_store_rolls_back_failed_cluster_creation() -> None:
    from execution_plane.cluster.cluster_store import ClusterStore

    store = ClusterStore("postgresql+asyncpg://localhost/syntara")
    session = _Session(fail_commit=True)
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    with pytest.raises(RuntimeError, match="database unavailable"):
        await store.create(uuid.uuid4(), "cluster-a", "https://cluster.example", uuid.uuid4(), uuid.uuid4())

    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_store_request_delete_drains_cluster_and_its_targets() -> None:
    from execution_plane.cluster.cluster_store import ClusterStore

    cluster = _cluster(status=ClusterStatus.ACTIVE)
    targets = [_target(cluster_id=cluster.id), _target(cluster_id=cluster.id)]
    store = ClusterStore("postgresql+asyncpg://localhost/syntara")
    session = _Session(cluster=cluster, targets=targets)
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]
    actor_id = uuid.uuid4()

    result = await store.request_delete(cluster.id, actor_id)

    assert result.status is ClusterStatus.DRAINING
    assert result.enabled is False
    assert all(target.status is TargetStatus.DRAINING and not target.enabled for target in targets)
    assert all(target.updated_by == actor_id for target in targets)
    await store.close()


@pytest.mark.asyncio
async def test_store_ignores_cluster_finalization_until_targets_are_removed() -> None:
    from execution_plane.cluster.cluster_store import ClusterStore

    cluster = _cluster(status=ClusterStatus.DRAINING)
    cluster.enabled = False
    store = ClusterStore("postgresql+asyncpg://localhost/syntara")
    session = _Session(cluster=cluster, targets=[_target(cluster_id=cluster.id)])
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    await store.finalize_delete(cluster.id)

    assert session.deleted is None
    await store.close()


@pytest.mark.asyncio
async def test_store_ignores_cluster_finalization_before_draining() -> None:
    from execution_plane.cluster.cluster_store import ClusterStore

    cluster = _cluster(status=ClusterStatus.ACTIVE)
    store = ClusterStore("postgresql+asyncpg://localhost/syntara")
    session = _Session(cluster=cluster)
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    await store.finalize_delete(cluster.id)

    assert session.deleted is None
    await store.close()
