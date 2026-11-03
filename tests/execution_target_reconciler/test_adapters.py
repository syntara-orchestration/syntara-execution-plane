"""Registry-aware snapshot adapters."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Self, cast

import pytest

from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.execution_target_reconciler.adapters import (
    ExecutionTargetSnapshotSourceAdapter,
    build_placement_resolver,
    cluster_snapshot_from_model,
    execution_target_snapshot_from_model,
    store_backed_snapshot_source,
)
from execution_plane.execution_target_reconciler.placement import PlacementResolver
from execution_plane.execution_target_reconciler.protocols import ExecutionTargetSnapshotSource
from execution_plane.execution_target_reconciler.types import WorkRequirements
from execution_plane.models.cluster import Cluster, ClusterStatus, ClusterType
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane_client.models.execution_target import KubernetesPlacement

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


def _cluster(
    *,
    status: ClusterStatus = ClusterStatus.ACTIVE,
    enabled: bool = True,
    labels: dict[str, str] | None = None,
    cluster_type: ClusterType = ClusterType.OPENSHIFT,
    is_default: bool = True,
) -> Cluster:
    now = datetime.now(UTC)
    return Cluster(
        id=uuid.uuid4(),
        name="local-openshift",
        endpoint="https://cluster.example",
        credential_ref=uuid.uuid4(),
        status=status,
        enabled=enabled,
        is_default=is_default,
        cluster_type=cluster_type,
        labels=labels or {"region": "us-east-1"},
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


def _target(
    cluster: Cluster,
    *,
    name: str = "ep-default",
    is_default: bool = True,
) -> ExecutionTarget:
    now = datetime.now(UTC)
    return ExecutionTarget(
        id=uuid.uuid4(),
        cluster_id=cluster.id,
        name=name,
        backend_type=BackendType.KUBERNETES,
        endpoint="https://target.example",
        placement=KubernetesPlacement(namespace="ao-execution", node_selectors=["k=v"]),
        credential_ref=uuid.uuid4(),
        is_default=is_default,
        status=TargetStatus.ACTIVE,
        labels={"env": "production"},
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


class _Result:
    def __init__(self, rows: list[Cluster] | list[ExecutionTarget]) -> None:
        self._rows = rows

    def scalars(self) -> Self:
        return self

    def all(self) -> list[Cluster] | list[ExecutionTarget]:
        return self._rows


class _Session:
    def __init__(self, rows: list[Cluster] | list[ExecutionTarget]) -> None:
        self._result = _Result(rows)

    async def execute(self, _statement: object) -> _Result:
        return self._result


def _cluster_store(clusters: list[Cluster]) -> ClusterStore:
    return ClusterStore.from_session(cast("AsyncSession", _Session(clusters)))


def _target_store(targets: list[ExecutionTarget]) -> ExecutionTargetStore:
    return ExecutionTargetStore.from_session(cast("AsyncSession", _Session(targets)))


def test_cluster_snapshot_maps_non_active_to_disabled() -> None:
    model = _cluster(status=ClusterStatus.DRAINING, enabled=True)
    model.is_default = True
    snapshot = cluster_snapshot_from_model(model)
    assert snapshot.enabled is False
    assert snapshot.is_default is True
    assert snapshot.cluster_type is ClusterType.OPENSHIFT
    assert snapshot.labels == {"region": "us-east-1"}


def test_cluster_snapshot_uses_persisted_cluster_type() -> None:
    snapshot = cluster_snapshot_from_model(_cluster(cluster_type=ClusterType.RHEL))
    assert snapshot.cluster_type is ClusterType.RHEL


def test_execution_target_snapshot_shares_cluster_and_maps_enums() -> None:
    cluster = _cluster()
    snapshot = cluster_snapshot_from_model(cluster)
    target = execution_target_snapshot_from_model(_target(cluster), snapshot)
    assert target.cluster is snapshot
    assert target.backend_type is BackendType.KUBERNETES
    assert target.lifecycle == TargetStatus.ACTIVE.value
    assert target.labels == {"env": "production"}
    assert target.is_default is True


def test_snapshot_source_adapter_inherits_snapshot_source_protocol() -> None:
    assert ExecutionTargetSnapshotSource in ExecutionTargetSnapshotSourceAdapter.__mro__


@pytest.mark.asyncio
async def test_store_backed_snapshot_source_interns_cluster_snapshots() -> None:
    cluster = _cluster()
    first = _target(cluster)
    second = _target(cluster, name="ep-gpu", is_default=False)
    source = store_backed_snapshot_source(_cluster_store([cluster]), _target_store([first, second]))

    listed = await source.list()

    assert len(listed) == 2
    assert listed[0].cluster is listed[1].cluster
    assert isinstance(listed[0].placement, KubernetesPlacement)
    assert listed[0].placement.namespace == "ao-execution"
    assert listed[0].placement.node_selectors == ["k=v"]


@pytest.mark.asyncio
async def test_store_backed_snapshot_source_omits_targets_without_a_cluster() -> None:
    cluster = _cluster()
    orphan = _target(_cluster())
    source = store_backed_snapshot_source(_cluster_store([cluster]), _target_store([orphan]))

    assert await source.list() == ()


@pytest.mark.asyncio
async def test_build_placement_resolver_returns_placement_facade() -> None:
    cluster = _cluster()
    resolver = build_placement_resolver(_cluster_store([cluster]), _target_store([_target(cluster)]))
    assert isinstance(resolver, PlacementResolver)
    result = await resolver.resolve(WorkRequirements())
    assert result.available_targets[0].name == "ep-default"
