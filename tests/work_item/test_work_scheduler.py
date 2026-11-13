"""WorkScheduler placement use cases."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from execution_plane.execution_target_reconciler.types import (
    ClusterSnapshot,
    ExecutionTargetSnapshot,
    ReconcileResult,
    ResolveOutcome,
    WorkRequirements,
)
from execution_plane.models.cluster import Cluster, ClusterStatus, ClusterType
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.work_item.work_scheduler import WorkScheduler
from execution_plane_client.models.execution_target import KubernetesPlacement

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


def _cluster() -> Cluster:
    now = datetime.now(UTC)
    return Cluster(
        id=uuid4(),
        name="local-cluster",
        endpoint="https://cluster.example",
        credential_ref=uuid4(),
        status=ClusterStatus.ACTIVE,
        enabled=True,
        cluster_type=ClusterType.OPENSHIFT,
        created_by=uuid4(),
        created_at=now,
        updated_by=uuid4(),
        updated_at=now,
    )


def _target(cluster: Cluster) -> ExecutionTarget:
    now = datetime.now(UTC)
    return ExecutionTarget(
        id=uuid4(),
        cluster_id=cluster.id,
        name="ep-default",
        backend_type=BackendType.KUBERNETES,
        endpoint="https://target.example",
        placement=KubernetesPlacement(namespace="ao-execution"),
        credential_ref=uuid4(),
        status=TargetStatus.ACTIVE,
        enabled=True,
        is_default=True,
        created_by=uuid4(),
        created_at=now,
        updated_by=uuid4(),
        updated_at=now,
    )


def _reserved_item() -> WorkItem:
    return WorkItem(
        id=uuid4(),
        client_id="test-client",
        status=WorkItemStatus.CLAIMED,
        payload={"selectors": {"env": "production"}},
        claim_owner_id=uuid4(),
        claim_generation=7,
        created_at=datetime.now(UTC),
    )


def _snapshot(cluster: Cluster, target: ExecutionTarget) -> ExecutionTargetSnapshot:
    cluster_snapshot = ClusterSnapshot(
        id=cluster.id,
        name=cluster.name,
        labels=dict(cluster.labels),
        cluster_type=cluster.cluster_type,
        enabled=cluster.enabled,
    )
    return ExecutionTargetSnapshot(
        id=target.id,
        cluster=cluster_snapshot,
        name=target.name,
        placement=target.placement,
        backend_type=target.backend_type,
        labels=dict(target.labels),
        lifecycle=target.status.value,
        enabled=target.enabled,
        is_default=target.is_default,
    )


class _PlacementResolver:
    def __init__(self, result: ReconcileResult, *, eligible: bool = True) -> None:
        self.result = result
        self.eligible = eligible
        self.requirements: list[WorkRequirements] = []

    async def resolve(self, requirements: WorkRequirements) -> ReconcileResult:
        self.requirements.append(requirements)
        return self.result

    async def target_is_eligible(
        self,
        cluster: ClusterSnapshot,
        target: ExecutionTargetSnapshot,
        requirements: WorkRequirements,
    ) -> bool:
        return self.eligible


class _WorkStore:
    def __init__(
        self,
        item: WorkItem,
        targets: dict[UUID, tuple[Cluster, ExecutionTarget]],
        *,
        assigned_target_id: UUID | None = None,
    ) -> None:
        self.item = item
        self.targets = targets
        self.assigned_target_id = assigned_target_id
        self.results: list[tuple[dict[str, Any], WorkItemStatus]] = []
        self.assigned_target_ids: list[UUID] = []
        self.released_claims: list[tuple[UUID, int]] = []

    async def reserve_one(self) -> WorkItem | None:
        return self.item

    async def set_result(
        self,
        item_id: UUID,
        result: dict[str, Any],
        status: WorkItemStatus,
        *,
        claim_owner_id: UUID,
        claim_generation: int,
    ) -> WorkItem:
        assert item_id == self.item.id
        assert claim_owner_id == self.item.claim_owner_id
        assert claim_generation == self.item.claim_generation
        self.results.append((result, status))
        return self.item

    async def assign_target(
        self,
        item_id: UUID,
        target_id: UUID,
        *,
        claim_owner_id: UUID,
        claim_generation: int,
        eligibility: Callable[[Cluster, ExecutionTarget], Awaitable[bool]],
    ) -> WorkItem | None:
        assert item_id == self.item.id
        assert claim_owner_id == self.item.claim_owner_id
        assert claim_generation == self.item.claim_generation
        self.assigned_target_ids.append(target_id)
        cluster, target = self.targets[target_id]
        if not await eligibility(cluster, target):
            return None
        return self.item if target_id == self.assigned_target_id else None

    async def release_claim(self, item_id: UUID, *, claim_owner_id: UUID, claim_generation: int) -> bool:
        assert item_id == self.item.id
        assert claim_owner_id == self.item.claim_owner_id
        assert claim_generation == self.item.claim_generation
        self.released_claims.append((claim_owner_id, claim_generation))
        return True


async def test_no_matching_target_fails_claimed_work():
    item = _reserved_item()
    store = _WorkStore(item, {})
    resolver = _PlacementResolver(ReconcileResult([], [], ResolveOutcome.NO_MATCHING_TARGETS))
    scheduler = WorkScheduler(store, resolver)

    assert await scheduler.claim_and_place_work() is None

    assert resolver.requirements == [WorkRequirements(selectors={"env": "production"})]
    assert store.results == [
        (
            {"error": "No execution target matched placement requirements", "error_type": "TargetUnavailable"},
            WorkItemStatus.FAILED,
        )
    ]
    assert store.released_claims == []


async def test_exhausted_candidates_release_claimed_work():
    cluster = _cluster()
    target = _target(cluster)
    item = _reserved_item()
    store = _WorkStore(item, {target.id: (cluster, target)})
    resolver = _PlacementResolver(ReconcileResult([_snapshot(cluster, target)], [], ResolveOutcome.MATCHED))
    scheduler = WorkScheduler(store, resolver)

    assert await scheduler.claim_and_place_work() is None

    assert store.assigned_target_ids == [target.id]
    assert store.released_claims == [(item.claim_owner_id, item.claim_generation)]
    assert store.results == []


async def test_assigned_candidate_is_returned_without_release():
    cluster = _cluster()
    target = _target(cluster)
    item = _reserved_item()
    store = _WorkStore(item, {target.id: (cluster, target)}, assigned_target_id=target.id)
    resolver = _PlacementResolver(ReconcileResult([_snapshot(cluster, target)], [], ResolveOutcome.MATCHED))
    scheduler = WorkScheduler(store, resolver)

    assert await scheduler.claim_and_place_work() is item

    assert store.assigned_target_ids == [target.id]
    assert store.released_claims == []
    assert store.results == []


async def test_cancelled_reservation_returns_without_placement():
    item = _reserved_item()
    item.status = WorkItemStatus.CANCELLED
    item.claim_owner_id = None
    store = _WorkStore(item, {})
    resolver = _PlacementResolver(ReconcileResult([], [], ResolveOutcome.NO_MATCHING_TARGETS))
    scheduler = WorkScheduler(store, resolver)

    assert await scheduler.claim_and_place_work() is item
    assert resolver.requirements == []
    assert store.assigned_target_ids == []
    assert store.results == []
    assert store.released_claims == []
