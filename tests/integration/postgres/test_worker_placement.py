"""PostgreSQL-backed placement integration for the worker."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.execution_target_reconciler.adapters import build_placement_resolver
from execution_plane.execution_target_reconciler.types import WorkRequirements
from execution_plane.models.cluster import Cluster, ClusterStatus, ClusterType
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.work_item.work_item_store import WorkStore
from execution_plane.work_item.work_scheduler import WorkScheduler

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from execution_plane.execution_target_reconciler.placement import PlacementResolver
    from execution_plane.execution_target_reconciler.types import (
        ClusterSnapshot,
        ExecutionTargetSnapshot,
        ReconcileResult,
    )

pytestmark = pytest.mark.integration


@dataclass
class _PlacementFixture:
    cluster_id: UUID
    target_id: UUID
    work_id: UUID
    sessions: async_sessionmaker[AsyncSession]
    work_scheduler: WorkScheduler
    resolver: PlacementResolver


class _StalePlacementResolver:
    def __init__(self, resolver: PlacementResolver, result: ReconcileResult) -> None:
        self._resolver = resolver
        self._result = result

    async def resolve(self, _requirements: WorkRequirements) -> ReconcileResult:
        return self._result

    async def target_is_eligible(
        self,
        cluster: ClusterSnapshot,
        target: ExecutionTargetSnapshot,
        requirements: WorkRequirements,
    ) -> bool:
        return await self._resolver.target_is_eligible(cluster, target, requirements)


@pytest.fixture
async def placement_database(
    migrated_database: str,
) -> AsyncIterator[tuple[AsyncEngine, async_sessionmaker[AsyncSession]]]:
    engine = create_async_engine(migrated_database)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    yield engine, sessions
    await engine.dispose()


async def _seed_work(sessions: async_sessionmaker[AsyncSession], *, matching: bool) -> tuple[UUID, UUID, UUID]:
    cluster_id, target_id, work_id = uuid4(), uuid4(), uuid4()
    selector_key = f"integration-{uuid4()}"
    target_selector_value = "match" if matching else "different"
    now = datetime.now(UTC)
    created_at = datetime(2000, 1, 1, tzinfo=UTC)

    async with sessions.begin() as session:
        session.add(
            Cluster(
                id=cluster_id,
                name=f"worker-placement-{cluster_id}",
                endpoint="https://cluster.test",
                credential_ref=uuid4(),
                status=ClusterStatus.ACTIVE,
                enabled=True,
                cluster_type=ClusterType.OPENSHIFT,
                labels={},
                created_by=uuid4(),
                updated_by=uuid4(),
                created_at=now,
                updated_at=now,
            )
        )
        await session.flush()
        session.add(
            ExecutionTarget(
                id=target_id,
                cluster_id=cluster_id,
                name=f"workers-{target_id}",
                backend_type=BackendType.VANILLA_K8S,
                endpoint="https://target.test",
                placement=KubernetesPlacement(namespace="ep"),
                credential_ref=uuid4(),
                status=TargetStatus.ACTIVE,
                enabled=True,
                is_default=False,
                labels={selector_key: target_selector_value},
                created_by=uuid4(),
                updated_by=uuid4(),
                created_at=now,
                updated_at=now,
            )
        )
        await session.flush()
        session.add(
            WorkItem(
                id=work_id,
                client_id="worker-placement-test",
                status=WorkItemStatus.PENDING,
                payload={"selectors": {selector_key: "match"}},
                created_at=created_at,
            )
        )

    return cluster_id, target_id, work_id


async def _cleanup_work(sessions: async_sessionmaker[AsyncSession], ids: tuple[UUID, UUID, UUID]) -> None:
    cluster_id, target_id, work_id = ids
    async with sessions.begin() as session:
        work_item = await session.get(WorkItem, work_id)
        if work_item is not None:
            await session.delete(work_item)
        target = await session.get(ExecutionTarget, target_id)
        if target is not None:
            await session.delete(target)
        cluster = await session.get(Cluster, cluster_id)
        if cluster is not None:
            await session.delete(cluster)


@asynccontextmanager
async def _placement(
    placement_database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    *,
    matching: bool,
) -> AsyncGenerator[_PlacementFixture, None]:
    engine, sessions = placement_database
    cluster_id, target_id, work_id = await _seed_work(sessions, matching=matching)

    try:
        async with (
            WorkStore.from_engine(engine) as work_item_store,
            ClusterStore.from_engine(engine) as cluster_store,
            ExecutionTargetStore.from_engine(engine) as target_store,
        ):
            resolver = build_placement_resolver(cluster_store, target_store)
            work_scheduler = WorkScheduler(work_item_store, resolver)
            yield _PlacementFixture(
                cluster_id=cluster_id,
                target_id=target_id,
                work_id=work_id,
                sessions=sessions,
                work_scheduler=work_scheduler,
                resolver=resolver,
            )
    finally:
        await _cleanup_work(sessions, (cluster_id, target_id, work_id))


async def _assert_work_item_is_pending(sessions: async_sessionmaker[AsyncSession], work_id: UUID) -> None:
    async with sessions() as session:
        persisted = await session.get(WorkItem, work_id)
        assert persisted is not None
        assert persisted.status is WorkItemStatus.PENDING
        assert persisted.execution_target_id is None
        assert persisted.claim_owner_id is None
        assert persisted.claimed_at is None
        assert persisted.result is None


async def _assert_stale_target_releases_reservation(
    placement_database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    make_target_stale: Callable[[_PlacementFixture], Awaitable[None]],
) -> None:
    async with _placement(placement_database, matching=True) as placement:
        payload_selectors = await _work_payload_selectors(placement.sessions, placement.work_id)
        stale_result = await placement.resolver.resolve(WorkRequirements(selectors=payload_selectors))
        assert stale_result.available_targets[0].id == placement.target_id

        await make_target_stale(placement)

        stale_resolver = _StalePlacementResolver(placement.resolver, stale_result)
        async with WorkStore.from_engine(placement_database[0]) as stale_store:
            stale_scheduler = WorkScheduler(stale_store, stale_resolver)
            assigned = await stale_scheduler.claim_and_place_work()

        assert assigned is None

        await _assert_work_item_is_pending(placement.sessions, placement.work_id)


async def _work_payload_selectors(
    sessions: async_sessionmaker[AsyncSession],
    work_id: UUID,
) -> dict[str, str]:
    async with sessions() as session:
        item = await session.get(WorkItem, work_id)
        assert item is not None
        selectors = item.payload.get("selectors", {})
        assert isinstance(selectors, dict)
        return dict(selectors)


async def _change_target_labels(placement: _PlacementFixture) -> None:
    selector_key = next(iter(await _work_payload_selectors(placement.sessions, placement.work_id)))
    async with placement.sessions.begin() as session:
        target = await session.get(ExecutionTarget, placement.target_id)
        assert target is not None
        target.labels = {selector_key: "changed-after-resolution"}


async def _disable_cluster(placement: _PlacementFixture) -> None:
    async with placement.sessions.begin() as session:
        cluster = await session.get(Cluster, placement.cluster_id)
        assert cluster is not None
        cluster.enabled = False


async def test_worker_claims_and_places_work_on_the_matching_postgres_target(
    placement_database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    async with _placement(placement_database, matching=True) as placement:
        assigned = await placement.work_scheduler.claim_and_place_work()

        assert assigned is not None
        assert assigned.id == placement.work_id
        assert assigned.execution_target_id == placement.target_id
        assert assigned.status is WorkItemStatus.CLAIMED
        assert assigned.claim_owner_id is not None
        assert assigned.claim_generation == 1

        async with placement.sessions() as session:
            persisted = await session.get(WorkItem, placement.work_id)
            assert persisted is not None
            assert persisted.execution_target_id == placement.target_id
            assert persisted.status is WorkItemStatus.CLAIMED


async def test_worker_fails_claimed_work_when_postgres_has_no_matching_target(
    placement_database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    async with _placement(placement_database, matching=False) as placement:
        assigned = await placement.work_scheduler.claim_and_place_work()

        assert assigned is None

        async with placement.sessions() as session:
            persisted = await session.get(WorkItem, placement.work_id)
            assert persisted is not None
            assert persisted.status is WorkItemStatus.FAILED
            assert persisted.result == {
                "error": "No execution target matched placement requirements",
                "error_type": "TargetUnavailable",
            }
            assert persisted.completed_at is not None
            assert persisted.claim_owner_id is None


async def test_worker_releases_reservation_when_target_labels_change_during_placement(
    placement_database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    await _assert_stale_target_releases_reservation(placement_database, _change_target_labels)


async def test_worker_releases_reservation_when_cluster_is_disabled_during_placement(
    placement_database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    await _assert_stale_target_releases_reservation(placement_database, _disable_cluster)
