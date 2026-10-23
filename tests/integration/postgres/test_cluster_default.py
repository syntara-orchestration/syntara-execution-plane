"""PostgreSQL integration coverage for global default Clusters."""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from execution_plane.cluster.cluster_store import ClusterStore, DefaultClusterError
from execution_plane.models.cluster import Cluster, ClusterStatus

pytestmark = pytest.mark.integration


async def _active_cluster(store: ClusterStore, actor_id: UUID, name: str) -> Cluster:
    cluster = await store.create(name, f"https://{name}.example", "secret", actor_id)
    return await store.record_discovery_state(cluster.id, ClusterStatus.ACTIVE, None, actor_id)


async def test_concurrent_cluster_default_promotions_leave_exactly_one_default(migrated_database: str) -> None:
    engine = create_async_engine(migrated_database)
    actor_id = uuid4()
    try:
        async with ClusterStore.from_engine(engine) as store:
            clusters = [await _active_cluster(store, actor_id, f"cluster-{uuid4()}") for _ in range(3)]
            await asyncio.gather(*(store.set_default(cluster.id, actor_id) for cluster in clusters))

            clusters = await store.list()
            defaults = [cluster for cluster in clusters if cluster.is_default]
            assert len(defaults) == 1
            assert defaults[0].id in {cluster.id for cluster in clusters}
    finally:
        await engine.dispose()


async def test_concurrent_cluster_default_bootstrap_leaves_exactly_one_default(migrated_database: str) -> None:
    engine = create_async_engine(migrated_database)
    actor_id = uuid4()
    try:
        async with ClusterStore.from_engine(engine) as store:
            clusters = [await _active_cluster(store, actor_id, f"cluster-{uuid4()}") for _ in range(3)]
            await asyncio.gather(*(store.ensure_default(cluster.id, actor_id) for cluster in clusters))

            clusters = await store.list()
            defaults = [cluster for cluster in clusters if cluster.is_default]
            assert len(defaults) == 1
            assert defaults[0].id in {cluster.id for cluster in clusters}
    finally:
        await engine.dispose()


async def test_default_cluster_delete_is_rejected_until_reassigned(migrated_database: str) -> None:
    engine = create_async_engine(migrated_database)
    actor_id = uuid4()
    try:
        async with ClusterStore.from_engine(engine) as store:
            default = await _active_cluster(store, actor_id, f"cluster-{uuid4()}")
            replacement = await _active_cluster(store, actor_id, f"cluster-{uuid4()}")
            default = await store.set_default(default.id, actor_id)

            with pytest.raises(DefaultClusterError):
                await store.request_delete(default.id, actor_id)

            await store.set_default(replacement.id, actor_id)
            drained = await store.request_delete(default.id, actor_id)
            assert drained.status is ClusterStatus.DRAINING
    finally:
        await engine.dispose()
