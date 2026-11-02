"""PostgreSQL concurrency coverage for ExecutionTarget default promotion."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.models.cluster import ClusterStatus
from execution_plane.models.execution_target import BackendType
from execution_plane.models.execution_target_placement import KubernetesPlacement

pytestmark = pytest.mark.integration


async def test_concurrent_default_promotions_leave_exactly_one_default(migrated_database: str) -> None:
    engine = create_async_engine(migrated_database)
    actor_id = uuid4()
    try:
        async with (
            ClusterStore.from_engine(engine) as cluster_store,
            ExecutionTargetStore.from_engine(engine) as target_store,
        ):
            cluster = await cluster_store.create(
                uuid4(),
                f"cluster-{uuid4()}",
                f"https://{uuid4()}.example",
                uuid4(),
                actor_id,
            )
            cluster = await cluster_store.record_discovery_state(
                cluster.id,
                ClusterStatus.ACTIVE,
                None,
                actor_id,
            )
            assert cluster.status is ClusterStatus.ACTIVE

            target_ids = []
            for name in ("target-a", "target-b", "target-c"):
                target = await target_store.create(
                    cluster_id=cluster.id,
                    name=name,
                    backend_type=BackendType.VANILLA_K8S,
                    endpoint=f"https://{name}.example",
                    credential_ref=uuid4(),
                    is_default=False,
                    created_by=actor_id,
                    placement=KubernetesPlacement(namespace="execution"),
                )
                await target_store.activate(target.id, actor_id)
                target_ids.append(target.id)

            await asyncio.gather(*(target_store.set_default(target_id, actor_id) for target_id in target_ids))

            targets = await target_store.list(cluster_id=cluster.id)
            defaults = [target for target in targets if target.is_default]
            assert len(defaults) == 1
            assert defaults[0].id in target_ids
    finally:
        await engine.dispose()
