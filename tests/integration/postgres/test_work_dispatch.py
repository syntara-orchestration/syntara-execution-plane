"""Worker dispatch must reject malformed work-item payloads before reaching Kubernetes."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from execution_plane import worker as worker_module
from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.config import get_ep_settings
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.execution_target_reconciler.placement import WorkerManagerRegistry
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.work_item.work_item_store import WorkStore
from execution_plane.worker_manager.vanilla_k8s import manager as manager_module
from execution_plane.worker_manager.vanilla_k8s.manager import VanillaK8sWorkerManager

pytestmark = pytest.mark.integration


@pytest.fixture
async def engine(migrated_database):
    created = create_async_engine(migrated_database)
    yield created
    await created.dispose()


async def _seed_claimed_item(sessions, *, payload) -> UUID:
    now = datetime.now(UTC)
    cluster_id, target_id, item_id, owner_id = uuid4(), uuid4(), uuid4(), uuid4()
    async with sessions.begin() as session:
        session.add(
            Cluster(
                id=cluster_id,
                name=str(cluster_id),
                endpoint="https://cluster.test",
                credential_ref=uuid4(),
                source_client_id="ao",
                status=ClusterStatus.ACTIVE,
                created_by=uuid4(),
                updated_by=uuid4(),
                created_at=now,
                updated_at=now,
            )
        )
        session.add(
            ExecutionTarget(
                id=target_id,
                cluster_id=cluster_id,
                name="workers",
                backend_type=BackendType.VANILLA_K8S,
                endpoint="https://cluster.test",
                placement=KubernetesPlacement(namespace="ep"),
                status=TargetStatus.ACTIVE,
                enabled=True,
                is_default=False,
                credential_ref=uuid4(),
                created_by=uuid4(),
                updated_by=uuid4(),
                created_at=now,
                updated_at=now,
            )
        )
    async with sessions.begin() as session:
        session.add(
            WorkItem(
                id=item_id,
                client_id="ao",
                status=WorkItemStatus.CLAIMED,
                execution_target_id=target_id,
                claim_owner_id=owner_id,
                claim_generation=0,
                payload=payload,
                created_at=now,
            )
        )
    return item_id


async def _process(engine, item_id) -> WorkItem:
    store = WorkStore.from_engine(engine)
    target_store = ExecutionTargetStore.from_engine(engine)
    cluster_store = ClusterStore.from_engine(engine)
    settings = get_ep_settings()
    registry = WorkerManagerRegistry()
    registry.register(
        BackendType.VANILLA_K8S,
        VanillaK8sWorkerManager(target_store, cluster_store, store, settings),
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session:
        item = await session.get(WorkItem, item_id)
    await worker_module._process_item(item, store, target_store, registry)
    async with sessions() as session:
        return await session.get(WorkItem, item_id)


async def test_missing_payload_fields_fail_the_work_item(engine, monkeypatch):
    """A payload with no image/invocation is rejected before any Kubernetes call."""
    allocation = MagicMock(name="KubernetesWorker")
    monkeypatch.setattr(manager_module, "KubernetesWorker", allocation)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    item_id = await _seed_claimed_item(sessions, payload={})

    item = await _process(engine, item_id)

    allocation.assert_not_called()
    assert item.status is WorkItemStatus.FAILED
    assert item.result["error_type"] == "WorkItemPayloadError"


async def test_semantically_invalid_invocation_is_rejected_before_kubernetes(engine, monkeypatch):
    """An invocation with a nonpositive timeout fails before dispatch reaches Kubernetes."""
    allocation = MagicMock(name="KubernetesWorker")
    monkeypatch.setattr(manager_module, "KubernetesWorker", allocation)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    item_id = await _seed_claimed_item(
        sessions,
        payload={
            "image": "node:test",
            "invocation": {
                "inputs": {},
                "credentials": {},
                "workflow_context": {},
                "settings": {},
                "timeout_seconds": -5,
                "max_output_bytes": 1024,
            },
            "workload_type": "script",
        },
    )

    item = await _process(engine, item_id)

    allocation.assert_not_called()
    assert item.status is WorkItemStatus.FAILED
    assert item.result["error_type"] == "WorkItemPayloadError"
