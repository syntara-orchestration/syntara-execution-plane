"""Shared stubs and factories for Vanilla Kubernetes WorkerManager tests."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.worker_manager.vanilla_k8s.manager import VanillaK8sWorkerManager

if TYPE_CHECKING:
    from execution_plane.config import EPSettings
    from execution_plane.credential_resolution.resolver import CredentialResolver


class TargetStoreStub:
    """Return one ExecutionTarget without database access."""

    def __init__(self, target: ExecutionTarget) -> None:
        """Store the target served to the manager."""
        self.target = target

    async def get_with_credentials(self, target_id: UUID) -> ExecutionTarget | None:
        return self.target if target_id == self.target.id else None


class ClusterStoreStub:
    """Return one Cluster without database access."""

    def __init__(self, cluster: Cluster) -> None:
        """Store the cluster served to the manager."""
        self.cluster = cluster

    async def get_with_credentials(self, cluster_id: UUID) -> Cluster | None:
        return self.cluster if cluster_id == self.cluster.id else None


class WorkStoreStub:
    """Record WorkItem boundary transitions without database access."""

    def __init__(self, *, running_result: bool = True) -> None:
        """Configure the result returned by ``mark_running``."""
        self.running_result = running_result
        self.dispatch_calls: list[UUID] = []
        self.running_calls: list[tuple[UUID, UUID, int]] = []

    async def set_result(self, *_args: object, **_kwargs: object) -> None:
        return None

    async def mark_dispatched(self, item_id: UUID, **_kwargs: object) -> bool:
        self.dispatch_calls.append(item_id)
        return True

    async def mark_running(
        self,
        item_id: UUID,
        *,
        claim_owner_id: UUID,
        claim_generation: int,
    ) -> bool:
        self.running_calls.append((item_id, claim_owner_id, claim_generation))
        return self.running_result


def vanilla_k8s_resources() -> tuple[Cluster, ExecutionTarget, WorkItem]:
    now = datetime.now(UTC)
    cluster = Cluster(
        name="workers",
        endpoint="https://cluster.example",
        credential_ref=uuid4(),
        status=ClusterStatus.ACTIVE,
        source_client_id="syntara-orchestration",
        created_by=uuid4(),
        created_at=now,
        updated_by=uuid4(),
        updated_at=now,
    )
    target = ExecutionTarget(
        cluster_id=cluster.id,
        name="default",
        backend_type=BackendType.VANILLA_K8S,
        endpoint=cluster.endpoint,
        placement=KubernetesPlacement(namespace="execution"),
        credential_ref=uuid4(),
        status=TargetStatus.ACTIVE,
        created_by=uuid4(),
        created_at=now,
        updated_by=uuid4(),
        updated_at=now,
    )
    work_item = WorkItem(
        id=uuid4(),
        client_id="syntara-orchestration",
        status=WorkItemStatus.CLAIMED,
        execution_target_id=target.id,
        payload={
            "image": "registry.example/node",
            "invocation": {
                "inputs": {},
                "credentials": {},
                "workflow_context": {},
                "settings": {},
                "timeout_seconds": 60,
                "max_output_bytes": 1024,
            },
        },
        created_at=now,
        claim_owner_id=uuid4(),
        claim_generation=1,
    )
    return cluster, target, work_item


def vanilla_k8s_manager(
    settings: EPSettings,
    resolver: CredentialResolver,
    resources: tuple[Cluster, ExecutionTarget, WorkItem],
    work_store: WorkStoreStub | None = None,
) -> VanillaK8sWorkerManager:
    cluster, target, _ = resources
    return VanillaK8sWorkerManager(
        TargetStoreStub(target),
        ClusterStoreStub(cluster),
        work_store or WorkStoreStub(),
        settings,
        credential_resolver=resolver,
    )
