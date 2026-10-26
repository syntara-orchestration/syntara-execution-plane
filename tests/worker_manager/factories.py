"""Shared stubs and factories for Vanilla Kubernetes WorkerManager tests."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.worker_manager.lifecycle import WorkAttempt
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
    """Record lifecycle decisions without opening a database connection."""

    def __init__(self, *, dispatch_result: bool = True, running_result: bool = True) -> None:
        """Configure the dispatch fence and execution-progress race."""
        self.dispatch_result = dispatch_result
        self.running_result = running_result
        self.running_error: Exception | None = None
        self.status = WorkItemStatus.CLAIMED
        self.lost = False
        self.events: list[str] = []
        self.results: list[tuple[WorkItemStatus, dict[str, Any]]] = []
        self.running_calls: list[tuple[UUID, UUID, int]] = []
        self.requeues = 0
        self.reconciliations: list[str] = []

    async def mark_dispatched(self, item_id: UUID, **_kwargs: object) -> bool:
        self.events.append("dispatched")
        if self.dispatch_result:
            self.status = WorkItemStatus.DISPATCHED
        return self.dispatch_result

    async def refresh_claim(self, *_args: object, **_kwargs: object) -> bool | None:
        if self.lost:
            return None
        return self.status is WorkItemStatus.CANCEL_REQUESTED

    async def mark_running(
        self,
        item_id: UUID,
        *,
        claim_owner_id: UUID,
        claim_generation: int,
    ) -> bool:
        self.running_calls.append((item_id, claim_owner_id, claim_generation))
        if self.running_error is not None:
            raise self.running_error
        if not self.running_result:
            self.status = WorkItemStatus.CANCEL_REQUESTED
            return False
        self.events.append("running")
        self.status = WorkItemStatus.RUNNING
        return True

    async def set_result(
        self,
        item_id: UUID,
        result: dict[str, Any],
        status: WorkItemStatus,
        **_kwargs: object,
    ) -> None:
        self.events.append(status.value)
        self.status = status
        self.results.append((status, result))

    async def update_backend_resource(self, *_args: object, cleanup_status: str, **_kwargs: object) -> None:
        self.events.append("resource_" + cleanup_status)

    async def requeue_pre_execute_failure(self, *_args: object, **_kwargs: object) -> SimpleNamespace:
        self.events.append("requeue")
        if self.status is WorkItemStatus.DISPATCHED:
            self.requeues += 1
            self.status = WorkItemStatus.PENDING
        return SimpleNamespace(status=self.status)

    async def mark_reconciliation_required(self, item_id: UUID, reason: str, **_kwargs: object) -> None:
        self.events.append("reconciliation_required")
        self.status = WorkItemStatus.RECONCILIATION_REQUIRED
        self.reconciliations.append(reason)


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


def work_attempt(item: WorkItem) -> WorkAttempt:
    """Snapshot a claimed test WorkItem for backend preparation."""
    assert item.claim_owner_id is not None
    assert item.execution_target_id is not None
    return WorkAttempt(item.id, item.claim_owner_id, item.claim_generation, item.execution_target_id)
