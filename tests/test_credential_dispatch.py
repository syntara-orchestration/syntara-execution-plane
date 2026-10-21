"""Credential-reference resolution at workload dispatch time."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from syntara_plugin.runtime import ContainerResult

from execution_plane.config import EPSettings
from execution_plane.credential_resolution.resolver import (
    CredentialResolutionUnavailableError,
    CredentialResolver,
)
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.worker_manager.vanilla_k8s import manager as manager_module
from execution_plane.worker_manager.vanilla_k8s.manager import RetryableDispatchError, VanillaK8sWorkerManager


class _TargetStore:
    def __init__(self, target: ExecutionTarget) -> None:
        self.target = target

    async def get_with_credentials(self, target_id: UUID) -> ExecutionTarget | None:
        return self.target if target_id == self.target.id else None


class _ClusterStore:
    def __init__(self, cluster: Cluster) -> None:
        self.cluster = cluster

    async def get_with_credentials(self, cluster_id: UUID) -> Cluster | None:
        return self.cluster if cluster_id == self.cluster.id else None


class _WorkStore:
    async def set_result(self, *_args: object, **_kwargs: object) -> None:
        return None


def _resources() -> tuple[Cluster, ExecutionTarget, WorkItem]:
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
                "workflow_run_id": "workflow-1",
                "step_revision_id": "script.v1",
                "inputs": {},
                "timeout_seconds": 60,
            },
        },
        created_at=now,
        claim_owner_id=uuid4(),
        claim_generation=1,
    )
    return cluster, target, work_item


def _manager(
    settings: EPSettings,
    resolver: CredentialResolver,
    resources: tuple[Cluster, ExecutionTarget, WorkItem],
) -> VanillaK8sWorkerManager:
    cluster, target, _ = resources
    return VanillaK8sWorkerManager(
        _TargetStore(target),
        _ClusterStore(cluster),
        _WorkStore(),
        settings,
        credential_resolver=resolver,
    )


@pytest.mark.asyncio
async def test_dispatch_resolves_the_cluster_reference_and_passes_the_token_to_kubernetes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = EPSettings()
    resolver = CredentialResolver(settings)
    resources = _resources()
    cluster, target, work_item = resources
    calls: list[dict[str, Any]] = []

    async def resolve(credential_ref: UUID, *, client_id: str) -> str:
        calls.append({"credential_ref": credential_ref, "client_id": client_id})
        return "resolved-token"

    monkeypatch.setattr(resolver, "resolve", resolve)

    def run_job(*, target: dict[str, Any], **_kwargs: object) -> dict[str, Any]:
        calls.append({"target": target})
        return {
            "kind": "result",
            "result": ContainerResult(
                "sha256:" + "a" * 64,
                output={},
                status_code=0,
                error_code="",
                error_message="",
            ),
        }

    monkeypatch.setattr(manager_module, "run_job", run_job)
    manager = _manager(settings, resolver, resources)

    result = await manager.dispatch(work_item)

    assert result["output"] == {}
    assert result["status_code"] == 0
    assert calls[0] == {"credential_ref": cluster.credential_ref, "client_id": cluster.source_client_id}
    assert calls[1]["target"]["token"] == "resolved-token"  # noqa: S105
    assert target.credential_ref != cluster.credential_ref


@pytest.mark.asyncio
async def test_dispatch_maps_credential_resolution_failure_to_a_retryable_pre_execution_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = EPSettings()
    resolver = CredentialResolver(settings)

    async def resolve(_credential_ref: UUID, **_kwargs: object) -> str:
        raise CredentialResolutionUnavailableError

    monkeypatch.setattr(resolver, "resolve", resolve)
    resources = _resources()
    manager = _manager(settings, resolver, resources)
    work_item = resources[2]

    with pytest.raises(RetryableDispatchError, match="Cluster credential could not be resolved"):
        await manager.dispatch(work_item)
