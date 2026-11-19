"""Credential-reference resolution at workload dispatch time."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from uuid import UUID

import pytest
from syntara_plugin.runtime import ContainerResult

from execution_plane.config.settings import Settings
from execution_plane.credential_resolution.errors import CredentialResolutionUnavailableError
from execution_plane.worker_manager.vanilla_k8s import manager as manager_module
from execution_plane.worker_manager.vanilla_k8s.manager import RetryableDispatchError
from tests.worker_manager.factories import (
    vanilla_k8s_credential_resolvers,
    vanilla_k8s_manager,
    vanilla_k8s_resources,
)


@pytest.mark.asyncio
async def test_dispatch_resolves_the_cluster_reference_and_passes_the_token_to_kubernetes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings()
    async with vanilla_k8s_credential_resolvers(settings) as (cluster_resolver, workload_resolver):
        resources = vanilla_k8s_resources()
        cluster, target, work_item = resources
        calls: list[dict[str, Any]] = []

        async def resolve(credential_ref: UUID, *, client_id: str) -> str:
            calls.append({"credential_ref": credential_ref, "client_id": client_id})
            return "resolved-token"

        monkeypatch.setattr(cluster_resolver, "resolve", resolve)

        def run_job(*, target: dict[str, Any], **_kwargs: object) -> dict[str, Any]:
            calls.append({"target": target})
            return {
                "kind": "result",
                "result": ContainerResult(
                    "sha256:" + "a" * 64,
                    output={},
                ),
            }

        monkeypatch.setattr(manager_module, "run_job", run_job)
        manager = vanilla_k8s_manager(settings, cluster_resolver, workload_resolver, resources)

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
    settings = Settings()
    async with vanilla_k8s_credential_resolvers(settings) as (cluster_resolver, workload_resolver):

        async def resolve(_credential_ref: UUID, **_kwargs: object) -> str:
            raise CredentialResolutionUnavailableError

        monkeypatch.setattr(cluster_resolver, "resolve", resolve)
        resources = vanilla_k8s_resources()
        manager = vanilla_k8s_manager(settings, cluster_resolver, workload_resolver, resources)
        work_item = resources[2]

        with pytest.raises(RetryableDispatchError, match="Cluster credential could not be resolved"):
            await manager.dispatch(work_item)
