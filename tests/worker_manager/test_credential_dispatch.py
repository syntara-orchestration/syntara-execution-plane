"""Credential-reference resolution at workload dispatch time."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from uuid import UUID

import pytest

from execution_plane.config import EPSettings
from execution_plane.credential_resolution.resolver import (
    CredentialResolutionUnavailableError,
    CredentialResolver,
)
from execution_plane.worker_manager.lifecycle import RetryableDispatchError
from execution_plane_client.commands.work import WorkExecute
from tests.worker_manager.factories import vanilla_k8s_manager, vanilla_k8s_resources, work_attempt


@pytest.mark.asyncio
async def test_dispatch_resolves_the_cluster_reference_and_passes_the_token_to_kubernetes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = EPSettings()
    resolver = CredentialResolver(settings)
    resources = vanilla_k8s_resources()
    cluster, target, work_item = resources
    calls: list[dict[str, Any]] = []

    async def resolve(credential_ref: UUID, *, client_id: str) -> str:
        calls.append({"credential_ref": credential_ref, "client_id": client_id})
        return "resolved-token"

    monkeypatch.setattr(resolver, "resolve", resolve)

    manager = vanilla_k8s_manager(settings, resolver, resources)
    allocation = await manager.prepare(work_attempt(work_item), WorkExecute.model_validate(work_item.payload))

    assert calls == [{"credential_ref": cluster.credential_ref, "client_id": cluster.source_client_id}]
    assert allocation._target.token == "resolved-token"  # noqa: S105
    assert allocation.resource.uid is None
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
    resources = vanilla_k8s_resources()
    manager = vanilla_k8s_manager(settings, resolver, resources)
    work_item = resources[2]

    with pytest.raises(RetryableDispatchError, match="Cluster credential could not be resolved"):
        await manager.prepare(work_attempt(work_item), WorkExecute.model_validate(work_item.payload))
