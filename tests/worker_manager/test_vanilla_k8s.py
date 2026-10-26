"""Kubernetes preparation is inert and exposes durable allocation intent."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from execution_plane.config import EPSettings
from execution_plane.credential_resolution.resolver import CredentialResolver
from execution_plane.worker_manager.vanilla_k8s import transport
from execution_plane_client.commands.work import WorkExecute
from tests.worker_manager.factories import vanilla_k8s_manager, vanilla_k8s_resources, work_attempt


@pytest.mark.asyncio
async def test_prepare_exposes_identity_without_creating_external_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = EPSettings()
    resolver = CredentialResolver(settings)
    monkeypatch.setattr(resolver, "resolve", AsyncMock(return_value="token"))
    api_client = MagicMock()
    monkeypatch.setattr(transport.client, "ApiClient", api_client)
    resources = vanilla_k8s_resources()
    item = resources[2]
    manager = vanilla_k8s_manager(settings, resolver, resources)

    allocation = await manager.prepare(work_attempt(item), WorkExecute.model_validate(item.payload))

    assert allocation.resource.name.startswith("syntara-node-")
    assert allocation.resource.uid is None
    api_client.assert_not_called()
    assert allocation._node is None
