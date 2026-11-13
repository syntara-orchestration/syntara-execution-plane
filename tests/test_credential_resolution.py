"""Credential-resolution callback client contracts."""

from __future__ import annotations

import json
from uuid import uuid4

import httpx
import pytest

from execution_plane.callback_client import CallbackHTTPClient
from execution_plane.config.settings import Settings
from execution_plane.credential_resolution import client as client_module
from execution_plane.credential_resolution.client import CredentialResolutionClient
from execution_plane.credential_resolution.cluster import ClusterCredentialResolver
from execution_plane.credential_resolution.errors import CredentialResolutionMismatchError
from execution_plane.credential_resolution.workload import WorkloadCredentialResolver


def test_credential_resolver_uses_dedicated_callback_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Credential resolution gets its own callback identity and timeout."""
    captured_kwargs: dict[str, object] = {}

    class RecordingCallbackClient:
        def __init__(self, **kwargs: object) -> None:
            captured_kwargs.update(kwargs)

    monkeypatch.setattr(client_module, "CallbackHTTPClient", RecordingCallbackClient)
    settings = Settings(
        EP_CREDENTIAL_RESOLUTION_CALLBACK_URL="https://ao.example.test/api/execution_plane/v1/commands",
        EP_CREDENTIAL_RESOLUTION_CALLBACK_CA_CERT_PATH="/secrets/credential-ca.pem",
        EP_CREDENTIAL_RESOLUTION_CALLBACK_CERT_PATH="/secrets/ep-credentials.crt",
        EP_CREDENTIAL_RESOLUTION_CALLBACK_KEY_PATH="/secrets/ep-credentials.key",
        EP_CREDENTIAL_RESOLUTION_CALLBACK_TIMEOUT_SECONDS=3.5,
    )

    CredentialResolutionClient(settings)

    assert captured_kwargs == {
        "url": "https://ao.example.test/api/execution_plane/v1/commands",
        "ca_cert_path": "/secrets/credential-ca.pem",
        "cert_path": "/secrets/ep-credentials.crt",
        "key_path": "/secrets/ep-credentials.key",
        "timeout_seconds": 3.5,
    }


@pytest.mark.asyncio
async def test_credential_resolver_context_manager_closes_callback_client() -> None:
    class ClosingCallbackClient(CallbackHTTPClient):
        def __init__(self) -> None:
            super().__init__(url=None)
            self.close_count = 0

        async def close(self) -> None:
            self.close_count += 1

    callback_client = ClosingCallbackClient()
    credential_client = CredentialResolutionClient(
        Settings(EP_DATABASE_URL="postgresql+asyncpg://localhost/ep"),
        client=callback_client,
    )

    async with credential_client as entered:
        assert entered is credential_client

    assert callback_client.close_count == 1


@pytest.mark.asyncio
async def test_credential_resolver_posts_command_and_returns_correlated_value():
    credential_ref = uuid4()
    requests: list[httpx.Request] = []

    def receive(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        body = json.loads(request.content)
        assert isinstance(body, dict)
        message_id = body["message_id"]
        return httpx.Response(
            200,
            json={
                "received": True,
                "message_id": message_id,
                "resource": None,
                "result": {
                    "command_type": "credential.resolve",
                    "command_revision": 1,
                    "credential": "resolved-token",
                },
            },
        )

    settings = Settings(EP_CREDENTIAL_RESOLUTION_CALLBACK_URL="https://ao.example.test/api/execution_plane/v1/commands")
    callback_client = CallbackHTTPClient(url=settings.credential_resolution_callback_url)
    await callback_client.close()
    callback_client._client = httpx.AsyncClient(transport=httpx.MockTransport(receive))
    credential_client = CredentialResolutionClient(settings, client=callback_client)
    resolver = ClusterCredentialResolver(settings, credential_client)

    try:
        credential = await resolver.resolve(credential_ref, client_id="syntara-orchestration")
    finally:
        await credential_client.close()

    assert credential == "resolved-token"
    assert len(requests) == 1
    body = json.loads(requests[0].content)
    assert body["command"]["command_type"] == "credential.resolve"
    assert body["command"]["payload"] == {"credential_ref": str(credential_ref)}
    assert body["source_id"] == "execution-plane"
    assert body["destination_id"] == "syntara-orchestration"
    assert body["scope"]["client_id"] == "syntara-orchestration"


@pytest.mark.asyncio
async def test_credential_resolver_posts_workload_command_and_returns_correlated_document():
    credential_ref = uuid4()
    parent_command_id = uuid4()
    requests: list[httpx.Request] = []

    def receive(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        body = json.loads(request.content)
        assert isinstance(body, dict)
        return httpx.Response(
            200,
            json={
                "received": True,
                "message_id": body["message_id"],
                "resource": {"kind": "work_item", "resource_id": str(parent_command_id)},
                "result": {
                    "command_type": "credential.resolve.workload",
                    "command_revision": 1,
                    "credential": {"username": "service-account", "token": "resolved-token"},
                },
            },
        )

    settings = Settings(EP_CREDENTIAL_RESOLUTION_CALLBACK_URL="https://ao.example.test/api/execution_plane/v1/commands")
    callback_client = CallbackHTTPClient(url=settings.credential_resolution_callback_url)
    await callback_client.close()
    callback_client._client = httpx.AsyncClient(transport=httpx.MockTransport(receive))
    credential_client = CredentialResolutionClient(settings, client=callback_client)
    resolver = WorkloadCredentialResolver(settings, credential_client)

    try:
        credential = await resolver.resolve_workload(
            credential_ref,
            "acme.echo.api-token",
            parent_command_id=parent_command_id,
            attempt_id=f"{parent_command_id}-2",
            workflow_run_id="workflow-1",
            step_revision_id="script.v1",
        )
    finally:
        await credential_client.close()

    assert credential == {"username": "service-account", "token": "resolved-token"}
    assert len(requests) == 1
    body = json.loads(requests[0].content)
    assert body["command"]["command_type"] == "credential.resolve.workload"
    assert body["command"]["parent_command_id"] == str(parent_command_id)
    assert body["command"]["payload"] == {
        "attempt_id": f"{parent_command_id}-2",
        "credential_ref": str(credential_ref),
        "credential_recipe_id": "acme.echo.api-token",
        "workflow_run_id": "workflow-1",
        "step_revision_id": "script.v1",
    }
    assert body["source_id"] == "execution-plane"
    assert body["destination_id"] == "syntara-orchestration"
    assert body["scope"]["client_id"] == "syntara-orchestration"


@pytest.mark.asyncio
async def test_credential_resolver_rejects_an_unrelated_response():
    credential_ref = uuid4()
    settings = Settings(EP_CREDENTIAL_RESOLUTION_CALLBACK_URL="https://ao.example.test/api/execution_plane/v1/commands")
    callback_client = CallbackHTTPClient(url=settings.credential_resolution_callback_url)
    await callback_client.close()
    callback_client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                json={
                    "received": True,
                    "message_id": str(uuid4()),
                    "resource": None,
                    "result": {
                        "command_type": "credential.resolve",
                        "command_revision": 1,
                        "credential": "x",
                    },
                },
            )
        )
    )
    credential_client = CredentialResolutionClient(settings, client=callback_client)
    resolver = ClusterCredentialResolver(settings, credential_client)

    try:
        with pytest.raises(CredentialResolutionMismatchError):
            await resolver.resolve(credential_ref, client_id="syntara-orchestration")
    finally:
        await credential_client.close()
