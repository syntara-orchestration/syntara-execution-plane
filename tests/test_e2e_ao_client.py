"""Unit tests for the live E2E AO API client."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import ModuleType


def _load_conftest() -> ModuleType:
    path = Path(__file__).parent / "e2e" / "conftest.py"
    spec = importlib.util.spec_from_file_location("e2e_conftest", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _ao_client(handler: Callable[[httpx.Request], httpx.Response]) -> Any:  # noqa: ANN401
    conftest = _load_conftest()
    client = object.__new__(conftest.AoClient)
    client.api = "https://ao.example.test/api/v1"
    client._token = "test-token"  # noqa: S105
    client._client = httpx.Client(transport=httpx.MockTransport(handler))
    return client


def test_ao_client_lists_credential_types() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={"resources": [{"id": "credential-type-id", "name": "HTTP Bearer Token"}]},
        )

    client = _ao_client(handler)
    try:
        credential_types = client.list_credential_types()
    finally:
        client.close()

    assert credential_types == [{"id": "credential-type-id", "name": "HTTP Bearer Token"}]
    assert len(requests) == 1
    assert requests[0].url == "https://ao.example.test/api/v1/credential_types"
    assert requests[0].headers["Authorization"] == "Bearer test-token"


def test_ao_client_creates_credential() -> None:
    requests: list[httpx.Request] = []
    body = {
        "name": "e2e credential",
        "credential_type_id": "credential-type-id",
        "project_id": "project-id",
        "inputs": {"token": "test-token"},
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(201, json={**body, "id": "credential-id"})

    client = _ao_client(handler)
    try:
        created = client.create_credential(body)
    finally:
        client.close()

    assert created["id"] == "credential-id"
    assert len(requests) == 1
    assert requests[0].method == "POST"
    assert requests[0].url == "https://ao.example.test/api/v1/credentials"
    assert requests[0].read() == httpx.Request("POST", requests[0].url, json=body, headers=requests[0].headers).read()
