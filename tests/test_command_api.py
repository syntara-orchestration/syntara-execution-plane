"""The replacement API accepts commands and exposes only domain resource state."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest

from execution_plane.api.auth import ServiceIdentity, get_service_identity
from execution_plane.api.main import create_app
from execution_plane.commands.targets import TargetUpdateCommandHandler
from execution_plane.execution_target.execution_target_store import TargetNotEligibleForDefaultError
from execution_plane.work_item.work_item_store import WorkStore
from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.commands.control import CancelCommand
from execution_plane_client.commands.work import WorkExecute


def command_message(kind="command.cancel", payload=None):
    command_id = uuid4()
    return {
        "message_id": str(uuid4()),
        "command_id": str(command_id),
        "source_id": "ao",
        "destination_id": "execution-plane",
        "scope": {"client_id": "ao"},
        "command": {
            "command_id": str(command_id),
            "command_type": kind,
            "command_revision": 1,
            "occurred_at": datetime.now(UTC).isoformat(),
            "payload": payload if payload is not None else {"target_command_id": str(uuid4())},
        },
    }


def test_only_command_mutations_and_resource_queries_are_exposed():
    paths = create_app().openapi()["paths"]
    assert "/v1/commands" in paths
    assert "/v1/commands/{command_id}" not in paths
    assert "/v1/commands/{command_id}/children" not in paths
    assert not any("cluster-bindings" in path for path in paths)
    assert [
        (path, method)
        for path, methods in paths.items()
        for method in methods
        if method in {"post", "put", "patch", "delete"}
    ] == [("/v1/commands", "post")]
    for resource in ("work-items", "clusters", "execution-targets"):
        assert f"/v1/{resource}" in paths
        assert f"/v1/{resource}/{{resource_id}}" in paths


@pytest.mark.parametrize("command_type", [WorkExecute, CancelCommand])
async def test_ingress_handles_work_with_one_payload_conversion(monkeypatch, command_type):
    work_id = uuid4()
    payload = (
        {
            "image": "node:test",
            "invocation": {
                "workflow_run_id": "workflow-1",
                "step_revision_id": "script.v1",
                "inputs": {},
                "timeout_seconds": 60,
            },
        }
        if command_type is WorkExecute
        else {"target_command_id": str(work_id)}
    )
    message = command_message(command_type.command_type, payload)
    if command_type is WorkExecute:
        work_id = UUID(message["command_id"])
    store = AsyncMock(spec=WorkStore)
    store.dispatch.return_value = store.request_cancel.return_value = SimpleNamespace(id=work_id)
    monkeypatch.setattr(WorkStore, "from_engine", lambda _engine: store)
    parsed_payloads = []
    validate = command_type.model_validate

    def track_validation(value) -> CommandPayload:
        parsed = validate(value)
        parsed_payloads.append(parsed)
        return parsed

    monkeypatch.setattr(command_type, "model_validate", track_validation)
    app = create_app()
    app.state.ep_engine = object()
    app.dependency_overrides[get_service_identity] = lambda: ServiceIdentity(
        client_id="ao", scopes=frozenset({command_type.command_type})
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
        response = await client.post("/v1/commands", json=message)
    assert response.status_code == 202
    assert response.json()["message_id"] == message["message_id"]
    assert response.json()["resource"]["resource_id"] == str(work_id)
    assert len(parsed_payloads) == 1
    if command_type is WorkExecute:
        store.dispatch.assert_awaited_once_with(
            client_id="ao", item_id=work_id, payload=parsed_payloads[0].model_dump(mode="json")
        )
    else:
        store.request_cancel.assert_awaited_once_with(work_id, client_id="ao")


@pytest.mark.parametrize(
    ("change", "status"), [("owner", 403), ("revision", 422), ("private", 422), ("permission", 403)]
)
async def test_ingress_rejects_before_domain_effects(change, status):
    app = create_app()
    scopes = frozenset() if change == "permission" else frozenset({"command.cancel"})
    app.dependency_overrides[get_service_identity] = lambda: ServiceIdentity(client_id="ao", scopes=scopes)
    message = command_message()
    if change == "owner":
        message["scope"]["client_id"] = "other"
    elif change == "revision":
        message["command"]["command_revision"] = 99
    elif change == "private":
        message["command"]["payload"] = {"credential": "private-secret"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
        response = await client.post("/v1/commands", json=message)
    assert response.status_code == status
    assert "private-secret" not in response.text


async def test_ingress_maps_ineligible_default_promotion_to_conflict(monkeypatch):
    async def reject_promotion(*_args: object, **_kwargs: object) -> None:
        raise TargetNotEligibleForDefaultError(uuid4())

    monkeypatch.setattr(TargetUpdateCommandHandler, "handle", reject_promotion)
    app = create_app()
    app.state.ep_engine = object()
    app.dependency_overrides[get_service_identity] = lambda: ServiceIdentity(
        client_id="ao", scopes=frozenset({"execution_target.update"})
    )
    message = command_message("execution_target.update", {"resource_id": str(uuid4()), "is_default": True})

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
        response = await client.post("/v1/commands", json=message)

    assert response.status_code == 409


async def test_strict_json_rejects_duplicate_keys():
    app = create_app()
    app.dependency_overrides[get_service_identity] = lambda: ServiceIdentity(client_id="ao", scopes=frozenset())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
        response = await client.post("/v1/commands", content=b'{"private":"secret","private":"other"}')
    assert response.status_code == 422
    assert "secret" not in response.text


async def test_ingress_rejects_excessive_json_nesting():
    app = create_app()
    app.dependency_overrides[get_service_identity] = lambda: ServiceIdentity(client_id="ao", scopes=frozenset())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
        response = await client.post("/v1/commands", content=b"[" * 100000 + b"0" + b"]" * 100000)
    assert response.status_code == 422


async def test_ingress_bounds_body_and_requires_authentication():
    app = create_app()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
        unauthenticated = await client.post("/v1/commands", json=command_message())
        assert unauthenticated.status_code == 401
        app.dependency_overrides[get_service_identity] = lambda: ServiceIdentity(client_id="ao", scopes=frozenset())
        oversized = await client.post("/v1/commands", content=b" " * (1024 * 1024 + 1))
        assert oversized.status_code == 413


async def test_capabilities_do_not_advertise_uninstalled_completion_handlers():
    app = create_app()
    app.dependency_overrides[get_service_identity] = lambda: ServiceIdentity(
        client_id="ao", scopes=frozenset({"capabilities:read"})
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
        response = await client.get("/v1/capabilities")
    assert response.status_code == 200
    commands = response.json()["commands"]
    assert len(commands) == 9
    assert commands["command.cancel"] == [1]
    assert "work.execute.completed" not in commands
