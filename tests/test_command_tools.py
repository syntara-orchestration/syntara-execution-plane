"""Local development helpers produce the replacement wire protocol."""

import sys
from uuid import uuid4

from execution_plane import work_item_client
from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.protocol import MessageEnvelope
from tools import deploy_kind_execution_target, submit_work_item


def test_default_token_scopes_authorize_command_tools():
    scopes = str(work_item_client.service_token_claims()["scope"]).split()
    assert {"work.execute", "command.cancel", "cluster.provision", "clusters:read", "work-items:read"} <= set(scopes)


def test_submit_tool_builds_valid_work_command(monkeypatch):
    requests = []

    def request(method, url, token, body=None) -> tuple[int, dict]:
        requests.append((method, url, body))
        return 202, {"received": True}

    monkeypatch.setattr(work_item_client, "ep_request", request)
    payload = submit_work_item.build_payload(
        language="python", code="print('hello')", timeout_seconds=41, environment={"EXAMPLE": "value"}
    )
    work_id = uuid4()
    submit_work_item.submit_work_item("https://ep.test", "token", work_id=work_id, payload=payload)
    method, url, body = requests[0]
    assert (method, url) == ("POST", "https://ep.test/v1/commands")
    message = MessageEnvelope.model_validate(body)
    CommandCatalog().validate(message)
    assert message.command_id == work_id
    assert message.command.payload["invocation"]["inputs"]["code"] == "print('hello')"
    assert message.command.payload["invocation"]["timeout_seconds"] == 41


def test_submit_cli_preserves_image_override_and_waits_for_work(monkeypatch, capsys):
    requests = []
    work_id = uuid4()

    def request(method, url, token, body=None) -> tuple[int, dict]:
        requests.append((method, url, body))
        if method == "POST":
            return 202, {"received": True}
        return 200, {"id": str(work_id), "status": "reconciliation_required"}

    monkeypatch.setattr(work_item_client, "ep_request", request)
    monkeypatch.setattr(submit_work_item, "ep_service_token", lambda: "token")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "submit_work_item.py",
            "--api-url",
            "https://ep.test",
            "--work-id",
            str(work_id),
            "--image",
            "quay.io/example/custom-node:test",
            "--wait",
        ],
    )
    assert submit_work_item.main() == 0
    assert len(requests) == 2
    message = MessageEnvelope.model_validate(requests[0][2])
    assert message.command_id == work_id
    assert message.command.payload["image"] == "quay.io/example/custom-node:test"
    assert requests[1][:2] == ("GET", f"https://ep.test/v1/work-items/{work_id}")
    assert "reconciliation_required" in capsys.readouterr().out


def test_cluster_tool_submits_provision_command(monkeypatch):
    requests = []

    def request(method, url, token, body=None) -> tuple[int, dict]:
        requests.append((method, url, body))
        return 202, {"received": True}

    monkeypatch.setattr(deploy_kind_execution_target, "ep_request", request)
    integration_id = uuid4()
    deploy_kind_execution_target.provision_cluster(
        "https://ep.test",
        "token",
        integration_id,
        name="workers",
        endpoint="https://cluster.test",
        namespace="ep",
        credential_ref=uuid4(),
        ca_certificate=None,
    )
    assert len(requests) == 1
    method, url, body = requests[0]
    assert (method, url) == ("POST", "https://ep.test/v1/commands")
    message = MessageEnvelope.model_validate(body)
    CommandCatalog().validate(message)
    assert message.command.command_type == "cluster.provision"
    assert message.command.payload["desired_revision"] == 1
    assert message.command.payload["resource_key"] == str(integration_id)
    assert message.command.payload["is_default"] is True
