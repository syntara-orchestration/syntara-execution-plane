"""Local development helpers produce the replacement wire protocol."""

import sys
from uuid import uuid4

from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.protocol import MessageEnvelope
from tools import deploy_kind_execution_target, submit_work_item, work_item_client


def test_default_token_scopes_authorize_command_tools():
    scopes = str(work_item_client.service_token_claims()["scope"]).split()
    assert {"work.execute", "command.cancel", "cluster.register", "clusters:read", "work-items:read"} <= set(scopes)


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


def test_cluster_tool_submits_register_command(monkeypatch):
    requests = []

    def request(method, url, token, body=None) -> tuple[int, dict]:
        requests.append((method, url, body))
        return 202, {"received": True}

    monkeypatch.setattr(deploy_kind_execution_target, "ep_request", request)
    integration_id = uuid4()
    deploy_kind_execution_target.register_cluster(
        "https://ep.test",
        "token",
        integration_id,
        name="workers",
        endpoint="https://cluster.test",
        credential_ref=uuid4(),
        ca_certificate=None,
    )
    assert len(requests) == 1
    method, url, body = requests[0]
    assert (method, url) == ("POST", "https://ep.test/v1/commands")
    message = MessageEnvelope.model_validate(body)
    CommandCatalog().validate(message)
    assert message.command.command_type == "cluster.register"
    assert message.command.payload["resource_id"] == str(integration_id)
    assert message.command.payload["is_default"] is True


def test_kind_setup_registers_cluster_then_target_with_explicit_namespace(monkeypatch):
    requests = []
    monkeypatch.setattr(deploy_kind_execution_target, "ep_service_token", lambda: "token")
    monkeypatch.setattr(deploy_kind_execution_target, "wait_for_cluster", lambda *_args: {"status": "active"})
    monkeypatch.setattr(deploy_kind_execution_target, "list_execution_targets", lambda *_args, **_kwargs: [])

    def request(method, url, token, body=None) -> tuple[int, dict]:
        requests.append(MessageEnvelope.model_validate(body))
        return 202, {"received": True}

    monkeypatch.setattr(deploy_kind_execution_target, "ep_request", request)
    deploy_kind_execution_target.register_kind_cluster(
        api_url="https://ep.test",
        cluster_name="kind",
        binding_name="local",
        endpoint="https://cluster.test",
        namespace="explicit-namespace",
        credential_ref=uuid4(),
        ca_certificate=None,
        wait_seconds=10,
    )
    assert [message.command.command_type for message in requests] == ["cluster.register", "execution_target.register"]
    for message in requests:
        CommandCatalog().validate(message)
    cluster = requests[0].command.payload
    target = requests[1].command.payload
    assert "namespace" not in cluster
    assert target["cluster_id"] == cluster["resource_id"]
    assert target["placement"] == {
        "type": "kubernetes",
        "namespace": "explicit-namespace",
        "node_selectors": [],
        "tolerations": [],
    }
    assert target["is_default"] is True


def test_target_setup_reuses_existing_target_without_registering_another(monkeypatch):
    cluster_id, target_id = uuid4(), uuid4()
    requests = []
    monkeypatch.setattr(
        deploy_kind_execution_target,
        "list_execution_targets",
        lambda *_args, **_kwargs: [{"id": str(target_id), "cluster_id": str(cluster_id), "name": "local"}],
    )

    def request(method, url, token, body=None) -> tuple[int, dict]:
        requests.append(MessageEnvelope.model_validate(body))
        return 202, {"received": True}

    monkeypatch.setattr(deploy_kind_execution_target, "ep_request", request)
    deploy_kind_execution_target.register_target(
        "https://ep.test",
        "token",
        cluster_id,
        name="local",
        endpoint="https://cluster.test",
        namespace="new",
    )
    assert len(requests) == 1
    command = CommandCatalog().validate(requests[0])
    assert command.command_type == "execution_target.update"
    assert command.resource_id == target_id
    assert command.placement.namespace == "new"
    assert command.is_default is True


def test_target_setup_never_adopts_another_clusters_target(monkeypatch):
    cluster_id = uuid4()
    requests = []
    monkeypatch.setattr(
        deploy_kind_execution_target,
        "list_execution_targets",
        lambda *_args, **_kwargs: [{"id": str(uuid4()), "cluster_id": str(uuid4()), "name": "local"}],
    )

    def request(method, url, token, body=None) -> tuple[int, dict]:
        requests.append(MessageEnvelope.model_validate(body))
        return 202, {"received": True}

    monkeypatch.setattr(deploy_kind_execution_target, "ep_request", request)
    for _ in range(2):
        deploy_kind_execution_target.register_target(
            "https://ep.test",
            "token",
            cluster_id,
            name="local",
            endpoint="https://cluster.test",
            namespace="chosen",
        )
    assert all(message.command.command_type == "execution_target.register" for message in requests)
    assert requests[0].command.payload["resource_id"] == requests[1].command.payload["resource_id"]
    assert requests[0].command.payload["cluster_id"] == str(cluster_id)


def test_target_lookup_filters_by_cluster_and_name_on_the_server(monkeypatch):
    from urllib.parse import parse_qs, urlparse

    requests = []
    cluster_id = uuid4()

    def request(method, url, token, body=None) -> tuple[int, dict]:
        requests.append(url)
        return 200, {"resources": []}

    monkeypatch.setattr(work_item_client, "ep_request", request)
    assert (
        work_item_client.list_execution_targets(
            "https://ep.test",
            "token",
            cluster_id=cluster_id,
            name="local target",
        )
        == []
    )
    assert parse_qs(urlparse(requests[0]).query) == {"cluster_id": [str(cluster_id)], "name": ["local target"]}
