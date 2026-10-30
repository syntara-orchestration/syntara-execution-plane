"""Shared contracts remain strict and independent of EP infrastructure."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError


def test_client_has_no_infrastructure_imports():
    import ast
    from pathlib import Path

    root = Path(__file__).resolve().parents[2] / "src/execution_plane_client"
    assert root.is_dir(), "The standalone client package must exist"
    forbidden = {"execution_plane", "fastapi", "sqlalchemy", "sqlmodel", "httpx", "jwt", "cryptography"}
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert not {alias.name.split(".")[0] for alias in node.names} & forbidden
            elif isinstance(node, ast.ImportFrom):
                assert (node.module or "").split(".")[0] not in forbidden


def test_registry_validates_and_advertises_only_installed_handlers():
    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.errors import ProtocolConflict, UnsupportedCommand
    from execution_plane_client.registry import CommandRegistry

    class Handler:
        async def handle(self, message, payload, context) -> None:
            return None

    registry = CommandRegistry()
    definition = CommandCatalog().definition("work.execute", 1)
    handler = Handler()
    registry.register(definition, handler)
    assert registry.get("work.execute", 1) is handler
    assert registry.capabilities().commands == {"work.execute": [1]}
    with pytest.raises(ProtocolConflict):
        registry.register(definition, handler)
    with pytest.raises(UnsupportedCommand):
        registry.get("work.execute", 2)
    with pytest.raises(UnsupportedCommand):
        registry.get("work.execute.completed", 1)


def test_completion_is_a_command_with_a_parent():
    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.errors import ProtocolConflict
    from execution_plane_client.protocol import Command, CommandScope, MessageEnvelope

    command_id = uuid4()
    message = MessageEnvelope(
        message_id=uuid4(),
        command_id=command_id,
        source_id="ep",
        destination_id="ao",
        scope=CommandScope(client_id="ao"),
        command=Command(
            command_id=command_id,
            command_type="work.execute.completed",
            command_revision=1,
            payload={"status": "succeeded", "result": {}},
            occurred_at=datetime.now(UTC),
        ),
    )
    with pytest.raises(ProtocolConflict):
        CommandCatalog().validate(message)
    complete = message.model_copy(update={"command": message.command.model_copy(update={"parent_command_id": uuid4()})})
    CommandCatalog().validate(complete)


@pytest.mark.parametrize("bad", [True, 0, -1])
def test_invocation_rejects_invalid_timeout(bad):
    from execution_plane_client.commands.work import WorkloadInvocation

    with pytest.raises(ValidationError):
        WorkloadInvocation(workflow_run_id="workflow-1", step_revision_id="script.v1", inputs={}, timeout_seconds=bad)


def test_cluster_commands_use_uuid_credential_refs():
    from execution_plane_client.commands.clusters import ClusterRegister

    credential_ref = uuid4()
    command = ClusterRegister(
        resource_id=uuid4(),
        name="workers",
        endpoint="https://cluster.example",
        credential_ref=credential_ref,
    )

    payload = command.model_dump(mode="json")

    assert payload["credential_ref"] == str(credential_ref)
    assert "credential" not in payload


def test_credential_resolution_contract_hides_value_from_repr_and_correlates_by_message_id():
    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.commands.credentials import CredentialResolution, CredentialResolve

    credential_ref = uuid4()
    message = CredentialResolve(credential_ref=credential_ref).to_message(
        source_id="execution-plane",
        destination_id="syntara-orchestration",
        client_id="syntara-orchestration",
    )

    payload = CommandCatalog().validate(message)
    resolution = CredentialResolution(message_id=message.message_id, credential="secret-token")

    assert isinstance(payload, CredentialResolve)
    assert payload.credential_ref == credential_ref
    assert message.command.payload == {"credential_ref": str(credential_ref)}
    assert resolution.message_id == message.message_id
    assert "secret-token" not in repr(resolution)
    with pytest.raises(ValidationError, match="credential"):
        CredentialResolution(message_id=message.message_id, credential="")
    with pytest.raises(ValidationError, match="credential"):
        CredentialResolution(message_id=message.message_id, credential="x" * 65537)
