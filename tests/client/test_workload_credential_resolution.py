"""Contract tests for workload credential resolution."""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import ValidationError

from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.commands.credentials import CredentialResolveWorkload
from execution_plane_client.commands.results import CredentialResolveWorkloadCommandResult
from execution_plane_client.errors import ProtocolConflict


def _command() -> CredentialResolveWorkload:
    return CredentialResolveWorkload(
        attempt_id=f"{uuid4()}-2",
        credential_ref=uuid4(),
        credential_recipe_id="acme.echo.api-token",
        workflow_run_id="workflow-1",
        step_revision_id="script.v1",
    )


def test_workload_resolution_command_correlates_to_its_work_item() -> None:
    command = _command()
    parent_command_id = uuid4()
    message = command.to_message(
        source_id="execution-plane",
        destination_id="syntara-orchestration",
        client_id="execution-plane",
        parent_command_id=parent_command_id,
    )

    payload = CommandCatalog().validate(message)

    assert isinstance(payload, CredentialResolveWorkload)
    assert payload == command
    assert message.command.parent_command_id == parent_command_id
    assert message.command.payload == command.model_dump(mode="json")
    assert "work_item_id" not in message.command.payload
    assert "correlation_ids" not in message.command.payload


def test_workload_resolution_command_requires_the_work_item_parent() -> None:
    command = _command()

    with pytest.raises(ProtocolConflict):
        command.to_message(
            source_id="execution-plane",
            destination_id="syntara-orchestration",
            client_id="execution-plane",
        )


def test_workload_resolution_result_supports_bounded_structured_documents() -> None:
    credential = {"username": "service-account", "token": "secret-token"}
    result = CredentialResolveWorkloadCommandResult(credential=credential)

    assert result.credential["token"] == credential["token"]
    assert "secret-token" not in repr(result)


@pytest.mark.parametrize(
    ("credential", "match"),
    [
        ({}, "credential"),
        ({"field": "x" * 8193}, "credential"),
        ({f"field-{index}": "x" for index in range(33)}, "credential"),
        ({"field": "x", "extra": "x"} | {"secret": "y" * 32769}, "credential"),
    ],
)
def test_workload_resolution_result_rejects_oversized_documents(credential: dict[str, str], match: str) -> None:
    with pytest.raises(ValidationError, match=match):
        CredentialResolveWorkloadCommandResult(credential=credential)


def test_workload_resolution_command_rejects_malformed_context() -> None:
    command = _command().model_dump(mode="json")
    command["attempt_id"] = "not an identifier"

    with pytest.raises(ValidationError, match="attempt_id"):
        CredentialResolveWorkload.model_validate(command)

    with pytest.raises(ValidationError, match="credential_recipe_id"):
        CredentialResolveWorkload.model_validate(command | {"credential_recipe_id": "not-qualified"})
