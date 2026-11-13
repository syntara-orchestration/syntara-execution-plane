"""Resource identity is independent of delivery messages and caller metadata."""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.commands.control import Completion, WorkCancel
from execution_plane_client.commands.credentials import CredentialResolveWorkload
from execution_plane_client.commands.work import WorkExecute
from execution_plane_client.protocol import MessageEnvelope
from tests.test_api_schemas import _payload


def test_rebuilt_submission_changes_only_delivery_identity():
    command = WorkExecute.model_validate(_payload() | {"metadata": {"nested": {"flags": [True, None, 3]}}})
    first, second = [command.to_message(source_id="ao", destination_id="ep", client_id="ao") for _ in range(2)]
    assert first.message_id != second.message_id
    assert first.command.payload == second.command.payload
    assert CommandCatalog().validate(second).work_item_id == command.work_item_id
    assert "metadata" not in command.invocation.model_dump()


@pytest.mark.parametrize("location", ["envelope", "command"])
@pytest.mark.parametrize("field", ["command_id", "parent_command_id"])
def test_legacy_identity_fields_are_rejected(location, field):
    body = (
        WorkCancel(work_item_id=uuid4())
        .to_message(source_id="ao", destination_id="ep", client_id="ao")
        .model_dump(mode="json")
    )
    target = body if location == "envelope" else body["command"]
    target[field] = str(uuid4())
    with pytest.raises(ValidationError, match="Extra inputs"):
        MessageEnvelope.model_validate(body)


@pytest.mark.parametrize("value", [None, "bad-uuid", 42])
@pytest.mark.parametrize("model", [WorkExecute, WorkCancel, Completion, CredentialResolveWorkload])
def test_work_item_identity_is_required_and_validated(model, value):
    payloads = {
        WorkExecute: _payload(),
        WorkCancel: {},
        Completion: {"status": "completed"},
        CredentialResolveWorkload: {
            "attempt_id": "attempt-1",
            "credential_ref": str(uuid4()),
            "credential_recipe_id": "acme.token",
            "workflow_run_id": "run",
            "step_revision_id": "script.v1",
        },
    }
    payload = payloads[model]
    payload.pop("work_item_id", None)
    with pytest.raises(ValidationError, match="work_item_id"):
        model.model_validate(payload)
    with pytest.raises(ValidationError, match="work_item_id"):
        model.model_validate(payload | {"work_item_id": value})


@pytest.mark.parametrize("metadata", [[], "text", {"number": float("nan")}, {1: "key"}, {"large": "x" * (512 * 1024)}])
@pytest.mark.parametrize("model", [WorkExecute, Completion])
def test_metadata_requires_bounded_finite_json_object(model, metadata):
    payload = _payload() if model is WorkExecute else {"work_item_id": str(uuid4()), "status": "completed"}
    with pytest.raises(ValidationError):
        model.model_validate(payload | {"metadata": metadata})


def test_cancel_rejects_legacy_target_command_id():
    with pytest.raises(ValidationError):
        WorkCancel.model_validate({"target_command_id": str(uuid4())})
