"""Resource identity is independent of delivery messages and caller runtime_context."""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.commands.control import Completion
from execution_plane_client.commands.credentials import CredentialResolveWorkload
from execution_plane_client.commands.work import WorkExecute
from tests.test_api_schemas import _payload


def test_rebuilt_submission_changes_only_delivery_identity():
    command = WorkExecute.model_validate(_payload() | {"runtime_context": {"nested": {"flags": [True, None, 3]}}})
    first, second = [command.to_message(source_id="ao", destination_id="ep", client_id="ao") for _ in range(2)]
    assert first.message_id != second.message_id
    assert first.command.payload == second.command.payload
    assert CommandCatalog().validate(second).work_item_id == command.work_item_id


@pytest.mark.parametrize("value", [None, "bad-uuid", 42])
@pytest.mark.parametrize("model", [WorkExecute, Completion, CredentialResolveWorkload])
def test_work_item_identity_is_required_and_validated(model, value):
    payloads = {
        WorkExecute: _payload(),
        Completion: {"status": "completed"},
        CredentialResolveWorkload: {
            "attempt_id": "attempt-1",
            "credential_ref": str(uuid4()),
            "credential_recipe_id": "acme.token",
            "step_revision_id": "script.v1",
        },
    }
    payload = payloads[model]
    payload.pop("work_item_id", None)
    with pytest.raises(ValidationError, match="work_item_id"):
        model.model_validate(payload)
    with pytest.raises(ValidationError, match="work_item_id"):
        model.model_validate(payload | {"work_item_id": value})


@pytest.mark.parametrize(
    "runtime_context", [[], "text", {"number": float("nan")}, {1: "key"}, {"large": "x" * (512 * 1024)}]
)
@pytest.mark.parametrize("model", [WorkExecute, Completion])
def test_runtime_context_requires_bounded_finite_json_object(model, runtime_context):
    payload = _payload() if model is WorkExecute else {"work_item_id": str(uuid4()), "status": "completed"}
    with pytest.raises(ValidationError):
        model.model_validate(payload | {"runtime_context": runtime_context})
