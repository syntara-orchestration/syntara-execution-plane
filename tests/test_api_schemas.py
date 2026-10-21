"""Request-schema validation for the standalone Execution Plane API."""

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from execution_plane.api.schemas import WorkItemRead
from execution_plane.models.work_item import WorkItemStatus
from execution_plane_client.commands.work import WorkExecute


def _payload(**invocation_overrides: object) -> dict[str, Any]:
    invocation: dict[str, Any] = {
        "workflow_run_id": "workflow-1",
        "step_revision_id": "script.v1",
        "inputs": {},
        "timeout_seconds": 60,
    }
    invocation.update(invocation_overrides)
    return {"invocation": invocation, "image": "registry.example/node:latest"}


def test_work_item_submit_accepts_a_versioned_node_invocation() -> None:
    WorkExecute.model_validate(_payload())


def test_work_item_read_excludes_completion_delivery_internals() -> None:
    item = SimpleNamespace(
        id=uuid4(),
        client_id="ao",
        status=WorkItemStatus.COMPLETED,
        result={"output": "ok"},
        created_at=datetime.now(UTC),
        claimed_at=None,
        completed_at=datetime.now(UTC),
        resource_cleanup_status="not_required",
        resource_cleanup_error=None,
        completion_notification_state="delivering",
    )

    read = WorkItemRead.model_validate(item)

    assert set(read.model_dump()) == {
        "id",
        "status",
        "result",
        "result_omitted",
        "result_size_bytes",
        "created_at",
        "claimed_at",
        "completed_at",
        "resource_cleanup_status",
        "resource_cleanup_error",
    }


def test_non_object_credentials_raise_validation_error_not_type_error() -> None:
    with pytest.raises(
        ValidationError,
        match=r"invocation\.inputs",
    ) as caught:
        WorkExecute.model_validate(_payload(inputs="not-an-object"))
    assert not isinstance(caught.value.__cause__, TypeError)


@pytest.mark.parametrize("field", ["inputs", "runtime_context"])
def test_work_item_submit_rejects_non_object_invocation_fields(field: str) -> None:
    payload = _payload(**{field: "not-an-object"})
    with pytest.raises(ValidationError, match=rf"invocation\.{field}"):
        WorkExecute.model_validate(payload)


@pytest.mark.parametrize("field", ["credentials", "credential_file_paths", "workflow_context", "settings", "version"])
def test_legacy_and_unmounted_credential_fields_are_rejected(field: str) -> None:
    payload = _payload(**{field: {}})
    with pytest.raises(ValidationError, match=rf"invocation\.{field}"):
        WorkExecute.model_validate(payload)


def test_script_environment_variables_remain_inputs() -> None:
    payload = WorkExecute.model_validate(_payload(inputs={"environment": {"GREETING": "hello"}}))
    assert payload.invocation.inputs["environment"] == {"GREETING": "hello"}


@pytest.mark.parametrize("field", ["workflow_run_id", "step_revision_id"])
@pytest.mark.parametrize("value", ["", "bad/id", "*", "x" * 257])
def test_sdk_identities_are_validated_before_dispatch(field: str, value: str) -> None:
    payload = _payload(**{field: value})
    with pytest.raises(ValidationError, match=rf"invocation\.{field}"):
        WorkExecute.model_validate(payload)


def test_sdk_timeout_limit_is_validated_before_dispatch() -> None:
    payload = _payload(timeout_seconds=86_401)
    with pytest.raises(ValidationError, match="timeout_seconds"):
        WorkExecute.model_validate(payload)
