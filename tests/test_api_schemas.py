"""Request-schema validation for the standalone Execution Plane API."""

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from execution_plane.api.schemas import WorkItemRead
from execution_plane.models.work_item import NotificationState, WorkItemStatus
from execution_plane_client.commands.work import WorkExecute


def _payload(**invocation_overrides: object) -> dict[str, Any]:
    invocation: dict[str, Any] = {
        "version": 1,
        "operation": "execute",
        "inputs": {},
        "credentials": {},
        "workflow_context": {},
        "settings": {},
        "timeout_seconds": 60,
        "max_output_bytes": 1024,
    }
    invocation.update(invocation_overrides)
    return {"invocation": invocation, "image": "registry.example/node:latest"}


def test_work_item_submit_accepts_a_versioned_node_invocation() -> None:
    WorkExecute.model_validate(_payload())


def test_work_item_read_exposes_inline_completion_command_id() -> None:
    completion_command_id = uuid4()
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
        completion_command_id=completion_command_id,
        notification_state=NotificationState.DELIVERED,
    )

    read = WorkItemRead.model_validate(item)

    assert read.completion_command_id == completion_command_id


def test_non_object_credentials_raise_validation_error_not_type_error() -> None:
    with pytest.raises(
        ValidationError,
        match=r"invocation\.credentials",
    ) as caught:
        WorkExecute.model_validate(_payload(credentials="not-an-object"))
    assert not isinstance(caught.value.__cause__, TypeError)


@pytest.mark.parametrize("field", ["inputs", "credentials", "workflow_context", "settings"])
def test_work_item_submit_rejects_non_object_invocation_fields(field: str) -> None:
    with pytest.raises(ValidationError, match=rf"invocation\.{field}"):
        WorkExecute.model_validate(_payload(**{field: "not-an-object"}))
