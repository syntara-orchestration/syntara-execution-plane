"""Request-schema validation for the standalone Execution Plane API."""

from typing import Any

import pytest
from pydantic import ValidationError

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


def test_running_work_item_serializes_through_the_public_response_model() -> None:
    from datetime import UTC, datetime
    from uuid import uuid4

    from execution_plane.api.schemas import WorkItemRead
    from execution_plane.models.work_item import WorkItemStatus

    response = WorkItemRead(
        id=uuid4(),
        status=WorkItemStatus.RUNNING,
        result=None,
        created_at=datetime.now(UTC),
        claimed_at=datetime.now(UTC),
        completed_at=None,
        resource_cleanup_status="pending",
        resource_cleanup_error=None,
    )
    assert response.model_dump(mode="json")["status"] == "running"
