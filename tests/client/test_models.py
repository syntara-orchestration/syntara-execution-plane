"""Shared Execution Plane resource response contracts."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from execution_plane_client.commands.targets import TargetRegister, TargetUpdate
from execution_plane_client.models.cluster import ClusterListParams, ClusterRead
from execution_plane_client.models.execution_target import (
    ExecutionTargetCreate,
    ExecutionTargetDeleteAccepted,
    ExecutionTargetHealthResult,
    ExecutionTargetListParams,
    ExecutionTargetRead,
    ExecutionTargetUpdate,
)
from execution_plane_client.models.shared import ResourcesResponse
from execution_plane_client.models.work_item import WorkItemListParams, WorkItemRead


def _work_item_payload() -> dict[str, object]:
    return {
        "id": str(uuid4()),
        "status": "completed",
        "result": {"output": "ok"},
        "result_omitted": False,
        "result_size_bytes": 2,
        "created_at": datetime.now(UTC),
        "claimed_at": None,
        "completed_at": datetime.now(UTC),
        "resource_cleanup_status": "not_required",
        "resource_cleanup_error": None,
    }


def test_work_item_read_validates_shared_response() -> None:
    read = WorkItemRead.model_validate(_work_item_payload())

    assert read.status.value == "completed"
    assert read.result == {"output": "ok"}


def test_work_item_read_rejects_unknown_status() -> None:
    payload = _work_item_payload()
    payload["status"] = "unknown"

    with pytest.raises(ValidationError, match="status"):
        WorkItemRead.model_validate(payload)


def test_execution_target_read_validates_discriminated_placement() -> None:
    payload: dict[str, object] = {
        "id": str(uuid4()),
        "cluster_id": str(uuid4()),
        "name": "target",
        "backend_type": "kubernetes",
        "endpoint": "https://cluster.example",
        "placement": {"type": "kubernetes", "namespace": "execution"},
        "status": "active",
        "enabled": True,
        "is_default": False,
        "status_message": None,
        "labels": {},
        "created_at": datetime.now(UTC),
        "last_ran_at": None,
    }

    read = ExecutionTargetRead.model_validate(payload)

    assert read.status.value == "active"
    assert read.placement.type == "kubernetes"


def test_execution_target_read_rejects_invalid_placement_selector() -> None:
    payload: dict[str, object] = {
        "id": str(uuid4()),
        "cluster_id": str(uuid4()),
        "name": "target",
        "backend_type": "kubernetes",
        "endpoint": "https://cluster.example",
        "placement": {
            "type": "kubernetes",
            "namespace": "execution",
            "node_selectors": ["invalid"],
        },
        "status": "active",
        "enabled": True,
        "is_default": False,
        "status_message": None,
        "labels": {},
        "created_at": datetime.now(UTC),
        "last_ran_at": None,
    }

    with pytest.raises(ValidationError, match="node_selectors"):
        ExecutionTargetRead.model_validate(payload)


def test_cluster_read_validates_safe_cluster_state() -> None:
    payload: dict[str, object] = {
        "id": str(uuid4()),
        "name": "cluster",
        "endpoint": "https://cluster.example",
        "status": "active",
        "status_message": None,
        "enabled": True,
        "is_default": True,
        "labels": {},
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    }

    read = ClusterRead.model_validate(payload)

    assert read.name == "cluster"


def test_resources_response_serializes_concrete_models() -> None:
    response = ResourcesResponse[WorkItemRead](resources=[WorkItemRead.model_validate(_work_item_payload())])

    assert response.resources[0].status.value == "completed"


def test_list_params_share_resource_specific_sort_defaults() -> None:
    assert WorkItemListParams().sort == "-created_at"
    assert ExecutionTargetListParams().sort == "created_at"
    assert ClusterListParams().sort == "created_at"


def test_list_params_have_filter_fields() -> None:
    """List params have filter fields with simple descriptions matching syntara."""
    # Filter fields exist
    assert "id" in ClusterListParams.model_fields
    assert "name" in ClusterListParams.model_fields
    assert "status" in ClusterListParams.model_fields
    assert "execution_target_id" in WorkItemListParams.model_fields

    # Simple "Filter by X" descriptions like syntara
    assert ClusterListParams.model_fields["name"].description == "Filter by name"
    assert ExecutionTargetListParams.model_fields["endpoint"].description == "Filter by endpoint"
    assert WorkItemListParams.model_fields["status"].description == "Filter by status"
    assert WorkItemListParams.model_fields["execution_target_id"].description == "Filter by execution_target_id"

    # All filter fields are optional strings (query params)
    assert ClusterListParams.model_fields["name"].annotation == str | None
    assert ClusterListParams.model_fields["name"].default is None


def test_execution_target_write_contracts_build_shared_commands() -> None:
    create = ExecutionTargetCreate.model_validate(
        {
            "name": "batch",
            "placement": {"type": "kubernetes", "namespace": "workloads", "node_selectors": ["pool=batch"]},
            "labels": {"environment": "test"},
            "is_default": True,
        }
    )
    register = TargetRegister(
        resource_id=uuid4(),
        cluster_id=uuid4(),
        name=create.name,
        backend_type="kubernetes",
        endpoint="https://cluster.example",
        credential_ref=uuid4(),
        placement=create.placement,
        labels=create.labels,
        is_default=create.is_default,
    )

    assert register.placement is create.placement
    assert register.placement.namespace == "workloads"

    update = ExecutionTargetUpdate.model_validate({"name": "renamed"})
    command = TargetUpdate(resource_id=uuid4(), **update.model_dump(exclude_unset=True))

    assert command.name == "renamed"


def test_execution_target_update_rejects_empty_and_null_patches() -> None:
    with pytest.raises(ValidationError, match="at least one target field"):
        ExecutionTargetUpdate.model_validate({})

    with pytest.raises(ValidationError, match="cannot be null"):
        ExecutionTargetUpdate.model_validate({"name": None})


def test_execution_target_lifecycle_results_are_shared_contracts() -> None:
    target_id = uuid4()
    accepted = ExecutionTargetDeleteAccepted(id=target_id)
    health = ExecutionTargetHealthResult(healthy=True, message="probe succeeded", skipped=False)

    assert accepted.model_dump(mode="json") == {"id": str(target_id), "accepted": True}
    assert health.model_dump(mode="json") == {
        "healthy": True,
        "message": "probe succeeded",
        "skipped": False,
    }
