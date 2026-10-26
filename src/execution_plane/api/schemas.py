"""Credential-free EP resource responses; command contracts live in the client."""

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from execution_plane.models.execution_target import BackendType, TargetStatus
from execution_plane.models.execution_target_placement import ExecutionTargetPlacement
from execution_plane.models.work_item import WorkItemStatus


class WorkItemRead(BaseModel):
    """Safe work-item representation; excludes credentials and persistence internals."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    status: WorkItemStatus
    result: dict[str, Any] | None
    result_omitted: bool = False
    result_size_bytes: int | None = None
    created_at: datetime
    claimed_at: datetime | None
    completed_at: datetime | None
    resource_cleanup_status: str
    resource_cleanup_error: str | None
    completion_command_id: uuid.UUID | None = None


class ExecutionTargetRead(BaseModel):
    """Safe execution-target representation without management credentials."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    cluster_id: uuid.UUID
    name: str
    backend_type: BackendType
    endpoint: str
    placement: ExecutionTargetPlacement
    status: TargetStatus
    enabled: bool
    is_default: bool
    status_message: str | None
    labels: dict[str, str]
    created_at: datetime
    last_ran_at: datetime | None


class ClusterRead(BaseModel):
    """Desired and observed cluster binding state, keyed by integration UUID."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID = Field(validation_alias="source_integration_id")
    name: str
    endpoint: str
    namespace: str
    desired_revision: int = Field(validation_alias="revision")
    observed_revision: int
    cluster_id: uuid.UUID | None
    status: str
    status_message: str | None
    enabled: bool
    is_default: bool
    labels: dict[str, str]
    project_ids: list[uuid.UUID] | None
    created_at: datetime
    updated_at: datetime
