"""Shared work-item list and response contracts."""

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from execution_plane_client.models.shared import _SORT_PATTERN, EPListParams, ResourcesResponse

# Work item query metadata - single source of truth for EP and client
WORK_ITEM_FILTERABLE_FIELDS = [
    "id",
    "execution_target_id",
    "status",
    "created_at",
    "claimed_at",
    "completed_at",
    "resource_cleanup_status",
    "resource_cleanup_error",
]

WORK_ITEM_SORTABLE_FIELDS = [
    "created_at",
    "status",
    "resource_cleanup_status",
    "id",
]


class WorkItemStatus(StrEnum):
    """Lifecycle states of a dispatched work item."""

    PENDING = "pending"
    CLAIMED = "claimed"
    DISPATCHED = "dispatched"
    RUNNING = "running"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class WorkItemRead(BaseModel):
    """Safe work-item representation; excludes credentials and persistence internals."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(description="Work item unique identifier")
    status: WorkItemStatus = Field(description="Work item lifecycle status")
    cancellation_requested: bool = Field(
        default=False,
        description=(
            "Whether cancellation intent has been persisted. A true value does not confirm that execution "
            "has stopped: status remains dispatched or running until an outcome is known. A result may "
            "win the cancellation race. This flag remains true after completion and is also set by deletion."
        ),
    )
    deletion_requested: bool = Field(
        default=False,
        description=(
            "Whether deletion intent has been persisted. Always implies cancellation_requested=true. "
            "Active work is cancelled asynchronously, and the record remains readable until resource "
            "cleanup is confirmed and the record is removed. This flag does not change an existing "
            "terminal outcome or confirm that deletion has completed."
        ),
    )
    result: dict[str, Any] | None = Field(default=None, description="Work execution result payload")
    result_omitted: bool = Field(default=False, description="Whether result was omitted due to size")
    result_size_bytes: int | None = Field(default=None, description="Size of the result in bytes")
    created_at: datetime = Field(description="Creation timestamp")
    claimed_at: datetime | None = Field(default=None, description="Claim timestamp")
    completed_at: datetime | None = Field(default=None, description="Completion timestamp")
    resource_cleanup_status: str = Field(description="Resource cleanup status")
    resource_cleanup_error: str | None = Field(default=None, description="Resource cleanup error details")


class WorkItemListParams(EPListParams):
    """Query parameters for listing work items."""

    id: str | None = Field(default=None, description="Filter by id")
    execution_target_id: str | None = Field(default=None, description="Filter by execution_target_id")
    status: str | None = Field(default=None, description="Filter by status")
    created_at: str | None = Field(default=None, description="Filter by created_at")
    claimed_at: str | None = Field(default=None, description="Filter by claimed_at")
    completed_at: str | None = Field(default=None, description="Filter by completed_at")
    resource_cleanup_status: str | None = Field(default=None, description="Filter by resource_cleanup_status")
    resource_cleanup_error: str | None = Field(default=None, description="Filter by resource_cleanup_error")

    sort: Annotated[str, StringConstraints(pattern=_SORT_PATTERN)] = Field(
        default="-created_at", description="Sort field; prefix with '-' for descending order."
    )


class WorkItemListResponse(ResourcesResponse[WorkItemRead]):
    """Paginated list response for work items."""
