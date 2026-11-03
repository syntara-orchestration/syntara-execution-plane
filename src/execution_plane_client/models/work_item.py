"""Shared work-item list and response contracts."""

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from execution_plane_client.models.shared import EPListParams


class WorkItemStatus(StrEnum):
    """Lifecycle states of a dispatched work item."""

    PENDING = "pending"
    CLAIMED = "claimed"
    DISPATCHED = "dispatched"
    RUNNING = "running"
    CANCEL_REQUESTED = "cancel_requested"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class WorkItemListParams(EPListParams):
    """Work-item query controls and documented filter fields."""

    id: str | None = Field(default=None, description="Work UUID; id[in]=uuid1,uuid2 for membership")
    status: str | None = Field(default=None, description="Work status; supports eq and in")
    created_at: str | None = Field(
        default=None, description="Creation time; supports eq, in, gt, gte, lt, lte, and isnull"
    )
    claimed_at: str | None = Field(default=None, description="Claim time; supports ranges and isnull")
    completed_at: str | None = Field(default=None, description="Completion time; supports ranges and isnull")
    resource_cleanup_status: str | None = Field(default=None, description="Cleanup status; supports eq and in")
    resource_cleanup_error: str | None = Field(
        default=None, description="Cleanup error; supports text operators and isnull"
    )
    sort: Literal[
        "created_at",
        "-created_at",
        "status",
        "-status",
        "resource_cleanup_status",
        "-resource_cleanup_status",
        "id",
        "-id",
    ] = Field(default="-created_at", description="Work-item sort field; newest first by default.")


class WorkItemRead(BaseModel):
    """Safe work-item representation; excludes credentials and persistence internals."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    status: WorkItemStatus
    result: dict[str, Any] | None
    result_omitted: bool = False
    result_size_bytes: int | None = None
    created_at: datetime
    claimed_at: datetime | None
    completed_at: datetime | None
    resource_cleanup_status: str
    resource_cleanup_error: str | None
