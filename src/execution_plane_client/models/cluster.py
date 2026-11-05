"""Shared cluster list and response contracts."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from execution_plane_client.models.shared import EPListParams, ResourcesResponse

# Cluster query metadata - single source of truth for EP and client
CLUSTER_FILTERABLE_FIELDS = [
    "id",
    "created_at",
    "updated_at",
    "name",
    "endpoint",
    "status",
    "status_message",
    "enabled",
    "is_default",
]

CLUSTER_SORTABLE_FIELDS = [
    "id",
    "created_at",
    "updated_at",
    "name",
    "endpoint",
    "status",
    "enabled",
    "is_default",
]


class ClusterRead(BaseModel):
    """Provisioned cluster state, keyed by the actual cluster UUID."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(description="Cluster unique identifier")
    name: str = Field(description="Cluster name")
    endpoint: str = Field(description="Cluster API endpoint URL")
    status: str = Field(description="Cluster lifecycle status")
    status_message: str | None = Field(default=None, description="Human-readable status message or error details")
    enabled: bool = Field(description="Whether the cluster is enabled for use")
    is_default: bool = Field(description="Whether this is the default cluster")
    labels: dict[str, str] = Field(description="Key-value labels for the cluster")
    created_at: datetime = Field(description="Creation timestamp")
    updated_at: datetime = Field(description="Last update timestamp")


class ClusterListParams(EPListParams):
    """Query parameters for listing clusters."""

    id: str | None = Field(default=None, description="Filter by id")
    name: str | None = Field(default=None, description="Filter by name")
    endpoint: str | None = Field(default=None, description="Filter by endpoint")
    status: str | None = Field(default=None, description="Filter by status")
    status_message: str | None = Field(default=None, description="Filter by status_message")
    enabled: str | None = Field(default=None, description="Filter by enabled")
    is_default: str | None = Field(default=None, description="Filter by is_default")
    created_at: str | None = Field(default=None, description="Filter by created_at")
    updated_at: str | None = Field(default=None, description="Filter by updated_at")


class ClusterListResponse(ResourcesResponse[ClusterRead]):
    """Paginated list response for clusters."""
