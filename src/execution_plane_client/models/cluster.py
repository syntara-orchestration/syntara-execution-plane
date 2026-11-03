"""Shared cluster list and response contracts."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from execution_plane_client.models.shared import EPListParams


class ClusterListParams(EPListParams):
    """Cluster query controls and documented filter fields."""

    id: str | None = Field(default=None, description="Cluster UUID; id[in]=uuid1,uuid2 for membership")
    created_at: str | None = Field(
        default=None, description="Creation time; supports eq, in, gt, gte, lt, lte, and isnull"
    )
    updated_at: str | None = Field(default=None, description="Update time; supports ranges and isnull")
    name: str | None = Field(default=None, description="Name; supports eq, in, contains, and starts_with")
    endpoint: str | None = Field(default=None, description="Endpoint; supports eq, in, contains, and starts_with")
    status: str | None = Field(default=None, description="Cluster status; supports eq and in")
    status_message: str | None = Field(default=None, description="Status message; supports text operators and isnull")
    enabled: str | None = Field(default=None, description="Boolean; supports eq, in, and isnull")
    is_default: str | None = Field(default=None, description="Boolean; supports eq, in, and isnull")
    sort: Literal[
        "id",
        "-id",
        "created_at",
        "-created_at",
        "name",
        "-name",
        "endpoint",
        "-endpoint",
        "status",
        "-status",
        "enabled",
        "-enabled",
        "is_default",
        "-is_default",
        "updated_at",
        "-updated_at",
    ] = Field(default="created_at", description="Cluster sort field; oldest first by default.")


class ClusterRead(BaseModel):
    """Provisioned cluster state, keyed by the actual cluster UUID."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    endpoint: str
    status: str
    status_message: str | None
    enabled: bool
    is_default: bool
    labels: dict[str, str]
    created_at: datetime
    updated_at: datetime
