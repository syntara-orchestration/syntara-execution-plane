"""Shared execution-target list, placement, and response contracts."""

import re
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.json_schema import SkipJsonSchema

from execution_plane_client.commands.targets import KubernetesPlacement as CommandKubernetesPlacement
from execution_plane_client.models.shared import EPListParams, ResourcesResponse

_LABEL_NAME = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?$")
_DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_TOLERATION_EFFECTS = {"", "NoSchedule", "PreferNoSchedule", "NoExecute"}
_MAX_LABEL_NAME_LENGTH = 63
_MAX_DNS_SUBDOMAIN_LENGTH = 253

# Execution target query metadata - single source of truth for EP and client
EXECUTION_TARGET_FILTERABLE_FIELDS = [
    "id",
    "created_at",
    "name",
    "endpoint",
    "status",
    "backend_type",
    "cluster_id",
    "enabled",
    "is_default",
    "last_ran_at",
    "status_message",
]

EXECUTION_TARGET_SORTABLE_FIELDS = [
    "id",
    "created_at",
    "name",
    "endpoint",
    "status",
    "backend_type",
    "cluster_id",
    "enabled",
    "is_default",
]


class BackendType(StrEnum):
    """Supported execution target backends."""

    KUBERNETES = "kubernetes"
    OPENSHELL = "openshell"


class TargetStatus(StrEnum):
    """Lifecycle states of an execution target."""

    REGISTERING = "registering"
    VALIDATING = "validating"
    BOOTSTRAPPING = "bootstrapping"
    ACTIVE = "active"
    DEGRADED = "degraded"
    DRAINING = "draining"
    FAILED = "failed"


def _is_valid_label_key(value: str) -> bool:
    """Return whether a value uses Kubernetes label-key syntax."""
    prefix, separator, name = value.partition("/")
    if separator:
        prefix_labels = prefix.split(".")
        if (
            not prefix
            or len(prefix) > _MAX_DNS_SUBDOMAIN_LENGTH
            or any(len(label) > _MAX_LABEL_NAME_LENGTH or not _DNS_LABEL.fullmatch(label) for label in prefix_labels)
        ):
            return False
    else:
        name = prefix
    return len(name) <= _MAX_LABEL_NAME_LENGTH and bool(_LABEL_NAME.fullmatch(name))


def _is_valid_label_value(value: str) -> bool:
    """Return whether a value uses Kubernetes label-value syntax."""
    return len(value) <= _MAX_LABEL_NAME_LENGTH and (value == "" or bool(_LABEL_NAME.fullmatch(value)))


class KubernetesPlacement(BaseModel):
    """Metadata required to place workers on Kubernetes."""

    type: Literal["kubernetes"] = "kubernetes"
    namespace: str = Field(
        min_length=1,
        max_length=63,
        pattern=r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$",
        description="Kubernetes namespace reserved for execution workloads",
    )
    node_selectors: list[str] = Field(default_factory=list)
    tolerations: list[str] = Field(default_factory=list)

    @field_validator("node_selectors")
    @classmethod
    def validate_node_selectors(cls, values: list[str]) -> list[str]:
        """Validate compact Kubernetes node-selector expressions."""
        for value in values:
            key, separator, selector_value = value.partition("=")
            if not separator or not _is_valid_label_key(key) or not _is_valid_label_value(selector_value):
                message = "must use a valid Kubernetes label selector in key=value form"
                raise ValueError(message)
        return values

    @field_validator("tolerations")
    @classmethod
    def validate_tolerations(cls, values: list[str]) -> list[str]:
        """Validate compact Kubernetes toleration expressions."""
        for value in values:
            key_value, separator, effect = value.rpartition(":")
            if not separator:
                key_value = value
                effect = ""
            key, equals, toleration_value = key_value.partition("=")
            if equals:
                valid_key = bool(key) and _is_valid_label_key(key)
                valid_value = _is_valid_label_value(toleration_value)
            else:
                valid_key = not key or _is_valid_label_key(key)
                valid_value = not toleration_value
            if not valid_key or not valid_value or effect not in _TOLERATION_EFFECTS:
                message = "must use a valid Kubernetes toleration short form"
                raise ValueError(message)
        return values


class RHELPlacement(BaseModel):
    """Placeholder for RHEL-specific execution-target metadata."""

    type: Literal["rhel"] = "rhel"


ExecutionTargetPlacementTypes = KubernetesPlacement | RHELPlacement
ExecutionTargetPlacement = Annotated[
    ExecutionTargetPlacementTypes,
    Field(discriminator="type"),
]


class ExecutionTargetRead(BaseModel):
    """Safe execution-target representation without management credentials."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(description="Execution target unique identifier")
    cluster_id: UUID = Field(description="Parent cluster identifier")
    name: str = Field(description="Execution target name")
    backend_type: BackendType = Field(description="Backend execution platform type")
    endpoint: str = Field(description="Execution target API endpoint URL")
    placement: ExecutionTargetPlacement = Field(description="Platform-specific placement configuration")
    status: TargetStatus = Field(description="Execution target lifecycle status")
    enabled: bool = Field(description="Whether the target is enabled for use")
    is_default: bool = Field(description="Whether this is the default target for the cluster")
    status_message: str | None = Field(default=None, description="Human-readable status message or error details")
    labels: dict[str, str] = Field(description="Key-value labels for the target")
    created_at: datetime = Field(description="Creation timestamp")
    last_ran_at: datetime | None = Field(default=None, description="Last execution timestamp")


class ExecutionTargetListParams(EPListParams):
    """Query parameters for listing execution targets."""

    id: str | None = Field(default=None, description="Filter by id")
    cluster_id: str | None = Field(default=None, description="Filter by cluster_id")
    name: str | None = Field(default=None, description="Filter by name")
    backend_type: str | None = Field(default=None, description="Filter by backend_type")
    endpoint: str | None = Field(default=None, description="Filter by endpoint")
    status: str | None = Field(default=None, description="Filter by status")
    enabled: str | None = Field(default=None, description="Filter by enabled")
    is_default: str | None = Field(default=None, description="Filter by is_default")
    status_message: str | None = Field(default=None, description="Filter by status_message")
    created_at: str | None = Field(default=None, description="Filter by created_at")
    last_ran_at: str | None = Field(default=None, description="Filter by last_ran_at")


class ExecutionTargetListResponse(ResourcesResponse[ExecutionTargetRead]):
    """Paginated list response for execution targets."""


class KubernetesTargetPlacement(CommandKubernetesPlacement):
    """Validated Kubernetes placement for target-management writes."""


class ExecutionTargetCreate(BaseModel):
    """A Kubernetes target on the single cluster provisioned at installation."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=255)
    placement: KubernetesTargetPlacement
    labels: dict[str, str] = Field(default_factory=dict)
    is_default: bool = False


class ExecutionTargetUpdate(BaseModel):
    """Patch mutable target fields; placement requires a namespace."""

    model_config = ConfigDict(extra="forbid", json_schema_extra={"minProperties": 1})

    name: Annotated[str, Field(min_length=1, max_length=255)] | SkipJsonSchema[None] = None
    placement: KubernetesTargetPlacement | SkipJsonSchema[None] = None
    labels: dict[str, str] | SkipJsonSchema[None] = None
    is_default: bool | SkipJsonSchema[None] = None
    enabled: bool | SkipJsonSchema[None] = None

    @model_validator(mode="after")
    def check_fields(self) -> Self:
        """Reject empty patches and clearing required values before sending a command."""
        if not self.model_fields_set or any(getattr(self, key) is None for key in self.model_fields_set):
            message = "Supply at least one target field; target fields cannot be null"
            raise ValueError(message)
        return self


class ExecutionTargetDeleteAccepted(BaseModel):
    """EP accepted deletion; draining and physical removal happen independently."""

    id: UUID
    accepted: Literal[True] = True


class ExecutionTargetHealthResult(BaseModel):
    """Shallow view of an EP on-demand target health probe."""

    model_config = ConfigDict(extra="forbid")

    healthy: bool
    message: str | None = None
    skipped: bool = False
