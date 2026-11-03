"""Shared execution-target list, placement, and response contracts."""

import re
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.json_schema import SkipJsonSchema

from execution_plane_client.commands.targets import KubernetesPlacement as CommandKubernetesPlacement
from execution_plane_client.models.shared import EPListParams

_LABEL_NAME = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?$")
_DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_TOLERATION_EFFECTS = {"", "NoSchedule", "PreferNoSchedule", "NoExecute"}
_MAX_LABEL_NAME_LENGTH = 63
_MAX_DNS_SUBDOMAIN_LENGTH = 253


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


class ExecutionTargetListParams(EPListParams):
    """Execution-target query controls and documented filter fields."""

    id: str | None = Field(default=None, description="Target UUID; id[in]=uuid1,uuid2 for membership")
    cluster_id: str | None = Field(default=None, description="Physical cluster UUID; supports eq and in")
    name: str | None = Field(default=None, description="Name; supports eq, in, contains, and starts_with")
    endpoint: str | None = Field(default=None, description="Endpoint; supports eq, in, contains, and starts_with")
    status: str | None = Field(default=None, description="Target status; supports eq and in")
    backend_type: str | None = Field(default=None, description="Backend type; supports eq and in")
    enabled: str | None = Field(default=None, description="Boolean; supports eq, in, and isnull")
    is_default: str | None = Field(default=None, description="Boolean; supports eq, in, and isnull")
    created_at: str | None = Field(
        default=None, description="Creation time; supports eq, in, gt, gte, lt, lte, and isnull"
    )
    last_ran_at: str | None = Field(default=None, description="Last run time; supports ranges and isnull")
    status_message: str | None = Field(default=None, description="Status message; supports text operators and isnull")
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
        "backend_type",
        "-backend_type",
        "cluster_id",
        "-cluster_id",
        "enabled",
        "-enabled",
        "is_default",
        "-is_default",
    ] = Field(default="created_at", description="Execution-target sort field; oldest first by default.")


class ExecutionTargetRead(BaseModel):
    """Safe execution-target representation without management credentials."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    cluster_id: UUID
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
