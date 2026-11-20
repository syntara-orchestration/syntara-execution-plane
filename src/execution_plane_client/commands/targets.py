"""Execution-target configuration and independent registration commands."""

from __future__ import annotations

import re
from typing import Annotated, ClassVar, Literal, Self
from uuid import UUID  # noqa: TC003 - Pydantic evaluates fields.

from pydantic import Field, StrictBool, StrictStr, field_validator, model_validator

from execution_plane_client.commands import ResourceIdentity, validate_endpoint
from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.protocol import WireModel
from execution_plane_client.types import BackendType

_LABEL_NAME = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?$")
_DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_LABEL_LIMIT = 63
_DOMAIN_LIMIT = 253


def _label_key(value: str) -> bool:
    prefix, separator, name = value.partition("/")
    if separator:
        if (
            not prefix
            or len(prefix) > _DOMAIN_LIMIT
            or any(len(part) > _LABEL_LIMIT or not _DNS_LABEL.fullmatch(part) for part in prefix.split("."))
        ):
            return False
    else:
        name = prefix
    return len(name) <= _LABEL_LIMIT and bool(_LABEL_NAME.fullmatch(name))


def _label_value(value: str) -> bool:
    return len(value) <= _LABEL_LIMIT and (value == "" or bool(_LABEL_NAME.fullmatch(value)))


class KubernetesPlacement(WireModel):
    """Kubernetes placement contract, independent of Kubernetes libraries."""

    type: Literal["kubernetes"] = "kubernetes"
    namespace: Annotated[StrictStr, Field(min_length=1, max_length=63, pattern=r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")] = (
        Field(description="Existing Kubernetes namespace in which EP creates workload pods and checks target health.")
    )
    node_selectors: list[str] = Field(
        default_factory=list,
        description=(
            "Pod nodeSelector entries in key=value form. Nodes must match every entry; an empty "
            "list imposes no label restriction. On update, omission preserves the stored list and "
            "[] clears it."
        ),
    )
    tolerations: list[str] = Field(
        default_factory=list,
        description=(
            "Pod taints to tolerate, using key=value:effect (Equal) or key:effect (Exists). Effect "
            "may be NoSchedule, PreferNoSchedule, or NoExecute; omitting it matches all effects. "
            "An empty key with Exists matches all keys. On update, omission preserves the list and "
            "[] clears it."
        ),
    )

    @field_validator("node_selectors")
    @classmethod
    def check_selectors(cls, values: list[str]) -> list[str]:
        """Accept only Kubernetes key=value selectors."""
        for value in values:
            key, separator, label = value.partition("=")
            if not separator or not _label_key(key) or not _label_value(label):
                message = "Invalid Kubernetes node selector"
                raise ValueError(message)
        return values

    @field_validator("tolerations")
    @classmethod
    def check_tolerations(cls, values: list[str]) -> list[str]:
        """Accept compact tolerations with supported scheduling effects."""
        for value in values:
            key_value, separator, effect = value.rpartition(":")
            if not separator:
                key_value, effect = value, ""
            key, equals, label = key_value.partition("=")
            valid_key = bool(key) and _label_key(key) if equals else not key or _label_key(key)
            if (
                not valid_key
                or not _label_value(label)
                or effect not in {"", "NoSchedule", "PreferNoSchedule", "NoExecute"}
            ):
                message = "Invalid Kubernetes toleration"
                raise ValueError(message)
        return values


class RHELPlacement(WireModel):
    """Placement for a supported RHEL backend."""

    type: Literal["rhel"] = "rhel"


Placement = Annotated[KubernetesPlacement | RHELPlacement, Field(discriminator="type")]


class TargetDefinition(WireModel):
    """Desired execution-target configuration."""

    name: Annotated[StrictStr, Field(min_length=1, max_length=255)]
    backend_type: BackendType
    placement: Placement = Field(
        description=(
            "Namespace and pod scheduling rules for this target. Updates require namespace; "
            "omitted scheduling lists retain their stored values. The placement type cannot be "
            "changed."
        )
    )
    endpoint: Annotated[StrictStr, Field(min_length=1, max_length=2048)] = Field(
        description=(
            "HTTPS provider management endpoint, without embedded credentials, query, or fragment. "
            "Kubernetes workloads use the parent cluster connection."
        )
    )
    credential_ref: UUID = Field(
        description=(
            "Opaque management credential reference. Kubernetes execution resolves the parent "
            "cluster credential instead; the AO facade supplies the zero UUID here."
        )
    )
    labels: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "User labels matched by work.execute selectors when routing workloads. Updates replace "
            "user labels; {} clears them. EP retains its reserved identity labels."
        ),
    )

    @field_validator("endpoint")
    @classmethod
    def check_endpoint(cls, value: str) -> str:
        """Preserve verified HTTPS for provider management credentials."""
        return validate_endpoint(value)

    @model_validator(mode="after")
    def check_backend(self) -> Self:
        """Reject backend/placement pairs unsupported by the provider adapters."""
        if (self.backend_type == BackendType.KUBERNETES) != isinstance(self.placement, KubernetesPlacement):
            message = "Backend does not support this placement"
            raise ValueError(message)
        return self


class TargetRegister(TargetDefinition, ResourceIdentity, CommandPayload):
    """Register an independently identified target on an existing cluster."""

    command_type: ClassVar[str] = "execution_target.register"

    cluster_id: UUID
    is_default: StrictBool = Field(
        default=False,
        description=(
            "Promote this target to the cluster default, replacing the previous default. The "
            "target and cluster must be active and enabled. To demote a default, promote another "
            "target rather than setting this field to false."
        ),
    )


class TargetDelete(ResourceIdentity, CommandPayload):
    """Request draining of an independently managed execution target."""

    command_type: ClassVar[str] = "execution_target.delete"


class TargetUpdate(ResourceIdentity, CommandPayload):
    """Mutable target fields; `is_default=true` promotes this target."""

    command_type: ClassVar[str] = "execution_target.update"

    name: Annotated[StrictStr, Field(min_length=1, max_length=255)] | None = None
    endpoint: Annotated[StrictStr, Field(min_length=1, max_length=2048)] | None = Field(
        default=None,
        description=(
            "Replacement HTTPS provider management endpoint. Omit to keep the stored endpoint; "
            "explicit null is rejected."
        ),
    )
    placement: Placement | None = Field(
        default=None,
        description=(
            "Namespace and pod scheduling rules for this target. Updates require namespace; "
            "omitted scheduling lists retain their stored values. The placement type cannot be "
            "changed."
        ),
    )
    credential_ref: UUID | None = Field(
        default=None,
        description=(
            "Replacement opaque provider management credential reference; Kubernetes continues to "
            "use the parent cluster credential."
        ),
    )
    labels: dict[str, str] | None = Field(
        default=None,
        description=(
            "User labels matched by work.execute selectors when routing workloads. Updates replace "
            "user labels; {} clears them. EP retains its reserved identity labels."
        ),
    )
    is_default: StrictBool | None = Field(
        default=None,
        description=(
            "Promote this target to the cluster default, replacing the previous default. The "
            "target and cluster must be active and enabled. To demote a default, promote another "
            "target rather than setting this field to false."
        ),
    )
    enabled: StrictBool | None = Field(
        default=None,
        description=(
            "Whether the scheduler may assign new workloads to this target; disabling it does not "
            "request cancellation of work already assigned."
        ),
    )

    def _message_payload(self) -> dict[str, object]:
        payload = self.model_dump(mode="json", exclude_unset=True)
        if self.placement is not None:
            payload["placement"] = self.placement.model_dump(
                mode="json", include=self.placement.model_fields_set | {"type"}
            )
        return payload

    @field_validator("endpoint")
    @classmethod
    def check_endpoint(cls, value: str | None) -> str | None:
        """Validate explicit endpoint updates before applying them."""
        return None if value is None else validate_endpoint(value)


class TargetHealth(ResourceIdentity, CommandPayload):
    """Query the current health of one ExecutionTarget."""

    command_type: ClassVar[str] = "execution_target.health"
