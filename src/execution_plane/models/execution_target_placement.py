"""Typed platform-specific metadata for execution targets."""

from __future__ import annotations

import re
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

_LABEL_NAME = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?$")
_DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_TOLERATION_EFFECTS = {"", "NoSchedule", "PreferNoSchedule", "NoExecute"}
_MAX_LABEL_NAME_LENGTH = 63
_MAX_DNS_SUBDOMAIN_LENGTH = 253


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
                msg = "must use a valid Kubernetes label selector in key=value form"
                raise ValueError(msg)
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
                msg = "must use a valid Kubernetes toleration short form"
                raise ValueError(msg)
        return values


class RHELPlacement(BaseModel):
    """Placeholder for RHEL-specific execution-target metadata."""

    type: Literal["rhel"] = "rhel"


ExecutionTargetPlacementTypes = KubernetesPlacement | RHELPlacement
ExecutionTargetPlacement = Annotated[
    ExecutionTargetPlacementTypes,
    Field(discriminator="type"),
]
