"""Contracts for typed ExecutionTarget metadata."""

from __future__ import annotations

import pytest
from pydantic import TypeAdapter, ValidationError

from execution_plane.models.execution_target_placement import (
    ExecutionTargetPlacement,
    KubernetesPlacement,
    RHELPlacement,
)

_PLACEMENT_ADAPTER: TypeAdapter[ExecutionTargetPlacement] = TypeAdapter(ExecutionTargetPlacement)


def test_kubernetes_metadata_defaults_optional_lists() -> None:
    metadata = KubernetesPlacement(namespace="execution")

    assert metadata.type == "kubernetes"
    assert metadata.node_selectors == []
    assert metadata.tolerations == []


def test_kubernetes_metadata_accepts_valid_namespace_selector_and_toleration() -> None:
    metadata = KubernetesPlacement(
        namespace="ao-execution",
        node_selectors=["kubernetes.io/os=linux"],
        tolerations=["dedicated=execution:NoSchedule"],
    )

    assert metadata.namespace == "ao-execution"


@pytest.mark.parametrize(
    "selector",
    [
        "key=",
        "node-role.kubernetes.io/worker=",
        "kubernetes.io/os=linux",
    ],
)
def test_kubernetes_metadata_accepts_node_selector_short_forms(selector: str) -> None:
    KubernetesPlacement(namespace="execution", node_selectors=[selector])


@pytest.mark.parametrize(
    "toleration",
    [
        "node-role.kubernetes.io/control-plane=:NoSchedule",
        "dedicated:NoSchedule",
        ":NoSchedule",
        "dedicated=execution",
        "dedicated=execution:",
    ],
)
def test_kubernetes_metadata_accepts_toleration_short_forms(toleration: str) -> None:
    KubernetesPlacement(namespace="execution", tolerations=[toleration])


@pytest.mark.parametrize("namespace", ["", "UpperCase", "has_underscore", "-leading", "trailing-"])
def test_kubernetes_metadata_rejects_invalid_namespace(namespace: str) -> None:
    with pytest.raises(ValidationError):
        KubernetesPlacement(namespace=namespace)


@pytest.mark.parametrize("selector", ["missing-value", "bad key=value", "=value"])
def test_kubernetes_metadata_rejects_invalid_node_selector(selector: str) -> None:
    with pytest.raises(ValidationError):
        KubernetesPlacement(namespace="execution", node_selectors=[selector])


@pytest.mark.parametrize(
    "toleration",
    [
        "bad key=value:NoSchedule",
        "=value:NoSchedule",
        "key=value:InvalidEffect",
        "key=value:NoSchedule:extra",
    ],
)
def test_kubernetes_metadata_rejects_invalid_toleration(toleration: str) -> None:
    with pytest.raises(ValidationError):
        KubernetesPlacement(namespace="execution", tolerations=[toleration])


def test_discriminator_selects_rhel_metadata() -> None:
    metadata = _PLACEMENT_ADAPTER.validate_python({"type": "rhel"})

    assert isinstance(metadata, RHELPlacement)
    assert metadata.type == "rhel"


@pytest.mark.parametrize("payload", [{}, {"type": "unknown"}])
def test_metadata_rejects_missing_or_unknown_discriminator(payload: dict[str, str]) -> None:
    with pytest.raises(ValidationError):
        _PLACEMENT_ADAPTER.validate_python(payload)
