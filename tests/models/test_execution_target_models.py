"""Persistence-model contracts for typed execution-target metadata."""

from __future__ import annotations

import uuid

from execution_plane.models.cluster import Cluster  # noqa: F401
from execution_plane.models.execution_target import BackendType, ExecutionTarget
from execution_plane.models.execution_target_placement import (
    ExecutionTargetPlacement,
    KubernetesPlacement,
    RHELPlacement,
)
from execution_plane.models.sqlmodel_types import DiscriminatedJSONB


def test_execution_target_requires_placement() -> None:
    table = ExecutionTarget.__table__  # type: ignore[attr-defined]

    assert table.c.placement.nullable is False
    assert table.c.placement.type.__class__ is DiscriminatedJSONB

    assert ExecutionTarget.model_fields["placement"].is_required()


def test_metadata_json_type_round_trips_each_concrete_variant() -> None:
    column_type = DiscriminatedJSONB(ExecutionTargetPlacement)  # type: ignore[arg-type]
    kubernetes = KubernetesPlacement(namespace="execution", node_selectors=["k=v"])
    rhel = RHELPlacement()

    kubernetes_value = column_type.process_bind_param(kubernetes, None)  # type: ignore[arg-type]
    rhel_value = column_type.process_bind_param(rhel, None)  # type: ignore[arg-type]

    assert kubernetes_value == {
        "type": "kubernetes",
        "namespace": "execution",
        "node_selectors": ["k=v"],
        "tolerations": [],
    }
    assert isinstance(column_type.process_result_value(kubernetes_value, None), KubernetesPlacement)  # type: ignore[arg-type]
    assert isinstance(column_type.process_result_value(rhel_value, None), RHELPlacement)  # type: ignore[arg-type]


def test_execution_target_serializes_placement_with_public_name() -> None:
    target = ExecutionTarget(
        cluster_id=uuid.uuid4(),
        name="default",
        backend_type=BackendType.VANILLA_K8S,
        endpoint="https://target.example",
        placement=KubernetesPlacement(namespace="default"),
        api_key="secret",
    )

    serialized = target.model_dump()

    assert "placement" in serialized
