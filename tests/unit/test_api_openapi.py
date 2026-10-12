"""Public OpenAPI contracts for the standalone Execution Plane API."""

from execution_plane.api.main import create_app
from execution_plane.api.schemas import ClusterBindingUpsert


def test_execution_target_response_uses_discriminated_placement() -> None:
    schema = create_app().openapi()
    target_schema = schema["components"]["schemas"]["ExecutionTargetRead"]

    assert "placement" in target_schema["properties"]
    assert "namespace" not in target_schema["properties"]
    assert target_schema["properties"]["placement"]["discriminator"]["propertyName"] == "type"


def test_cluster_binding_request_remains_namespace_based() -> None:
    assert "namespace" in ClusterBindingUpsert.model_fields
    assert "placement" not in ClusterBindingUpsert.model_fields
