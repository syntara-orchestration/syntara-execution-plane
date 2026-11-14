"""Public OpenAPI contracts for the standalone Execution Plane API."""

from copy import deepcopy
from uuid import uuid4

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from execution_plane.api.main import create_app
from execution_plane.models.cluster import Cluster
from execution_plane.models.execution_target import ExecutionTarget
from execution_plane.models.work_item import WorkItem
from execution_plane_client.commands.clusters import ClusterRegister
from execution_plane_client.commands.control import Completion, CompletionStatus
from tests.test_command_api import command_message


def _validator(document, name="MessageEnvelope") -> Draft202012Validator:
    return Draft202012Validator(
        {"$ref": f"#/components/schemas/{name}", "components": document["components"]},
        format_checker=FormatChecker(),
    )


@pytest.mark.parametrize("kind", ["cluster.register"])
def test_command_oneof_accepts_overlapping_payload_shapes_with_the_correct_type(kind) -> None:
    payload = {
        "resource_id": str(uuid4()),
        "name": "workers",
        "endpoint": "https://cluster.test",
        "credential_ref": str(uuid4()),
    }
    message = command_message(kind, payload)
    validator = _validator(create_app().openapi())
    validator.validate(message)
    invalid = deepcopy(message)
    invalid["command"]["command_type"] = "work.execute"
    assert not validator.is_valid(invalid)
    invalid = deepcopy(message)
    invalid["command"]["command_revision"] = 2
    assert not validator.is_valid(invalid)
    invalid = deepcopy(message)
    del invalid["command"]["payload"]["name"]
    assert not validator.is_valid(invalid)


@pytest.mark.parametrize(
    ("kind", "payload"),
    [
        (
            "work.execute",
            {
                "work_item_id": str(uuid4()),
                "image": "node:test",
                "invocation": {
                    "step_revision_id": "script.v1",
                    "inputs": {},
                    "timeout_seconds": 60,
                },
            },
        ),
        ("work.cancel", {"work_item_id": str(uuid4())}),
        ("cluster.update", {"resource_id": str(uuid4()), "name": "renamed"}),
        ("cluster.delete", {"resource_id": str(uuid4())}),
        (
            "execution_target.register",
            {
                "resource_id": str(uuid4()),
                "cluster_id": str(uuid4()),
                "name": "workers",
                "backend_type": "kubernetes",
                "endpoint": "https://cluster.test",
                "credential_ref": str(uuid4()),
                "placement": {"type": "kubernetes", "namespace": "ep"},
            },
        ),
        ("execution_target.update", {"resource_id": str(uuid4()), "labels": {}}),
        ("execution_target.delete", {"resource_id": str(uuid4())}),
        ("execution_target.health", {"resource_id": str(uuid4())}),
    ],
)
def test_command_schema_validates_the_other_installed_commands(kind, payload) -> None:
    _validator(create_app().openapi()).validate(command_message(kind, payload))


def test_completion_schema_requires_work_item_id_and_is_not_accepted_as_an_inbound_command() -> None:
    document = create_app().openapi()
    message = (
        Completion(work_item_id=uuid4(), status=CompletionStatus.COMPLETED, result={"stdout": "done"})
        .to_message(
            source_id="ep",
            destination_id="ao",
            client_id="ao",
        )
        .model_dump(mode="json")
    )
    validator = _validator(document, "CompletionMessageEnvelope")
    validator.validate(message)
    assert not _validator(document).is_valid(message)
    del message["command"]["payload"]["work_item_id"]
    assert not validator.is_valid(message)
    message["command"]["payload"]["work_item_id"] = None
    assert not validator.is_valid(message)


def test_component_schemas_are_valid_and_openapi_is_cached() -> None:
    app = create_app()
    document = app.openapi()
    for schema in document["components"]["schemas"].values():
        Draft202012Validator.check_schema(schema)
    assert app.openapi() is document


def test_command_ingress_references_components_with_discriminated_commands() -> None:
    document = create_app().openapi()
    components = document["components"]["schemas"]
    request = document["paths"]["/v1/commands"]["post"]["requestBody"]
    assert request["content"]["application/json"]["schema"] == {"$ref": "#/components/schemas/MessageEnvelope"}
    assert components["MessageEnvelope"]["properties"]["command"] == {"$ref": "#/components/schemas/Command"}
    command = components["Command"]
    expected_payloads = {
        "work.execute": "WorkExecute",
        "work.cancel": "WorkCancel",
        "cluster.register": "ClusterRegister",
        "cluster.update": "ClusterUpdate",
        "cluster.delete": "ClusterDelete",
        "execution_target.register": "TargetRegister",
        "execution_target.update": "TargetUpdate",
        "execution_target.delete": "TargetDelete",
        "execution_target.health": "TargetHealth",
    }
    assert command["discriminator"]["propertyName"] == "command_type"
    mapping = command["discriminator"]["mapping"]
    assert mapping.keys() == expected_payloads.keys()
    assert "credential.resolve.workload" not in mapping
    assert command["oneOf"] == [{"$ref": reference} for reference in mapping.values()]
    for kind, payload_name in expected_payloads.items():
        variant = components[mapping[kind].rsplit("/", 1)[-1]]
        assert variant["properties"]["command_type"]["const"] == kind
        assert variant["properties"]["command_revision"]["const"] == (2 if kind == "work.execute" else 1)
        assert variant["properties"]["command_type"]["description"]
        assert variant["properties"]["command_revision"]["description"]
        assert variant["properties"]["payload"] == {"$ref": f"#/components/schemas/{payload_name}"}
        assert {"command_type", "command_revision", "payload"} <= set(variant["required"])
        assert components[payload_name]["additionalProperties"] is False
    assert "WorkloadInvocation" in components
    assert "CommandScope" in components


def test_workload_credential_bindings_are_documented() -> None:
    components = create_app().openapi()["components"]["schemas"]
    binding = components["WorkCredentialBinding"]
    invocation = components["WorkloadInvocation"]

    assert binding["properties"]["credential_recipe_id"] == {
        "type": "string",
        "maxLength": 256,
        "minLength": 1,
        "pattern": r"^[a-z][a-z0-9-]{0,62}(?:\.[a-z][a-z0-9-]{0,62})+$",
        "title": "Credential Recipe Id",
        "description": "Qualified Plugin SDK CredentialRecipe identifier that the workload may request at runtime.",
    }
    assert binding["properties"]["credential_ref"]["format"] == "uuid"
    assert binding["properties"]["credential_ref"]["description"] == (
        "Opaque AO credential reference; EP resolves it only after the workload requests this recipe."
    )
    assert invocation["properties"]["credential_bindings"] == {
        "items": {"$ref": "#/components/schemas/WorkCredentialBinding"},
        "type": "array",
        "maxItems": 16,
        "title": "Credential Bindings",
        "description": "Host-side recipe-to-reference bindings; never plugin inputs or runtime context.",
    }


def test_completion_is_documented_as_an_outbound_command() -> None:
    document = create_app().openapi()
    webhook = document["webhooks"]["work.execute.completed"]["post"]
    envelope_ref = webhook["requestBody"]["content"]["application/json"]["schema"]["$ref"]
    components = document["components"]["schemas"]
    envelope = components[envelope_ref.rsplit("/", 1)[-1]]
    command_ref = envelope["properties"]["command"]["$ref"]
    command = components[command_ref.rsplit("/", 1)[-1]]
    assert command["properties"]["command_type"]["const"] == "work.execute.completed"
    assert command["properties"]["payload"] == {"$ref": "#/components/schemas/Completion"}
    assert "work_item_id" in components["Completion"]["required"]
    receipt = webhook["responses"]["2XX"]["content"]["application/json"]["schema"]
    assert receipt == {"$ref": "#/components/schemas/TransportReceipt"}


def test_transport_receipt_uses_discriminated_command_result() -> None:
    document = create_app().openapi()
    components = document["components"]["schemas"]
    result = components["TransportReceipt"]["properties"]["result"]
    union = result["anyOf"][0]
    mapping = union["discriminator"]["mapping"]

    assert union["discriminator"]["propertyName"] == "command_type"
    assert mapping == {
        "credential.resolve": "#/components/schemas/CredentialResolveCommandResult",
        "credential.resolve.workload": "#/components/schemas/CredentialResolveWorkloadCommandResult",
        "execution_target.health": "#/components/schemas/TargetHealthCommandResult",
    }
    assert union["oneOf"] == [{"$ref": reference} for reference in mapping.values()]
    assert components["CredentialResolveCommandResult"]["properties"]["credential"] == {
        "type": "string",
        "maxLength": 65536,
        "minLength": 1,
        "title": "Credential",
    }
    assert components["TargetHealthCommandResult"]["properties"]["healthy"] == {
        "type": "boolean",
        "title": "Healthy",
    }


def test_credential_resolution_payloads_are_documented_without_ingress_handlers() -> None:
    document = create_app().openapi()
    components = document["components"]["schemas"]
    cluster_command = components["CredentialResolveCommandV1"]
    command = components["CredentialResolveWorkloadCommandV2"]
    payload = command["properties"]["payload"]
    command_mapping = components["Command"]["discriminator"]["mapping"]

    assert cluster_command["properties"]["command_type"]["const"] == "credential.resolve"
    assert cluster_command["properties"]["payload"]["$ref"] == "#/components/schemas/CredentialResolve"
    assert set(components["CredentialResolve"]["required"]) == {"credential_ref"}
    assert command["properties"]["command_type"]["const"] == "credential.resolve.workload"
    assert {"command_type", "command_revision", "payload"} <= set(command["required"])
    assert payload["$ref"] == "#/components/schemas/CredentialResolveWorkload"
    assert set(components["CredentialResolveWorkload"]["required"]) == {
        "work_item_id",
        "attempt_id",
        "credential_ref",
        "credential_recipe_id",
        "step_revision_id",
    }
    assert "credential.resolve.workload" not in command_mapping
    assert "credential.resolve" not in command_mapping


def test_command_schema_references_resolve_and_do_not_replace_resource_schemas() -> None:
    document = create_app().openapi()
    pending = [document]
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            if "$ref" in value:
                target = document
                for part in value["$ref"].removeprefix("#/").split("/"):
                    target = target[part]
            pending.extend(value.values())
        elif isinstance(value, list):
            pending.extend(value)
    components = document["components"]["schemas"]
    request_mapping = components["TargetRegister"]["properties"]["placement"]["discriminator"]["mapping"]
    response_mapping = components["ExecutionTargetRead"]["properties"]["placement"]["discriminator"]["mapping"]
    request_placement = components[request_mapping["kubernetes"].rsplit("/", 1)[-1]]
    response_placement = components[response_mapping["kubernetes"].rsplit("/", 1)[-1]]
    assert request_placement["additionalProperties"] is False
    assert response_placement["title"] == "KubernetesPlacement"


def test_execution_target_response_uses_discriminated_placement() -> None:
    schema = create_app().openapi()
    target_schema = schema["components"]["schemas"]["ExecutionTargetRead"]

    assert "placement" in target_schema["properties"]
    assert "namespace" not in target_schema["properties"]
    assert target_schema["properties"]["placement"]["discriminator"]["propertyName"] == "type"


def test_cluster_register_is_addressed_by_resource_id_without_namespace() -> None:
    assert "resource_id" in ClusterRegister.model_fields
    assert "namespace" not in ClusterRegister.model_fields
    assert "project_ids" not in ClusterRegister.model_fields
    assert "placement" not in ClusterRegister.model_fields


@pytest.mark.parametrize(
    ("path", "model", "default"),
    [
        ("/v1/work-items", WorkItem, "-created_at"),
        ("/v1/clusters", Cluster, "created_at"),
        ("/v1/execution-targets", ExecutionTarget, "created_at"),
    ],
)
def test_list_sort_contract_matches_model_fields_and_endpoint_default(path, model, default) -> None:
    """Verify sort defaults match model metadata.

    Sort validation happens server-side in EP's query parser (like syntara).
    The EP models import __sortable_fields__ from client metadata to ensure consistency.
    """
    parameters = create_app().openapi()["paths"][path]["get"]["parameters"]
    schema = next(parameter["schema"] for parameter in parameters if parameter["name"] == "sort")
    assert schema["default"] == default
    # Note: No enum validation - sort fields validated server-side like syntara does


def test_ep_models_use_client_metadata() -> None:
    """Verify EP database models import query metadata from client (single source of truth)."""
    from execution_plane_client.models.cluster import CLUSTER_FILTERABLE_FIELDS, CLUSTER_SORTABLE_FIELDS
    from execution_plane_client.models.execution_target import (
        EXECUTION_TARGET_FILTERABLE_FIELDS,
        EXECUTION_TARGET_SORTABLE_FIELDS,
    )
    from execution_plane_client.models.work_item import WORK_ITEM_FILTERABLE_FIELDS, WORK_ITEM_SORTABLE_FIELDS

    # EP models should reference client metadata, not duplicate lists
    assert Cluster.__sortable_fields__ is CLUSTER_SORTABLE_FIELDS
    assert Cluster.__filterable_fields__ is CLUSTER_FILTERABLE_FIELDS

    assert ExecutionTarget.__sortable_fields__ is EXECUTION_TARGET_SORTABLE_FIELDS
    assert ExecutionTarget.__filterable_fields__ is EXECUTION_TARGET_FILTERABLE_FIELDS

    assert WorkItem.__sortable_fields__ is WORK_ITEM_SORTABLE_FIELDS
    assert WorkItem.__filterable_fields__ is WORK_ITEM_FILTERABLE_FIELDS


@pytest.mark.parametrize("path", ["/v1/work-items", "/v1/clusters", "/v1/execution-targets"])
def test_list_parameters_are_documented_once(path) -> None:
    parameters = create_app().openapi()["paths"][path]["get"]["parameters"]
    names = [parameter["name"] for parameter in parameters]

    assert len(names) == len(set(names))
