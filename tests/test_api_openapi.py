"""Public OpenAPI contracts for the standalone Execution Plane API."""

from copy import deepcopy
from uuid import uuid4

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from execution_plane.api.main import create_app
from execution_plane.models.cluster_binding import ClusterBinding
from execution_plane.models.execution_target import ExecutionTarget
from execution_plane.models.work_item import WorkItem
from execution_plane_client.commands.clusters import ClusterRegister
from execution_plane_client.commands.control import Completion
from tests.test_command_api import command_message


def _validator(document, name="MessageEnvelope") -> Draft202012Validator:
    return Draft202012Validator(
        {"$ref": f"#/components/schemas/{name}", "components": document["components"]},
        format_checker=FormatChecker(),
    )


@pytest.mark.parametrize("kind", ["cluster.register", "cluster.provision"])
def test_command_oneof_accepts_overlapping_payload_shapes_with_the_correct_type(kind) -> None:
    payload = {
        "resource_key": str(uuid4()),
        "desired_revision": 1,
        "name": "workers",
        "endpoint": "https://cluster.test",
        "namespace": "ep",
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
    del invalid["command"]["payload"]["namespace"]
    assert not validator.is_valid(invalid)


@pytest.mark.parametrize(
    ("kind", "payload"),
    [
        (
            "work.execute",
            {
                "image": "node:test",
                "invocation": {
                    "inputs": {},
                    "credentials": {},
                    "workflow_context": {},
                    "settings": {},
                    "timeout_seconds": 60,
                    "max_output_bytes": 1024,
                },
            },
        ),
        ("command.cancel", {"target_command_id": str(uuid4())}),
        ("cluster.update", {"resource_key": str(uuid4()), "desired_revision": 2, "project_ids": None}),
        ("cluster.delete", {"resource_key": str(uuid4()), "desired_revision": 3}),
        (
            "execution_target.register",
            {
                "resource_id": str(uuid4()),
                "cluster_id": str(uuid4()),
                "name": "workers",
                "backend_type": "vanilla_k8s",
                "endpoint": "https://cluster.test",
                "credential_ref": str(uuid4()),
                "placement": {"type": "kubernetes", "namespace": "ep"},
            },
        ),
        ("execution_target.update", {"resource_id": str(uuid4()), "labels": {}}),
        ("execution_target.delete", {"resource_id": str(uuid4())}),
    ],
)
def test_command_schema_validates_the_other_installed_commands(kind, payload) -> None:
    _validator(create_app().openapi()).validate(command_message(kind, payload))


def test_completion_schema_requires_parent_and_is_not_accepted_as_an_inbound_command() -> None:
    document = create_app().openapi()
    message = (
        Completion(status="succeeded", result={"stdout": "done"})
        .to_message(
            source_id="ep",
            destination_id="ao",
            client_id="ao",
            parent_command_id=uuid4(),
        )
        .model_dump(mode="json")
    )
    validator = _validator(document, "CompletionMessageEnvelope")
    validator.validate(message)
    assert not _validator(document).is_valid(message)
    del message["command"]["parent_command_id"]
    assert not validator.is_valid(message)
    message["command"]["parent_command_id"] = None
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
        "command.cancel": "CancelCommand",
        "cluster.register": "ClusterRegister",
        "cluster.provision": "ClusterProvision",
        "cluster.update": "ClusterUpdate",
        "cluster.delete": "ClusterDelete",
        "execution_target.register": "TargetRegister",
        "execution_target.update": "TargetUpdate",
        "execution_target.delete": "TargetDelete",
    }
    assert command["discriminator"]["propertyName"] == "command_type"
    mapping = command["discriminator"]["mapping"]
    assert mapping.keys() == expected_payloads.keys()
    assert command["oneOf"] == [{"$ref": reference} for reference in mapping.values()]
    for kind, payload_name in expected_payloads.items():
        variant = components[mapping[kind].rsplit("/", 1)[-1]]
        assert variant["properties"]["command_type"]["const"] == kind
        assert variant["properties"]["command_revision"]["const"] == 1
        assert variant["properties"]["payload"] == {"$ref": f"#/components/schemas/{payload_name}"}
        assert {"command_type", "command_revision", "payload"} <= set(variant["required"])
        assert components[payload_name]["additionalProperties"] is False
    assert "NodeInvocation" in components
    assert "CommandScope" in components


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
    assert command["properties"]["parent_command_id"]["type"] == "string"
    assert "parent_command_id" in command["required"]
    receipt = webhook["responses"]["2XX"]["content"]["application/json"]["schema"]
    assert receipt == {"$ref": "#/components/schemas/TransportReceipt"}


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


def test_cluster_binding_request_remains_namespace_based() -> None:
    assert "namespace" in ClusterRegister.model_fields
    assert "placement" not in ClusterRegister.model_fields


@pytest.mark.parametrize(
    ("path", "model", "default"),
    [
        ("/v1/work-items", WorkItem, "-created_at"),
        ("/v1/clusters", ClusterBinding, "created_at"),
        ("/v1/execution-targets", ExecutionTarget, "created_at"),
    ],
)
def test_list_sort_contract_matches_model_fields_and_endpoint_default(path, model, default) -> None:
    parameters = create_app().openapi()["paths"][path]["get"]["parameters"]
    schema = next(parameter["schema"] for parameter in parameters if parameter["name"] == "sort")
    assert schema["default"] == default
    assert set(schema["enum"]) == {sort for field in model.__sortable_fields__ for sort in (field, f"-{field}")}


def test_running_is_exposed_as_a_work_item_status() -> None:
    document = create_app().openapi()
    schema = document["components"]["schemas"]["WorkItemStatus"]
    assert "running" in schema["enum"]
    response = document["components"]["schemas"]["WorkItemRead"]
    assert response["properties"]["status"] == {"$ref": "#/components/schemas/WorkItemStatus"}
