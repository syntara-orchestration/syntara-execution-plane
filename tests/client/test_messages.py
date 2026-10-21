"""Typed commands construct valid wire messages without envelope boilerplate."""

from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.commands import clusters, control, targets, work
from execution_plane_client.errors import ProtocolConflict
from execution_plane_client.protocol import MessageEnvelope

RESOURCE_ID = "00000000-0000-0000-0000-000000000001"
CLUSTER = {
    "resource_key": RESOURCE_ID,
    "desired_revision": 1,
    "name": "workers",
    "endpoint": "https://cluster.test",
    "namespace": "ep",
    "credential_ref": str(uuid4()),
}


@pytest.mark.parametrize(
    ("module", "name", "kind", "payload"),
    [
        (
            work,
            "WorkExecute",
            "work.execute",
            {
                "image": "node:test",
                "invocation": {
                    "workflow_run_id": "workflow-1",
                    "step_revision_id": "script.v1",
                    "inputs": {},
                    "timeout_seconds": 60,
                },
            },
        ),
        (control, "Completion", "work.execute.completed", {"status": "succeeded", "result": {"stdout": "done"}}),
        (control, "CancelCommand", "command.cancel", {"target_command_id": RESOURCE_ID}),
        (clusters, "ClusterRegister", "cluster.register", CLUSTER),
        (clusters, "ClusterProvision", "cluster.provision", CLUSTER),
        (
            clusters,
            "ClusterUpdate",
            "cluster.update",
            {"resource_key": RESOURCE_ID, "desired_revision": 2, "name": "new"},
        ),
        (clusters, "ClusterDelete", "cluster.delete", {"resource_key": RESOURCE_ID, "desired_revision": 3}),
        (
            targets,
            "TargetRegister",
            "execution_target.register",
            {
                "resource_id": RESOURCE_ID,
                "cluster_id": RESOURCE_ID,
                "name": "target",
                "endpoint": "https://cluster.test",
                "backend_type": "vanilla_k8s",
                "credential_ref": RESOURCE_ID,
                "placement": {"type": "kubernetes", "namespace": "ep"},
            },
        ),
        (targets, "TargetUpdate", "execution_target.update", {"resource_id": RESOURCE_ID, "name": "new"}),
        (targets, "TargetDelete", "execution_target.delete", {"resource_id": RESOURCE_ID}),
    ],
)
def test_typed_commands_supply_type_revision_and_consistent_envelope(module, name, kind, payload):
    command = getattr(module, name).model_validate(payload)
    before = datetime.now(UTC)
    message = command.to_message(
        source_id="ao",
        destination_id="ep",
        client_id="ao",
        parent_command_id=uuid4(),
    )
    assert message.command.command_type == kind
    assert message.command.command_revision == 1
    assert isinstance(message.command_id, UUID)
    assert message.message_id == message.command_id == message.command.command_id
    assert before <= message.command.occurred_at <= datetime.now(UTC)
    assert message.source_id == "ao"
    assert message.destination_id == "ep"
    assert message.scope.client_id == "ao"
    assert message.message_type == "command.submit"
    assert message.sequence == 0
    assert "command_type" not in message.command.payload
    round_trip = MessageEnvelope.model_validate_json(message.model_dump_json())
    parsed = CommandCatalog().validate(round_trip)
    assert type(parsed) is type(command)
    assert parsed == command


def test_completion_preserves_explicit_identity_and_time_across_reconstruction():
    command_id, parent_id = uuid4(), uuid4()
    occurred_at = datetime(2026, 10, 14, 10, tzinfo=timezone(timedelta(hours=1)))
    messages = [
        control.Completion(status="succeeded", result={"stdout": "x" * (600 * 1024)}).to_message(
            source_id="ep",
            destination_id="ao",
            client_id="ao",
            command_id=command_id,
            parent_command_id=parent_id,
            occurred_at=occurred_at,
        )
        for _ in range(2)
    ]
    assert messages[0] == messages[1]
    assert messages[0].message_id == command_id
    assert messages[0].command.parent_command_id == parent_id
    assert messages[0].command.occurred_at == datetime(2026, 10, 14, 9, tzinfo=UTC)
    assert len(messages[0].command.payload["result"]["stdout"]) == 600 * 1024


def test_completion_requires_parent_before_it_can_be_sent():
    with pytest.raises(ProtocolConflict):
        control.Completion(status="failed").to_message(source_id="ep", destination_id="ao", client_id="ao")


def test_cluster_patch_keeps_explicit_null_and_omits_unchanged_fields():
    message = clusters.ClusterUpdate(
        resource_key=UUID(RESOURCE_ID), desired_revision=2, ca_certificate=None
    ).to_message(
        source_id="ao",
        destination_id="ep",
        client_id="ao",
    )
    assert message.command.payload == {"resource_key": RESOURCE_ID, "desired_revision": 2, "ca_certificate": None}
    parsed = CommandCatalog().validate(message)
    assert isinstance(parsed, clusters.ClusterUpdate)
    assert parsed.resource_key == UUID(RESOURCE_ID)
    assert parsed.model_fields_set == {"resource_key", "desired_revision", "ca_certificate"}


@pytest.mark.parametrize("payload_class", [clusters.ClusterRegister, clusters.ClusterProvision])
def test_cluster_creation_commands_can_set_the_global_default(payload_class):
    message = payload_class(
        resource_key=UUID(RESOURCE_ID),
        desired_revision=1,
        name="workers",
        endpoint="https://cluster.test",
        namespace="ep",
        credential_ref=uuid4(),
        is_default=True,
    ).to_message(source_id="ao", destination_id="ep", client_id="ao")

    assert message.command.payload["is_default"] is True
    parsed = CommandCatalog().validate(message)
    assert parsed.is_default is True


def test_cluster_patch_can_reassign_the_global_default() -> None:
    message = clusters.ClusterUpdate(
        resource_key=UUID(RESOURCE_ID),
        desired_revision=2,
        is_default=True,
    ).to_message(source_id="ao", destination_id="ep", client_id="ao")

    assert message.command.payload == {"resource_key": RESOURCE_ID, "desired_revision": 2, "is_default": True}
    parsed = CommandCatalog().validate(message)
    assert parsed.is_default is True


def test_target_patch_retains_discriminator_without_resetting_placement_defaults():
    message = targets.TargetUpdate(
        resource_id=UUID(RESOURCE_ID),
        placement=targets.KubernetesPlacement(namespace="new"),
    ).to_message(source_id="ao", destination_id="ep", client_id="ao")
    assert message.command.payload == {
        "resource_id": RESOURCE_ID,
        "placement": {"type": "kubernetes", "namespace": "new"},
    }
    parsed = CommandCatalog().validate(message)
    assert isinstance(parsed, targets.TargetUpdate)
    assert isinstance(parsed.placement, targets.KubernetesPlacement)
    assert parsed.placement.model_fields_set == {"type", "namespace"}


def test_target_patch_can_reassign_the_cluster_default() -> None:
    message = targets.TargetUpdate(resource_id=UUID(RESOURCE_ID), is_default=True).to_message(
        source_id="ao",
        destination_id="ep",
        client_id="ao",
    )

    assert message.command.payload == {"resource_id": RESOURCE_ID, "is_default": True}
    parsed = CommandCatalog().validate(message)
    assert isinstance(parsed, targets.TargetUpdate)
    assert parsed.is_default is True


def test_message_can_have_an_explicit_transport_identity():
    message_id, command_id = uuid4(), uuid4()
    message = control.CancelCommand(target_command_id=uuid4()).to_message(
        source_id="ao",
        destination_id="ep",
        client_id="ao",
        command_id=command_id,
        message_id=message_id,
        sequence=2,
    )
    assert message.message_id == message_id
    assert message.command_id == command_id
    assert message.sequence == 2


@pytest.mark.parametrize(
    "metadata",
    [
        {"source_id": ""},
        {"occurred_at": datetime(2026, 10, 14)},  # noqa: DTZ001 - Exercise rejection of naive timestamps.
        {"sequence": -1},
    ],
)
def test_message_helper_retains_envelope_validation(metadata):
    with pytest.raises(ValidationError):
        control.CancelCommand(target_command_id=uuid4()).to_message(
            **{"source_id": "ao", "destination_id": "ep", "client_id": "ao", **metadata},
        )
