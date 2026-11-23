"""Tests for credential-safe command payload logging."""

from execution_plane.logging.command_payload import redacted_command_payload


def test_command_payload_keeps_only_non_sensitive_known_fields() -> None:
    payload = {
        "resource_id": "resource-id",
        "work_item_id": "work-item-id",
        "attempt_id": "attempt-id",
        "name": "name",
        "endpoint": "https://example.test",
        "backend_type": "kubernetes",
        "cluster_id": "cluster-id",
        "enabled": True,
        "is_default": False,
        "labels": {"environment": "test"},
        "selectors": {"environment": "test"},
        "image": "example.test/work:1",
        "workload_type": "container",
        "output_config": {"result": "result"},
        "error_code": "ERROR",
        "status": "completed",
        "selector": {"work_item_ids": ["work-item-id"]},
        "placement": {
            "type": "kubernetes",
            "namespace": "execution-plane",
            "node_selectors": ["node=one"],
            "tolerations": ["node:NoSchedule"],
        },
        "invocation": {
            "step_revision_id": "step",
            "timeout_seconds": 30,
            "inputs": {"token": "secret"},
            "credential_bindings": [{"credential_recipe_id": "recipe", "credential_ref": "credential-id"}],
        },
        "runtime_context": {"project_id": "project"},
        "result": {"token": "secret"},
        "credential_ref": "credential-id",
        "credential_recipe_id": "recipe",
        "ca_certificate": "-----BEGIN CERTIFICATE-----",
    }

    assert redacted_command_payload(payload) == {
        "resource_id": "resource-id",
        "work_item_id": "work-item-id",
        "attempt_id": "attempt-id",
        "name": "name",
        "endpoint": "https://example.test",
        "backend_type": "kubernetes",
        "cluster_id": "cluster-id",
        "enabled": True,
        "is_default": False,
        "labels": {"environment": "test"},
        "selectors": {"environment": "test"},
        "image": "example.test/work:1",
        "workload_type": "container",
        "output_config": {"result": "result"},
        "error_code": "ERROR",
        "status": "completed",
        "selector": {"work_item_ids": ["work-item-id"]},
        "placement": {
            "type": "kubernetes",
            "namespace": "execution-plane",
            "node_selectors": ["node=one"],
            "tolerations": ["node:NoSchedule"],
        },
        "invocation": {"step_revision_id": "step", "timeout_seconds": 30},
        "credential_recipe_id": "recipe",
    }


def test_command_payload_preserves_non_object_values() -> None:
    assert redacted_command_payload(["known", {"token": "secret", "name": "name"}]) == ["known", {"name": "name"}]
    assert redacted_command_payload("value") == "value"
