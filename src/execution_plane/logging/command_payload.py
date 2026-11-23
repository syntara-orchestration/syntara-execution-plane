"""Prepare command payloads for structured logs without exposing credentials."""

from __future__ import annotations

_ALLOWED_FIELD_NAMES = frozenset(
    {
        "attempt_id",
        "backend_type",
        "cluster_id",
        "credential_recipe_id",
        "enabled",
        "endpoint",
        "error_code",
        "image",
        "invocation",
        "is_default",
        "labels",
        "name",
        "namespace",
        "node_selectors",
        "output_config",
        "placement",
        "resource_id",
        "selector",
        "selectors",
        "status",
        "step_revision_id",
        "timeout_seconds",
        "tolerations",
        "type",
        "work_item_id",
        "work_item_ids",
        "workload_type",
    }
)
_PASSTHROUGH_FIELD_NAMES = frozenset({"labels", "output_config", "selectors"})


def redacted_command_payload(payload: object) -> object:
    """Return a JSON value containing only known non-sensitive command fields."""
    if isinstance(payload, dict):
        return {
            key: value if key.lower() in _PASSTHROUGH_FIELD_NAMES else redacted_command_payload(value)
            for key, value in payload.items()
            if key.lower() in _ALLOWED_FIELD_NAMES
        }
    if isinstance(payload, list):
        return [redacted_command_payload(value) for value in payload]
    return payload
