"""Historical context is consolidated without overwriting conflicting values."""

from importlib import import_module

import pytest

migration = import_module("execution_plane.migrations.versions.21fd6bda6f1d_store_queryable_work_item_runtime_")


def test_extract_preserves_nested_context_and_removes_legacy_fields():
    original = {
        "metadata": {"project_id": "project"},
        "invocation": {
            "workflow_run_id": "temporal-run",
            "step_revision_id": "script.v1",
            "runtime_context": {"nested": {"values": [True, None, 3]}},
        },
    }
    payload, context = migration._extract(original)
    assert payload == {"invocation": {"step_revision_id": "script.v1"}}
    assert context == {
        "project_id": "project",
        "temporal_run_id": "temporal-run",
        "nested": {"values": [True, None, 3]},
    }
    assert "runtime_context" in original["invocation"]


def test_extract_refuses_conflicting_context_without_disclosing_values():
    with pytest.raises(ValueError, match="Conflicting stored runtime context") as error:
        migration._extract(
            {"metadata": {"project_id": "one"}, "invocation": {"runtime_context": {"project_id": "two"}}}
        )
    assert "one" not in str(error.value)
    assert "two" not in str(error.value)


@pytest.mark.parametrize(("left", "right"), [(True, 1), ({"nested": True}, {"nested": 1}), ([False], [0])])
def test_json_type_conflicts_are_not_silently_merged(left, right):
    with pytest.raises(ValueError, match="Conflicting stored runtime context"):
        migration._extract({"metadata": {"value": right}, "invocation": {"runtime_context": {"value": left}}})
