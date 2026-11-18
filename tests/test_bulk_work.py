"""Bulk command selectors cannot accidentally select every client's work."""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from execution_plane_client.commands.bulk_work import WorkItemSelector


@pytest.mark.parametrize(
    "value",
    [
        {},
        {"work_item_ids": []},
        {"runtime_context": None},
        {"runtime_context": {}},
        {"runtime_context": []},
        {"runtime_context": "invalid"},
        {"unknown": "value"},
    ],
)
def test_invalid_selector_is_rejected(value):
    with pytest.raises(ValidationError):
        WorkItemSelector.model_validate(value)


def test_multiple_selectors_and_oversized_id_batch_are_rejected():
    with pytest.raises(ValidationError):
        WorkItemSelector(work_item_ids=[uuid4()], runtime_context={"group": "a"})
    with pytest.raises(ValidationError):
        WorkItemSelector(work_item_ids=[uuid4() for _ in range(101)])
