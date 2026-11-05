"""Persistence-model contracts for work items."""

from __future__ import annotations

from execution_plane.models.constants import EP_SCHEMA
from execution_plane.models.work_item import WorkItem


def test_work_item_target_and_status_are_indexed_together() -> None:
    """Target drain checks filter by both assignment and lifecycle state."""
    table = WorkItem.__table__  # type: ignore[attr-defined]
    index = next(index for index in table.indexes if index.name == "ix_work_items_execution_target_status")

    assert table.schema == EP_SCHEMA
    assert [column.name for column in index.columns] == ["execution_target_id", "status"]
    assert index.unique is False
