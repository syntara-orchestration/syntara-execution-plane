"""Work queries expose only explicitly supplied caller context."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from execution_plane.commands.queries import EPResourceQueries
from execution_plane.models.work_item import WorkItem


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {
            "metadata": {"project_id": "project", "nested": {"tags": ["one", "two"]}},
            "invocation": {"inputs": {"private": "hidden"}},
        },
    ],
)
async def test_queries_project_metadata_without_exposing_invocation(payload):
    item = WorkItem(id=uuid4(), client_id="ao", payload=payload, created_at=datetime.now(UTC))
    queries = object.__new__(EPResourceQueries)
    queries._client_id = "ao"
    queries._work = AsyncMock()
    queries._work.get.return_value = item
    queries._work.list_for_client.return_value = [item]
    read = await queries.work_item(item.id)
    listed = await queries.work_items(10)
    assert read.metadata == payload.get("metadata", {})
    assert listed[0].metadata == read.metadata
    assert "invocation" not in read.model_dump()
    queries._work.get.assert_awaited_once_with(item.id, client_id="ao")
