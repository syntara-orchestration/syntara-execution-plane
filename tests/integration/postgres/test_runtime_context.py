"""Caller context is persisted once and filtered before pagination and totals."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import text

from execution_plane.models.work_item import WorkItem
from execution_plane.work_item.work_item_store import WorkStore

pytestmark = pytest.mark.integration


async def test_project_workflow_and_execution_filters_select_exact_work_items(pg_client):
    client, sessions, client_id = pg_client
    project, workflow, execution, other_project, other_workflow, other_execution = [str(uuid4()) for _ in range(6)]
    context = {"project_id": project, "workflow_id": workflow, "execution_id": execution}
    contexts = [
        context,
        context,
        context,
        {**context, "project_id": other_project},
        {**context, "workflow_id": other_workflow},
        {**context, "execution_id": other_execution},
        {"project_id": project, "workflow_id": workflow},
        {},
    ]
    store = WorkStore(engine=sessions.kw["bind"])
    items = [
        await store.dispatch(client_id, uuid4(), {"runtime_context": value, "invocation": {"inputs": {}}})
        for value in contexts
    ]
    # A full context match belonging to another client must not affect pages or totals.
    foreign = await store.dispatch(f"{client_id}-foreign", uuid4(), {"runtime_context": context})

    for filters, expected_indices in [
        ({"project_id": project}, [0, 1, 2, 4, 5, 6]),
        ({"workflow_id": workflow}, [0, 1, 2, 3, 5, 6]),
        ({"execution_id": execution}, [0, 1, 2, 3, 4]),
        (context, [0, 1, 2]),
        ({**context, "execution_id": other_execution}, [5]),
        ({"execution_id": str(uuid4())}, []),
    ]:
        params = {f"runtime_context[{key}]": value for key, value in filters.items()}
        params.update({"limit": "2", "sort": "id", "include_total": "true"})
        expected_ids = {str(items[index].id) for index in expected_indices}
        seen = []
        while True:
            response = await client.get("/v1/work-items", params=params)
            assert response.status_code == 200, response.text
            page = response.json()
            assert page["total"] == len(expected_ids)
            assert len(page["resources"]) <= 2
            seen.extend(item["id"] for item in page["resources"])
            assert len(seen) == len(set(seen)), "Pagination repeated a work item"
            assert str(foreign.id) not in seen
            assert set(seen) <= expected_ids
            if page["next"] is None:
                break
            params["cursor"] = page["next"]
        assert set(seen) == expected_ids


async def test_runtime_context_filters_are_scoped_paginated_and_exact(pg_client):
    client, sessions, client_id = pg_client
    contexts = [
        {"project_id": "p", "execution_id": "one", "flag": True},
        {"project_id": "p", "execution_id": "two", "flag": "true"},
        {"project_id": "other"},
        {"project_id": 123},
        {"project_id": "123"},
        {"project_id": "a,b", "nested.key": "literal", "nested": {"key": "value"}},
    ]
    async with sessions() as session:
        for context in contexts:
            session.add(
                WorkItem(id=uuid4(), client_id=client_id, runtime_context=context, created_at=datetime.now(UTC))
            )
        session.add(
            WorkItem(
                id=uuid4(),
                client_id="another-client",
                runtime_context={"project_id": "p"},
                created_at=datetime.now(UTC),
            )
        )
        await session.commit()

    params = {"runtime_context[project_id]": "p", "limit": 1, "include_total": "true"}
    first = await client.get("/v1/work-items", params=params)
    assert first.status_code == 200, first.text
    page = first.json()
    assert page["total"] == 2
    assert len(page["resources"]) == 1
    second = (await client.get("/v1/work-items", params={**params, "cursor": page["next"]})).json()
    assert second["total"] == 2
    assert second["next"] is None
    assert second["resources"][0]["id"] != page["resources"][0]["id"]
    for query, expected in [
        ({"runtime_context[execution_id]": ""}, 2),
        ({"runtime_context[project_id]": "p", "runtime_context[flag]": "true"}, 1),
        ({"runtime_context[project_id]": "123"}, 1),
        ({"runtime_context[project_id]": "a,b", "runtime_context[nested.key]": "literal"}, 1),
        ({"runtime_context[nested.key]": "value"}, 0),
        ({"runtime_context[missing]": ""}, 0),
    ]:
        response = await client.get("/v1/work-items", params={**query, "include_total": "true"})
        assert response.status_code == 200, response.text
        assert response.json()["total"] == expected
        assert len(response.json()["resources"]) == expected


@pytest.mark.parametrize("query", ["runtime_context", "runtime_context[]", "runtime_context[x][eq]"])
async def test_malformed_runtime_context_filters_are_rejected(pg_client, query):
    client, _, _ = pg_client
    assert (await client.get("/v1/work-items", params={query: "x"})).status_code == 422


async def test_other_resources_reject_runtime_context_filters(pg_client):
    client, _, _ = pg_client
    assert (await client.get("/v1/clusters", params={"runtime_context[x]": "x"})).status_code == 422


async def test_dispatch_stores_context_once_outside_encrypted_payload(pg_client):
    _, sessions, client_id = pg_client
    payload = {"runtime_context": {"project_id": "p"}, "invocation": {"inputs": {"private": "hidden"}}}
    store = WorkStore(engine=sessions.kw["bind"])
    item = await store.dispatch(client_id, uuid4(), payload)
    assert payload["runtime_context"] == {"project_id": "p"}
    saved = await store.get(item.id, client_id=client_id)
    assert saved.runtime_context == {"project_id": "p"}
    assert "runtime_context" not in saved.payload
    async with sessions() as session:
        row = (
            await session.execute(
                text("SELECT payload, runtime_context FROM execution_plane.work_items WHERE id=:id"), {"id": item.id}
            )
        ).one()
    assert set(row.payload) == {"_ep_encrypted_payload_v1"}
    assert row.runtime_context == {"project_id": "p"}
