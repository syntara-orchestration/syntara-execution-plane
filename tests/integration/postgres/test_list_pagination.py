"""PostgreSQL pagination and sorting requests using the same seeds as test_list_filters.

Shared dataset and HTTP client definitions live in conftest.py/list_support.py.
Tests that insert their own rows use a separate authenticated client namespace.
"""

from __future__ import annotations

import base64
import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from execution_plane.api.pagination import encode_cursor
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from tests.integration.postgres.list_support import BASE_TIME as _BASE_TIME
from tests.integration.postgres.list_support import LIST_SIZE

pytestmark = pytest.mark.integration

_TOTAL_SORT_ITEMS = LIST_SIZE
_ITEMS_PER_GROUP = LIST_SIZE // 2


# =============================================================================
# Insert helpers (for pg_client tests that need fresh data)
# =============================================================================


async def _insert_work_items(sessions, client_id: str, count: int) -> list[WorkItem]:
    items = []
    base = datetime.now(UTC)
    async with sessions() as session:
        for i in range(count):
            item = WorkItem(
                id=uuid.uuid4(),
                client_id=client_id,
                status=WorkItemStatus.PENDING,
                payload={"seq": i},
                created_at=base - timedelta(seconds=count - i),
            )
            session.add(item)
            items.append(item)
        await session.commit()
    return items


async def _insert_cluster_and_targets(sessions, client_id: str, count: int) -> tuple[Cluster, list[ExecutionTarget]]:
    base = datetime.now(UTC)
    cluster = Cluster(
        id=uuid.uuid4(),
        source_client_id=client_id,
        name="test-cluster",
        endpoint="https://cluster.test",
        credential_ref=uuid.uuid4(),
        status=ClusterStatus.ACTIVE,
        created_by=uuid.uuid4(),
        updated_by=uuid.uuid4(),
        created_at=base,
        updated_at=base,
    )
    targets = []
    async with sessions() as session:
        session.add(cluster)
        await session.flush()
        for i in range(count):
            target = ExecutionTarget(
                id=uuid.uuid4(),
                cluster_id=cluster.id,
                name=f"target-{i}",
                backend_type=BackendType.VANILLA_K8S,
                endpoint="https://target.test",
                placement=KubernetesPlacement(namespace="ep"),
                status=TargetStatus.ACTIVE,
                enabled=True,
                is_default=False,
                credential_ref=uuid.uuid4(),
                labels={},
                created_by=uuid.uuid4(),
                updated_by=uuid.uuid4(),
                created_at=base + timedelta(seconds=i),
                updated_at=base + timedelta(seconds=i),
            )
            session.add(target)
            targets.append(target)
        await session.commit()
    return cluster, targets


# Helpers for sort tests


async def _all_items(client, sort: str, limit: int = 100) -> list[dict]:
    response = await client.get("/v1/work-items", params={"limit": limit, "sort": sort})
    assert response.status_code == 200, response.text
    return response.json()["resources"]


async def _paginate_all(client, sort: str, page_size: int = 2) -> list[dict]:
    items: list[dict] = []
    cursor = None
    for _ in range(_TOTAL_SORT_ITEMS + 5):
        params: dict = {"limit": page_size, "sort": sort}
        if cursor:
            params["cursor"] = cursor
        body = (await client.get("/v1/work-items", params=params)).json()
        items.extend(body["resources"])
        cursor = body.get("next")
        if cursor is None:
            break
    return items


async def _assert_page_round_trip(client, path, expected_ids, *, sort=None, page_size=2, omit_sort_value=False) -> None:
    """Visit every page forward, backward to page one, and forward again."""
    expected_pages = [expected_ids[i : i + page_size] for i in range(0, len(expected_ids), page_size)]
    assert len(expected_pages) >= 3
    params = {"limit": page_size}
    if sort is not None:
        params["sort"] = sort

    async def page(cursor=None) -> dict:
        if cursor is not None and omit_sort_value:
            # Syntara's _make_cursor omits sort_value for created_at sorting.
            data = json.loads(base64.b64decode(cursor))
            assert data["sort_field"] == "created_at"
            assert data.pop("sort_value") == data["created_at"]
            cursor = encode_cursor(data)
        query = {**params, "cursor": cursor} if cursor is not None else params
        response = await client.get(path, params=query)
        assert response.status_code == 200, response.text
        return response.json()

    body = await page()
    for index, expected in enumerate(expected_pages):
        assert [row["id"] for row in body["resources"]] == expected
        assert (body["prev"] is None) == (index == 0)
        assert (body["next"] is None) == (index == len(expected_pages) - 1)
        if body["next"] is not None:
            body = await page(body["next"])

    for index in range(len(expected_pages) - 2, -1, -1):
        body = await page(body["prev"])
        assert [row["id"] for row in body["resources"]] == expected_pages[index]
        assert (body["prev"] is None) == (index == 0)
        assert body["next"] is not None

    for index in range(1, len(expected_pages)):
        body = await page(body["next"])
        assert [row["id"] for row in body["resources"]] == expected_pages[index]
        assert body["prev"] is not None
        assert (body["next"] is None) == (index == len(expected_pages) - 1)


@pytest.mark.parametrize(
    "sort",
    [None, "created_at", "status", "-status", "resource_cleanup_status", "-resource_cleanup_status", "id", "-id"],
)
async def test_work_items_backward_pagination_round_trip(sort_client, sort):
    expected = await _all_items(sort_client, sort=sort or "-created_at")
    await _assert_page_round_trip(
        sort_client, "/v1/work-items", [row["id"] for row in expected], sort=sort, page_size=2
    )


@pytest.mark.parametrize("sort", [None, "created_at", "-created_at"])
async def test_work_items_syntara_created_at_cursor_round_trip(sort_client, sort):
    expected = await _all_items(sort_client, sort=sort or "-created_at")
    await _assert_page_round_trip(
        sort_client,
        "/v1/work-items",
        [row["id"] for row in expected],
        sort=sort,
        page_size=2,
        omit_sort_value=True,
    )


@pytest.mark.parametrize("omit_sort_value", [False, True], ids=["ep-cursor", "syntara-cursor"])
@pytest.mark.parametrize("sort", [None, "created_at", "-created_at"])
async def test_clusters_backward_pagination_round_trip(clusters_client, sort, omit_sort_value):
    client, cluster_ids = clusters_client
    expected = list(reversed(cluster_ids)) if sort == "-created_at" else cluster_ids
    await _assert_page_round_trip(client, "/v1/clusters", expected, sort=sort, omit_sort_value=omit_sort_value)


@pytest.mark.parametrize("omit_sort_value", [False, True], ids=["ep-cursor", "syntara-cursor"])
@pytest.mark.parametrize("sort", [None, "created_at", "-created_at"])
async def test_targets_backward_pagination_round_trip(targets_client, sort, omit_sort_value):
    client, target_ids = targets_client
    expected = list(reversed(target_ids)) if sort == "-created_at" else target_ids
    await _assert_page_round_trip(client, "/v1/execution-targets", expected, sort=sort, omit_sort_value=omit_sort_value)


@pytest.mark.parametrize("sort", ["claimed_at", "-claimed_at", "completed_at", "-completed_at"])
async def test_nullable_work_item_sorts_rejected(sort_client, sort):
    response = await sort_client.get("/v1/work-items", params={"sort": sort})
    assert response.status_code == 422, response.text


@pytest.mark.parametrize("path", ["/v1/clusters", "/v1/execution-targets"])
@pytest.mark.parametrize("sort", ["placement", "credential_ref", "last_ran_at", "cowabunga"])
async def test_unsupported_resource_sort_rejected(pg_client, path, sort):
    client, _, _ = pg_client
    response = await client.get(path, params={"sort": sort})
    assert response.status_code == 422, response.text


_VALID_CURSOR = {
    "id": "00000000-0000-0000-0000-000000000001",
    "created_at": _BASE_TIME.isoformat(),
    "direction": "next",
    "sort_field": "created_at",
    "sort_direction": "asc",
    "sort_value": _BASE_TIME.isoformat(),
}
_BAD_CURSORS = [
    pytest.param([], id="array"),
    pytest.param(None, id="null"),
    pytest.param("cursor", id="string"),
    *[
        pytest.param({k: v for k, v in _VALID_CURSOR.items() if k != field}, id=f"missing-{field}")
        for field in _VALID_CURSOR
        if field != "sort_value"
    ],
    *[
        pytest.param({**_VALID_CURSOR, field: value}, id=f"invalid-{field}-{value!r}")
        for field, value in [
            ("id", []),
            ("id", 123),
            ("id", "invalid-uuid"),
            ("created_at", []),
            ("created_at", 123),
            ("created_at", "2025-06-01T12:00:00"),
            ("created_at", "invalid-date"),
            ("sort_value", None),
            ("sort_value", []),
            ("sort_value", 123),
            ("sort_value", "invalid-date"),
            ("sort_value", "2025-06-01T12:00:00"),
            ("direction", "sideways"),
            ("sort_direction", "sideways"),
            ("sort_field", []),
        ]
    ],
]


@pytest.mark.parametrize("path", ["/v1/work-items", "/v1/clusters", "/v1/execution-targets"])
@pytest.mark.parametrize("payload", _BAD_CURSORS)
async def test_structurally_malformed_cursor_returns_422(pg_client, path, payload):
    client, _, _ = pg_client
    token = base64.b64encode(json.dumps(payload).encode()).decode()
    response = await client.get(path, params={"cursor": token, "sort": "created_at"})
    assert response.status_code == 422, response.text


@pytest.mark.parametrize("path", ["/v1/work-items", "/v1/clusters", "/v1/execution-targets"])
@pytest.mark.parametrize(
    "cursor",
    [
        {**_VALID_CURSOR, "sort_direction": "desc"},
        {**_VALID_CURSOR, "sort_field": "status", "sort_value": "pending"},
    ],
)
async def test_cursor_must_match_requested_sort(pg_client, path, cursor):
    client, _, _ = pg_client
    response = await client.get(path, params={"cursor": encode_cursor(cursor), "sort": "created_at"})
    assert response.status_code == 422, response.text


@pytest.mark.parametrize(
    "sort", ["status", "-status", "resource_cleanup_status", "-resource_cleanup_status", "id", "-id"]
)
async def test_non_created_at_cursor_requires_sort_value(sort_client, sort):
    body = (await sort_client.get("/v1/work-items", params={"sort": sort, "limit": 2})).json()
    data = json.loads(base64.b64decode(body["next"]))
    del data["sort_value"]
    response = await sort_client.get("/v1/work-items", params={"sort": sort, "cursor": encode_cursor(data)})
    assert response.status_code == 422, response.text


# =============================================================================
# Work items — pagination
# =============================================================================


async def test_work_items_first_page(pg_client):
    """First page: correct item count, next cursor present, no prev cursor."""
    client, sessions, client_id = pg_client
    await _insert_work_items(sessions, client_id, 5)

    body = (await client.get("/v1/work-items", params={"limit": 2})).json()
    assert len(body["resources"]) == 2
    assert body["next"] is not None
    assert body["prev"] is None
    assert body["total"] is None


async def test_work_items_last_page_has_no_next(pg_client):
    """The final page has next=null."""
    client, sessions, client_id = pg_client
    await _insert_work_items(sessions, client_id, 3)

    p1 = (await client.get("/v1/work-items", params={"limit": 2})).json()
    p2 = (await client.get("/v1/work-items", params={"limit": 2, "cursor": p1["next"]})).json()
    assert p2["next"] is None


async def test_work_items_include_total(pg_client):
    """include_total=true returns an integer total."""
    client, sessions, client_id = pg_client
    items = await _insert_work_items(sessions, client_id, 4)

    body = (await client.get("/v1/work-items", params={"limit": 2, "include_total": "true"})).json()
    assert body["total"] is not None
    assert body["total"] >= len(items)


async def test_work_items_invalid_limit(pg_client):
    client, _, _cid = pg_client
    assert (await client.get("/v1/work-items", params={"limit": 0})).status_code == 422
    assert (await client.get("/v1/work-items", params={"limit": 101})).status_code == 422


async def test_work_items_invalid_sort_pattern(pg_client):
    client, _, _cid = pg_client
    assert (await client.get("/v1/work-items", params={"sort": "INVALID SORT"})).status_code == 422


async def test_work_items_unknown_sort_field(pg_client):
    """A syntactically valid but unknown sort field returns 422."""
    client, _, _cid = pg_client
    assert (await client.get("/v1/work-items", params={"sort": "cowabunga"})).status_code == 422


async def test_work_items_sort_by_encrypted_field_rejected(pg_client):
    """Sorting by a credential reference is not allowed and returns 422."""
    client, _, _cid = pg_client
    assert (await client.get("/v1/work-items", params={"sort": "credential_ref"})).status_code == 422


async def test_work_items_malformed_cursor(pg_client):
    client, _, _cid = pg_client
    assert (await client.get("/v1/work-items", params={"cursor": "not-valid-base64!!!"})).status_code == 422


async def test_work_items_cursor_past_last_row(pg_client):
    """A cursor anchored before all data returns an empty page without error."""
    client, sessions, client_id = pg_client
    await _insert_work_items(sessions, client_id, 2)

    past_cursor = encode_cursor(
        {
            "id": str(uuid.uuid4()),
            "created_at": "2000-01-01T00:00:00+00:00",
            "direction": "next",
            "sort_field": "created_at",
            "sort_direction": "desc",
            "sort_value": "2000-01-01T00:00:00+00:00",
        }
    )
    body = (await client.get("/v1/work-items", params={"limit": 10, "cursor": past_cursor})).json()
    assert body["resources"] == []
    assert body["next"] is None


# =============================================================================
# Work items — sort  (shared list_datasets: 6 work items with tied creation times)
# =============================================================================


async def test_sort_created_at_asc_first_item_is_oldest(sort_client):
    """sort=created_at returns the oldest item first."""
    items = await _all_items(sort_client, sort="created_at")
    assert len(items) == _TOTAL_SORT_ITEMS
    created_ats = [item["created_at"] for item in items]
    assert created_ats == sorted(created_ats)


async def test_sort_created_at_asc_is_opposite_of_desc(sort_client):
    """Ascending and descending created_at sorts are exact reverses."""
    asc_items = await _all_items(sort_client, sort="created_at")
    desc_items = await _all_items(sort_client, sort="-created_at")
    assert [i["id"] for i in asc_items] == [i["id"] for i in reversed(desc_items)]


async def test_sort_created_at_desc_first_item_is_newest(sort_client):
    """sort=-created_at (the default) returns the newest item first."""
    items = await _all_items(sort_client, sort="-created_at")
    assert len(items) == _TOTAL_SORT_ITEMS
    created_ats = [item["created_at"] for item in items]
    assert created_ats == sorted(created_ats, reverse=True)


async def test_sort_created_at_desc_pagination_covers_all_items(sort_client):
    """Paginating with sort=-created_at yields every item exactly once in order."""
    items = await _paginate_all(sort_client, sort="-created_at", page_size=2)
    assert len(items) == _TOTAL_SORT_ITEMS
    assert len({i["id"] for i in items}) == _TOTAL_SORT_ITEMS
    created_ats = [item["created_at"] for item in items]
    assert created_ats == sorted(created_ats, reverse=True)


async def test_sort_status_asc_order(sort_client):
    """sort=status groups the shared rows alphabetically: failed, pending."""
    items = await _all_items(sort_client, sort="status")
    assert len(items) == _TOTAL_SORT_ITEMS
    statuses = [item["status"] for item in items]
    assert statuses == sorted(statuses)
    assert statuses == ["failed"] * _ITEMS_PER_GROUP + ["pending"] * _ITEMS_PER_GROUP


async def test_sort_status_asc_pagination_covers_all_items(sort_client):
    """Paginating with sort=status yields every item exactly once in status order."""
    items = await _paginate_all(sort_client, sort="status", page_size=2)
    assert len(items) == _TOTAL_SORT_ITEMS
    assert len({i["id"] for i in items}) == _TOTAL_SORT_ITEMS
    statuses = [item["status"] for item in items]
    assert statuses == sorted(statuses)


async def test_sort_status_desc_order(sort_client):
    """sort=-status returns items in reverse alphabetical status order."""
    items = await _all_items(sort_client, sort="-status")
    assert len(items) == _TOTAL_SORT_ITEMS
    statuses = [item["status"] for item in items]
    assert statuses == sorted(statuses, reverse=True)


async def test_sort_resource_cleanup_status_asc(sort_client):
    """sort=resource_cleanup_status returns 'done' items before 'pending'."""
    items = await _all_items(sort_client, sort="resource_cleanup_status")
    assert len(items) == _TOTAL_SORT_ITEMS
    cleanup_statuses = [item["resource_cleanup_status"] for item in items]
    assert cleanup_statuses == sorted(cleanup_statuses)
    assert all(s == "done" for s in cleanup_statuses[:_ITEMS_PER_GROUP])
    assert all(s == "pending" for s in cleanup_statuses[_ITEMS_PER_GROUP:])


# =============================================================================
# Clusters  (shared list_datasets: 6 clusters, oldest-first)
# =============================================================================


async def test_clusters_default_sort_oldest_first(clusters_client):
    """Default ordering is created_at ascending (oldest first)."""
    client, cluster_ids = clusters_client
    body = (await client.get("/v1/clusters", params={"limit": 10})).json()
    assert [r["id"] for r in body["resources"]] == cluster_ids


async def test_clusters_all_pages_cover_all_items(clusters_client):
    """Walking next cursors yields every inserted item exactly once."""
    client, cluster_ids = clusters_client
    seen_ids: set[str] = set()
    cursor = None
    while True:
        params: dict[str, str | int] = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        body = (await client.get("/v1/clusters", params=params)).json()
        seen_ids.update(r["id"] for r in body["resources"])
        cursor = body.get("next")
        if cursor is None:
            break
    assert seen_ids == set(cluster_ids)


async def test_clusters_first_page_cursor_shape(clusters_client):
    """First page: next cursor present, no prev cursor."""
    client, _ = clusters_client
    body = (await client.get("/v1/clusters", params={"limit": 2})).json()
    assert len(body["resources"]) == 2
    assert body["next"] is not None
    assert body["prev"] is None


async def test_clusters_include_total(clusters_client):
    client, cluster_ids = clusters_client
    body = (await client.get("/v1/clusters", params={"limit": 2, "include_total": "true"})).json()
    assert body["total"] == len(cluster_ids)


async def test_clusters_invalid_limit(clusters_client):
    client, _ = clusters_client
    assert (await client.get("/v1/clusters", params={"limit": 0})).status_code == 422
    assert (await client.get("/v1/clusters", params={"limit": 101})).status_code == 422


async def test_clusters_malformed_cursor(clusters_client):
    client, _ = clusters_client
    assert (await client.get("/v1/clusters", params={"cursor": "!!!"})).status_code == 422


# =============================================================================
# Execution targets  (shared list_datasets: 6 targets, oldest-first)
# =============================================================================


async def test_execution_targets_default_sort_oldest_first(targets_client):
    """Default ordering is created_at ascending (oldest first)."""
    client, target_ids = targets_client
    body = (await client.get("/v1/execution-targets", params={"limit": 10})).json()
    assert [r["id"] for r in body["resources"]] == target_ids


async def test_execution_targets_all_pages_cover_all_items(targets_client):
    """Walking next cursors yields every inserted item exactly once."""
    client, target_ids = targets_client
    seen_ids: set[str] = set()
    cursor = None
    while True:
        params: dict[str, str | int] = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        body = (await client.get("/v1/execution-targets", params=params)).json()
        seen_ids.update(r["id"] for r in body["resources"])
        cursor = body.get("next")
        if cursor is None:
            break
    assert seen_ids == set(target_ids)


async def test_execution_targets_first_page_cursor_shape(targets_client):
    """First page: next cursor present, no prev cursor."""
    client, _ = targets_client
    body = (await client.get("/v1/execution-targets", params={"limit": 2})).json()
    assert len(body["resources"]) == 2
    assert body["next"] is not None
    assert body["prev"] is None


async def test_execution_targets_include_total(targets_client):
    client, target_ids = targets_client
    body = (await client.get("/v1/execution-targets", params={"limit": 2, "include_total": "true"})).json()
    assert body["total"] == len(target_ids)


async def test_execution_targets_invalid_limit(targets_client):
    client, _ = targets_client
    assert (await client.get("/v1/execution-targets", params={"limit": 0})).status_code == 422
    assert (await client.get("/v1/execution-targets", params={"limit": 101})).status_code == 422


async def test_execution_targets_malformed_cursor(targets_client):
    client, _ = targets_client
    assert (await client.get("/v1/execution-targets", params={"cursor": "!!!"})).status_code == 422


async def test_execution_targets_cursor_unknown_fields_stripped(targets_client):
    """A cursor with extra fields does not cause a server error."""
    client, _ = targets_client
    p1 = (await client.get("/v1/execution-targets", params={"limit": 1})).json()
    data = json.loads(base64.b64decode(p1["next"]))
    data["unknown_field"] = "should be stripped"
    tampered = base64.b64encode(json.dumps(data).encode()).decode()

    body = (await client.get("/v1/execution-targets", params={"limit": 1, "cursor": tampered})).json()
    assert len(body["resources"]) > 0
