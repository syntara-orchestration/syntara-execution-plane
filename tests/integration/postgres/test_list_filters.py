"""PostgreSQL HTTP contracts; parameter IDs display the exact GET query being tested."""

from __future__ import annotations

import uuid
from datetime import timedelta
from urllib.parse import parse_qsl

import pytest

from execution_plane.api.pagination import encode_cursor
from tests.integration.postgres.list_support import BASE_TIME, PATHS

pytestmark = pytest.mark.integration


def _params(api, query) -> dict[str, str]:
    """Expand UUID/date placeholders while keeping request strings readable in test IDs."""
    substitutions = {f"id{i}": row["id"] for i, row in enumerate(api.records)}
    substitutions.update(
        foreign=api.foreign_ids[0],
        unknown=str(uuid.uuid4()),
        ready="pending" if api.path == "/v1/work-items" else "active",
        not_ready="error" if api.path == "/v1/clusters" else "failed",
    )
    substitutions.update({f"time{i}": (BASE_TIME + timedelta(seconds=i)).isoformat() for i in range(3)})
    # Parse before expansion so the '+' in timezone offsets is not URL-decoded as a space.
    return {key: value.format(**substitutions) for key, value in parse_qsl(query, keep_blank_values=True)}


FILTER_REQUESTS = [
    ("id={id0}", [0]),
    ("id[eq]={id1}", [1]),
    ("id[in]={id0},{id2},{id4}", [0, 2, 4]),
    ("id[in]= {id0},,{id2},{id0}, ", [0, 2]),
    ("id={id0},{id3}", [0, 3]),
    ("id[in]=", []),
    ("id[in]=,,", []),
    ("id[in]={unknown}", []),
    ("id[in]={foreign}", []),
    ("id[in]={foreign},{id2}", [2]),
    ("status={ready}", [0, 2, 4]),
    ("status[in]={ready},{not_ready}", list(range(6))),
    ("status={ready},{not_ready}", list(range(6))),
    ("id[in]={id0},{id1},{id2}&status={ready}", [0, 2]),
    ("created_at[gt]={time0}", [2, 3, 4, 5]),
    ("created_at[gte]={time1}", [2, 3, 4, 5]),
    ("created_at[lt]={time1}", [0, 1]),
    ("created_at[lte]={time1}", [0, 1, 2, 3]),
    ("created_at[gte]={time1}&created_at[lte]={time2}", [2, 3, 4, 5]),
    ("created_at[gte]=2025-06-01", list(range(6))),
    ("created_at[gte]=2025-06-01T12:00:01Z", [2, 3, 4, 5]),
    ("id[isnull]=true", []),
    ("id[isnull]=false", list(range(6))),
]


@pytest.mark.parametrize(("query", "indices"), FILTER_REQUESTS, ids=[f"GET ?{q}" for q, _ in FILTER_REQUESTS])
async def test_get_list_filters_and_filtered_total(list_api, query, indices):
    await _assert_filtered_request(list_api, query, indices)


async def _assert_filtered_request(list_api, query, indices) -> None:
    """Assert a concrete GET request against PostgreSQL, including its filtered count."""
    response = await list_api.client.get(
        list_api.path, params={**_params(list_api, query), "sort": "created_at", "include_total": "true"}
    )
    assert response.status_code == 200, response.text
    body = response.json()
    expected = sorted((list_api.records[i] for i in indices), key=lambda row: (row["created_at"], row["id"]))
    assert [row["id"] for row in body["resources"]] == [row["id"] for row in expected]
    assert body["total"] == len(expected)
    assert body["next"] is None
    assert body["prev"] is None


INVALID_REQUESTS = [
    "id=not-a-uuid",
    "id[in]={id0},not-a-uuid",
    "id__in={id0}",
    "id[contains]=abc",
    "created_at=not-a-date",
    "created_at[contains]=2025",
    "created_at[isnull]=maybe",
    "status[bogus]=failed",
    "unknown=foo",
    "client_id=other",
    "credential_ref=00000000-0000-0000-0000-000000000000",
    "payload[contains]=secret",
    "sort=credential_ref",
    "sort=unknown",
    "sort=last_ran_at",
]


@pytest.mark.parametrize("query", INVALID_REQUESTS, ids=[f"GET ?{q} -> 422" for q in INVALID_REQUESTS])
async def test_get_list_rejects_invalid_filter_or_sort(list_api, query):
    response = await list_api.client.get(list_api.path, params=_params(list_api, query))
    assert response.status_code == 422, response.text


CATALOG_REQUESTS = [
    ("name[contains]=ALP", [0]),
    ("name[starts_with]=Br", [1]),
    ("name[contains]=%25_", [4]),
    ("name=false", [5]),
    ("enabled=true", [0, 2, 4]),
    ("enabled=off", [1, 3, 5]),
    ("labels[env]=prod", [0, 2, 4]),
    ("labels[optional]=", [4]),
    ("labels[missing]=", []),
    ("labels[env]=prod&labels[region]=west", [0, 2, 4]),
    ("labels[env]=prod&enabled=false", []),
]


@pytest.mark.parametrize("list_data", PATHS[1:], indirect=True, ids=PATHS[1:])
@pytest.mark.parametrize(("query", "indices"), CATALOG_REQUESTS, ids=[f"GET ?{q}" for q, _ in CATALOG_REQUESTS])
async def test_get_targets_and_clusters_named_boolean_and_label_filters(list_api, query, indices):
    await _assert_filtered_request(list_api, query, indices)


SORT_FIELDS = {
    "/v1/work-items": ["id", "created_at", "status", "resource_cleanup_status"],
    "/v1/execution-targets": [
        "id",
        "created_at",
        "name",
        "endpoint",
        "status",
        "backend_type",
        "cluster_id",
        "enabled",
        "is_default",
    ],
    "/v1/clusters": [
        "id",
        "created_at",
        "updated_at",
        "name",
        "endpoint",
        "status",
        "enabled",
        "is_default",
    ],
}


SORT_REQUESTS = [
    (path, f"{prefix}{field}") for path, fields in SORT_FIELDS.items() for field in fields for prefix in ("", "-")
]


@pytest.mark.parametrize(
    ("list_data", "sort"),
    SORT_REQUESTS,
    indirect=["list_data"],
    ids=[f"GET {p}?sort={s}&limit=2&id[in]=five-ids" for p, s in SORT_REQUESTS],
)
async def test_get_sorted_filtered_list_forward_and_backward_pages(list_api, sort):
    field = sort.removeprefix("-")
    descending = sort.startswith("-")
    query = {
        "sort": sort,
        "limit": 2,
        "include_total": "true",
        "id[in]": ",".join(row["id"] for row in list_api.records[:5]),
    }
    expected = sorted(list_api.records[:5], key=lambda row: (row[field], row["id"]), reverse=descending)
    expected_pages = [[row["id"] for row in expected[i : i + 2]] for i in range(0, 5, 2)]
    body = None
    for index, expected_ids in enumerate(expected_pages):
        params = {**query, "cursor": body["next"]} if body else query
        response = await list_api.client.get(list_api.path, params=params)
        assert response.status_code == 200, f"GET {list_api.path} {params}: {response.text}"
        body = response.json()
        assert [row["id"] for row in body["resources"]] == expected_ids, sort
        assert body["total"] == 5
        assert (body["prev"] is None) == (index == 0)
        assert (body["next"] is None) == (index == 2)
    for index in (1, 0):
        response = await list_api.client.get(list_api.path, params={**query, "cursor": body["prev"]})
        assert response.status_code == 200, f"GET {list_api.path} sort={sort}: {response.text}"
        body = response.json()
        assert [row["id"] for row in body["resources"]] == expected_pages[index], sort
        assert (body["prev"] is None) == (index == 0)
        assert body["next"] is not None


SPECIAL_REQUESTS = [
    ("/v1/work-items", "claimed_at[isnull]=true", [0, 2, 4]),
    ("/v1/work-items", "claimed_at[isnull]=false", [1, 3, 5]),
    ("/v1/work-items", "completed_at[isnull]=true", list(range(6))),
    ("/v1/work-items", "status=not-a-status", None),
    ("/v1/work-items", "labels[env]=prod", None),
    ("/v1/clusters", "source_integration_id={id0}", [0]),
    ("/v1/clusters", "source_integration_id=not-a-uuid", None),
    ("/v1/clusters", "status_message[isnull]=true", list(range(6))),
    ("/v1/execution-targets", "is_default=true", [0]),
    ("/v1/execution-targets", "backend_type=not-a-backend", None),
    ("/v1/execution-targets", "status=not-a-status", None),
    ("/v1/execution-targets", "enabled=maybe", None),
    ("/v1/clusters", "enabled=maybe", None),
]


@pytest.mark.parametrize(
    ("list_data", "query", "indices"),
    SPECIAL_REQUESTS,
    indirect=["list_data"],
    ids=[f"GET {p}?{q}" for p, q, _ in SPECIAL_REQUESTS],
)
async def test_get_resource_specific_typed_filters(list_api, query, indices):
    if indices is not None:
        await _assert_filtered_request(list_api, query, indices)
    else:
        response = await list_api.client.get(list_api.path, params=_params(list_api, query))
        assert response.status_code == 422, response.text


INVALID_SORT_CURSORS = [
    ("/v1/work-items", "status", "not-a-valid-value"),
    ("/v1/execution-targets", "enabled", "not-a-valid-value"),
    ("/v1/execution-targets", "cluster_id", "not-a-uuid"),
    ("/v1/clusters", "enabled", "not-a-valid-value"),
    ("/v1/clusters", "is_default", "not-a-valid-value"),
    ("/v1/clusters", "is_default", ""),
    ("/v1/clusters", "updated_at", "not-a-date"),
    ("/v1/clusters", "updated_at", "2025-06-01T12:00:00"),
    ("/v1/clusters", "updated_at", ""),
]


@pytest.mark.parametrize(
    ("list_data", "field", "sort_value"),
    INVALID_SORT_CURSORS,
    indirect=["list_data"],
    ids=[f"GET {p}?sort={f}&cursor=<sort_value={v!r}> -> 422" for p, f, v in INVALID_SORT_CURSORS],
)
async def test_get_list_rejects_invalid_typed_sort_cursor(list_api, field, sort_value):
    token = encode_cursor(
        {
            "id": list_api.records[0]["id"],
            "created_at": BASE_TIME.isoformat(),
            "direction": "next",
            "sort_field": field,
            "sort_direction": "asc",
            "sort_value": sort_value,
        }
    )
    response = await list_api.client.get(list_api.path, params={"sort": field, "cursor": token})
    assert response.status_code == 422, response.text


async def test_get_openapi_documents_list_filters_and_sort_fields(list_api):
    response = await list_api.client.get("/openapi.json")
    assert response.status_code == 200
    parameters = response.json()["paths"][list_api.path]["get"]["parameters"]
    by_name = {parameter["name"]: parameter for parameter in parameters}
    assert {"id", "created_at", "status", "limit", "cursor", "include_total"} <= by_name.keys()
    assert "in" in by_name["id"]["description"]
    assert "gte" in by_name["created_at"]["description"]
    assert "id[in]" not in by_name
    assert "created_at[gte]" not in by_name
    assert set(by_name["sort"]["schema"]["enum"]) == {
        f"{prefix}{field}" for field in SORT_FIELDS[list_api.path] for prefix in ("", "-")
    }
    assert ("labels[key]" in by_name) == (list_api.path != "/v1/work-items")
