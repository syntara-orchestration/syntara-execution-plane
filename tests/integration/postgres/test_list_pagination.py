"""Integration tests for EP list endpoints: pagination, default ordering, and sort parameter.

Module-scoped fixtures insert data once per module to avoid repeated DB round-trips.
The sort tests share 30 work items (10 per status group); cluster and target tests
share 5 items each.

Work-item sort dataset layout:
  Items  0- 9  status=completed  resource_cleanup_status=done
  Items 10-19  status=failed     resource_cleanup_status=done
  Items 20-29  status=pending    resource_cleanup_status=pending
  Alphabetical status order: completed < failed < pending
"""

from __future__ import annotations

import base64
import json
import os
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from execution_plane.api.auth import ServiceIdentity, get_service_identity
from execution_plane.api.main import create_app
from execution_plane.api.pagination import encode_cursor
from execution_plane.config import get_ep_settings
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.cluster_binding import ClusterBinding
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.models.work_item import WorkItem, WorkItemStatus

pytestmark = pytest.mark.integration

ALL_SCOPES = frozenset(
    {
        "work-items:read",
        "clusters:read",
        "execution-targets:read",
        "work.execute",
        "cluster.register",
        "execution_target.register",
    }
)

_BASE_TIME = datetime(2025, 6, 1, 12, 0, 0, tzinfo=UTC)

_SORT_GROUPS = [
    (WorkItemStatus.COMPLETED, "done"),
    (WorkItemStatus.FAILED, "done"),
    (WorkItemStatus.PENDING, "pending"),
]
_ITEMS_PER_GROUP = 10
_TOTAL_SORT_ITEMS = _ITEMS_PER_GROUP * len(_SORT_GROUPS)  # 30


# =============================================================================
# Module-scoped data fixtures
# =============================================================================


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def shared_work_item_data(migrated_database):
    """Insert 30 work items once; yield (client_id, db_url)."""
    client_id = f"test-sort-{uuid.uuid4()}"
    engine = create_async_engine(migrated_database)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session:
        for group_index, (status, cleanup_status) in enumerate(_SORT_GROUPS):
            for i in range(_ITEMS_PER_GROUP):
                item_index = group_index * _ITEMS_PER_GROUP + i
                session.add(
                    WorkItem(
                        id=uuid.uuid4(),
                        client_id=client_id,
                        status=status,
                        payload={"index": item_index},
                        resource_cleanup_status=cleanup_status,
                        created_at=_BASE_TIME + timedelta(seconds=item_index),
                    )
                )
        await session.commit()
    await engine.dispose()
    yield client_id, migrated_database


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def shared_cluster_data(migrated_database):
    """Insert 5 cluster bindings once; yield (client_id, binding_ids, db_url).

    Sets EP_DATABASE_URL directly because the monkeypatch-based integration fixture
    is function-scoped and does not run during module fixture setup.
    """
    _prev = os.environ.get("EP_DATABASE_URL")
    os.environ["EP_DATABASE_URL"] = migrated_database
    get_ep_settings.cache_clear()

    client_id = f"test-clusters-{uuid.uuid4()}"
    engine = create_async_engine(migrated_database)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    binding_ids = []
    async with sessions() as session:
        for i in range(5):
            binding = ClusterBinding(
                client_id=client_id,
                source_integration_id=uuid.uuid4(),
                name=f"shared-cluster-{i}",
                endpoint=f"https://shared-cluster-{i}.test",
                namespace="ep",
                credential="test-key",
                revision=1,
                observed_revision=0,
                status="pending",
                enabled=True,
                labels={},
                created_at=_BASE_TIME + timedelta(seconds=i),
                updated_at=_BASE_TIME + timedelta(seconds=i),
            )
            session.add(binding)
            binding_ids.append(str(binding.source_integration_id))
        await session.commit()
    await engine.dispose()

    if _prev is None:
        os.environ.pop("EP_DATABASE_URL", None)
    else:
        os.environ["EP_DATABASE_URL"] = _prev
    get_ep_settings.cache_clear()

    yield client_id, binding_ids, migrated_database


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def shared_target_data(migrated_database):
    """Insert 5 execution targets once; yield (client_id, target_ids, db_url).

    Sets EP_DATABASE_URL directly for the same reason as shared_cluster_data.
    """
    _prev = os.environ.get("EP_DATABASE_URL")
    os.environ["EP_DATABASE_URL"] = migrated_database
    get_ep_settings.cache_clear()

    client_id = f"test-targets-{uuid.uuid4()}"
    engine = create_async_engine(migrated_database)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    target_ids = []
    async with sessions() as session:
        cluster = Cluster(
            id=uuid.uuid4(),
            source_client_id=client_id,
            source_integration_id=uuid.uuid4(),
            name="shared-cluster",
            endpoint="https://shared-cluster.test",
            api_key="secret",
            status=ClusterStatus.ACTIVE,
            created_by=uuid.uuid4(),
            updated_by=uuid.uuid4(),
            created_at=_BASE_TIME,
            updated_at=_BASE_TIME,
        )
        session.add(cluster)
        await session.flush()
        for i in range(5):
            target = ExecutionTarget(
                id=uuid.uuid4(),
                cluster_id=cluster.id,
                name=f"shared-target-{i}",
                backend_type=BackendType.VANILLA_K8S,
                endpoint="https://target.test",
                placement=KubernetesPlacement(namespace="ep"),
                status=TargetStatus.ACTIVE,
                enabled=True,
                is_default=False,
                api_key="secret",
                labels={},
                created_by=uuid.uuid4(),
                updated_by=uuid.uuid4(),
                created_at=_BASE_TIME + timedelta(seconds=i),
                updated_at=_BASE_TIME + timedelta(seconds=i),
            )
            session.add(target)
            target_ids.append(str(target.id))
        await session.commit()
    await engine.dispose()

    if _prev is None:
        os.environ.pop("EP_DATABASE_URL", None)
    else:
        os.environ["EP_DATABASE_URL"] = _prev
    get_ep_settings.cache_clear()

    yield client_id, target_ids, migrated_database


# =============================================================================
# Function-scoped test fixtures
# =============================================================================


@pytest.fixture
async def pg_client(migrated_database):
    """Yield (http_client, sessions, client_id) with a unique client_id per test."""
    client_id = f"test-pagination-{uuid.uuid4()}"
    engine = create_async_engine(migrated_database)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    app = create_app()
    app.state.ep_engine = engine
    app.state.ep_session_factory = sessions
    app.dependency_overrides[get_service_identity] = lambda: ServiceIdentity(client_id=client_id, scopes=ALL_SCOPES)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
        yield client, sessions, client_id
    await engine.dispose()


@pytest.fixture
async def sort_client(shared_work_item_data):
    """Function-scoped HTTP client backed by the module-scoped work-item data."""
    client_id, db_url = shared_work_item_data
    engine = create_async_engine(db_url)
    app = create_app()
    app.state.ep_engine = engine
    app.state.ep_session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.dependency_overrides[get_service_identity] = lambda: ServiceIdentity(
        client_id=client_id, scopes=frozenset({"work-items:read"})
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
        yield client
    await engine.dispose()


@pytest.fixture
async def clusters_client(shared_cluster_data):
    """Function-scoped HTTP client backed by the module-scoped cluster data."""
    client_id, binding_ids, db_url = shared_cluster_data
    engine = create_async_engine(db_url)
    app = create_app()
    app.state.ep_engine = engine
    app.state.ep_session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.dependency_overrides[get_service_identity] = lambda: ServiceIdentity(
        client_id=client_id, scopes=frozenset({"clusters:read"})
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
        yield client, binding_ids
    await engine.dispose()


@pytest.fixture
async def targets_client(shared_target_data):
    """Function-scoped HTTP client backed by the module-scoped target data."""
    client_id, target_ids, db_url = shared_target_data
    engine = create_async_engine(db_url)
    app = create_app()
    app.state.ep_engine = engine
    app.state.ep_session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.dependency_overrides[get_service_identity] = lambda: ServiceIdentity(
        client_id=client_id, scopes=frozenset({"execution-targets:read"})
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
        yield client, target_ids
    await engine.dispose()


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
        source_integration_id=uuid.uuid4(),
        name="test-cluster",
        endpoint="https://cluster.test",
        api_key="secret",
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
                api_key="secret",
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


async def _paginate_all(client, sort: str, page_size: int = 7) -> list[dict]:
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
    """Sorting by an encrypted field (api_key) is not allowed and returns 422."""
    client, _, _cid = pg_client
    assert (await client.get("/v1/work-items", params={"sort": "api_key"})).status_code == 422


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
# Work items — sort  (shared_work_item_data: 30 items across 3 status groups)
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
    items = await _paginate_all(sort_client, sort="-created_at", page_size=7)
    assert len(items) == _TOTAL_SORT_ITEMS
    assert len({i["id"] for i in items}) == _TOTAL_SORT_ITEMS
    created_ats = [item["created_at"] for item in items]
    assert created_ats == sorted(created_ats, reverse=True)


async def test_sort_status_asc_order(sort_client):
    """sort=status groups items alphabetically: completed, failed, pending."""
    items = await _all_items(sort_client, sort="status")
    assert len(items) == _TOTAL_SORT_ITEMS
    statuses = [item["status"] for item in items]
    assert statuses == sorted(statuses)
    assert all(s == "completed" for s in statuses[:_ITEMS_PER_GROUP])
    assert all(s == "failed" for s in statuses[_ITEMS_PER_GROUP : _ITEMS_PER_GROUP * 2])
    assert all(s == "pending" for s in statuses[_ITEMS_PER_GROUP * 2 :])


async def test_sort_status_asc_pagination_covers_all_items(sort_client):
    """Paginating with sort=status yields every item exactly once in status order."""
    items = await _paginate_all(sort_client, sort="status", page_size=7)
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
    assert all(s == "done" for s in cleanup_statuses[: _ITEMS_PER_GROUP * 2])
    assert all(s == "pending" for s in cleanup_statuses[_ITEMS_PER_GROUP * 2 :])


# =============================================================================
# Clusters  (shared_cluster_data: 5 bindings, oldest-first)
# =============================================================================


async def test_clusters_default_sort_oldest_first(clusters_client):
    """Default ordering is created_at ascending (oldest first)."""
    client, binding_ids = clusters_client
    body = (await client.get("/v1/clusters", params={"limit": 10})).json()
    assert [r["id"] for r in body["resources"]] == binding_ids


async def test_clusters_all_pages_cover_all_items(clusters_client):
    """Walking next cursors yields every inserted item exactly once."""
    client, binding_ids = clusters_client
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
    assert seen_ids == set(binding_ids)


async def test_clusters_first_page_cursor_shape(clusters_client):
    """First page: next cursor present, no prev cursor."""
    client, _ = clusters_client
    body = (await client.get("/v1/clusters", params={"limit": 2})).json()
    assert len(body["resources"]) == 2
    assert body["next"] is not None
    assert body["prev"] is None


async def test_clusters_include_total(clusters_client):
    client, binding_ids = clusters_client
    body = (await client.get("/v1/clusters", params={"limit": 2, "include_total": "true"})).json()
    assert body["total"] == len(binding_ids)


async def test_clusters_invalid_limit(clusters_client):
    client, _ = clusters_client
    assert (await client.get("/v1/clusters", params={"limit": 0})).status_code == 422
    assert (await client.get("/v1/clusters", params={"limit": 101})).status_code == 422


async def test_clusters_malformed_cursor(clusters_client):
    client, _ = clusters_client
    assert (await client.get("/v1/clusters", params={"cursor": "!!!"})).status_code == 422


# =============================================================================
# Execution targets  (shared_target_data: 5 targets, oldest-first)
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
