"""Common seeded resources for PostgreSQL filtering, sorting, and pagination requests."""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from execution_plane.config import get_ep_settings
from tests.integration.postgres.list_support import PATHS, list_http_client, ordered_ids, seed_list_dataset


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def list_datasets(migrated_database):
    """Seed each endpoint once; both list-test modules reuse these read-only datasets."""
    engine = create_async_engine(migrated_database)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        with pytest.MonkeyPatch.context() as env:
            env.setenv("EP_DATABASE_URL", migrated_database)
            get_ep_settings.cache_clear()
            async with sessions() as session:
                datasets = {path: await seed_list_dataset(session, path, migrated_database) for path in PATHS}
                await session.commit()
        return datasets
    finally:
        get_ep_settings.cache_clear()
        await engine.dispose()


@pytest.fixture(scope="module", params=PATHS, ids=PATHS)
def list_data(request, list_datasets):
    """Select one shared dataset, including for explicitly parameterized request cases."""
    return list_datasets[request.param]


@pytest.fixture
async def list_api(list_data):
    """Serve the selected dataset using the shared authenticated HTTP test harness."""
    async with list_http_client(list_data.db_url, list_data.client_id) as (client, _sessions):
        yield SimpleNamespace(client=client, **vars(list_data))


@pytest.fixture
async def sort_client(list_datasets):
    """Use exactly the same work items as the filtering request tests."""
    data = list_datasets["/v1/work-items"]
    async with list_http_client(data.db_url, data.client_id) as (client, _sessions):
        yield client


@pytest.fixture
async def clusters_client(list_datasets):
    """Use exactly the same clusters as the filtering request tests."""
    data = list_datasets["/v1/clusters"]
    async with list_http_client(data.db_url, data.client_id) as (client, _sessions):
        yield client, ordered_ids(data)


@pytest.fixture
async def targets_client(list_datasets):
    """Use exactly the same targets as the filtering request tests."""
    data = list_datasets["/v1/execution-targets"]
    async with list_http_client(data.db_url, data.client_id) as (client, _sessions):
        yield client, ordered_ids(data)


@pytest.fixture
async def pg_client(migrated_database):
    """Give mutation tests a fresh client namespace without changing shared seeded resources."""
    client_id = f"test-pagination-{uuid.uuid4()}"
    async with list_http_client(migrated_database, client_id) as (client, sessions):
        yield client, sessions, client_id
