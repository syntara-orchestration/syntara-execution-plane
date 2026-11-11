"""Caller-owned Cluster transactions commit and roll back actual database changes."""

from uuid import uuid4

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from execution_plane.cluster.cluster_store import ClusterNotFoundError, ClusterStore
from execution_plane.models.cluster import Cluster, ClusterStatus

pytestmark = pytest.mark.integration


@pytest.fixture
async def cluster_sessions(migrated_database):
    engine = create_async_engine(migrated_database)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield sessions
    finally:
        await engine.dispose()


@pytest.mark.parametrize("commit", [False, True], ids=["rollback", "commit"])
async def test_caller_controls_composed_cluster_changes(cluster_sessions, commit):
    cluster_id, actor = uuid4(), uuid4()
    try:
        async with cluster_sessions() as session:
            await session.begin()
            store = ClusterStore.from_transaction(session)
            await store.create(cluster_id, str(cluster_id), f"https://{cluster_id}.example", uuid4(), actor)
            await store.activate(cluster_id, actor)
            await store.update(cluster_id, updated_by=actor, name="updated")
            assert session.in_transaction()
            async with cluster_sessions() as observer:
                assert await observer.get(Cluster, cluster_id) is None
            if commit:
                await session.commit()
            else:
                await session.rollback()
        async with cluster_sessions() as observer:
            cluster = await observer.get(Cluster, cluster_id)
            if commit:
                assert cluster is not None
                assert cluster.name == "updated"
                assert cluster.status is ClusterStatus.ACTIVE
            else:
                assert cluster is None
    finally:
        async with cluster_sessions.begin() as session:
            await session.execute(delete(Cluster).where(Cluster.id == cluster_id))


@pytest.mark.parametrize(
    "method",
    [
        "update",
        "set_default",
        "ensure_default",
        "reactivate",
        "activate",
        "mark_drain_failed",
        "request_delete",
        "finalize_delete",
    ],
)
async def test_outer_rollback_restores_existing_cluster(cluster_sessions, method):
    cluster_id, actor = uuid4(), uuid4()
    store_args = (cluster_id, actor)
    store_kwargs = {}
    try:
        async with cluster_sessions() as session:
            store = ClusterStore.from_session(session)
            await store.create(cluster_id, str(cluster_id), f"https://{cluster_id}.example", uuid4(), actor)
            if method != "activate":
                await store.activate(cluster_id, actor)
            if method in {"reactivate", "finalize_delete"}:
                await store.request_delete(cluster_id, actor)
        async with cluster_sessions() as session:
            baseline = (await session.get(Cluster, cluster_id)).model_dump()
            defaults_before = set((await session.scalars(select(Cluster.id).where(Cluster.is_default))).all())
        if method == "update":
            store_args = (cluster_id,)
            store_kwargs = {"updated_by": actor, "name": "changed"}
        elif method == "reactivate":
            store_args = (cluster_id,)
            store_kwargs = {"updated_by": actor}
        elif method == "activate":
            store_args = (cluster_id, actor)
        elif method == "mark_drain_failed":
            store_args = (cluster_id, "failed", actor)
        elif method == "finalize_delete":
            store_args = (cluster_id,)
        async with cluster_sessions() as session:
            await session.begin()
            store = ClusterStore.from_transaction(session)
            await getattr(store, method)(*store_args, **store_kwargs)
            assert session.in_transaction()
            await session.rollback()
        async with cluster_sessions() as observer:
            cluster = await observer.get(Cluster, cluster_id)
            assert cluster is not None
            assert cluster.model_dump() == baseline
            defaults_after = set((await observer.scalars(select(Cluster.id).where(Cluster.is_default))).all())
            assert defaults_after == defaults_before
    finally:
        async with cluster_sessions.begin() as session:
            await session.execute(delete(Cluster).where(Cluster.id == cluster_id))


async def test_domain_error_does_not_rollback_callers_earlier_changes(cluster_sessions):
    cluster_id, actor = uuid4(), uuid4()
    try:
        async with cluster_sessions.begin() as session:
            store = ClusterStore.from_transaction(session)
            await store.create(cluster_id, str(cluster_id), f"https://{cluster_id}.example", uuid4(), actor)
            with pytest.raises(ClusterNotFoundError):
                await store.update(uuid4(), updated_by=actor, name="missing")
            assert session.in_transaction()
            await store.update(cluster_id, updated_by=actor, name="survived")
        async with cluster_sessions() as observer:
            cluster = await observer.get(Cluster, cluster_id)
            assert cluster is not None
            assert cluster.name == "survived"
    finally:
        async with cluster_sessions.begin() as session:
            await session.execute(delete(Cluster).where(Cluster.id == cluster_id))
