"""Cluster mutations respect the caller's transaction ownership."""

from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from execution_plane.cluster.cluster_store import ClusterStore, DefaultClusterError
from execution_plane.models.cluster import ClusterStatus
from tests.cluster.test_cluster_store import _cluster

_MUTATIONS = (
    "create",
    "update",
    "set_default",
    "ensure_default",
    "reactivate",
    "record_discovery_state",
    "mark_drain_failed",
    "request_delete",
    "finalize_delete",
)


def _mutation(method: str) -> tuple[AsyncMock, tuple[object, ...], dict[str, object]]:
    cluster = _cluster()
    actor = uuid4()
    args = (cluster.id,)
    kwargs = {}
    if method == "create":
        args = (cluster.id, cluster.name, cluster.endpoint, cluster.credential_ref, actor)
    elif method in {"update", "reactivate"}:
        kwargs = {"updated_by": actor, "name": "changed"} if method == "update" else {"updated_by": actor}
    elif method == "record_discovery_state":
        cluster.status = ClusterStatus.REGISTERING
        args = (cluster.id, ClusterStatus.ACTIVE, None, actor)
    elif method == "mark_drain_failed":
        args = (cluster.id, "drain failed", actor)
    elif method == "finalize_delete":
        cluster.status = ClusterStatus.DRAINING
        cluster.enabled = False
    else:
        args = (cluster.id, actor)
    session = AsyncMock(spec=AsyncSession)
    session.get.return_value = cluster
    result = Mock()
    result.scalar_one_or_none.return_value = None
    result.scalars.return_value.all.return_value = []
    session.execute.return_value = result
    return session, args, kwargs


@pytest.mark.parametrize("method", _MUTATIONS)
@pytest.mark.parametrize("borrow_transaction", [False, True])
async def test_mutation_respects_transaction_owner(method, borrow_transaction):
    session, args, kwargs = _mutation(method)
    store = ClusterStore.from_transaction(session) if borrow_transaction else ClusterStore.from_session(session)

    await getattr(store, method)(*args, **kwargs)

    session.rollback.assert_not_awaited()
    session.close.assert_not_awaited()
    if borrow_transaction:
        session.flush.assert_awaited_once()
        session.commit.assert_not_awaited()
    else:
        session.commit.assert_awaited_once()
        session.flush.assert_not_awaited()


@pytest.mark.parametrize("method", _MUTATIONS)
@pytest.mark.parametrize("borrow_transaction", [False, True])
async def test_failed_mutation_leaves_rollback_to_transaction_owner(method, borrow_transaction):
    session, args, kwargs = _mutation(method)
    store = ClusterStore.from_transaction(session) if borrow_transaction else ClusterStore.from_session(session)
    persist = session.flush if borrow_transaction else session.commit
    persist.side_effect = RuntimeError("persistence failed")

    with pytest.raises(RuntimeError, match="persistence failed"):
        await getattr(store, method)(*args, **kwargs)

    if borrow_transaction:
        session.commit.assert_not_awaited()
        session.rollback.assert_not_awaited()
    else:
        session.rollback.assert_awaited_once()
    session.close.assert_not_awaited()


async def test_default_constraint_failure_preserves_caller_transaction_ownership():
    session, args, kwargs = _mutation("set_default")
    session.flush.side_effect = IntegrityError("update", {}, RuntimeError("duplicate default"))
    store = ClusterStore.from_transaction(session)

    with pytest.raises(DefaultClusterError) as error:
        await store.set_default(*args, **kwargs)

    assert isinstance(error.value.__cause__, IntegrityError)
    session.commit.assert_not_awaited()
    session.rollback.assert_not_awaited()
