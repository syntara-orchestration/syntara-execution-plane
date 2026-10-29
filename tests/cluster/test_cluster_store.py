"""Additional unit tests for ClusterStore persistence branches."""

from __future__ import annotations

import gc
import uuid
from datetime import UTC, datetime
from typing import Self

import pytest
from sqlalchemy.orm import make_transient_to_detached

from execution_plane.cluster.cluster_store import (
    ClusterNotEligibleForDefaultError,
    ClusterNotFoundError,
    ClusterStore,
    DefaultClusterError,
)
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane.models.execution_target_placement import KubernetesPlacement

_DATABASE_UNAVAILABLE = "database unavailable"


def _cluster(*, status: ClusterStatus = ClusterStatus.ACTIVE, is_default: bool = False) -> Cluster:
    now = datetime.now(UTC)
    return Cluster(
        id=uuid.uuid4(),
        name="cluster-a",
        endpoint="https://cluster.example",
        credential_ref=uuid.uuid4(),
        status=status,
        enabled=status is not ClusterStatus.ERROR,
        is_default=is_default,
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


def _target(cluster_id: uuid.UUID) -> ExecutionTarget:
    now = datetime.now(UTC)
    return ExecutionTarget(
        id=uuid.uuid4(),
        cluster_id=cluster_id,
        name="target-a",
        backend_type=BackendType.VANILLA_K8S,
        endpoint="https://target.example",
        placement=KubernetesPlacement(namespace="execution"),
        credential_ref=uuid.uuid4(),
        status=TargetStatus.ACTIVE,
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


class _Result:
    def __init__(self, clusters: list[Cluster] | None = None, targets: list[ExecutionTarget] | None = None) -> None:
        self.clusters = clusters or []
        self.targets = targets or []

    def scalars(self) -> Self:
        return self

    def all(self) -> list[Cluster] | list[ExecutionTarget]:
        return self.clusters or self.targets

    def scalar_one_or_none(self) -> Cluster | None:
        return self.clusters[0] if len(self.clusters) == 1 else None


class _Session:
    def __init__(
        self,
        *,
        cluster: Cluster | None = None,
        targets: list[ExecutionTarget] | None = None,
        fail_commit: bool = False,
    ) -> None:
        self.cluster = cluster
        self.result = _Result(targets=targets)
        self.fail_commit = fail_commit
        self.deleted: Cluster | None = None
        self.added: Cluster | None = None
        self.rollbacks = 0
        self.executed: list[object] = []
        self.flushes: list[tuple[bool, bool]] = []
        self.get_for_update: list[bool] = []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def add(self, cluster: Cluster) -> None:
        self.added = cluster

    async def get(self, _model: object, _cluster_id: uuid.UUID, **kwargs: object) -> Cluster | None:
        self.get_for_update.append(bool(kwargs.get("with_for_update", False)))
        return self.cluster

    async def execute(self, statement: object) -> _Result:
        self.executed.append(statement)
        return self.result

    async def flush(self) -> None:
        assert self.cluster is not None
        previous_default = self.result.scalar_one_or_none()
        previous_is_default = previous_default.is_default if previous_default is not None else False
        self.flushes.append((self.cluster.is_default, previous_is_default))

    async def commit(self) -> None:
        if self.fail_commit:
            raise RuntimeError(_DATABASE_UNAVAILABLE)

    async def rollback(self) -> None:
        self.rollbacks += 1

    async def delete(self, cluster: Cluster) -> None:
        self.deleted = cluster


class _SessionFactory:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def __call__(self) -> _Session:
        return self.session


def _store(session: _Session) -> ClusterStore:
    store = ClusterStore("postgresql+asyncpg://localhost/syntara")
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]
    return store


@pytest.mark.asyncio
async def test_get_redacts_credential_refs_and_returns_none_when_missing() -> None:
    cluster = _cluster()
    store = _store(_Session(cluster=cluster))

    result = await store.get(cluster.id)
    assert result is not None
    assert result.credential_ref == uuid.UUID(int=0)
    with_secret = await store.get_with_credentials(cluster.id)
    assert with_secret is cluster
    assert with_secret.credential_ref == cluster.credential_ref
    assert await _store(_Session()).get(uuid.uuid4()) is None
    await store.close()


def test_public_cluster_is_independent_of_the_original_orm_instance() -> None:
    cluster = _cluster()
    make_transient_to_detached(cluster)

    public = ClusterStore._without_secret(cluster)
    del cluster
    gc.collect()

    public.status = ClusterStatus.ERROR
    assert public.status is ClusterStatus.ERROR


@pytest.mark.asyncio
async def test_create_preserves_database_rejection_for_a_duplicate_endpoint() -> None:
    from sqlalchemy.exc import IntegrityError

    class DuplicateEndpointSession(_Session):
        async def commit(self) -> None:
            statement = "insert"
            raise IntegrityError(statement, {}, RuntimeError("clusters_endpoint_key"))

    session = DuplicateEndpointSession()
    store = _store(session)

    with pytest.raises(IntegrityError):
        await store.create(uuid.uuid4(), "cluster-a", "https://cluster.example", uuid.uuid4(), uuid.uuid4())

    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_list_redacts_credential_refs_and_supports_status_and_enabled_filters() -> None:
    cluster = _cluster()
    store = _store(_Session())
    session = _Session()
    session.result = _Result([cluster])
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    result = await store.list(status=ClusterStatus.ACTIVE, enabled=True)

    assert [item.credential_ref for item in result] == [uuid.UUID(int=0)]
    await store.close()


@pytest.mark.asyncio
async def test_record_discovery_state_rolls_back_for_a_missing_cluster() -> None:
    session = _Session()
    store = _store(session)

    with pytest.raises(ClusterNotFoundError):
        await store.record_discovery_state(uuid.uuid4(), ClusterStatus.ERROR, "failed", uuid.uuid4())

    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_record_discovery_state_does_not_undo_a_delete_request() -> None:
    cluster = _cluster(status=ClusterStatus.DRAINING)
    cluster.enabled = False
    session = _Session(cluster=cluster)
    store = _store(session)

    result = await store.record_discovery_state(cluster.id, ClusterStatus.ACTIVE, None, uuid.uuid4())

    assert result.status is ClusterStatus.DRAINING
    assert result.enabled is False
    await store.close()


@pytest.mark.asyncio
async def test_mark_drain_failed_persists_error_on_a_draining_cluster() -> None:
    cluster = _cluster(status=ClusterStatus.DRAINING)
    cluster.enabled = False
    session = _Session(cluster=cluster)
    store = _store(session)
    actor_id = uuid.uuid4()

    result = await store.mark_drain_failed(cluster.id, "cluster drain failed", actor_id)

    assert result.status is ClusterStatus.ERROR
    assert result.enabled is False
    assert result.status_message == "cluster drain failed"
    assert result.updated_by == actor_id
    await store.close()


@pytest.mark.asyncio
async def test_request_delete_rolls_back_for_a_missing_cluster() -> None:
    session = _Session()
    store = _store(session)

    with pytest.raises(ClusterNotFoundError):
        await store.request_delete(uuid.uuid4(), uuid.uuid4())

    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_finalize_delete_removes_a_drained_cluster_without_targets() -> None:
    cluster = _cluster(status=ClusterStatus.DRAINING)
    cluster.enabled = False
    session = _Session(cluster=cluster)
    store = _store(session)

    await store.finalize_delete(cluster.id)

    assert session.deleted is cluster
    await store.close()


@pytest.mark.asyncio
async def test_finalize_delete_ignores_a_missing_cluster() -> None:
    session = _Session()
    store = _store(session)

    await store.finalize_delete(uuid.uuid4())

    assert session.rollbacks == 0
    await store.close()


@pytest.mark.asyncio
async def test_finalize_delete_rolls_back_when_commit_fails() -> None:
    cluster = _cluster(status=ClusterStatus.DRAINING)
    cluster.enabled = False
    session = _Session(cluster=cluster, fail_commit=True)
    store = _store(session)

    with pytest.raises(RuntimeError, match="database unavailable"):
        await store.finalize_delete(cluster.id)

    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_request_delete_marks_each_target_as_draining() -> None:
    cluster = _cluster()
    targets = [_target(cluster.id)]
    session = _Session(cluster=cluster, targets=targets)
    store = _store(session)

    result = await store.request_delete(cluster.id, uuid.uuid4())

    assert result.status is ClusterStatus.DRAINING
    assert targets[0].status is TargetStatus.DRAINING
    await store.close()


@pytest.mark.asyncio
async def test_finalize_delete_ignores_a_cluster_with_remaining_targets() -> None:
    cluster = _cluster(status=ClusterStatus.DRAINING)
    cluster.enabled = False
    session = _Session(cluster=cluster, targets=[_target(cluster.id)])
    store = _store(session)

    await store.finalize_delete(cluster.id)

    await store.close()


@pytest.mark.asyncio
async def test_finalize_delete_ignores_an_enabled_cluster() -> None:
    cluster = _cluster(status=ClusterStatus.DRAINING)
    cluster.enabled = True
    session = _Session(cluster=cluster)
    store = _store(session)

    await store.finalize_delete(cluster.id)

    await store.close()


@pytest.mark.asyncio
async def test_update_patches_mutable_fields_and_redacts_credential_refs() -> None:
    cluster = _cluster()
    actor_id = uuid.uuid4()
    credential_ref = uuid.uuid4()
    session = _Session(cluster=cluster)
    store = _store(session)

    result = await store.update(
        cluster.id,
        updated_by=actor_id,
        name="renamed-cluster",
        endpoint="https://new.example",
        credential_ref=credential_ref,
    )

    assert cluster.name == "renamed-cluster"
    assert cluster.endpoint == "https://new.example"
    assert cluster.credential_ref == credential_ref
    assert cluster.updated_by == actor_id
    assert result.credential_ref == uuid.UUID(int=0)
    await store.close()


@pytest.mark.asyncio
async def test_update_leaves_unspecified_fields_unchanged() -> None:
    cluster = _cluster()
    original_name = cluster.name
    original_endpoint = cluster.endpoint
    credential_ref = uuid.uuid4()
    session = _Session(cluster=cluster)
    store = _store(session)

    await store.update(cluster.id, updated_by=uuid.uuid4(), credential_ref=credential_ref)

    assert cluster.name == original_name
    assert cluster.endpoint == original_endpoint
    assert cluster.credential_ref == credential_ref
    await store.close()


@pytest.mark.asyncio
async def test_update_rolls_back_for_a_missing_cluster() -> None:
    session = _Session()
    store = _store(session)

    with pytest.raises(ClusterNotFoundError):
        await store.update(uuid.uuid4(), updated_by=uuid.uuid4(), name="new-name")

    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_update_rolls_back_when_commit_fails() -> None:
    cluster = _cluster()
    session = _Session(cluster=cluster, fail_commit=True)
    store = _store(session)

    with pytest.raises(RuntimeError, match=_DATABASE_UNAVAILABLE):
        await store.update(cluster.id, updated_by=uuid.uuid4(), name="new-name")

    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_set_default_demotes_the_previous_global_default_atomically() -> None:
    cluster = _cluster()
    previous_default = _cluster(is_default=True)
    actor_id = uuid.uuid4()
    session = _Session(cluster=cluster)
    session.result = _Result([previous_default])
    store = _store(session)

    result = await store.set_default(cluster.id, actor_id)

    assert result.is_default is True
    assert cluster.is_default is True
    assert previous_default.is_default is False
    assert cluster.updated_by == actor_id
    assert previous_default.updated_by == actor_id
    assert session.get_for_update == [True]
    assert session.flushes == [(False, False)]
    await store.close()


@pytest.mark.asyncio
async def test_set_default_rejects_an_inactive_cluster() -> None:
    cluster = _cluster(status=ClusterStatus.DRAINING)
    cluster.enabled = False
    session = _Session(cluster=cluster)
    store = _store(session)

    with pytest.raises(ClusterNotEligibleForDefaultError):
        await store.set_default(cluster.id, uuid.uuid4())

    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_ensure_default_promotes_the_cluster_when_none_exists() -> None:
    cluster = _cluster()
    actor_id = uuid.uuid4()
    session = _Session(cluster=cluster)
    store = _store(session)

    result = await store.ensure_default(cluster.id, actor_id)

    assert result.is_default is True
    assert cluster.updated_by == actor_id
    assert session.flushes == []
    await store.close()


@pytest.mark.asyncio
async def test_request_delete_rejects_the_current_global_default() -> None:
    cluster = _cluster(is_default=True)
    session = _Session(cluster=cluster)
    store = _store(session)

    with pytest.raises(DefaultClusterError):
        await store.request_delete(cluster.id, uuid.uuid4())

    assert cluster.status is ClusterStatus.ACTIVE
    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_reactivate_re_enables_a_draining_cluster() -> None:
    cluster = _cluster(status=ClusterStatus.DRAINING)
    cluster.enabled = False
    actor_id = uuid.uuid4()
    credential_ref = uuid.uuid4()
    session = _Session(cluster=cluster)
    store = _store(session)

    result = await store.reactivate(
        cluster.id,
        updated_by=actor_id,
        endpoint="https://new.example",
        credential_ref=credential_ref,
        labels={"env": "dev"},
    )

    assert cluster.enabled is True
    assert cluster.status is ClusterStatus.ACTIVE
    assert cluster.status_message is None
    assert cluster.endpoint == "https://new.example"
    assert cluster.credential_ref == credential_ref
    assert cluster.labels == {"env": "dev"}
    assert cluster.updated_by == actor_id
    assert result.credential_ref == uuid.UUID(int=0)
    await store.close()


@pytest.mark.asyncio
async def test_reactivate_rolls_back_for_a_missing_cluster() -> None:
    session = _Session()
    store = _store(session)

    with pytest.raises(ClusterNotFoundError):
        await store.reactivate(uuid.uuid4(), updated_by=uuid.uuid4())

    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_get_for_client_returns_none_when_cluster_belongs_to_another_client() -> None:
    cluster = _cluster()
    cluster.source_client_id = "owner-a"
    session = _Session(cluster=cluster)
    store = _store(session)

    result = await store.get_for_client("owner-b", cluster.id)

    assert result is None
    await store.close()
