"""Tests for execution-target persistence and lifecycle boundaries."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Self

import pytest

from execution_plane.database.engine import create_unpooled_engine
from execution_plane.execution_target.execution_target_store import (
    DefaultExecutionTargetError,
    ExecutionTargetNotFoundError,
    ExecutionTargetStore,
)
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane_client.models.execution_target import ExecutionTargetPlacement, KubernetesPlacement

_DATABASE_UNAVAILABLE = "database unavailable"

if TYPE_CHECKING:
    from collections.abc import Sequence


def _target(*, is_default: bool = False, status: TargetStatus = TargetStatus.REGISTERING) -> ExecutionTarget:
    """Build a target with concrete values independent of the store."""
    now = datetime.now(UTC)
    return ExecutionTarget(
        id=uuid.uuid4(),
        cluster_id=uuid.uuid4(),
        name="target-a",
        backend_type=BackendType.KUBERNETES,
        endpoint="https://target.example",
        placement=KubernetesPlacement(namespace="default"),
        credential_ref=uuid.uuid4(),
        is_default=is_default,
        status=status,
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


def _cluster() -> Cluster:
    now = datetime.now(UTC)
    return Cluster(
        id=uuid.uuid4(),
        name="cluster-a",
        endpoint="https://cluster.example",
        credential_ref=uuid.uuid4(),
        status=ClusterStatus.REGISTERING,
        enabled=True,
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


class _Result:
    def __init__(self, item: ExecutionTarget | None = None) -> None:
        self.item = item

    def scalar_one_or_none(self) -> ExecutionTarget | None:
        return self.item

    def scalars(self) -> Self:
        return self

    def all(self) -> list[ExecutionTarget]:
        return [] if self.item is None else [self.item]


class _Session:
    def __init__(self, *, result: _Result | None = None, fail_commit: bool = False) -> None:
        self.result = result or _Result()
        self.cluster = _cluster()
        self.fail_commit = fail_commit
        self.added: ExecutionTarget | None = None
        self.deleted: ExecutionTarget | None = None
        self.commits = 0
        self.rollbacks = 0

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def add(self, target: ExecutionTarget) -> None:
        self.added = target

    async def execute(self, _statement: object) -> _Result:
        return self.result

    async def get(self, model: object, _target_id: uuid.UUID, **_: object) -> ExecutionTarget | Cluster | None:
        if model is Cluster:
            return self.cluster
        return self.result.item

    async def commit(self) -> None:
        self.commits += 1
        if self.fail_commit:
            raise RuntimeError(_DATABASE_UNAVAILABLE)

    async def rollback(self) -> None:
        self.rollbacks += 1

    async def delete(self, target: ExecutionTarget) -> None:
        self.deleted = target


class _SessionFactory:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def __call__(self) -> _Session:
        return self.session


class _Store:
    def __init__(self, target: ExecutionTarget) -> None:
        self.target = target
        self.delete_requested = False
        self.updated: dict[str, object] = {}
        self.created: dict[str, object] = {}
        self.client_scoped_queries: list[tuple[str, int, uuid.UUID | None]] = []
        self.credential_reads: list[uuid.UUID] = []
        self.marked_failed = False
        self.finalized = False

    async def create(
        self,
        cluster_id: uuid.UUID,
        name: str,
        backend_type: BackendType,
        endpoint: str,
        credential_ref: uuid.UUID,
        is_default: bool,  # noqa: FBT001
        created_by: uuid.UUID,
        placement: ExecutionTargetPlacement,
        labels: dict[str, str] | None = None,
        *,
        target_id: uuid.UUID | None = None,
    ) -> ExecutionTarget:
        self.created = {
            "cluster_id": cluster_id,
            "name": name,
            "backend_type": backend_type,
            "endpoint": endpoint,
            "credential_ref": credential_ref,
            "is_default": is_default,
            "created_by": created_by,
            "placement": placement,
            "labels": labels,
            "target_id": target_id,
        }
        return self.target

    async def get(self, _target_id: uuid.UUID) -> ExecutionTarget | None:
        return self.target

    async def get_with_credentials(self, target_id: uuid.UUID) -> ExecutionTarget | None:
        self.credential_reads.append(target_id)
        return self.target

    async def list(
        self,
        cluster_id: uuid.UUID | None = None,
        eligible_only: bool = False,  # noqa: FBT001, FBT002
        status: TargetStatus | None = None,
        limit: int | None = None,
    ) -> Sequence[ExecutionTarget]:
        return [self.target]

    async def request_delete(self, _target_id: uuid.UUID, _updated_by: uuid.UUID) -> ExecutionTarget:
        if self.target.is_default:
            raise DefaultExecutionTargetError
        self.delete_requested = True
        return self.target

    async def finalize_delete(self, _target_id: uuid.UUID) -> None:
        self.finalized = True

    async def mark_failed(self, _target_id: uuid.UUID, _updated_by: uuid.UUID) -> ExecutionTarget:
        self.marked_failed = True
        return self.target

    async def activate(self, _target_id: uuid.UUID, _updated_by: uuid.UUID) -> ExecutionTarget:
        return self.target

    async def update(
        self,
        _target_id: uuid.UUID,
        *,
        updated_by: uuid.UUID,
        name: str | None = None,
        endpoint: str | None = None,
        placement: ExecutionTargetPlacement | None = None,
        labels: dict[str, str] | None = None,
        status_message: str | None = None,
        credential_ref: uuid.UUID | None = None,
    ) -> ExecutionTarget:
        self.updated = {
            "updated_by": updated_by,
            "name": name,
            "endpoint": endpoint,
            "placement": placement,
            "labels": labels,
            "status_message": status_message,
            "credential_ref": credential_ref,
        }
        return self.target

    async def reactivate(
        self,
        _target_id: uuid.UUID,
        *,
        updated_by: uuid.UUID,
    ) -> ExecutionTarget:
        self.updated = {"updated_by": updated_by}
        return self.target

    async def list_for_client(
        self, client_id: str, *, limit: int, target_id: uuid.UUID | None = None
    ) -> Sequence[ExecutionTarget]:
        self.client_scoped_queries.append((client_id, limit, target_id))
        return [self.target]


@pytest.mark.asyncio
async def test_store_creates_target_with_audit_fields_and_owns_the_commit() -> None:
    """Removing the store commit would leave newly registered targets unpersisted."""
    store = ExecutionTargetStore("postgresql+asyncpg://localhost/syntara")
    session = _Session()
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]
    cluster_id = uuid.uuid4()
    creator_id = uuid.uuid4()
    credential_ref = uuid.uuid4()

    target = await store.create(
        cluster_id,
        "target-a",
        BackendType.KUBERNETES,
        "https://target.example",
        credential_ref,
        is_default=False,
        created_by=creator_id,
        labels={"region": "eu-west"},
        placement=KubernetesPlacement(namespace="execution"),
    )

    assert target.cluster_id == cluster_id
    assert target.created_by == creator_id
    assert target.updated_by == creator_id
    assert target.labels == {"region": "eu-west", "_execution_target_id": str(target.id)}
    assert isinstance(target.placement, KubernetesPlacement)
    assert target.placement.namespace == "execution"
    assert target.credential_ref == uuid.UUID(int=0)
    assert session.added is not None
    assert session.added.credential_ref == credential_ref  # type: ignore[union-attr]
    assert session.commits == 1
    await store.close()


@pytest.mark.asyncio
async def test_target_store_creates_target_with_identity_label_and_cleanses_user_labels() -> None:
    target = _target()
    cluster = _cluster()
    cluster.status = ClusterStatus.ACTIVE
    session = _Session()
    session.cluster = cluster
    store = ExecutionTargetStore("postgresql+asyncpg://localhost/syntara")
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]
    commands = store

    result = await commands.create(
        target.cluster_id,
        target.name,
        target.backend_type,
        target.endpoint,
        target.credential_ref,
        is_default=False,
        created_by=uuid.uuid4(),
        placement=target.placement,
        labels={"region": "eu-west", "_execution_target_id": "user-supplied", "_internal": "user-supplied"},
    )

    assert result.labels == {"region": "eu-west", "_execution_target_id": str(result.id)}
    await store.close()


@pytest.mark.asyncio
async def test_store_rejects_a_second_default_target_for_a_cluster() -> None:
    """Dropping the default lookup would permit duplicate protected targets."""
    existing_default = _target(is_default=True)
    store = ExecutionTargetStore("postgresql+asyncpg://localhost/syntara")
    session = _Session(result=_Result(existing_default))
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]
    placement = KubernetesPlacement(namespace="default")

    with pytest.raises(DefaultExecutionTargetError):
        await store.create(
            existing_default.cluster_id,
            "target-b",
            BackendType.KUBERNETES,
            "https://target-b.example",
            uuid.uuid4(),
            is_default=True,
            created_by=uuid.uuid4(),
            placement=placement,
        )

    assert session.added is None
    assert session.commits == 0
    await store.close()


@pytest.mark.asyncio
async def test_store_rolls_back_when_target_creation_cannot_commit() -> None:
    """Removing rollback would retain a failed target transaction in the owned session."""
    store = ExecutionTargetStore("postgresql+asyncpg://localhost/syntara")
    session = _Session(fail_commit=True)
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]
    placement = KubernetesPlacement(namespace="default")

    with pytest.raises(RuntimeError, match=_DATABASE_UNAVAILABLE):
        await store.create(
            uuid.uuid4(),
            "target-a",
            BackendType.KUBERNETES,
            "https://target.example",
            uuid.uuid4(),
            is_default=False,
            created_by=uuid.uuid4(),
            placement=placement,
        )

    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_store_request_delete_disables_and_drains_non_default_target() -> None:
    """Skipping the state transition would allow deleted targets to receive work."""
    target = _target()
    store = ExecutionTargetStore("postgresql+asyncpg://localhost/syntara")
    session = _Session(result=_Result(target))
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]
    actor_id = uuid.uuid4()

    result = await store.request_delete(target.id, actor_id)

    assert result.status == TargetStatus.DRAINING
    assert result.enabled is False
    assert result.updated_by == actor_id
    assert session.commits == 1
    await store.close()


@pytest.mark.asyncio
async def test_store_updates_target_metadata_and_audit_fields_without_exposing_secret() -> None:
    """Target updates must be narrow and must preserve ownership invariants."""
    target = _target(is_default=True)
    original_cluster_id = target.cluster_id
    store = ExecutionTargetStore("postgresql+asyncpg://localhost/syntara")
    session = _Session(result=_Result(target))
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]
    actor_id = uuid.uuid4()

    result = await store.update(
        target.id,
        updated_by=actor_id,
        name="renamed-target",
        endpoint="https://updated.example",
        labels={"region": "eu-west"},
        placement=KubernetesPlacement(namespace="updated"),
        status_message="updated",
    )

    assert result.name == "renamed-target"
    assert result.endpoint == "https://updated.example"
    assert result.labels == {"region": "eu-west"}
    assert isinstance(result.placement, KubernetesPlacement)
    assert result.placement.namespace == "updated"
    assert result.status_message == "updated"
    assert result.cluster_id == original_cluster_id
    assert result.is_default is True
    assert result.credential_ref == uuid.UUID(int=0)
    assert result.updated_by == actor_id
    assert session.commits == 1
    await store.close()


@pytest.mark.asyncio
async def test_store_finalizes_a_default_target_during_cluster_deletion() -> None:
    """Default protection applies when deletion is requested, not finalized."""
    target = _target(is_default=True, status=TargetStatus.DRAINING)
    target.enabled = False
    store = ExecutionTargetStore("postgresql+asyncpg://localhost/syntara")
    session = _Session(result=_Result(target))
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    await store.finalize_delete(target.id)

    assert session.deleted is None
    await store.close()


@pytest.mark.asyncio
async def test_store_ignores_a_target_that_is_not_drained() -> None:
    """Finalization leaves a target alone when its state changed."""
    target = _target(status=TargetStatus.ACTIVE)
    store = ExecutionTargetStore("postgresql+asyncpg://localhost/syntara")
    session = _Session(result=_Result(target))
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    await store.finalize_delete(target.id)

    assert session.deleted is None
    await store.close()


@pytest.mark.asyncio
async def test_target_store_rejects_deletion_of_the_protected_default_target() -> None:
    """The atomic Store transition remains the source of default-target protection."""
    target = _target(is_default=True)
    store = _Store(target)
    commands = store

    with pytest.raises(DefaultExecutionTargetError):
        await commands.request_delete(target.id, uuid.uuid4())

    assert store.delete_requested is False


@pytest.mark.asyncio
async def test_target_store_persists_target_drain_request() -> None:
    """Deletion persists DRAINING; the worker monitor owns finalization."""
    target = _target()
    store = _Store(target)
    commands = store

    await commands.request_delete(target.id, uuid.uuid4())

    assert store.delete_requested is True


@pytest.mark.asyncio
async def test_target_store_lists_eligible_targets() -> None:
    """Target reads remain behind the Store's owned session boundary."""
    target = _target(status=TargetStatus.ACTIVE)
    commands = _Store(target)

    assert await commands.list(cluster_id=target.cluster_id, eligible_only=True) == [target]


@pytest.mark.asyncio
async def test_target_store_supports_registration_transitions() -> None:
    target = _target()
    store = _Store(target)
    commands = store

    assert (
        await commands.create(
            target.cluster_id,
            target.name,
            target.backend_type,
            target.endpoint,
            target.credential_ref,
            target.is_default,
            target.created_by,
            placement=KubernetesPlacement(namespace="execution"),
            target_id=target.id,
        )
        is target
    )
    assert store.created["target_id"] == target.id
    assert await commands.get(target.id) is target
    assert await commands.get_with_credentials(target.id) is target
    assert store.credential_reads == [target.id]
    assert await commands.activate(target.id, uuid.uuid4()) is target
    assert (
        await commands.update(
            target.id,
            updated_by=uuid.uuid4(),
            name="renamed",
            placement=KubernetesPlacement(namespace="execution"),
        )
        is target
    )


@pytest.mark.asyncio
async def test_target_store_update_replaces_user_labels_and_preserves_the_target_identity_label() -> None:
    target = _target()
    target.labels = {"_execution_target_id": str(target.id), "region": "old", "team": "old"}
    store = ExecutionTargetStore("postgresql+asyncpg://localhost/syntara")
    store._session_factory = _SessionFactory(_Session(result=_Result(target)))  # type: ignore[assignment]
    commands = store

    await commands.update(
        target.id,
        updated_by=uuid.uuid4(),
        labels={"region": "new", "_execution_target_id": "spoofed", "_internal": "spoofed"},
    )

    assert target.labels == {"_execution_target_id": str(target.id), "region": "new"}
    await store.close()


@pytest.mark.asyncio
async def test_target_store_lists_targets_for_the_authenticated_client() -> None:
    target = _target()
    store = _Store(target)
    commands = store

    result = await commands.list_for_client("client-a", limit=20, target_id=target.id)

    assert result == [target]
    assert store.client_scoped_queries == [("client-a", 20, target.id)]


@pytest.mark.asyncio
async def test_target_store_records_drain_failure_and_finalization() -> None:
    target = _target(status=TargetStatus.DRAINING)
    store = _Store(target)
    commands = store
    actor_id = uuid.uuid4()

    assert await commands.mark_failed(target.id, actor_id) is target
    await commands.finalize_delete(target.id)

    assert store.marked_failed is True
    assert store.finalized is True


@pytest.mark.asyncio
async def test_target_store_rejects_deletion_of_an_unknown_target() -> None:
    class EmptyStore(_Store):
        async def request_delete(self, _target_id: uuid.UUID, _updated_by: uuid.UUID) -> ExecutionTarget:
            raise ExecutionTargetNotFoundError(_target_id)

    commands = EmptyStore(_target())

    with pytest.raises(ExecutionTargetNotFoundError):
        await commands.request_delete(uuid.uuid4(), uuid.uuid4())


async def test_store_borrows_an_explicit_unpooled_engine() -> None:
    """An injected engine remains owned by its caller."""
    engine = create_unpooled_engine("postgresql+asyncpg://localhost/syntara")
    try:
        async with ExecutionTargetStore.from_engine(engine) as store:
            assert store._engine is engine
    finally:
        await engine.dispose()
