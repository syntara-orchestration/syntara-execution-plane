"""Tests for Cluster persistence and registration orchestration."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Self

import pytest

from execution_plane.cluster.cluster_registrar import ClusterRegistration, DiscoveryResult
from execution_plane.models.cluster import Cluster, ClusterStatus, ClusterType
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane.models.execution_target_placement import ExecutionTargetPlacement, KubernetesPlacement

_DATABASE_UNAVAILABLE = "database unavailable"
_TARGET_UNAVAILABLE = "target unavailable"
_DISCOVERY_UNAVAILABLE = "discovery unavailable"

if TYPE_CHECKING:
    from collections.abc import Sequence


def _cluster(*, status: ClusterStatus = ClusterStatus.REGISTERING, is_default: bool = False) -> Cluster:
    now = datetime.now(UTC)
    return Cluster(
        id=uuid.uuid4(),
        name="cluster-a",
        endpoint="https://cluster.example",
        credential_ref=uuid.uuid4(),
        status=status,
        is_default=is_default,
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


def _target(*, cluster_id: uuid.UUID | None = None, is_default: bool = False) -> ExecutionTarget:
    now = datetime.now(UTC)
    return ExecutionTarget(
        id=uuid.uuid4(),
        cluster_id=cluster_id or uuid.uuid4(),
        name="target-a",
        endpoint="https://target.example",
        credential_ref=uuid.uuid4(),
        backend_type=BackendType.VANILLA_K8S,
        is_default=is_default,
        status=TargetStatus.ACTIVE,
        placement=KubernetesPlacement(
            namespace="execution",
            node_selectors=["kubernetes.io/os=linux"],
            tolerations=["dedicated=execution:NoSchedule"],
        ),
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


class _Result:
    def __init__(self, cluster: Cluster | None = None, targets: list[ExecutionTarget] | None = None) -> None:
        self.cluster = cluster
        self.targets = targets or []

    def scalar_one_or_none(self) -> Cluster | ExecutionTarget | None:
        if self.cluster is not None:
            return self.cluster
        return self.targets[0] if self.targets else None

    def scalars(self) -> Self:
        return self

    def all(self) -> list[ExecutionTarget]:
        return self.targets


class _Session:
    def __init__(
        self, *, cluster: Cluster | None = None, targets: list[ExecutionTarget] | None = None, fail_commit: bool = False
    ) -> None:
        self.cluster = cluster
        self.targets = targets or []
        self.fail_commit = fail_commit
        self.added: Cluster | None = None
        self.deleted: Cluster | None = None
        self.commits = 0
        self.rollbacks = 0

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def add(self, cluster: Cluster) -> None:
        self.added = cluster

    async def get(self, _model: object, _cluster_id: uuid.UUID, **_: object) -> Cluster | None:
        return self.cluster if self.cluster is not None else self.added

    async def execute(self, _statement: object) -> _Result:
        return _Result(self.cluster, self.targets)

    async def commit(self) -> None:
        self.commits += 1
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


@pytest.mark.asyncio
async def test_store_creates_a_registering_cluster_and_owns_commit() -> None:
    from execution_plane.cluster.cluster_store import ClusterStore

    store = ClusterStore("postgresql+asyncpg://localhost/syntara")
    session = _Session()
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]
    actor_id = uuid.uuid4()
    credential_ref = uuid.uuid4()

    cluster = await store.create(
        "cluster-a", "https://cluster.example", credential_ref, actor_id, {"region": "eu-west"}
    )

    assert cluster.status is ClusterStatus.REGISTERING
    assert cluster.enabled is True
    assert cluster.cluster_type is ClusterType.OPENSHIFT
    assert cluster.credential_ref == uuid.UUID(int=0)
    assert session.added is not None
    assert session.added.credential_ref == credential_ref
    assert session.added.cluster_type is ClusterType.OPENSHIFT
    assert session.added.created_by == actor_id
    assert session.commits == 1
    await store.close()


@pytest.mark.asyncio
async def test_store_persists_an_explicit_cluster_type() -> None:
    from execution_plane.cluster.cluster_store import ClusterStore

    store = ClusterStore("postgresql+asyncpg://localhost/syntara")
    session = _Session()
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    cluster = await store.create(
        "cluster-a",
        "https://cluster.example",
        uuid.uuid4(),
        uuid.uuid4(),
        cluster_type=ClusterType.RHEL,
    )

    assert cluster.cluster_type is ClusterType.RHEL
    assert session.added is not None
    assert session.added.cluster_type is ClusterType.RHEL
    await store.close()


@pytest.mark.asyncio
async def test_store_rolls_back_failed_cluster_creation() -> None:
    from execution_plane.cluster.cluster_store import ClusterStore

    store = ClusterStore("postgresql+asyncpg://localhost/syntara")
    session = _Session(fail_commit=True)
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    with pytest.raises(RuntimeError, match="database unavailable"):
        await store.create("cluster-a", "https://cluster.example", uuid.uuid4(), uuid.uuid4())

    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_store_records_discovery_failure_on_persisted_cluster() -> None:
    from execution_plane.cluster.cluster_store import ClusterStore

    cluster = _cluster()
    store = ClusterStore("postgresql+asyncpg://localhost/syntara")
    session = _Session(cluster=cluster)
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    result = await store.record_discovery_state(cluster.id, ClusterStatus.ERROR, "authentication failed", uuid.uuid4())

    assert result.status is ClusterStatus.ERROR
    assert result.enabled is False
    assert result.status_message == "authentication failed"
    assert session.commits == 1
    await store.close()


@pytest.mark.asyncio
async def test_store_request_delete_drains_cluster_and_its_targets() -> None:
    from execution_plane.cluster.cluster_store import ClusterStore

    cluster = _cluster(status=ClusterStatus.ACTIVE)
    targets = [_target(cluster_id=cluster.id), _target(cluster_id=cluster.id)]
    store = ClusterStore("postgresql+asyncpg://localhost/syntara")
    session = _Session(cluster=cluster, targets=targets)
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]
    actor_id = uuid.uuid4()

    result = await store.request_delete(cluster.id, actor_id)

    assert result.status is ClusterStatus.DRAINING
    assert result.enabled is False
    assert all(target.status is TargetStatus.DRAINING and not target.enabled for target in targets)
    assert all(target.updated_by == actor_id for target in targets)
    await store.close()


@pytest.mark.asyncio
async def test_store_ignores_cluster_finalization_until_targets_are_removed() -> None:
    from execution_plane.cluster.cluster_store import ClusterStore

    cluster = _cluster(status=ClusterStatus.DRAINING)
    cluster.enabled = False
    store = ClusterStore("postgresql+asyncpg://localhost/syntara")
    session = _Session(cluster=cluster, targets=[_target(cluster_id=cluster.id)])
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    await store.finalize_delete(cluster.id)

    assert session.deleted is None
    await store.close()


@pytest.mark.asyncio
async def test_store_ignores_cluster_finalization_before_draining() -> None:
    from execution_plane.cluster.cluster_store import ClusterStore

    cluster = _cluster(status=ClusterStatus.ACTIVE)
    store = ClusterStore("postgresql+asyncpg://localhost/syntara")
    session = _Session(cluster=cluster)
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    await store.finalize_delete(cluster.id)

    assert session.deleted is None
    await store.close()


class _ClusterStore:
    def __init__(self, cluster: Cluster) -> None:
        self.cluster = cluster
        self.states: list[tuple[ClusterStatus, str | None]] = []
        self.created = False
        self.updates: list[dict[str, object]] = []
        self.defaults_set: list[uuid.UUID] = []
        self.defaults_ensured: list[uuid.UUID] = []

    async def create(
        self,
        _name: str,
        _endpoint: str,
        _credential_ref: uuid.UUID,
        _created_by: uuid.UUID,
        _labels: dict[str, str] | None = None,
        *,
        cluster_type: ClusterType = ClusterType.OPENSHIFT,
        source_client_id: str | None = None,
        source_integration_id: uuid.UUID | None = None,
        source_revision: int = 0,
        project_ids: list[uuid.UUID] | None = None,
        ca_certificate: str | None = None,
    ) -> Cluster:
        self.created = True
        return self.cluster

    async def update(
        self,
        _cluster_id: uuid.UUID,
        *,
        updated_by: uuid.UUID,
        name: str | None = None,
        endpoint: str | None = None,
        credential_ref: uuid.UUID | None = None,
        ca_certificate: str | None = None,
        labels: dict[str, str] | None = None,
        project_ids: object = None,
        source_revision: int | None = None,
    ) -> Cluster:
        kwargs: dict[str, object] = {
            "updated_by": updated_by,
            "name": name,
            "endpoint": endpoint,
            "credential_ref": credential_ref,
            "ca_certificate": ca_certificate,
            "labels": labels,
        }
        self.updates.append(kwargs)
        return self.cluster

    async def record_discovery_state(
        self, _cluster_id: uuid.UUID, status: ClusterStatus, status_message: str | None, _updated_by: uuid.UUID
    ) -> Cluster:
        self.states.append((status, status_message))
        self.cluster.status = status
        self.cluster.status_message = status_message
        self.cluster.enabled = status is not ClusterStatus.ERROR
        return self.cluster

    async def get_by_name(self, _name: str) -> Cluster | None:
        return self.cluster

    async def get_by_source(self, _client_id: str, _integration_id: uuid.UUID) -> Cluster | None:
        return self.cluster

    async def reactivate(
        self,
        _cluster_id: uuid.UUID,
        *,
        updated_by: uuid.UUID,
        endpoint: str | None = None,
        credential_ref: uuid.UUID | None = None,
        ca_certificate: str | None = None,
        labels: dict[str, str] | None = None,
        project_ids: object = None,
        source_revision: int | None = None,
    ) -> Cluster:
        return self.cluster

    async def set_default(self, cluster_id: uuid.UUID, _updated_by: uuid.UUID) -> Cluster:
        self.defaults_set.append(cluster_id)
        self.cluster.is_default = True
        return self.cluster

    async def ensure_default(self, cluster_id: uuid.UUID, _updated_by: uuid.UUID) -> Cluster:
        self.defaults_ensured.append(cluster_id)
        self.cluster.is_default = True
        return self.cluster


class _TargetService:
    def __init__(
        self,
        *,
        fail_default: bool = False,
        fail_named: str | None = None,
        targets: list[ExecutionTarget] | None = None,
    ) -> None:
        self.fail_default = fail_default
        self.fail_named = fail_named
        self.created: list[dict[str, object]] = []
        self.targets = targets or []
        self.updated: list[dict[str, object]] = []
        self.reactivated: list[dict[str, object]] = []

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
        self.created.append(
            {
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
        )
        if (is_default and self.fail_default) or name == self.fail_named:
            raise RuntimeError(_TARGET_UNAVAILABLE)
        return _target(cluster_id=cluster_id)

    async def activate(self, target_id: uuid.UUID, _updated_by: uuid.UUID) -> ExecutionTarget:
        return _target(cluster_id=self.targets[0].cluster_id if self.targets else uuid.uuid4())

    async def reactivate(
        self,
        target_id: uuid.UUID,
        *,
        updated_by: uuid.UUID,
    ) -> ExecutionTarget:
        self.reactivated.append({"target_id": target_id, "updated_by": updated_by})
        return self.targets[0]

    async def list(
        self,
        cluster_id: uuid.UUID | None = None,
        eligible_only: bool = False,  # noqa: FBT001, FBT002
        status: TargetStatus | None = None,
        limit: int | None = None,
    ) -> Sequence[ExecutionTarget]:
        return self.targets

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
        self.updated.append(
            {
                "updated_by": updated_by,
                "name": name,
                "endpoint": endpoint,
                "placement": placement,
                "labels": labels,
                "status_message": status_message,
                "credential_ref": credential_ref,
            }
        )
        return self.targets[0]


class _Discovery:
    def __init__(self, result: object) -> None:
        self.result = result
        self.received: object | None = None

    def discover(self, registration: ClusterRegistration) -> DiscoveryResult:
        self.received = registration
        assert isinstance(self.result, DiscoveryResult)
        return self.result


class _FailingDiscovery:
    def discover(self, _registration: ClusterRegistration) -> DiscoveryResult:
        raise RuntimeError(_DISCOVERY_UNAVAILABLE)


@pytest.mark.asyncio
async def test_registrar_creates_cluster_with_identity_label_and_cleanses_user_labels() -> None:
    from execution_plane.cluster.cluster_registrar import ClusterRegistrar
    from execution_plane.cluster.cluster_store import ClusterStore

    session = _Session()
    store = ClusterStore("postgresql+asyncpg://localhost/syntara")
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]
    service = ClusterRegistrar(store, _TargetService(), _Discovery(object()))

    cluster = await service.provision(
        "cluster-a",
        "https://cluster.example",
        uuid.uuid4(),
        KubernetesPlacement(namespace="execution"),
        uuid.uuid4(),
        labels={"region": "eu-west", "_cluster_id": "user-supplied", "_internal": "user-supplied"},
    )

    assert cluster.labels == {"region": "eu-west", "_cluster_id": str(cluster.id)}
    await store.close()


@pytest.mark.asyncio
async def test_registrar_provision_promotes_the_requested_global_default() -> None:
    from execution_plane.cluster.cluster_registrar import ClusterRegistrar

    cluster = _cluster()
    store = _ClusterStore(cluster)
    actor_id = uuid.uuid4()
    service = ClusterRegistrar(store, _TargetService(), _Discovery(object()))

    result = await service.provision(
        "cluster-a",
        "https://cluster.example",
        uuid.uuid4(),
        KubernetesPlacement(namespace="execution"),
        actor_id,
        is_default=True,
    )

    assert result.is_default is True
    assert store.defaults_set == [cluster.id]
    assert store.defaults_ensured == []


@pytest.mark.asyncio
async def test_registrar_provision_bootstraps_a_default_when_none_exists() -> None:
    from execution_plane.cluster.cluster_registrar import ClusterRegistrar

    cluster = _cluster()
    store = _ClusterStore(cluster)
    actor_id = uuid.uuid4()
    service = ClusterRegistrar(store, _TargetService(), _Discovery(object()))

    result = await service.provision(
        "cluster-a",
        "https://cluster.example",
        uuid.uuid4(),
        KubernetesPlacement(namespace="execution"),
        actor_id,
        is_default=False,
    )

    assert result.is_default is True
    assert store.defaults_set == []
    assert store.defaults_ensured == [cluster.id]


@pytest.mark.asyncio
async def test_registrar_update_replaces_user_labels_and_preserves_the_cluster_identity_label() -> None:
    from execution_plane.cluster.cluster_registrar import ClusterRegistrar
    from execution_plane.cluster.cluster_store import ClusterStore

    cluster = _cluster()
    cluster.labels = {"_cluster_id": str(cluster.id), "region": "old", "team": "old"}
    session = _Session(cluster=cluster)
    store = ClusterStore("postgresql+asyncpg://localhost/syntara")
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]
    service = ClusterRegistrar(store, _TargetService(), _Discovery(object()))

    await service.sync_update(
        cluster.id,
        updated_by=uuid.uuid4(),
        labels={"region": "new", "_cluster_id": "spoofed", "_internal": "spoofed"},
    )

    assert cluster.labels == {"_cluster_id": str(cluster.id), "region": "new"}
    await store.close()


@pytest.mark.asyncio
async def test_registrar_sync_update_promotes_the_requested_global_default() -> None:
    from execution_plane.cluster.cluster_registrar import ClusterRegistrar

    cluster = _cluster()
    store = _ClusterStore(cluster)
    actor_id = uuid.uuid4()
    service = ClusterRegistrar(store, _TargetService(), _Discovery(object()))

    await service.sync_update(cluster.id, updated_by=actor_id, is_default=True)

    assert store.defaults_set == [cluster.id]


@pytest.mark.asyncio
async def test_registrar_sync_update_rejects_demoting_the_current_global_default() -> None:
    from execution_plane.cluster.cluster_registrar import ClusterRegistrar
    from execution_plane.cluster.cluster_store import DefaultClusterError

    cluster = _cluster(is_default=True)
    store = _ClusterStore(cluster)
    service = ClusterRegistrar(store, _TargetService(), _Discovery(object()))

    with pytest.raises(DefaultClusterError):
        await service.sync_update(cluster.id, updated_by=uuid.uuid4(), is_default=False)

    assert cluster.is_default is True


@pytest.mark.asyncio
async def test_registrar_reactivate_preserves_the_cluster_identity_label() -> None:
    from execution_plane.cluster.cluster_registrar import ClusterRegistrar
    from execution_plane.cluster.cluster_store import ClusterStore

    cluster = _cluster(status=ClusterStatus.DRAINING)
    cluster.enabled = False
    cluster.labels = {"_cluster_id": str(cluster.id), "region": "old"}
    target = _target(cluster_id=cluster.id, is_default=True)
    session = _Session(cluster=cluster, targets=[target])
    store = ClusterStore("postgresql+asyncpg://localhost/syntara")
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]
    service = ClusterRegistrar(store, _TargetService(targets=[target]), _Discovery(object()))

    await service.provision(
        "cluster-a",
        "https://cluster.example",
        uuid.uuid4(),
        KubernetesPlacement(namespace="execution"),
        uuid.uuid4(),
        labels={"region": "new", "_cluster_id": "spoofed", "_internal": "spoofed"},
    )

    assert cluster.labels == {"_cluster_id": str(cluster.id), "region": "new"}
    await store.close()


@pytest.mark.asyncio
async def test_registrar_creates_every_discovered_target_via_target_port_then_activates_cluster() -> None:
    from execution_plane.cluster.cluster_registrar import (
        ClusterRegistrar,
        DiscoveredExecutionTarget,
        DiscoveryResult,
    )

    cluster = _cluster()
    store = _ClusterStore(cluster)
    targets = _TargetService()
    discovery = _Discovery(
        DiscoveryResult.discovered(
            [
                DiscoveredExecutionTarget(
                    "primary",
                    BackendType.VANILLA_K8S,
                    "https://one",
                    "key-1",
                    KubernetesPlacement(namespace="default"),
                    is_default=True,
                ),
                DiscoveredExecutionTarget(
                    "extra", BackendType.OPENSHELL, "https://two", "key-2", KubernetesPlacement(namespace="default")
                ),
            ]
        )
    )
    service = ClusterRegistrar(store, targets, discovery)

    result = await service.register("cluster-a", "https://cluster.example", uuid.uuid4(), uuid.uuid4())

    assert result.status is ClusterStatus.ACTIVE
    assert [target["is_default"] for target in targets.created] == [True, False]
    assert store.states == [(ClusterStatus.ACTIVE, None)]
    assert discovery.received is not None


@pytest.mark.asyncio
async def test_registrar_register_promotes_the_requested_global_default() -> None:
    from execution_plane.cluster.cluster_registrar import (
        ClusterRegistrar,
        DiscoveredExecutionTarget,
        DiscoveryResult,
    )

    cluster = _cluster()
    store = _ClusterStore(cluster)
    targets = _TargetService()
    discovery = _Discovery(
        DiscoveryResult.discovered(
            [
                DiscoveredExecutionTarget(
                    "primary",
                    BackendType.VANILLA_K8S,
                    "https://one",
                    "key",
                    KubernetesPlacement(namespace="default"),
                    is_default=True,
                )
            ]
        )
    )
    service = ClusterRegistrar(store, targets, discovery)
    actor_id = uuid.uuid4()

    result = await service.register(
        "cluster-a",
        "https://cluster.example",
        uuid.uuid4(),
        actor_id,
        is_default=True,
    )

    assert result.is_default is True
    assert store.defaults_set == [cluster.id]
    assert store.defaults_ensured == []


@pytest.mark.asyncio
async def test_registrar_marks_persisted_cluster_error_when_discovery_or_default_target_fails() -> None:
    from execution_plane.cluster.cluster_registrar import (
        ClusterRegistrar,
        DiscoveredExecutionTarget,
        DiscoveryResult,
    )

    cluster = _cluster()
    store = _ClusterStore(cluster)
    failed_discovery = ClusterRegistrar(store, _TargetService(), _Discovery(DiscoveryResult.failed("unreachable")))

    result = await failed_discovery.register("cluster-a", "https://cluster.example", uuid.uuid4(), uuid.uuid4())

    assert result.status is ClusterStatus.ERROR
    assert store.states == [(ClusterStatus.ERROR, "discovery failed")]

    cluster = _cluster()
    store = _ClusterStore(cluster)
    default_failure = ClusterRegistrar(
        store,
        _TargetService(fail_default=True),
        _Discovery(
            DiscoveryResult.discovered(
                [
                    DiscoveredExecutionTarget(
                        "primary",
                        BackendType.VANILLA_K8S,
                        "https://one",
                        "key-1",
                        KubernetesPlacement(namespace="default"),
                        is_default=True,
                    )
                ]
            )
        ),
    )

    result = await default_failure.register("cluster-a", "https://cluster.example", uuid.uuid4(), uuid.uuid4())

    assert result.status is ClusterStatus.ERROR
    assert "default target" in (result.status_message or "")


@pytest.mark.asyncio
async def test_registrar_marks_persisted_cluster_error_when_discovery_raises() -> None:
    from execution_plane.cluster.cluster_registrar import ClusterRegistrar

    cluster = _cluster()
    store = _ClusterStore(cluster)
    service = ClusterRegistrar(store, _TargetService(), _FailingDiscovery())

    result = await service.register("cluster-a", "https://cluster.example", uuid.uuid4(), uuid.uuid4())

    assert result.status is ClusterStatus.ERROR
    assert result.status_message == "discovery failed"


@pytest.mark.asyncio
async def test_registrar_marks_cluster_error_when_discovery_returns_no_single_default() -> None:
    from execution_plane.cluster.cluster_registrar import (
        ClusterRegistrar,
        DiscoveredExecutionTarget,
        DiscoveryResult,
    )

    cluster = _cluster()
    store = _ClusterStore(cluster)
    discovery = _Discovery(
        DiscoveryResult.discovered(
            [
                DiscoveredExecutionTarget(
                    "one", BackendType.VANILLA_K8S, "https://one", "key", KubernetesPlacement(namespace="default")
                )
            ]
        )
    )

    result = await ClusterRegistrar(store, _TargetService(), discovery).register(
        "cluster-a", "https://cluster.example", uuid.uuid4(), uuid.uuid4()
    )

    assert result.status is ClusterStatus.ERROR
    assert result.status_message == "Discovery did not provide exactly one default target"


@pytest.mark.asyncio
async def test_registrar_keeps_cluster_active_when_a_non_default_target_fails() -> None:
    from execution_plane.cluster.cluster_registrar import (
        ClusterRegistrar,
        DiscoveredExecutionTarget,
        DiscoveryResult,
    )

    cluster = _cluster()
    store = _ClusterStore(cluster)
    targets = _TargetService(fail_named="extra")
    discovery = _Discovery(
        DiscoveryResult.discovered(
            [
                DiscoveredExecutionTarget(
                    "primary",
                    BackendType.VANILLA_K8S,
                    "https://one",
                    "key",
                    KubernetesPlacement(namespace="default"),
                    is_default=True,
                ),
                DiscoveredExecutionTarget(
                    "extra", BackendType.OPENSHELL, "https://two", "key", KubernetesPlacement(namespace="default")
                ),
            ]
        )
    )

    result = await ClusterRegistrar(store, targets, discovery).register(
        "cluster-a", "https://cluster.example", uuid.uuid4(), uuid.uuid4()
    )

    assert result.status is ClusterStatus.ACTIVE
    assert result.status_message == "Target registration failures: extra"


@pytest.mark.asyncio
async def test_sync_update_does_not_rewrite_target_configuration() -> None:
    from execution_plane.cluster.cluster_registrar import ClusterRegistrar

    cluster = _cluster()
    target = _target(cluster_id=cluster.id, is_default=True)
    targets = _TargetService(targets=[target])
    service = ClusterRegistrar(_ClusterStore(cluster), targets, _Discovery(object()))

    await service.sync_update(
        cluster.id,
        updated_by=uuid.uuid4(),
    )

    assert targets.updated == []


@pytest.mark.asyncio
async def test_reactivate_preserves_the_current_default_target_configuration() -> None:
    from execution_plane.cluster.cluster_registrar import ClusterRegistrar

    cluster = _cluster(status=ClusterStatus.DRAINING)
    cluster.enabled = False
    target = _target(cluster_id=cluster.id, is_default=True)
    target.endpoint = "https://independent-target.example"
    independent_credential_ref = uuid.uuid4()
    target.credential_ref = independent_credential_ref
    target.placement = KubernetesPlacement(
        namespace="independent",
        node_selectors=["independent=true"],
        tolerations=["independent=:NoSchedule"],
    )
    targets = _TargetService(targets=[target])
    actor_id = uuid.uuid4()
    target_credential_ref = uuid.uuid4()
    service = ClusterRegistrar(_ClusterStore(cluster), targets, _Discovery(object()))

    await service.provision(
        "cluster-a",
        "https://cluster.example",
        target_credential_ref,
        KubernetesPlacement(namespace="updated"),
        actor_id,
    )

    assert targets.reactivated == [{"target_id": target.id, "updated_by": actor_id}]
    assert target.endpoint == "https://independent-target.example"
    assert target.credential_ref == independent_credential_ref
    assert isinstance(target.placement, KubernetesPlacement)
    assert target.placement.namespace == "independent"


@pytest.mark.asyncio
async def test_sync_update_forwards_cluster_labels_to_the_cluster_store() -> None:
    from execution_plane.cluster.cluster_registrar import ClusterRegistrar

    cluster = _cluster()
    store = _ClusterStore(cluster)
    target = _target(cluster_id=cluster.id, is_default=True)
    targets = _TargetService(targets=[target])
    service = ClusterRegistrar(store, targets, _Discovery(object()))

    await service.sync_update(
        cluster.id,
        updated_by=uuid.uuid4(),
        labels={"region": "eu-west", "_cluster_id": "spoofed"},
    )

    assert store.updates[0]["labels"] == {"region": "eu-west", "_cluster_id": "spoofed"}


@pytest.mark.asyncio
async def test_sync_update_forwards_cluster_ca_certificate_to_cluster_store() -> None:
    from execution_plane.cluster.cluster_registrar import ClusterRegistrar

    cluster = _cluster()
    store = _ClusterStore(cluster)
    service = ClusterRegistrar(store, _TargetService(), _Discovery(object()))

    await service.sync_update(
        cluster.id,
        updated_by=uuid.uuid4(),
        ca_certificate="ca-certificate",
    )

    assert store.updates[0]["ca_certificate"] == "ca-certificate"


def test_noop_discovery_returns_a_failed_result() -> None:
    from execution_plane.cluster.cluster_registrar import ClusterRegistration, NoopDiscoveryMechanism

    result = NoopDiscoveryMechanism().discover(ClusterRegistration("name", "endpoint", uuid.uuid4(), {}))

    assert result.state.value == "failed"
    assert result.status_message == "No discovery mechanism is configured"
