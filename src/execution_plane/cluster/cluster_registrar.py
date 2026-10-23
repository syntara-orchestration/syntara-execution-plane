"""Cluster registration and desired-state orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

from execution_plane.cluster.cluster_store import DefaultClusterError
from execution_plane.models.cluster import Cluster, ClusterStatus, ClusterType
from execution_plane.models.execution_target import BackendType

if TYPE_CHECKING:
    import uuid

    from execution_plane.cluster.cluster_store import ClusterStore
    from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
    from execution_plane.models.execution_target import ExecutionTarget
    from execution_plane.models.execution_target_placement import ExecutionTargetPlacement


@dataclass(frozen=True)
class ClusterRegistration:
    """Credential-bearing input passed only to the discovery mechanism."""

    name: str
    endpoint: str
    api_key: str = field(repr=False)
    labels: dict[str, str]


@dataclass(frozen=True)
class DiscoveredExecutionTarget:
    """Domain-level target definition returned by discovery."""

    name: str
    backend_type: BackendType
    endpoint: str
    api_key: str = field(repr=False)
    placement: ExecutionTargetPlacement
    is_default: bool = False


class DiscoveryState(StrEnum):
    """Outcome of a synchronous discovery attempt."""

    DISCOVERED = "discovered"
    FAILED = "failed"


@dataclass(frozen=True)
class DiscoveryResult:
    """Discovery state and domain target definitions."""

    state: DiscoveryState
    targets: tuple[DiscoveredExecutionTarget, ...] = ()
    status_message: str | None = None

    @classmethod
    def failed(cls, status_message: str) -> DiscoveryResult:
        """Build a failed discovery result."""
        return cls(DiscoveryState.FAILED, status_message=status_message)

    @classmethod
    def discovered(cls, targets: list[DiscoveredExecutionTarget]) -> DiscoveryResult:
        """Build a successful result containing discovered target definitions."""
        return cls(DiscoveryState.DISCOVERED, tuple(targets))


class DiscoveryMechanism(Protocol):
    """Synchronous, session-free boundary for provider-specific discovery."""

    def discover(self, registration: ClusterRegistration) -> DiscoveryResult:
        """Discover targets for a registered Cluster."""
        ...


class NoopDiscoveryMechanism:
    """Discovery implementation used until a provider-specific mechanism is configured."""

    def discover(self, _registration: ClusterRegistration) -> DiscoveryResult:
        """Report that no discovery mechanism has been configured."""
        return DiscoveryResult.failed("No discovery mechanism is configured")


class ClusterRegistrar:
    """Coordinate Cluster persistence, discovery, and default-target lifecycle."""

    def __init__(
        self,
        store: ClusterStore,
        execution_targets: ExecutionTargetStore,
        discovery: DiscoveryMechanism,
    ) -> None:
        """Use Cluster and ExecutionTarget stores for persistence and registration."""
        self._store = store
        self._execution_targets = execution_targets
        self._discovery = discovery

    async def register(
        self,
        name: str,
        endpoint: str,
        api_key: str,
        created_by: uuid.UUID,
        labels: dict[str, str] | None = None,
        *,
        cluster_type: ClusterType = ClusterType.OPENSHIFT,
        is_default: bool = False,
    ) -> Cluster:
        """Persist first, discover second, and persist the resulting state."""
        registration = ClusterRegistration(name, endpoint, api_key, labels or {})
        cluster = await self._store.create(name, endpoint, api_key, created_by, labels, cluster_type=cluster_type)
        try:
            result = self._discovery.discover(registration)
        except Exception:  # noqa: BLE001
            return await self._store.record_discovery_state(
                cluster.id, ClusterStatus.ERROR, "discovery failed", created_by
            )
        if result.state is DiscoveryState.FAILED:
            return await self._store.record_discovery_state(
                cluster.id, ClusterStatus.ERROR, "discovery failed", created_by
            )

        defaults = [target for target in result.targets if target.is_default]
        if len(defaults) != 1:
            return await self._store.record_discovery_state(
                cluster.id,
                ClusterStatus.ERROR,
                "Discovery did not provide exactly one default target",
                created_by,
            )
        failures: list[str] = []
        ordered_targets = [defaults[0], *(target for target in result.targets if not target.is_default)]
        for target in ordered_targets:
            try:
                created_target = await self._execution_targets.create(
                    cluster_id=cluster.id,
                    name=target.name,
                    backend_type=target.backend_type,
                    endpoint=target.endpoint,
                    placement=target.placement,
                    api_key=target.api_key,
                    is_default=target.is_default,
                    created_by=created_by,
                )
                await self._execution_targets.activate(created_target.id, created_by)
            except Exception:  # noqa: BLE001
                if target.is_default:
                    return await self._store.record_discovery_state(
                        cluster.id, ClusterStatus.ERROR, "default target registration failed", created_by
                    )
                failures.append(target.name)
        status_message = f"Target registration failures: {', '.join(failures)}" if failures else None
        await self._store.record_discovery_state(cluster.id, ClusterStatus.ACTIVE, status_message, created_by)
        if is_default:
            return await self._store.set_default(cluster.id, created_by)
        return await self._store.ensure_default(cluster.id, created_by)

    async def provision(
        self,
        name: str,
        endpoint: str,
        api_key: str,
        placement: ExecutionTargetPlacement,
        created_by: uuid.UUID,
        labels: dict[str, str] | None = None,
        *,
        cluster_type: ClusterType = ClusterType.OPENSHIFT,
        source_client_id: str | None = None,
        source_integration_id: uuid.UUID | None = None,
        source_revision: int = 0,
        project_ids: list[uuid.UUID] | None = None,
        is_default: bool = False,
        ca_certificate: str | None = None,
    ) -> Cluster:
        """Create or update a cluster and its sole default execution target from known values."""
        existing = (
            await self._store.get_by_source(source_client_id, source_integration_id)
            if source_client_id is not None and source_integration_id is not None
            else await self._store.get_by_name(name)
        )
        if existing is not None:
            if existing.status is ClusterStatus.DRAINING or not existing.enabled:
                return await self._reactivate(
                    existing,
                    endpoint,
                    api_key,
                    created_by,
                    labels,
                    is_default=is_default,
                    ca_certificate=ca_certificate,
                    source_revision=source_revision,
                    project_ids=project_ids,
                )
            # Existing active cluster: update without re-provisioning its targets.
            await self._store.update(
                existing.id,
                updated_by=created_by,
                name=name,
                endpoint=endpoint,
                api_key=api_key,
                ca_certificate=ca_certificate,
                labels=labels,
                project_ids=project_ids,
                source_revision=source_revision,
            )
            if is_default:
                return await self._store.set_default(existing.id, created_by)
            return await self._store.ensure_default(existing.id, created_by)

        cluster = await self._store.create(
            name,
            endpoint,
            api_key,
            created_by,
            labels,
            cluster_type=cluster_type,
            source_client_id=source_client_id,
            source_integration_id=source_integration_id,
            source_revision=source_revision,
            project_ids=project_ids,
            ca_certificate=ca_certificate,
        )
        try:
            target = await self._execution_targets.create(
                cluster_id=cluster.id,
                name=name,
                backend_type=BackendType.VANILLA_K8S,
                endpoint=endpoint,
                api_key=api_key,
                placement=placement,
                is_default=True,
                created_by=created_by,
            )
            await self._execution_targets.activate(target.id, created_by)
        except Exception:
            await self._store.record_discovery_state(
                cluster.id, ClusterStatus.ERROR, "default target creation failed", created_by
            )
            raise
        await self._store.record_discovery_state(cluster.id, ClusterStatus.ACTIVE, None, created_by)
        if is_default:
            return await self._store.set_default(cluster.id, created_by)
        return await self._store.ensure_default(cluster.id, created_by)

    async def sync_update(
        self,
        cluster_id: uuid.UUID,
        *,
        updated_by: uuid.UUID,
        name: str | None = None,
        endpoint: str | None = None,
        api_key: str | None = None,
        ca_certificate: str | None = None,
        labels: dict[str, str] | None = None,
        is_default: bool | None = None,
    ) -> None:
        """Update Cluster configuration without rewriting independently managed targets."""
        cluster = await self._store.update(
            cluster_id,
            updated_by=updated_by,
            name=name,
            endpoint=endpoint,
            api_key=api_key,
            ca_certificate=ca_certificate,
            labels=labels,
        )
        if is_default is True:
            await self._store.set_default(cluster_id, updated_by)
            return
        if is_default is False and cluster.is_default:
            raise DefaultClusterError
        await self._store.ensure_default(cluster_id, updated_by)

    async def _reactivate(
        self,
        cluster: Cluster,
        endpoint: str,
        api_key: str,
        updated_by: uuid.UUID,
        labels: dict[str, str] | None = None,
        *,
        is_default: bool = False,
        ca_certificate: str | None = None,
        source_revision: int | None = None,
        project_ids: list[uuid.UUID] | None = None,
    ) -> Cluster:
        """Reactivate a DRAINING cluster and its default target."""
        await self._store.reactivate(
            cluster.id,
            updated_by=updated_by,
            endpoint=endpoint,
            api_key=api_key,
            ca_certificate=ca_certificate,
            labels=labels,
            project_ids=project_ids,
            source_revision=source_revision,
        )
        default_target = await self._default_target(cluster.id)
        if default_target is not None:
            await self._execution_targets.reactivate(default_target.id, updated_by=updated_by)
        if is_default:
            return await self._store.set_default(cluster.id, updated_by)
        return await self._store.ensure_default(cluster.id, updated_by)

    async def _default_target(self, cluster_id: uuid.UUID) -> ExecutionTarget | None:
        """Return the default execution target for a cluster, or None if absent."""
        targets = await self._execution_targets.list(cluster_id=cluster_id)
        return next((target for target in targets if target.is_default), None)
