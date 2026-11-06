"""Process-owned Resource Monitor loop: probe, hysteresis, persist health."""

from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING

import structlog

from execution_plane.credential_resolution.resolver import CredentialResolutionError, CredentialResolver
from execution_plane.execution_target.execution_target_store import ExecutionTargetNotFoundError
from execution_plane.resource_monitor.probes import DispatchingTargetProbe, ProbeResult
from execution_plane_client.models.execution_target import TargetStatus

if TYPE_CHECKING:
    from execution_plane.cluster.cluster_store import ClusterStore
    from execution_plane.config.base import EPSettings
    from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
    from execution_plane.models.cluster import Cluster
    from execution_plane.models.execution_target import ExecutionTarget
    from execution_plane.resource_monitor.probes import TargetProbe

logger = structlog.stdlib.get_logger(__name__)

RESOURCE_MONITOR_ACTOR_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


class ResourceMonitor:
    """Probe registered compute and write ACTIVE ↔ DEGRADED on ExecutionTarget rows."""

    def __init__(
        self,
        target_queries: ExecutionTargetStore,
        cluster_credentials: ClusterStore,
        settings: EPSettings,
        *,
        probe: TargetProbe | None = None,
        credential_resolver: CredentialResolver | None = None,
        interval: float | None = None,
    ) -> None:
        """Create a monitor using Cluster and ExecutionTarget stores and EP settings."""
        self._target_queries = target_queries
        self._cluster_credentials = cluster_credentials
        self._probe = probe or DispatchingTargetProbe(timeout_seconds=settings.resource_monitor_probe_timeout_seconds)
        self._credential_resolver = credential_resolver or CredentialResolver(settings)
        self._interval = settings.resource_monitor_interval_seconds if interval is None else interval
        self._failure_threshold = settings.resource_monitor_failure_threshold
        self._success_threshold = settings.resource_monitor_success_threshold
        self._failures: dict[uuid.UUID, int] = {}
        self._successes: dict[uuid.UUID, int] = {}
        self._task: asyncio.Task[None] | None = None
        self._started = False

    async def start(self) -> None:
        """Start the periodic probe loop. The first scan runs immediately."""
        if self._started:
            return
        self._started = True
        try:
            self._task = asyncio.create_task(self._monitor_loop(), name="ep-resource-monitor")
        except Exception:
            self._started = False
            raise

    async def stop(self) -> None:
        """Cancel the probe loop before stores are disposed."""
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        self._started = False

    async def _monitor_loop(self) -> None:
        """Scan all targets, then sleep. One scan failure does not stop the loop."""
        while True:
            try:
                await self.scan()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Resource monitor scan failed")
            await asyncio.sleep(self._interval)

    async def scan(self) -> None:
        """Probe each registered target once. One target failure does not abort the scan."""
        targets = await self._target_queries.list()
        clusters: dict[uuid.UUID, Cluster | None] = {}
        seen: set[uuid.UUID] = set()
        for target in targets:
            seen.add(target.id)
            try:
                await self._probe_one(target, clusters)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Resource monitor probe failed", target_id=str(target.id))
        self._failures = {target_id: count for target_id, count in self._failures.items() if target_id in seen}
        self._successes = {target_id: count for target_id, count in self._successes.items() if target_id in seen}

    async def _probe_one(self, target: ExecutionTarget, clusters: dict[uuid.UUID, Cluster | None]) -> None:
        """Load Cluster state, resolve its credential, and probe after hysteresis."""
        if target.cluster_id not in clusters:
            clusters[target.cluster_id] = await self._cluster_credentials.get_with_credentials(target.cluster_id)
        cluster = clusters[target.cluster_id]
        if cluster is None:
            return
        if cluster.source_client_id is None:
            result = ProbeResult(healthy=False, message="Cluster credential could not be resolved")
        else:
            try:
                credential = await self._credential_resolver.resolve(
                    cluster.credential_ref,
                    client_id=cluster.source_client_id,
                )
            except CredentialResolutionError:
                result = ProbeResult(healthy=False, message="Cluster credential could not be resolved")
            else:
                result = await self._probe.probe(cluster, credential, target)
        if result.skipped:
            self._failures.pop(target.id, None)
            self._successes.pop(target.id, None)
            return
        if not self._hysteresis_ready(target, result):
            return
        await self._persist(target, result)

    def _hysteresis_ready(self, target: ExecutionTarget, result: ProbeResult) -> bool:
        """Require N failures or M successes before flipping persisted health."""
        if result.healthy:
            self._successes[target.id] = self._successes.get(target.id, 0) + 1
            self._failures[target.id] = 0
            if self._successes[target.id] < self._success_threshold:
                return False
            return target.status is TargetStatus.DEGRADED or bool(target.status_message)
        self._failures[target.id] = self._failures.get(target.id, 0) + 1
        self._successes[target.id] = 0
        if self._failures[target.id] < self._failure_threshold:
            return False
        return target.status is TargetStatus.ACTIVE or target.status_message != result.message

    async def _persist(self, target: ExecutionTarget, result: ProbeResult) -> None:
        """Write health when hysteresis has armed. Missing targets are ignored."""
        try:
            updated = await self._target_queries.record_health(
                target.id,
                healthy=result.healthy,
                message=result.message,
                updated_by=RESOURCE_MONITOR_ACTOR_ID,
            )
        except ExecutionTargetNotFoundError:
            logger.warning("resource monitor target disappeared", target_id=str(target.id))
            return
        target.status = updated.status
        target.status_message = updated.status_message
        logger.info(
            "recorded execution target health",
            target_id=str(target.id),
            status=updated.status.value,
        )
