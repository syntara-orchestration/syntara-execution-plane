"""TargetProbe protocol and Kubernetes namespace / skip implementations."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Protocol

import structlog
from kubernetes.client.exceptions import ApiException
from urllib3.exceptions import HTTPError

from execution_plane.models.cluster import ClusterStatus, ClusterType
from execution_plane.resource_monitor.client import KubernetesClientError, read_namespace
from execution_plane_client.models.execution_target import KubernetesPlacement, TargetStatus

if TYPE_CHECKING:
    from collections.abc import Callable

    from execution_plane.models.cluster import Cluster
    from execution_plane.models.execution_target import ExecutionTarget

logger = structlog.stdlib.get_logger(__name__)

_PROBEABLE_STATUSES = {TargetStatus.ACTIVE, TargetStatus.DEGRADED}


@dataclass(frozen=True)
class ProbeResult:
    """Outcome of one probe. ``message`` is operator-facing and must never include credentials."""

    healthy: bool
    message: str | None = None
    skipped: bool = False


class TargetProbe(Protocol):
    """Probe one Cluster / ExecutionTarget pair. RHEL later adds another implementation."""

    async def probe(self, cluster: Cluster, credential: str, target: ExecutionTarget) -> ProbeResult:
        """Return health for this target, or a skipped result that leaves status unchanged."""


class SkipProbe:
    """Leave persisted status unchanged (RHEL, local://, non-health lifecycle)."""

    async def probe(self, cluster: Cluster, _credential: str, target: ExecutionTarget) -> ProbeResult:
        """Return a skipped result so the monitor does not write health."""
        del cluster, target
        return ProbeResult(healthy=True, skipped=True)


class KubernetesNamespaceProbe:
    """GET the ExecutionTarget namespace through the Cluster API."""

    def __init__(
        self,
        *,
        timeout_seconds: float,
        reader: Callable[[Cluster, str, str], None] | None = None,
    ) -> None:
        """Use ``reader`` in tests; production talks to Kubernetes through the shared factory."""
        self._timeout_seconds = timeout_seconds
        self._reader = reader or self._read_namespace

    def _read_namespace(self, cluster: Cluster, credential: str, namespace: str) -> None:
        read_namespace(cluster, credential, namespace, timeout_seconds=self._timeout_seconds)

    async def probe(self, cluster: Cluster, credential: str, target: ExecutionTarget) -> ProbeResult:
        """Map namespace GET outcomes onto healthy / unhealthy without logging credentials."""
        if not isinstance(target.placement, KubernetesPlacement):
            return ProbeResult(healthy=True, skipped=True)
        namespace = target.placement.namespace
        try:
            await asyncio.wait_for(
                asyncio.to_thread(self._reader, cluster, credential, namespace),
                timeout=self._timeout_seconds,
            )
        except TimeoutError:
            logger.warning("namespace probe timed out", target_id=str(target.id))
            return ProbeResult(healthy=False, message="Kubernetes API timed out")
        except ApiException as exc:
            logger.warning(
                "namespace probe API error",
                target_id=str(target.id),
                status=exc.status,
            )
            return _from_api_exception(namespace, exc)
        except KubernetesClientError:
            logger.warning("namespace probe client error", target_id=str(target.id))
            return ProbeResult(healthy=False, message="Kubernetes API is unreachable")
        except (OSError, HTTPError):
            logger.warning(
                "namespace probe failed",
                target_id=str(target.id),
                error_type="unreachable",
            )
            return ProbeResult(healthy=False, message="Kubernetes API is unreachable")
        return ProbeResult(healthy=True)


class DispatchingTargetProbe:
    """Choose SkipProbe vs KubernetesNamespaceProbe without RHEL branches in the K8s probe."""

    def __init__(
        self,
        *,
        timeout_seconds: float,
        kubernetes_probe: TargetProbe | None = None,
        skip_probe: TargetProbe | None = None,
    ) -> None:
        """Bind the Kubernetes probe and the skip probe used for out-of-scope targets."""
        self._kubernetes = kubernetes_probe or KubernetesNamespaceProbe(timeout_seconds=timeout_seconds)
        self._skip = skip_probe or SkipProbe()

    async def probe(self, cluster: Cluster, credential: str, target: ExecutionTarget) -> ProbeResult:
        """Skip RHEL, local://, and non-health lifecycle; otherwise probe the namespace."""
        if should_skip_probe(cluster, target):
            return await self._skip.probe(cluster, credential, target)
        return await self._kubernetes.probe(cluster, credential, target)


def should_skip_probe(cluster: Cluster, target: ExecutionTarget) -> bool:
    """Return whether this pair is out of Kubernetes MVP probe scope."""
    if cluster.cluster_type is ClusterType.RHEL:
        return True
    if not cluster.enabled or cluster.status is not ClusterStatus.ACTIVE:
        return True
    if not cluster.endpoint.startswith(("http://", "https://")):
        return True
    if not target.enabled or target.status not in _PROBEABLE_STATUSES:
        return True
    return not isinstance(target.placement, KubernetesPlacement)


def _from_api_exception(namespace: str, exc: ApiException) -> ProbeResult:
    """Translate Kubernetes HTTP statuses into operator-facing messages without bodies."""
    status = exc.status
    if status == HTTPStatus.NOT_FOUND:
        return ProbeResult(healthy=False, message=f"Namespace {namespace} does not exist")
    if status in {HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN}:
        return ProbeResult(healthy=False, message="Kubernetes API rejected authorization for the target namespace")
    if status:
        return ProbeResult(healthy=False, message=f"Kubernetes API returned HTTP {status}")
    return ProbeResult(healthy=False, message="Kubernetes API is unreachable")
