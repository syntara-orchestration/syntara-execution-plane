"""Resolve Kubernetes placement and credentials for the shared execution lifecycle."""

from __future__ import annotations

from typing import TYPE_CHECKING

from execution_plane.credential_resolution.resolver import CredentialResolutionError, CredentialResolver
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.worker_manager.base import WorkerManager
from execution_plane.worker_manager.lifecycle import RetryableDispatchError, WorkAttempt, WorkItemPayloadError
from execution_plane.worker_manager.vanilla_k8s.transport import KubernetesTarget, KubernetesWorker

if TYPE_CHECKING:
    from execution_plane.cluster.cluster_store import ClusterStore
    from execution_plane.config import EPSettings
    from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
    from execution_plane.work_item.work_item_store import WorkStore
    from execution_plane_client.commands.work import WorkExecute


class VanillaK8sWorkerManager(WorkerManager):
    """Prepare an inert cold-start allocation; the base owns every WorkItem transition."""

    def __init__(
        self,
        target_credentials: ExecutionTargetStore,
        cluster_credentials: ClusterStore,
        work_progress: WorkStore,
        settings: EPSettings,
        credential_resolver: CredentialResolver | None = None,
    ) -> None:
        """Bind placement queries and just-in-time credential resolution."""
        super().__init__(work_progress, settings)
        self._target_credentials = target_credentials
        self._cluster_credentials = cluster_credentials
        self._credential_resolver = credential_resolver or CredentialResolver(settings)

    async def prepare(self, attempt: WorkAttempt, command: WorkExecute) -> KubernetesWorker:
        """Resolve connection settings and validate placement without creating resources."""
        target = await self._target_credentials.get_with_credentials(attempt.execution_target_id)
        if target is None or not target.enabled:
            message = "Assigned execution target is unavailable"
            raise RetryableDispatchError(message)
        cluster = await self._cluster_credentials.get_with_credentials(target.cluster_id)
        if cluster is None or not cluster.enabled:
            message = "Assigned cluster is unavailable"
            raise RetryableDispatchError(message)
        placement = target.placement
        if not isinstance(placement, KubernetesPlacement):
            message = f"vanilla-k8s manager requires a Kubernetes placement, got {placement.type!r}"
            raise WorkItemPayloadError(message)
        if cluster.source_client_id is None:
            message = "Assigned cluster has no credential-resolution client"
            raise RetryableDispatchError(message)
        try:
            credential = await self._credential_resolver.resolve(
                cluster.credential_ref, client_id=cluster.source_client_id
            )
        except CredentialResolutionError as exc:
            message = "Cluster credential could not be resolved"
            raise RetryableDispatchError(message) from exc
        return KubernetesWorker(
            attempt=attempt,
            target=KubernetesTarget(
                endpoint=cluster.endpoint,
                namespace=placement.namespace,
                token=credential,
                ca_certificate=cluster.ca_certificate,
                node_selectors=placement.node_selectors,
                tolerations=placement.tolerations,
            ),
            image=command.image,
            invocation=command.invocation.model_dump(mode="json"),
            output_config=command.output_config,
            startup=self._settings.node_startup_seconds,
            grace=self._settings.node_grace_seconds,
            allowed_egress_cidrs=self._settings.workload_allowed_egress_cidrs,
            forbidden_egress_cidrs=self._settings.workload_forbidden_egress_cidrs,
        )
