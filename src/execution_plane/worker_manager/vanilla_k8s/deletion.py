"""Finish explicitly requested WorkItem deletion after a controller loses its job."""

from __future__ import annotations

import asyncio
from http import HTTPStatus
from typing import TYPE_CHECKING

import structlog
from kubernetes import client
from kubernetes.client.exceptions import ApiException

from execution_plane.resource_monitor.client import api_client_from_cluster
from execution_plane_client.models.execution_target import BackendType, KubernetesPlacement

if TYPE_CHECKING:
    from execution_plane.cluster.cluster_store import ClusterStore
    from execution_plane.credential_resolution.resolver import CredentialResolver
    from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
    from execution_plane.models.cluster import Cluster
    from execution_plane.models.work_item import WorkItem
    from execution_plane.work_item.work_item_store import WorkStore

logger = structlog.stdlib.get_logger(__name__)


def _owned_metadata(metadata: client.V1ObjectMeta | None, item: WorkItem) -> bool:
    """Check attempt identity before removing resources by their recorded name."""
    labels = metadata.labels if metadata is not None else None
    return bool(
        labels
        and all(
            labels.get(key) == value
            for key, value in {
                "app.kubernetes.io/managed-by": "execution-plane",
                "execution-plane.syntara.io/work-item": str(item.id),
                "execution-plane.syntara.io/attempt": f"{item.id}-{item.claim_generation}",
            }.items()
        )
    )


def delete_work_item_resources(  # noqa: C901, PLR0911 - verify each resource boundary before deleting
    cluster: Cluster, credential: str, namespace: str, item: WorkItem
) -> bool:
    """Request owned Job deletion; confirm no Job/Pods remain before removing policy."""
    name = item.backend_resource_name
    if name is None:
        return False
    with api_client_from_cluster(cluster, credential) as api_client:
        batch = client.BatchV1Api(api_client)
        pods = client.CoreV1Api(api_client)
        network = client.NetworkingV1Api(api_client)
        try:
            job = batch.read_namespaced_job(name, namespace, _request_timeout=10)
        except ApiException as exc:
            if exc.status != HTTPStatus.NOT_FOUND:
                raise
        else:
            if not _owned_metadata(job.metadata, item):
                return False
            uid = str(job.metadata.uid) if job.metadata and job.metadata.uid else None
            if uid is None or (item.backend_resource_uid is not None and uid != item.backend_resource_uid):
                return False
            batch.delete_namespaced_job(
                name,
                namespace,
                body=client.V1DeleteOptions(
                    propagation_policy="Foreground", preconditions=client.V1Preconditions(uid=uid)
                ),
                _request_timeout=10,
            )
            # Retry on the next cleanup pass; acceptance does not prove termination.
            return False
        if pods.list_namespaced_pod(
            namespace, label_selector=f"batch.kubernetes.io/job-name={name}", _request_timeout=10
        ).items:
            return False
        policy_name = f"{name}-network"
        try:
            policy = network.read_namespaced_network_policy(policy_name, namespace, _request_timeout=10)
        except ApiException as exc:
            if exc.status != HTTPStatus.NOT_FOUND:
                raise
        else:
            if not _owned_metadata(policy.metadata, item) or not policy.metadata.uid:
                return False
            network.delete_namespaced_network_policy(
                policy_name,
                namespace,
                body=client.V1DeleteOptions(preconditions=client.V1Preconditions(uid=str(policy.metadata.uid))),
                _request_timeout=10,
            )
            return False
        return True


async def cleanup_requested_work_item_deletions(
    work_store: WorkStore,
    target_store: ExecutionTargetStore,
    cluster_store: ClusterStore,
    resolver: CredentialResolver,
) -> None:
    """Retry resource cleanup for existing WorkItems with explicit deletion intent."""
    after_id = None
    while items := await work_store.deletions_awaiting_cleanup(after_id=after_id):
        for item in items:
            try:
                if item.execution_target_id is None or item.backend_resource_name is None:
                    continue
                target = await target_store.get_with_credentials(item.execution_target_id)
                if target is None or target.backend_type is not BackendType.KUBERNETES:
                    continue
                if not isinstance(target.placement, KubernetesPlacement):
                    continue
                cluster = await cluster_store.get_with_credentials(target.cluster_id)
                if cluster is None or cluster.source_client_id is None:
                    continue
                credential = await resolver.resolve(cluster.credential_ref, client_id=cluster.source_client_id)
                if await asyncio.to_thread(
                    delete_work_item_resources, cluster, credential, target.placement.namespace, item
                ):
                    await work_store.update_backend_resource(
                        item.id,
                        resource_name=item.backend_resource_name,
                        resource_uid=item.backend_resource_uid,
                        cleanup_status="complete",
                        claim_owner_id=None,
                        claim_generation=item.claim_generation,
                    )
            except Exception as exc:  # noqa: BLE001 - retain intent and continue other explicit deletions
                logger.warning(
                    "Requested WorkItem resource cleanup will retry",
                    work_item_id=item.id,
                    error_type=type(exc).__name__,
                )
        after_id = items[-1].id
