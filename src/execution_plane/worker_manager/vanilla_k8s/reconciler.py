"""Remove orphaned per-attempt NetworkPolicies after cold-start Jobs disappear."""

from __future__ import annotations

import asyncio
import tempfile
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

import structlog
from kubernetes import client
from kubernetes.client.exceptions import ApiException

from execution_plane.credential_resolution.resolver import CredentialResolutionError, CredentialResolver
from execution_plane.store_errors import StaleClaimError
from execution_plane.worker_manager.vanilla_k8s.cleanup import cleanup_requested_work_item_resources
from execution_plane_client.models.execution_target import BackendType, KubernetesPlacement

if TYPE_CHECKING:
    from execution_plane.cluster.cluster_store import ClusterStore
    from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
    from execution_plane.models.cluster import Cluster
    from execution_plane.models.execution_target import ExecutionTarget
    from execution_plane.work_item.work_item_store import WorkStore

logger = structlog.stdlib.get_logger(__name__)
_ORPHAN_GRACE = timedelta(minutes=10)
_RECONCILE_INTERVAL_SECONDS = 60


def _target_config(
    target: ExecutionTarget,
    cluster: Cluster,
    credential: str,
) -> tuple[dict[str, str], str] | None:
    """Return the verified Kubernetes endpoint and namespace for this target."""
    if target.backend_type is not BackendType.KUBERNETES or not isinstance(target.placement, KubernetesPlacement):
        return None
    return (
        {
            "base_url": cluster.endpoint,
            "token": credential,
            "ca_certificate": cluster.ca_certificate or "",
        },
        target.placement.namespace,
    )


def _reconcile_target(config_values: dict[str, str], namespace: str) -> list[tuple[str, str, int]]:  # noqa: C901, PLR0912, PLR0915
    """Delete old policies only after their Job and every matching Pod are gone."""
    with tempfile.TemporaryDirectory(prefix="syntara-policy-reconcile-") as directory:
        configuration = client.Configuration()
        configuration.host = config_values["base_url"]
        token = config_values["token"]
        configuration.api_key["authorization"] = token
        configuration.api_key["BearerToken"] = token
        configuration.api_key_prefix["authorization"] = "Bearer"
        configuration.api_key_prefix["BearerToken"] = "Bearer"
        configuration.verify_ssl = True
        if config_values["ca_certificate"]:
            ca = Path(directory) / "ca.pem"
            ca.write_text(config_values["ca_certificate"], encoding="utf-8")
            configuration.ssl_ca_cert = str(ca)
        removed: list[tuple[str, str, int]] = []
        with client.ApiClient(configuration) as api_client:
            network_api = client.NetworkingV1Api(api_client)
            batch_api = client.BatchV1Api(api_client)
            pod_api = client.CoreV1Api(api_client)
            policies = network_api.list_namespaced_network_policy(
                namespace,
                label_selector="app.kubernetes.io/managed-by=execution-plane",
                _request_timeout=15,
            ).items
            for policy in policies:
                metadata = policy.metadata
                if metadata is None or not metadata.name or not metadata.labels:
                    continue
                resource_name = metadata.name.removesuffix("-network")
                if not resource_name.startswith("syntara-node-"):
                    continue
                created_at = metadata.creation_timestamp
                if created_at is None or datetime.now(UTC) - created_at < _ORPHAN_GRACE:
                    continue
                work_item_id = metadata.labels.get("execution-plane.syntara.io/work-item")
                attempt_id = metadata.labels.get("execution-plane.syntara.io/attempt", "")
                try:
                    parsed_item_id = UUID(work_item_id or "")
                    generation_prefix = f"{parsed_item_id}-"
                    if not attempt_id.startswith(generation_prefix):
                        continue
                    generation = int(attempt_id.removeprefix(generation_prefix))
                    if generation < 1:
                        continue
                except ValueError:
                    continue
                try:
                    job = batch_api.read_namespaced_job(resource_name, namespace, _request_timeout=10)
                except ApiException as exc:
                    if exc.status != HTTPStatus.NOT_FOUND:
                        continue
                    pods = pod_api.list_namespaced_pod(
                        namespace,
                        label_selector=f"batch.kubernetes.io/job-name={resource_name}",
                        _request_timeout=10,
                    ).items
                    if pods:
                        continue
                    network_api.delete_namespaced_network_policy(metadata.name, namespace, _request_timeout=10)
                    removed.append((str(parsed_item_id), resource_name, generation))
                    logger.info("Removed orphaned workload NetworkPolicy", namespace=namespace, policy=metadata.name)
                else:
                    job_uid = str(job.metadata.uid) if job.metadata and job.metadata.uid else None
                    annotations = metadata.annotations or {}
                    policy_job_uid = annotations.get("execution-plane.syntara.io/job-uid")
                    if policy_job_uid and job_uid and policy_job_uid != job_uid:
                        # A recreated Job must never lose the policy protecting its Pod.
                        continue
        return removed


async def run_orphan_policy_reconciler(  # noqa: C901 - isolate cleanup failures per target and item
    target_cleanup: ExecutionTargetStore,
    cluster_credentials: ClusterStore,
    work_progress: WorkStore,
    credential_resolver: CredentialResolver,
) -> None:
    """Periodically clean up aged policies left by a worker crash or API outage."""
    while True:
        try:
            await cleanup_requested_work_item_resources(
                work_progress, target_cleanup, cluster_credentials, credential_resolver
            )
            targets = await target_cleanup.list()
            for target_stub in targets:
                target = await target_cleanup.get_with_credentials(target_stub.id)
                if target is None:
                    continue
                cluster = await cluster_credentials.get_with_credentials(target.cluster_id)
                if cluster is None:
                    continue
                if cluster.source_client_id is None:
                    continue
                try:
                    credential = await credential_resolver.resolve(
                        cluster.credential_ref,
                        client_id=cluster.source_client_id,
                    )
                except CredentialResolutionError:
                    logger.warning("Could not resolve cluster credential for policy reconciliation")
                    continue
                connection = _target_config(target, cluster, credential)
                if connection is None:
                    continue
                config_values, namespace = connection
                removed = await asyncio.to_thread(_reconcile_target, config_values, namespace)
                for item_id, resource_name, generation in removed:
                    try:
                        await work_progress.update_backend_resource(
                            UUID(item_id),
                            resource_name=resource_name,
                            resource_uid=None,
                            cleanup_status="complete",
                            claim_owner_id=None,
                            claim_generation=generation,
                        )
                    except StaleClaimError:
                        # A later WorkItem attempt may already own the cleanup
                        # fields. The orphaned Kubernetes policy is still gone.
                        continue
                    except Exception as exc:  # noqa: BLE001 - keep reconciling other orphaned policies
                        logger.warning(
                            "Could not persist orphaned resource cleanup state",
                            work_item_id=item_id,
                            error_type=type(exc).__name__,
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - a failed cleanup pass must not stop dispatch
            logger.warning("NetworkPolicy orphan reconciliation failed", error_type=type(exc).__name__)
        await asyncio.sleep(_RECONCILE_INTERVAL_SECONDS)
