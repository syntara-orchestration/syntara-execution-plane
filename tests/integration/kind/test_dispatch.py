"""End-to-end dispatch integration test: submit → dispatch → COMPLETED.

Requires a real PostgreSQL database (``EP_TEST_DATABASE_URL``) and a live
Kubernetes cluster (``EP_IT_K8S_*`` env vars). Both are provisioned in CI by
the ``test-integration`` GitHub Actions job. Tests are skipped automatically
when either resource is absent.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import time
import uuid
from contextlib import suppress
from http import HTTPStatus
from typing import TYPE_CHECKING

import pytest
from kubernetes.client import ApiException, BatchV1Api, V1Job, V1PodSpec

from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.config import EPSettings, get_ep_settings
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.execution_target_reconciler.adapters import build_placement_resolver
from execution_plane.execution_target_reconciler.placement import WorkerManagerRegistry
from execution_plane.models.execution_target import BackendType
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.models.work_item import WorkItemStatus
from execution_plane.work_item_client import build_payload
from execution_plane.work_store import WorkStore
from execution_plane.worker import _place_reserved_item, _process_item
from execution_plane.worker_manager.vanilla_k8s.manager import VanillaK8sWorkerManager

if TYPE_CHECKING:
    from execution_plane.models.cluster import Cluster

_NODE_IMAGE = "quay.io/ahetheri/syntara-node-script:migration-test"
_WORKLOAD_WAIT_SECONDS = 5
_JOB_WAIT_TIMEOUT_SECONDS = 30

_SLEEP_SCRIPT_INVOCATION = {
    "version": 1,
    "operation": "execute",
    "inputs": {"language": "bash", "code": f"sleep {_WORKLOAD_WAIT_SECONDS}"},
    "credentials": {"resolved": {}},
    "workflow_context": {},
    "settings": {},
    "timeout_seconds": 90,
}


async def _read_job(batch_api: BatchV1Api, job_name: str, namespace: str) -> V1Job | None:
    try:
        return await asyncio.to_thread(
            batch_api.read_namespaced_job,
            job_name,
            namespace,
            _request_timeout=5,
        )
    except ApiException as exc:
        if exc.status == HTTPStatus.NOT_FOUND:
            return None
        raise


async def _wait_for_job(batch_api: BatchV1Api, job_name: str, namespace: str, timeout_seconds: int) -> V1Job:
    deadline = time.monotonic() + timeout_seconds
    while True:
        job = await _read_job(batch_api, job_name, namespace)
        if job is not None:
            return job
        if time.monotonic() >= deadline:
            pytest.fail(f"Job {job_name} was not created within {timeout_seconds}s")
        await asyncio.sleep(0.5)


def _assert_pod_spec(pod_spec: V1PodSpec) -> None:
    assert pod_spec.node_selector == {"kubernetes.io/os": "linux"}
    tolerations = pod_spec.tolerations
    assert len(tolerations) == 1
    toleration = tolerations[0]
    assert toleration.key == "node-role.kubernetes.io/control-plane"
    assert toleration.operator == "Equal"
    assert (toleration.value or "") == ""
    assert toleration.effect == "NoSchedule"


def _worker_managers(
    target_store: ExecutionTargetStore,
    cluster_store: ClusterStore,
    work_store: WorkStore,
    settings: EPSettings,
) -> WorkerManagerRegistry:
    registry = WorkerManagerRegistry()
    workload_manager = VanillaK8sWorkerManager(target_store, cluster_store, work_store, settings)
    registry.register(BackendType.VANILLA_K8S, workload_manager)
    return registry



@pytest.mark.integration
@pytest.mark.asyncio
async def test_work_item_dispatched_to_kind_cluster_reaches_completed(ep_cluster: Cluster) -> None:
    """Submit a script work item and assert it completes through a single dispatch cycle.

    The ep_cluster fixture provisions an ACTIVE Cluster+ExecutionTarget backed by
    the kind cluster provisioned in CI. The worker reserves the item, PlacementResolver
    assigns its target, and _process_item() drives the cold-start pod lifecycle.
    """
    database_url = os.environ["EP_TEST_DATABASE_URL"]
    settings = get_ep_settings()
    client_id = "ep-it-dispatch"

    async with (
        WorkStore.from_database_url(database_url) as work_store,
        ClusterStore.from_database_url(database_url) as cluster_store,
        ExecutionTargetStore.from_database_url(database_url) as target_store,
    ):
        item = await work_store.dispatch(
            client_id=client_id,
            item_id=uuid.uuid4(),
            payload=build_payload(
                language="bash",
                code="echo ep-integration-ok",
                timeout_seconds=120,
                environment={},
                image=_NODE_IMAGE,
            ),
        )

        worker_managers = _worker_managers(target_store, cluster_store, work_store, settings)
        placement_resolver = build_placement_resolver(cluster_store, target_store, worker_managers)

        reserved = await work_store.reserve_one()
        assert reserved is not None, "reserve_one() returned None — no queued work is available"
        assert reserved.id == item.id
        assert reserved.status is WorkItemStatus.CLAIMED

        assigned = await _place_reserved_item(reserved, work_store, placement_resolver)
        assert assigned is not None, "PlacementResolver assigned no eligible target"
        assert assigned.id == item.id
        assert assigned.execution_target_id is not None

        await _process_item(assigned, work_store, target_store, worker_managers, settings)

        final_item = await work_store.get(item.id, client_id=client_id)

    assert final_item is not None
    assert final_item.status is WorkItemStatus.COMPLETED, (
        f"Expected COMPLETED but got {final_item.status}; result={final_item.result}"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_work_item_dispatched_to_kind_cluster_includes_node_selector_in_pod_spec(
    ep_cluster: Cluster,
    kubernetes_batch_api: BatchV1Api,
) -> None:
    """AAP-97337: configured node selectors must reach the dispatched PodSpec."""
    database_url = os.environ["EP_TEST_DATABASE_URL"]
    settings = get_ep_settings()
    client_id = "ep-it-node-selector"

    async with (
        WorkStore.from_database_url(database_url) as work_store,
        ClusterStore.from_database_url(database_url) as cluster_store,
        ExecutionTargetStore.from_database_url(database_url) as target_store,
    ):
        targets = await target_store.list(cluster_id=ep_cluster.id)
        assert targets, "Expected the kind fixture to provision an ExecutionTarget"
        target = targets[0]
        original_placement = target.placement
        assert isinstance(original_placement, KubernetesPlacement)
        namespace = original_placement.namespace

        await target_store.update(
            target.id,
            updated_by=uuid.uuid4(),
            placement=KubernetesPlacement(
                namespace=namespace,
                node_selectors=["kubernetes.io/os=linux"],
                tolerations=["node-role.kubernetes.io/control-plane=:NoSchedule"],
            ),
        )

        try:
            item = await work_store.dispatch(
                client_id=client_id,
                item_id=uuid.uuid4(),
                payload={
                    "image": _NODE_IMAGE,
                    "invocation": _SLEEP_SCRIPT_INVOCATION,
                },
            )

            worker_managers = _worker_managers(target_store, cluster_store, work_store, settings)
            placement_resolver = build_placement_resolver(cluster_store, target_store, worker_managers)

            reserved = await work_store.reserve_one()
            assert reserved is not None
            assert reserved.id == item.id
            assert reserved.status is WorkItemStatus.CLAIMED

            assigned = await _place_reserved_item(reserved, work_store, placement_resolver)
            assert assigned is not None
            assert assigned.id == item.id
            assert assigned.execution_target_id is not None

            attempt_id = f"{assigned.id}-{assigned.claim_generation}"
            job_name = "syntara-node-" + hashlib.sha256(attempt_id.encode()).hexdigest()[:32]

            processing_task = asyncio.create_task(
                _process_item(assigned, work_store, target_store, worker_managers, settings)
            )

            try:
                job = await _wait_for_job(
                    kubernetes_batch_api,
                    job_name,
                    namespace,
                    _JOB_WAIT_TIMEOUT_SECONDS,
                )

                assert job.spec is not None
                assert job.spec.template is not None
                assert job.spec.template.spec is not None
                pod_spec = job.spec.template.spec
                _assert_pod_spec(pod_spec)

                dispatched_item = await work_store.get(item.id, client_id=client_id)
                assert dispatched_item is not None
                assert dispatched_item.status is WorkItemStatus.DISPATCHED

                await processing_task
            finally:
                if not processing_task.done():
                    processing_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await processing_task

            final_item = await work_store.get(item.id, client_id=client_id)
        finally:
            await target_store.update(
                target.id,
                updated_by=uuid.uuid4(),
                placement=original_placement,
            )

    assert final_item is not None
    assert final_item.status is WorkItemStatus.COMPLETED, (
        f"Expected COMPLETED but got {final_item.status}; result={final_item.result}"
    )
