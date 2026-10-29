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
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING

import pytest
from kubernetes.client import ApiException, BatchV1Api, CoreV1Api, V1Job, V1Pod, V1PodSpec

from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.config import EPSettings, get_ep_settings
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.execution_target_reconciler.adapters import build_placement_resolver
from execution_plane.execution_target_reconciler.placement import WorkerManagerRegistry
from execution_plane.models.execution_target import BackendType
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.work_item.work_item_store import WorkStore
from execution_plane.work_item.work_scheduler import WorkScheduler
from execution_plane.work_item_client import build_payload
from execution_plane.worker import _process_item
from execution_plane.worker_manager.vanilla_k8s.manager import VanillaK8sWorkerManager

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from execution_plane.credential_resolution.resolver import CredentialResolver
    from execution_plane.models.cluster import Cluster
    from execution_plane.models.execution_target import ExecutionTarget

_NODE_IMAGE = "quay.io/ahetheri/syntara-node-script:migration-test"
_WORKLOAD_WAIT_SECONDS = 5
_CANCEL_WORKLOAD_WAIT_SECONDS = 15
_JOB_WAIT_TIMEOUT_SECONDS = 30
_DISPATCH_CLEANUP_TIMEOUT_SECONDS = 30
# A short execution budget with a much longer script so the step reliably
# outruns its timeout without any cancellation request. The dispatch must then
# finish well inside the sleep window: if it took as long as the sleep, the
# timeout never preempted the step.
_TIMEOUT_WORKLOAD_BUDGET_SECONDS = 5
_TIMEOUT_WORKLOAD_SLEEP_SECONDS = 120
_TIMEOUT_MAX_ELAPSED_SECONDS = 60


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


async def _wait_for_running_pod(
    core_api: CoreV1Api,
    job_name: str,
    namespace: str,
    timeout_seconds: int,
) -> V1Pod:
    deadline = time.monotonic() + timeout_seconds
    while True:
        pods = await asyncio.to_thread(
            core_api.list_namespaced_pod,
            namespace,
            label_selector=f"batch.kubernetes.io/job-name={job_name}",
            _request_timeout=5,
        )
        running_pods = [pod for pod in pods.items if pod.status is not None and pod.status.phase == "Running"]
        if running_pods:
            assert len(running_pods) == 1
            return running_pods[0]
        if time.monotonic() >= deadline:
            pytest.fail(f"Job {job_name} had no Running worker Pod within {timeout_seconds}s")
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


async def _first_target(
    target_store: ExecutionTargetStore,
    cluster_id: uuid.UUID,
) -> ExecutionTarget:
    targets = await target_store.list(cluster_id=cluster_id)
    assert targets, "Expected the kind fixture to provision an ExecutionTarget"
    return targets[0]


@dataclass(frozen=True)
class _DispatchedItem:
    item: WorkItem
    assigned: WorkItem
    job_name: str
    worker_managers: WorkerManagerRegistry


@dataclass(frozen=True)
class _WorkFixture:
    store: WorkStore
    scheduler: WorkScheduler
    worker_managers: WorkerManagerRegistry


@asynccontextmanager
async def _work_service(
    database_url: str,
    cluster_store: ClusterStore,
    target_store: ExecutionTargetStore,
    settings: EPSettings,
    credential_resolver: CredentialResolver,
) -> AsyncIterator[_WorkFixture]:
    worker_managers = WorkerManagerRegistry()
    placement_resolver = build_placement_resolver(cluster_store, target_store, worker_managers)
    async with WorkStore.from_database_url(database_url) as work_item_store:
        work_scheduler = WorkScheduler(work_item_store, placement_resolver)
        workload_manager = VanillaK8sWorkerManager(
            target_store,
            cluster_store,
            work_item_store,
            settings,
            credential_resolver=credential_resolver,
        )
        worker_managers.register(BackendType.VANILLA_K8S, workload_manager)
        yield _WorkFixture(work_item_store, work_scheduler, worker_managers)


async def _dispatch_item(
    work: _WorkFixture,
    worker_managers: WorkerManagerRegistry,
    *,
    client_id: str,
    payload: dict[str, object],
) -> _DispatchedItem:
    item = await work.store.dispatch(
        client_id=client_id,
        item_id=uuid.uuid4(),
        payload=payload,
    )

    assigned = await work.scheduler.claim_and_place_work()
    assert assigned is not None
    assert assigned.id == item.id
    assert assigned.execution_target_id is not None

    attempt_id = f"{assigned.id}-{assigned.claim_generation}"
    job_name = "syntara-node-" + hashlib.sha256(attempt_id.encode()).hexdigest()[:32]
    return _DispatchedItem(item, assigned, job_name, worker_managers)


async def _cancel_processing_task(
    work: _WorkFixture,
    processing_task: asyncio.Task[None],
    *,
    item_id: uuid.UUID,
    client_id: str,
) -> None:
    if processing_task.done():
        return

    with suppress(Exception):
        await work.store.request_cancel(item_id, client_id=client_id)

    await asyncio.wait({processing_task}, timeout=_DISPATCH_CLEANUP_TIMEOUT_SECONDS)
    if processing_task.done():
        if not processing_task.cancelled():
            processing_task.exception()
        return

    processing_task.cancel()
    with suppress(asyncio.CancelledError):
        await processing_task


async def _assert_job_cleaned_up(
    batch_api: BatchV1Api,
    core_api: CoreV1Api,
    job_name: str,
    namespace: str,
) -> None:
    job = await _read_job(batch_api, job_name, namespace)
    pods = await asyncio.to_thread(
        core_api.list_namespaced_pod,
        namespace,
        label_selector=f"batch.kubernetes.io/job-name={job_name}",
        _request_timeout=5,
    )
    assert job is None
    assert not pods.items


@pytest.mark.integration
@pytest.mark.asyncio
async def test_work_item_dispatched_to_kind_cluster_reaches_completed(
    ep_cluster: Cluster,
    integration_credential_resolver: CredentialResolver,
) -> None:
    """Submit a script work item and assert it completes through a single dispatch cycle.

    The ep_cluster fixture provisions an ACTIVE Cluster+ExecutionTarget backed by
    the kind cluster provisioned in CI. The worker reserves the item, PlacementResolver
    assigns its target, and _process_item() drives the cold-start pod lifecycle.
    """
    database_url = os.environ["EP_TEST_DATABASE_URL"]
    settings = get_ep_settings()
    client_id = "ep-it-dispatch"

    async with (
        ClusterStore.from_database_url(database_url) as cluster_store,
        ExecutionTargetStore.from_database_url(database_url) as target_store,
    ):
        async with _work_service(
            database_url,
            cluster_store,
            target_store,
            settings,
            integration_credential_resolver,
        ) as work:
            dispatched = await _dispatch_item(
                work,
                work.worker_managers,
                client_id=client_id,
                payload=build_payload(
                    language="bash",
                    code="echo ep-integration-ok",
                    timeout_seconds=120,
                    environment={},
                    image=_NODE_IMAGE,
                ),
            )
            item = dispatched.item
            assigned = dispatched.assigned

            await _process_item(assigned, work.store, target_store, work.worker_managers, settings)

            final_item = await work.store.get(item.id, client_id=client_id)

    assert final_item is not None
    assert final_item.status is WorkItemStatus.COMPLETED, (
        f"Expected COMPLETED but got {final_item.status}; result={final_item.result}"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_work_item_dispatched_to_kind_cluster_includes_node_selector_in_pod_spec(
    ep_cluster: Cluster,
    kubernetes_batch_api: BatchV1Api,
    integration_credential_resolver: CredentialResolver,
) -> None:
    """AAP-97337: configured node selectors must reach the dispatched PodSpec."""
    database_url = os.environ["EP_TEST_DATABASE_URL"]
    settings = get_ep_settings()
    client_id = "ep-it-node-selector"

    async with (
        ClusterStore.from_database_url(database_url) as cluster_store,
        ExecutionTargetStore.from_database_url(database_url) as target_store,
    ):
        target = await _first_target(target_store, ep_cluster.id)
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
            async with _work_service(
                database_url,
                cluster_store,
                target_store,
                settings,
                integration_credential_resolver,
            ) as work:
                dispatched = await _dispatch_item(
                    work,
                    work.worker_managers,
                    client_id=client_id,
                    payload=build_payload(
                        language="bash",
                        code=f"sleep {_WORKLOAD_WAIT_SECONDS}",
                        timeout_seconds=90,
                        environment={},
                        image=_NODE_IMAGE,
                    ),
                )
                item = dispatched.item
                assigned = dispatched.assigned
                job_name = dispatched.job_name

                processing_task = asyncio.create_task(
                    _process_item(assigned, work.store, target_store, work.worker_managers, settings)
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

                    dispatched_item = await work.store.get(item.id, client_id=client_id)
                    assert dispatched_item is not None
                    assert dispatched_item.status is WorkItemStatus.DISPATCHED

                    await processing_task
                finally:
                    await _cancel_processing_task(
                        work,
                        processing_task,
                        item_id=item.id,
                        client_id=client_id,
                    )

                final_item = await work.store.get(item.id, client_id=client_id)
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_work_item_cancel_reaches_cancelled(
    ep_cluster: Cluster,
    kubernetes_batch_api: BatchV1Api,
    integration_credential_resolver: CredentialResolver,
) -> None:
    """AAP-95962: cancel a running script work item and confirm it reaches CANCELLED."""
    database_url = os.environ["EP_TEST_DATABASE_URL"]
    settings = get_ep_settings()
    client_id = "ep-it-cancel"

    async with (
        ClusterStore.from_database_url(database_url) as cluster_store,
        ExecutionTargetStore.from_database_url(database_url) as target_store,
    ):
        target = await _first_target(target_store, ep_cluster.id)
        assert isinstance(target.placement, KubernetesPlacement)
        namespace = target.placement.namespace

        async with _work_service(
            database_url,
            cluster_store,
            target_store,
            settings,
            integration_credential_resolver,
        ) as work:
            dispatched = await _dispatch_item(
                work,
                work.worker_managers,
                client_id=client_id,
                payload=build_payload(
                    language="bash",
                    code=f"sleep {_CANCEL_WORKLOAD_WAIT_SECONDS}",
                    timeout_seconds=60,
                    environment={},
                    image=_NODE_IMAGE,
                ),
            )
            item = dispatched.item
            assigned = dispatched.assigned
            job_name = dispatched.job_name
            core_api = CoreV1Api(kubernetes_batch_api.api_client)

            processing_task = asyncio.create_task(
                _process_item(assigned, work.store, target_store, work.worker_managers, settings)
            )

            try:
                await _wait_for_running_pod(
                    core_api,
                    job_name,
                    namespace,
                    _JOB_WAIT_TIMEOUT_SECONDS,
                )

                dispatched_item = await work.store.get(item.id, client_id=client_id)
                assert dispatched_item is not None
                assert dispatched_item.status is WorkItemStatus.DISPATCHED

                cancel_requested = await work.store.request_cancel(item.id, client_id=client_id)
                assert cancel_requested.status is WorkItemStatus.CANCEL_REQUESTED

                await processing_task
                await _assert_job_cleaned_up(
                    kubernetes_batch_api,
                    core_api,
                    job_name,
                    namespace,
                )

                final_item = await work.store.get(item.id, client_id=client_id)
            finally:
                await _cancel_processing_task(
                    work,
                    processing_task,
                    item_id=item.id,
                    client_id=client_id,
                )

    assert final_item is not None
    assert final_item.status is WorkItemStatus.CANCELLED, (
        f"Expected CANCELLED but got {final_item.status}; result={final_item.result}"
    )
    assert final_item.result is not None
    assert final_item.result["cancelled"] is True
    assert final_item.result["execution_started"] is True


@pytest.mark.integration
@pytest.mark.asyncio
async def test_work_item_timeout_reaches_terminal_status(
    ep_cluster: Cluster,
    kubernetes_batch_api: BatchV1Api,
    integration_credential_resolver: CredentialResolver,
) -> None:
    """A workload that exceeds its timeout budget reaches a terminal status.

    Dispatch a script that sleeps well past a short ``timeout_seconds`` budget,
    with no cancellation request, and assert the terminal status it settles into.
    Also assert the dispatch finishes well inside the sleep window — otherwise the
    timeout never actually preempted the step and the script simply ran to its end.
    """
    database_url = os.environ["EP_TEST_DATABASE_URL"]
    settings = get_ep_settings()
    client_id = "ep-it-timeout"

    async with (
        ClusterStore.from_database_url(database_url) as cluster_store,
        ExecutionTargetStore.from_database_url(database_url) as target_store,
    ):
        target = await _first_target(target_store, ep_cluster.id)
        assert isinstance(target.placement, KubernetesPlacement)
        namespace = target.placement.namespace

        async with _work_service(
            database_url,
            cluster_store,
            target_store,
            settings,
            integration_credential_resolver,
        ) as work:
            dispatched = await _dispatch_item(
                work,
                work.worker_managers,
                client_id=client_id,
                payload=build_payload(
                    language="bash",
                    code=f"sleep {_TIMEOUT_WORKLOAD_SLEEP_SECONDS}",
                    timeout_seconds=_TIMEOUT_WORKLOAD_BUDGET_SECONDS,
                    environment={},
                    image=_NODE_IMAGE,
                ),
            )
            item = dispatched.item
            assigned = dispatched.assigned
            job_name = dispatched.job_name
            core_api = CoreV1Api(kubernetes_batch_api.api_client)

            started = time.monotonic()
            await _process_item(assigned, work.store, target_store, work.worker_managers, settings)
            elapsed = time.monotonic() - started

            await _assert_job_cleaned_up(
                kubernetes_batch_api,
                core_api,
                job_name,
                namespace,
            )
            final_item = await work.store.get(item.id, client_id=client_id)

    # The timeout must have preempted the step: the dispatch has to settle well
    # before the script's own sleep would have ended, or it never actually fired.
    assert elapsed < _TIMEOUT_MAX_ELAPSED_SECONDS, (
        f"Dispatch took {elapsed:.1f}s for a {_TIMEOUT_WORKLOAD_BUDGET_SECONDS}s "
        f"budget; the {_TIMEOUT_WORKLOAD_SLEEP_SECONDS}s script was not preempted"
    )

    assert final_item is not None
    assert final_item.status is WorkItemStatus.FAILED, (
        f"Expected FAILED but got {final_item.status}; result={final_item.result}"
    )


_PARALLEL_SLEEP_SECONDS = 4


@pytest.mark.integration
@pytest.mark.asyncio
async def test_multiple_work_items_dispatch_concurrently(
    ep_cluster: Cluster,
    integration_credential_resolver: CredentialResolver,
) -> None:
    """Two sleep workloads dispatched concurrently must complete faster than sequential would allow.

    Sequential execution of two 4-second scripts would take >8s. Concurrent
    execution should finish well under that bound, proving the poll loop
    dispatches work items in parallel rather than one at a time.
    """
    database_url = os.environ["EP_TEST_DATABASE_URL"]
    settings = get_ep_settings()
    client_id = "ep-it-parallel"

    async with (
        ClusterStore.from_database_url(database_url) as cluster_store,
        ExecutionTargetStore.from_database_url(database_url) as target_store,
    ):
        async with _work_service(
            database_url,
            cluster_store,
            target_store,
            settings,
            integration_credential_resolver,
        ) as work:
            payload = build_payload(
                language="bash",
                code=f"sleep {_PARALLEL_SLEEP_SECONDS}",
                timeout_seconds=120,
                environment={},
                image=_NODE_IMAGE,
            )
            dispatched_a = await _dispatch_item(work, work.worker_managers, client_id=client_id, payload=payload)
            dispatched_b = await _dispatch_item(work, work.worker_managers, client_id=client_id, payload=payload)

            await asyncio.gather(
                _process_item(dispatched_a.assigned, work.store, target_store, work.worker_managers, settings),
                _process_item(dispatched_b.assigned, work.store, target_store, work.worker_managers, settings),
            )

            item_a = await work.store.get(dispatched_a.item.id, client_id=client_id)
            item_b = await work.store.get(dispatched_b.item.id, client_id=client_id)

    assert item_a is not None
    assert item_b is not None
    assert item_a.completed_at is not None
    assert item_b.completed_at is not None
    assert item_a.status is WorkItemStatus.COMPLETED, f"item_a: {item_a.status}; result={item_a.result}"
    assert item_b.status is WorkItemStatus.COMPLETED, f"item_b: {item_b.status}; result={item_b.result}"
    completion_gap = abs((item_a.completed_at - item_b.completed_at).total_seconds())
    assert completion_gap < _PARALLEL_SLEEP_SECONDS, (
        f"Items completed {completion_gap:.1f}s apart — expected concurrent dispatch to finish them "
        f"within {_PARALLEL_SLEEP_SECONDS}s of each other"
    )
