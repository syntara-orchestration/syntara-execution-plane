"""Shared sequential / parallel dispatch runners for EP perf tests.

Metrics: full_cycle_ms, optional pre/sched/post/pod_lifecycle_ms, parallel wall_clock_ms.
pod_lifecycle uses creationTimestamp→completed_at (not container finishedAt / grace).
"""

from __future__ import annotations

import asyncio
import functools
import json
import os
import statistics
import threading
import time
import uuid
from concurrent.futures import Executor, ThreadPoolExecutor
from contextlib import suppress
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, NamedTuple

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlmodel import col

from execution_plane.config.settings import get_settings
from execution_plane.database.ssl import ssl_connect_args_from_env, strip_unsupported_asyncpg_ssl_query
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.work_item.work_item_store import WorkStore
from tools.work_item_client import DEFAULT_JWT_CLIENT_ID, build_payload, submit_work_item

if TYPE_CHECKING:
    from collections.abc import Callable

    from kubernetes.client import CoreV1Api
    from kubernetes.client.models import V1Pod, V1PodList

# Wait past node grace (~30s) + Job delete confirmation before deleting DB rows.
CLEANUP_SETTLE_SECONDS = 40
PARALLEL_CLEANUP_SETTLE_SECONDS = 60  # many Jobs tearing down at once

# Must match submit_work_item / ep_service_token client_id.
CLIENT_ID = os.environ.get("EP_AO_CLIENT_ID", DEFAULT_JWT_CLIENT_ID)

PERF_TIMINGS_PREFIX = "EP_PERF_TIMINGS="


async def _in_thread[**P, T](
    executor: Executor | None, func: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs
) -> T:
    """Run blocking *func* on *executor*, or the default thread pool when None."""
    if executor is None:
        return await asyncio.to_thread(func, *args, **kwargs)
    return await asyncio.get_running_loop().run_in_executor(executor, functools.partial(func, *args, **kwargs))


def percentile(data: list[float], p: int) -> float:
    """Return the p-th percentile of *data* via ``statistics.quantiles``.

    Args:
        data: Sample values.
        p: Percentile in ``1..100``.

    Returns:
        The p-th percentile value.

    """
    return statistics.quantiles(data, n=100, method="inclusive")[p - 1]


class TrendResult(NamedTuple):
    """Latency trend across a run series: ratio and the two half-means (ms)."""

    ratio: float
    first_mean: float
    second_mean: float


def trend_ratio(data: list[float]) -> TrendResult:
    """Return second-half-mean / first-half-mean (>1 means growing latency).

    Args:
        data: Latency samples in run order.

    Returns:
        Ratio and the two half-series means.

    """
    midpoint = len(data) // 2
    first_mean = statistics.mean(data[:midpoint]) if data[:midpoint] else 0.0
    second_mean = statistics.mean(data[midpoint:]) if data[midpoint:] else 0.0
    ratio = second_mean / first_mean if first_mean > 0 else 1.0
    return TrendResult(ratio=ratio, first_mean=first_mean, second_mean=second_mean)


def post_pod_ms_from_result(result: dict[str, Any] | None) -> float:
    """Parse ``post_pod_overhead_ms`` from the worker's stderr timings marker."""
    if result is None:
        msg = "work item result is missing; cannot read post-pod timings"
        raise ValueError(msg)
    output = result.get("output") or {}
    stderr = output.get("stderr") or ""
    for line in stderr.splitlines():
        if line.startswith(PERF_TIMINGS_PREFIX):
            payload = json.loads(line.removeprefix(PERF_TIMINGS_PREFIX))
            return float(payload["post_pod_overhead_ms"])
    msg = f"EP_PERF_TIMINGS marker not found in work item result stderr: {stderr!r}"
    raise ValueError(msg)


async def poll_job_created(work_store: WorkStore, work_id: uuid.UUID) -> tuple[float, float]:
    """Poll until backend_resource_uid is set.

    Args:
        work_store: Work-item store used for DB polls.
        work_id: Work item to watch.

    Returns:
        ``(pre_pod_ms, uid_seen_mono)`` where pre-pod is created_at→DB now.

    """
    poll_interval = 0.025
    timeout = 30
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        async with work_store._session_context() as session:
            row = await session.execute(
                select(WorkItem.created_at, WorkItem.backend_resource_uid, func.now()).where(
                    col(WorkItem.id) == work_id,
                    col(WorkItem.client_id) == CLIENT_ID,
                )
            )
            result = row.first()
        if result is not None:
            created_at, uid, db_now = result
            if uid is not None and created_at is not None:
                t_detected = time.monotonic()
                return (db_now - created_at).total_seconds() * 1000.0, t_detected
        await asyncio.sleep(poll_interval)
    msg = f"Kubernetes Job not created within {timeout} s for {work_id}"
    raise TimeoutError(msg)


def _as_utc(value: datetime) -> datetime:
    """Return *value* as timezone-aware UTC (naive values are treated as UTC)."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _pod_creation_timestamp(pod: V1Pod) -> datetime | None:
    """Return ``metadata.creationTimestamp`` when present and parseable."""
    created = pod.metadata.creation_timestamp if pod.metadata else None
    return created if isinstance(created, datetime) else None


def _newest_running_pod(pods: list[V1Pod]) -> V1Pod | None:
    """Newest Running pod by creationTimestamp (retries leave older failed pods)."""
    newest: V1Pod | None = None
    newest_created: datetime | None = None
    for pod in pods:
        if not pod.status or pod.status.phase != "Running":
            continue
        created = _pod_creation_timestamp(pod)
        if created is None:
            continue
        if newest_created is None or _as_utc(created) > _as_utc(newest_created):
            newest = pod
            newest_created = created
    return newest


def _list_namespaced_pod(
    k8s_core: CoreV1Api,
    namespace: str,
    *,
    label_selector: str,
    k8s_lock: threading.Lock | None,
) -> V1PodList:
    """List pods; hold *k8s_lock* when the shared CoreV1Api is used from many threads."""

    def list_pods() -> V1PodList:
        return k8s_core.list_namespaced_pod(
            namespace,
            label_selector=label_selector,
            _request_timeout=15,
        )

    if k8s_lock is None:
        return list_pods()
    with k8s_lock:
        return list_pods()


async def poll_pod_running(
    k8s_core: CoreV1Api,
    work_id: uuid.UUID,
    namespace: str,
    *,
    t_uid_detected: float,
    timeout_seconds: float = 120,
    poll_interval: float = 0.100,
    executor: Executor | None = None,
    k8s_lock: threading.Lock | None = None,
) -> float:
    """Approx scheduling ms from uid-seen mono until newest Running pod (informational).

    Args:
        k8s_core: Kubernetes CoreV1Api client.
        work_id: Work item whose pod to watch.
        namespace: Namespace of the worker pod.
        t_uid_detected: Monotonic time when backend_resource_uid was first seen.
        timeout_seconds: Max wait for Running.
        poll_interval: Sleep between list calls.
        executor: Optional thread pool for blocking k8s calls.
        k8s_lock: Lock when sharing one CoreV1Api across threads.

    Returns:
        Approximate scheduling duration in milliseconds.

    """
    label_selector = f"execution-plane.syntara.io/work-item={work_id}"
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        pods = await _in_thread(
            executor,
            _list_namespaced_pod,
            k8s_core,
            namespace,
            label_selector=label_selector,
            k8s_lock=k8s_lock,
        )
        if _newest_running_pod(list(pods.items)) is not None:
            return (time.monotonic() - t_uid_detected) * 1000.0
        await asyncio.sleep(poll_interval)
    msg = f"Worker pod for work item {work_id} did not enter Running phase within {timeout_seconds} s"
    raise TimeoutError(msg)


def pod_lifecycle_ms(pod_created_at: datetime, work_completed_at: datetime) -> float:
    """Pod create → task complete ms (excludes Job teardown).

    Args:
        pod_created_at: Pod ``creationTimestamp``.
        work_completed_at: Work-item ``completed_at``.

    Returns:
        Duration in milliseconds.

    """
    return (_as_utc(work_completed_at) - _as_utc(pod_created_at)).total_seconds() * 1000.0


async def poll_pod_creation_timestamp(
    k8s_core: CoreV1Api,
    work_id: uuid.UUID,
    namespace: str,
    *,
    timeout_seconds: float = 120,
    poll_interval: float = 0.100,
    executor: Executor | None = None,
    k8s_lock: threading.Lock | None = None,
) -> datetime:
    """Return creationTimestamp of the newest Running work-item pod.

    Args:
        k8s_core: Kubernetes CoreV1Api client.
        work_id: Work item whose pod to watch.
        namespace: Namespace of the worker pod.
        timeout_seconds: Max wait for Running.
        poll_interval: Sleep between list calls.
        executor: Optional thread pool for blocking k8s calls.
        k8s_lock: Lock when sharing one CoreV1Api across threads.

    Returns:
        Pod ``creationTimestamp``.

    """
    label_selector = f"execution-plane.syntara.io/work-item={work_id}"
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        pods = await _in_thread(
            executor,
            _list_namespaced_pod,
            k8s_core,
            namespace,
            label_selector=label_selector,
            k8s_lock=k8s_lock,
        )
        newest = _newest_running_pod(list(pods.items))
        if newest is not None:
            created = _pod_creation_timestamp(newest)
            if created is not None:
                return created
        await asyncio.sleep(poll_interval)
    msg = f"Worker pod for work item {work_id} did not enter Running within {timeout_seconds} s"
    raise TimeoutError(msg)


class CompletedWorkSample(NamedTuple):
    """Terminal work-item fields needed by perf runners (no encrypted payload)."""

    status: WorkItemStatus
    created_at: datetime
    completed_at: datetime
    result: dict[str, Any] | None


async def wait_completed(
    work_store: WorkStore,
    work_id: uuid.UUID,
    *,
    scenario: str,
    run_index: int,
    timeout_seconds: float = 120,
    poll_interval: float = 0.025,
) -> CompletedWorkSample:
    """Poll until COMPLETED (status/timestamps/result only — no encrypted payload).

    Args:
        work_store: Work-item store used for DB polls.
        work_id: Work item to wait on.
        scenario: Label for error messages.
        run_index: 0-based run index for error messages.
        timeout_seconds: Max wait for a terminal state.
        poll_interval: Sleep between DB polls.

    Returns:
        Terminal sample with COMPLETED status and timestamps.

    """
    terminal = {
        WorkItemStatus.COMPLETED,
        WorkItemStatus.FAILED,
        WorkItemStatus.CANCELLED,
        WorkItemStatus.RECONCILIATION_REQUIRED,
    }
    row: tuple[WorkItemStatus, datetime | None, datetime | None, dict[str, Any] | None] | None = None
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        async with work_store._session_context() as session:
            result = await session.execute(
                select(WorkItem.status, WorkItem.created_at, WorkItem.completed_at, WorkItem.result).where(
                    col(WorkItem.id) == work_id,
                    col(WorkItem.client_id) == CLIENT_ID,
                )
            )
            row = result.first()
        if row is not None and row[0] in terminal:
            break
        await asyncio.sleep(poll_interval)

    if row is None or row[0] not in terminal:
        msg = (
            f"[{scenario}] run {run_index}: work item {work_id} did not reach "
            f"a terminal state within {timeout_seconds} s"
        )
        raise TimeoutError(msg)

    status, created_at, completed_at, item_result = row
    if status is not WorkItemStatus.COMPLETED:
        # raise (not assert) so this survives python -O
        msg = f"[{scenario}] run {run_index}: expected COMPLETED, got {status}; result={item_result}"
        raise AssertionError(msg)
    if created_at is None or completed_at is None:
        msg = f"[{scenario}] run {run_index}: missing created_at/completed_at on {work_id}"
        raise RuntimeError(msg)
    return CompletedWorkSample(
        status=status,
        created_at=created_at,
        completed_at=completed_at,
        result=item_result,
    )


def post_pod_ms_or_stale_image(
    result: dict[str, Any] | None,
    *,
    scenario: str,
    run_index: int,
) -> float:
    """Parse post-pod ms; on the first item, name a stale worker image explicitly."""
    try:
        return post_pod_ms_from_result(result)
    except ValueError as exc:
        if run_index == 0:
            msg = (
                f"[{scenario}] EP_PERF_TIMINGS missing on first work item — "
                "deployed worker image likely lacks EP_PERF_WORKER_TIMESTAMPS "
                "support; rebuild/redeploy EP_NODE_IMAGE"
            )
            raise AssertionError(msg) from exc
        raise


async def _cancel_tasks(*tasks: asyncio.Task[Any] | None) -> None:
    """Cancel side polls without re-raising their stored exceptions."""
    for task in tasks:
        if task is None:
            continue
        if not task.done():
            task.cancel()
        with suppress(BaseException):
            await task


async def _await_completed_and_side_polls(
    work_store: WorkStore,
    work_id: uuid.UUID,
    *,
    scenario: str,
    run_index: int,
    sched_task: asyncio.Task[float] | None,
    pod_created_task: asyncio.Task[datetime] | None,
    timeout_seconds: float = 120,
    poll_interval: float = 0.025,
) -> tuple[CompletedWorkSample, float | None, datetime | None, float]:
    """Wait for COMPLETED, then join side polls; done_mono is stamped before joins."""
    try:
        item = await wait_completed(
            work_store,
            work_id,
            scenario=scenario,
            run_index=run_index,
            timeout_seconds=timeout_seconds,
            poll_interval=poll_interval,
        )
        done_mono = time.monotonic()
        sched_elapsed = await sched_task if sched_task is not None else None
        pod_created_at = await pod_created_task if pod_created_task is not None else None
    except BaseException:
        await _cancel_tasks(sched_task, pod_created_task)
        raise
    return item, sched_elapsed, pod_created_at, done_mono


def _require_k8s(k8s_core: CoreV1Api | None, namespace: str | None, *, flag: str) -> tuple[CoreV1Api, str]:
    """Return k8s clients or raise if a measure flag needs them."""
    if k8s_core is None:
        msg = f"{flag}=True requires k8s_core"
        raise ValueError(msg)
    if namespace is None:
        msg = f"{flag}=True requires namespace"
        raise ValueError(msg)
    return k8s_core, namespace


async def _dispatch_one(
    work_store: WorkStore,
    *,
    api_url: str,
    token: str,
    k8s_core: CoreV1Api | None,
    namespace: str | None,
    scenario: str,
    run_index: int,
    workflow_run_id: str,
    environment: dict[str, str],
    measure_pre_pod: bool,
    measure_scheduling: bool,
    measure_post_pod: bool,
    measure_pod_lifecycle: bool,
    submitted_ids: list[uuid.UUID],
    timeout_seconds: float = 120,
    poll_interval: float = 0.025,
    k8s_poll_interval: float = 0.100,
    executor: Executor | None = None,
    k8s_lock: threading.Lock | None = None,
) -> DispatchSample:
    """Submit one item; return samples. done_mono is COMPLETED time before side-poll joins.

    Args:
        work_store: Work-item store for completion polls.
        api_url: Deployed EP API base URL.
        token: Service JWT for submissions.
        k8s_core: Optional CoreV1Api (required for scheduling/lifecycle measures).
        namespace: Optional worker namespace (required with k8s measures).
        scenario: Label for error messages.
        run_index: 0-based index in the batch.
        workflow_run_id: Workflow id stamped on the payload.
        environment: Extra env vars for the echo payload.
        measure_pre_pod: Collect pre-pod overhead.
        measure_scheduling: Collect approximate scheduling ms.
        measure_post_pod: Collect post-pod overhead from result stderr.
        measure_pod_lifecycle: Collect creationTimestamp→completed_at.
        submitted_ids: Mutable list; work_id is appended before submit.
        timeout_seconds: COMPLETED / k8s poll budget.
        poll_interval: DB poll interval.
        k8s_poll_interval: Kubernetes list poll interval.
        executor: Optional thread pool for blocking I/O.
        k8s_lock: Lock when sharing one CoreV1Api across threads.

    Returns:
        Per-item timing sample including ``done_mono``.

    """
    work_id = uuid.uuid4()
    payload = build_payload(
        language="bash",
        code="echo 'Hello, Execution Plane!'",
        timeout_seconds=120,
        environment=environment,
        workflow_run_id=workflow_run_id,  # no "/"; EP regex is ^[A-Za-z0-9._:-]+$
    )
    # Append before submit so a timed-out/cancelled submit that persisted is still cleaned up.
    submitted_ids.append(work_id)
    await _in_thread(executor, submit_work_item, api_url, token, work_id=work_id, payload=payload)

    pre_pod_elapsed: float | None = None
    sched_task: asyncio.Task[float] | None = None
    pod_created_task: asyncio.Task[datetime] | None = None

    if measure_pod_lifecycle:
        core, ns = _require_k8s(k8s_core, namespace, flag="measure_pod_lifecycle")
        pod_created_task = asyncio.create_task(
            poll_pod_creation_timestamp(
                core,
                work_id,
                ns,
                timeout_seconds=timeout_seconds,
                poll_interval=k8s_poll_interval,
                executor=executor,
                k8s_lock=k8s_lock,
            )
        )

    if measure_pre_pod or measure_scheduling:
        try:
            pre_elapsed, t_uid_detected = await poll_job_created(work_store, work_id)
        except BaseException:
            await _cancel_tasks(pod_created_task)
            raise
        if measure_pre_pod:
            pre_pod_elapsed = pre_elapsed
        if measure_scheduling:
            core, ns = _require_k8s(k8s_core, namespace, flag="measure_scheduling")
            sched_task = asyncio.create_task(
                poll_pod_running(
                    core,
                    work_id,
                    ns,
                    t_uid_detected=t_uid_detected,
                    timeout_seconds=timeout_seconds,
                    poll_interval=k8s_poll_interval,
                    executor=executor,
                    k8s_lock=k8s_lock,
                )
            )

    item, sched_elapsed, pod_created_at, done_mono = await _await_completed_and_side_polls(
        work_store,
        work_id,
        scenario=scenario,
        run_index=run_index,
        sched_task=sched_task,
        pod_created_task=pod_created_task,
        timeout_seconds=timeout_seconds,
        poll_interval=poll_interval,
    )
    post_elapsed = (
        post_pod_ms_or_stale_image(item.result, scenario=scenario, run_index=run_index) if measure_post_pod else None
    )
    full_cycle = (item.completed_at - item.created_at).total_seconds() * 1000.0
    lifecycle_elapsed = pod_lifecycle_ms(pod_created_at, item.completed_at) if pod_created_at is not None else None
    return DispatchSample(
        pre_pod_ms=pre_pod_elapsed,
        scheduling_ms=sched_elapsed,
        post_pod_ms=post_elapsed,
        full_cycle_ms=full_cycle,
        pod_lifecycle_ms=lifecycle_elapsed,
        done_mono=done_mono,
    )


class DispatchSample(NamedTuple):
    """Per-item samples from a single ``_dispatch_one`` call."""

    pre_pod_ms: float | None
    scheduling_ms: float | None
    post_pod_ms: float | None
    full_cycle_ms: float
    pod_lifecycle_ms: float | None
    done_mono: float


class SequentialBatchResult(NamedTuple):
    """Per-phase sample lists from a sequential dispatch batch."""

    pre_pod_ms: list[float]
    pod_scheduling_ms: list[float]
    post_pod_ms: list[float]
    full_cycle_ms: list[float]
    pod_lifecycle_ms: list[float]


class ParallelBatchResult(NamedTuple):
    """Wall-clock and per-item samples from a concurrent dispatch batch."""

    wall_clock_ms: float
    full_cycle_ms: list[float]
    pod_lifecycle_ms: list[float]


# Full-parallelism path: one cold-start window, not N sequential waves.
PARALLEL_ITEM_TIMEOUT_SECONDS = 300.0
# Slower polls + pool sized to batch_size so waiters do not queue on pool_timeout.
PARALLEL_DB_POLL_INTERVAL_SECONDS = 0.250
PARALLEL_K8S_POLL_INTERVAL_SECONDS = 0.500


def _runner_engine(database_url: str, *, pool_size: int) -> AsyncEngine:
    """Pooled async engine; pool_size should be ≥ concurrent wait_completed loops."""
    return create_async_engine(
        strip_unsupported_asyncpg_ssl_query(database_url),
        connect_args=ssl_connect_args_from_env(),
        pool_size=pool_size,
        max_overflow=0,
        pool_timeout=get_settings().db_pool_timeout,
        pool_pre_ping=True,
    )


async def run_parallel_batch(
    database_url: str,
    api_url: str,
    token: str,
    k8s_core: CoreV1Api | None,
    namespace: str | None,
    *,
    batch_size: int,
    scenario: str,
    workflow_prefix: str = "perf-parallel-dispatch",
    measure_pod_lifecycle: bool = True,
) -> ParallelBatchResult:
    """Submit all items concurrently; wall_clock is t0 → last done_mono.

    Args:
        database_url: Perf PostgreSQL URL.
        api_url: Deployed EP API base URL.
        token: Service JWT for submissions.
        k8s_core: CoreV1Api when measuring pod lifecycle.
        namespace: Worker namespace when measuring pod lifecycle.
        batch_size: Number of items to submit together.
        scenario: Label for error messages.
        workflow_prefix: Prefix for workflow_run_id values.
        measure_pod_lifecycle: Collect per-item creationTimestamp→completed_at.

    Returns:
        Batch wall-clock plus per-item full-cycle / lifecycle samples.

    """
    if measure_pod_lifecycle and (k8s_core is None or namespace is None):
        msg = "measure_pod_lifecycle=True requires k8s_core and namespace"
        raise ValueError(msg)

    submitted_ids: list[uuid.UUID] = []
    run_id = uuid.uuid4()
    item_timeout = PARALLEL_ITEM_TIMEOUT_SECONDS

    engine = _runner_engine(database_url, pool_size=max(1, batch_size))
    # CoreV1Api is not thread-safe — serialize k8s calls with k8s_lock.
    executor = ThreadPoolExecutor(max_workers=2 * batch_size + 4, thread_name_prefix="perf-parallel")
    k8s_lock = threading.Lock()
    async with WorkStore.from_engine(engine) as work_store:
        try:
            t0 = time.monotonic()
            dispatch_tasks = [
                asyncio.create_task(
                    _dispatch_one(
                        work_store,
                        api_url=api_url,
                        token=token,
                        k8s_core=k8s_core,
                        namespace=namespace,
                        scenario=scenario,
                        run_index=run_index,
                        workflow_run_id=f"{workflow_prefix}:{run_id}:{run_index}",
                        environment={},
                        measure_pre_pod=False,
                        measure_scheduling=False,
                        measure_post_pod=False,
                        measure_pod_lifecycle=measure_pod_lifecycle,
                        submitted_ids=submitted_ids,
                        timeout_seconds=item_timeout,
                        poll_interval=PARALLEL_DB_POLL_INTERVAL_SECONDS,
                        k8s_poll_interval=PARALLEL_K8S_POLL_INTERVAL_SECONDS,
                        executor=executor,
                        k8s_lock=k8s_lock,
                    )
                )
                for run_index in range(batch_size)
            ]
            try:
                results = await asyncio.gather(*dispatch_tasks)
            except BaseException:
                await _cancel_tasks(*dispatch_tasks)  # gather leaves siblings running
                raise
            wall_clock_ms = (max(sample.done_mono for sample in results) - t0) * 1000.0
        finally:
            # Nested finally so cancel during settle still cleans up.
            try:
                if submitted_ids:
                    await asyncio.sleep(PARALLEL_CLEANUP_SETTLE_SECONDS)
            finally:
                try:
                    await _cleanup_submitted_work_items(work_store, submitted_ids)
                finally:
                    executor.shutdown(wait=False, cancel_futures=True)
                    await engine.dispose()

    full_cycle_ms = [sample.full_cycle_ms for sample in results]
    lifecycle_samples_ms = [sample.pod_lifecycle_ms for sample in results if sample.pod_lifecycle_ms is not None]
    return ParallelBatchResult(
        wall_clock_ms=wall_clock_ms,
        full_cycle_ms=full_cycle_ms,
        pod_lifecycle_ms=lifecycle_samples_ms,
    )


async def run_sequential_batch(
    database_url: str,
    api_url: str,
    token: str,
    k8s_core: CoreV1Api | None,
    namespace: str | None,
    *,
    batch_size: int,
    scenario: str,
    workflow_prefix: str = "perf-dispatch",
    measure_pre_pod: bool = True,
    measure_scheduling: bool = True,
    measure_post_pod: bool = True,
    measure_pod_lifecycle: bool = False,
) -> SequentialBatchResult:
    """Submit items one-at-a-time; each waits for COMPLETED before the next.

    Args:
        database_url: Perf PostgreSQL URL.
        api_url: Deployed EP API base URL.
        token: Service JWT for submissions.
        k8s_core: CoreV1Api when measuring scheduling/lifecycle.
        namespace: Worker namespace when measuring scheduling/lifecycle.
        batch_size: Number of sequential items.
        scenario: Label for error messages.
        workflow_prefix: Prefix for workflow_run_id values.
        measure_pre_pod: Collect pre-pod overhead samples.
        measure_scheduling: Collect approximate scheduling samples.
        measure_post_pod: Collect post-pod overhead samples.
        measure_pod_lifecycle: Collect creationTimestamp→completed_at samples.

    Returns:
        Per-phase sample lists for the batch.

    """
    if measure_scheduling and (k8s_core is None or namespace is None):
        msg = "measure_scheduling=True requires k8s_core and namespace"
        raise ValueError(msg)
    if measure_pod_lifecycle and (k8s_core is None or namespace is None):
        msg = "measure_pod_lifecycle=True requires k8s_core and namespace"
        raise ValueError(msg)

    pre_pod_ms: list[float] = []
    pod_scheduling_ms: list[float] = []
    post_pod_ms: list[float] = []
    full_cycle_ms: list[float] = []
    lifecycle_samples_ms: list[float] = []
    submitted_ids: list[uuid.UUID] = []
    run_id = uuid.uuid4()
    environment = {"EP_PERF_WORKER_TIMESTAMPS": "1"} if measure_post_pod else {}

    async with WorkStore.from_database_url(database_url) as work_store:
        try:
            for run_index in range(batch_size):
                sample = await _dispatch_one(
                    work_store,
                    api_url=api_url,
                    token=token,
                    k8s_core=k8s_core,
                    namespace=namespace,
                    scenario=scenario,
                    run_index=run_index,
                    workflow_run_id=f"{workflow_prefix}:{run_id}:{run_index}",
                    environment=environment,
                    measure_pre_pod=measure_pre_pod,
                    measure_scheduling=measure_scheduling,
                    measure_post_pod=measure_post_pod,
                    measure_pod_lifecycle=measure_pod_lifecycle,
                    submitted_ids=submitted_ids,
                )
                if sample.pre_pod_ms is not None:
                    pre_pod_ms.append(sample.pre_pod_ms)
                if sample.scheduling_ms is not None:
                    pod_scheduling_ms.append(sample.scheduling_ms)
                if sample.post_pod_ms is not None:
                    post_pod_ms.append(sample.post_pod_ms)
                full_cycle_ms.append(sample.full_cycle_ms)
                if sample.pod_lifecycle_ms is not None:
                    lifecycle_samples_ms.append(sample.pod_lifecycle_ms)
        finally:
            try:
                if submitted_ids:
                    await asyncio.sleep(CLEANUP_SETTLE_SECONDS)
            finally:
                await _cleanup_submitted_work_items(work_store, submitted_ids)

    return SequentialBatchResult(
        pre_pod_ms=pre_pod_ms,
        pod_scheduling_ms=pod_scheduling_ms,
        post_pod_ms=post_pod_ms,
        full_cycle_ms=full_cycle_ms,
        pod_lifecycle_ms=lifecycle_samples_ms,
    )


async def _cleanup_submitted_work_items(work_store: WorkStore, work_ids: list[uuid.UUID]) -> None:
    """Delete work-item rows created by this run from the shared perf database.

    Args:
        work_store: Work-item store used for the delete.
        work_ids: Work item ids to remove.

    """
    if not work_ids:
        return
    async with work_store._session_context() as session:
        await session.execute(delete(WorkItem).where(col(WorkItem.id).in_(work_ids)))
        await session.commit()
