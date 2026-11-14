"""EP dispatch overhead — how much latency the Execution Plane itself adds.

These tests check EP overhead only — not Kubernetes scheduling time, and not
how long the script runs.

Test 1.1 (pre-pod): From the API accepting a work item until the Kubernetes Job
  exists. "Is EP's setup before the pod starts acceptably fast?"

Test 1.2 (post-pod): From the worker being ready to talk until Execute is
  actually invoked. "Is EP's connect-and-submit path after the pod is up
  acceptably fast?"

Together they answer: EP dispatch overhead stays within budget, separately from
cluster scheduling and workload runtime.

Data retention:
  Each batch tags ``runtime_context.benchmark_run`` as ``perf-dispatch-overhead:<run_id>:<index>``
  and deletes those work-item rows from the shared DB in a ``finally`` block,
  after a 40 s settle so the last item's Job teardown can finish
  (runs are sequential, so only that pod can still be in grace).

Run with:
    pytest -m perf -s                  # -s keeps p50/p95/p99 diagnostics on stdout
    EP_PERF_REQUIRED=1 pytest -m perf -s
"""

from __future__ import annotations

import asyncio
import json
import os
import statistics
import time
import uuid
from contextlib import suppress
from typing import TYPE_CHECKING, Any, NamedTuple

import pytest
from sqlalchemy import delete, func, select
from sqlmodel import col

from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.work_item.work_item_store import WorkStore
from tools.work_item_client import DEFAULT_JWT_CLIENT_ID, build_payload, submit_work_item

if TYPE_CHECKING:
    from kubernetes.client import CoreV1Api

pytestmark = pytest.mark.perf

# ---------------------------------------------------------------------------
# Thresholds and constants
# ---------------------------------------------------------------------------

TARGET_P99_PRE_POD_OVERHEAD_MS = 500.0  # test 1.1
TARGET_P99_POST_POD_OVERHEAD_MS = 300.0  # test 1.2
TARGET_TREND_RATIO_MAX = 1.10

# Default 100: minimum for a meaningful p99 (1 sample in 100 may exceed the limit).
# The perf plan also asks for 50-run batches: set EP_PERF_BATCH_SIZE=50.
BATCH_SIZE = int(os.environ.get("EP_PERF_BATCH_SIZE", "100"))

# COMPLETED is persisted before Job/pod teardown. Sequential runs mean only the
# last item can still be cleaning up. Default node grace is 30 s; transport waits
# ~grace+10 for Job delete confirmation — settle past that before deleting rows.
CLEANUP_SETTLE_SECONDS = 40

# Must match the JWT client_id used by submit_work_item / ep_service_token so
# WorkStore.get() finds rows the API persisted. Same source as the token mint.
CLIENT_ID = os.environ.get("EP_AO_CLIENT_ID", DEFAULT_JWT_CLIENT_ID)

PERF_TIMINGS_PREFIX = "EP_PERF_TIMINGS="

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def percentile(data: list[float], p: int) -> float:
    """Return the p-th percentile of *data* via ``statistics.quantiles``."""
    return statistics.quantiles(data, n=100, method="inclusive")[p - 1]


class TrendResult(NamedTuple):
    """Latency trend across a batch: ratio and the two half-means (ms)."""

    ratio: float
    first_mean: float
    second_mean: float


def trend_ratio(data: list[float]) -> TrendResult:
    """Return second-half-mean / first-half-mean and the two half-means.

    A ratio of 1.0 means no drift; > 1.0 means overhead grew over the batch.
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
    """Poll until ``backend_resource_uid`` is set; return pre-pod ms and uid-seen mono time.

    Pre-pod uses database clocks only: ``created_at`` (API accept) to ``now()`` on
    the same poll query. Excludes the submit HTTP round-trip from the runner.

    poll_interval=25 ms — remaining bias is claim latency + one poll gap.
    timeout=30 s is generous; Job creation normally completes in < 1 s.
    """
    poll_interval = 0.025  # 25 ms
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


async def poll_pod_running(
    k8s_core: CoreV1Api,
    work_id: uuid.UUID,
    namespace: str,
    *,
    t_uid_detected: float,
) -> float:
    """Poll until the worker pod is Running; return approximate scheduling ms.

    Start is runner mono when the DB poll first saw ``backend_resource_uid``, not
    true Job-created time — so this includes poll overshoot and is informational
    only (not asserted for test 1.1 / 1.2). Overlaps the COMPLETED wait so it does
    not extend the critical path.
    """
    label_selector = f"execution-plane.syntara.io/work-item={work_id}"
    poll_interval = 0.100
    timeout = 120
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        pods = await asyncio.to_thread(
            k8s_core.list_namespaced_pod,
            namespace,
            label_selector=label_selector,
            _request_timeout=15,
        )
        for pod in pods.items:
            if pod.status and pod.status.phase == "Running":
                return (time.monotonic() - t_uid_detected) * 1000.0
        await asyncio.sleep(poll_interval)
    msg = f"Worker pod for work item {work_id} did not enter Running phase within {timeout} s"
    raise TimeoutError(msg)


async def wait_completed(
    work_store: WorkStore,
    work_id: uuid.UUID,
    *,
    scenario: str,
    run_index: int,
    timeout_seconds: float = 120,
) -> WorkItem:
    """Poll until the work item is COMPLETED with created_at/completed_at set."""
    terminal = {
        WorkItemStatus.COMPLETED,
        WorkItemStatus.FAILED,
        WorkItemStatus.CANCELLED,
        WorkItemStatus.RECONCILIATION_REQUIRED,
    }
    item = None
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        item = await work_store.get(work_id, client_id=CLIENT_ID)
        if item is not None and item.status in terminal:
            break
        await asyncio.sleep(0.025)

    if item is None or item.status not in terminal:
        msg = (
            f"[{scenario}] run {run_index}: work item {work_id} did not reach "
            f"a terminal state within {timeout_seconds} s"
        )
        raise TimeoutError(msg)

    assert item.status is WorkItemStatus.COMPLETED, (
        f"[{scenario}] run {run_index}: expected COMPLETED, got {item.status}; result={item.result}"
    )
    if item.created_at is None or item.completed_at is None:
        msg = f"[{scenario}] run {run_index}: missing created_at/completed_at on {work_id}"
        raise RuntimeError(msg)
    return item


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


async def run_sequential_batch(
    database_url: str,
    api_url: str,
    token: str,
    k8s_core: CoreV1Api,
    namespace: str,
    *,
    batch_size: int,
    scenario: str,
) -> tuple[list[float], list[float], list[float], list[float]]:
    """Submit *batch_size* work items via the deployed EP API one-at-a-time.

    Returns ``(pre_pod_ms, pod_scheduling_ms, post_pod_ms, full_cycle_ms)``:

    1. pre_pod_ms — ``created_at`` → DB ``now()`` when ``backend_resource_uid`` appears
    2. scheduling_ms — uid first seen → pod Running (approx; informational; concurrent)
    3. post_pod_ms — pod worker marker (listening → Execute entered)
    4. full_cycle_ms — ``completed_at - created_at`` from the work-item row
    """
    pre_pod_ms: list[float] = []
    pod_scheduling_ms: list[float] = []
    post_pod_ms: list[float] = []
    full_cycle_ms: list[float] = []
    submitted_ids: list[uuid.UUID] = []
    run_id = uuid.uuid4()

    async with WorkStore.from_database_url(database_url) as work_store:
        try:
            for run_index in range(batch_size):
                work_id = uuid.uuid4()
                # Timings are written to stderr; build_payload must keep stderr in
                # output_config or post_pod_ms_from_result will not see EP_PERF_TIMINGS.
                payload = build_payload(
                    language="bash",
                    code="echo 'Hello, Execution Plane!'",
                    timeout_seconds=120,
                    environment={"EP_PERF_WORKER_TIMESTAMPS": "1"},
                    runtime_context={"benchmark_run": f"perf-dispatch-overhead:{run_id}:{run_index}"},
                )

                await asyncio.to_thread(submit_work_item, api_url, token, work_id=work_id, payload=payload)
                submitted_ids.append(work_id)

                pre_pod_elapsed, t_uid_detected = await poll_job_created(work_store, work_id)
                pre_pod_ms.append(pre_pod_elapsed)

                # Informational scheduling overlaps COMPLETED wait — wall-clock is
                # the slower of the two, not their sum.
                sched_task = asyncio.create_task(
                    poll_pod_running(k8s_core, work_id, namespace, t_uid_detected=t_uid_detected)
                )
                try:
                    item = await wait_completed(work_store, work_id, scenario=scenario, run_index=run_index)
                    sched_elapsed = await sched_task
                except BaseException:
                    sched_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await sched_task
                    raise

                pod_scheduling_ms.append(sched_elapsed)
                post_pod_ms.append(post_pod_ms_or_stale_image(item.result, scenario=scenario, run_index=run_index))
                full_cycle_ms.append((item.completed_at - item.created_at).total_seconds() * 1000.0)
        finally:
            if submitted_ids:
                await asyncio.sleep(CLEANUP_SETTLE_SECONDS)
            await _cleanup_submitted_work_items(work_store, submitted_ids)

    return pre_pod_ms, pod_scheduling_ms, post_pod_ms, full_cycle_ms


async def _cleanup_submitted_work_items(work_store: WorkStore, work_ids: list[uuid.UUID]) -> None:
    """Delete work-item rows created by this batch from the shared perf database."""
    if not work_ids:
        return
    async with work_store._session_context() as session:
        await session.execute(delete(WorkItem).where(col(WorkItem.id).in_(work_ids)))
        await session.commit()


def assert_dispatch_overhead(
    pre_pod_ms: list[float],
    pod_scheduling_ms: list[float],
    post_pod_ms: list[float],
    full_cycle_ms: list[float],
    *,
    scenario: str,
) -> None:
    """Assert test 1.1, test 1.2, and trend KPIs; always print diagnostics for CI baselines."""
    n = len(pre_pod_ms)

    pre_p50 = percentile(pre_pod_ms, 50)
    pre_p95 = percentile(pre_pod_ms, 95)
    pre_p99 = percentile(pre_pod_ms, 99)
    pre_mean = statistics.mean(pre_pod_ms)
    pre_trend = trend_ratio(pre_pod_ms)

    post_p50 = percentile(post_pod_ms, 50)
    post_p95 = percentile(post_pod_ms, 95)
    post_p99 = percentile(post_pod_ms, 99)
    post_mean = statistics.mean(post_pod_ms)
    post_trend = trend_ratio(post_pod_ms)

    sched_p50 = percentile(pod_scheduling_ms, 50)
    sched_p99 = percentile(pod_scheduling_ms, 99)
    cycle_p99 = percentile(full_cycle_ms, 99)

    diag = (
        f"\n--- Dispatch overhead ({scenario}, n={n}) ---\n"
        f"  pre-pod:  p50={pre_p50:.1f} ms  p95={pre_p95:.1f} ms"
        f"  p99={pre_p99:.1f} ms  mean={pre_mean:.1f} ms\n"
        f"    trend: first-half={pre_trend.first_mean:.1f} ms"
        f"  second-half={pre_trend.second_mean:.1f} ms  ratio={pre_trend.ratio:.3f}\n"
        f"  post-pod: p50={post_p50:.1f} ms  p95={post_p95:.1f} ms"
        f"  p99={post_p99:.1f} ms  mean={post_mean:.1f} ms\n"
        f"    trend: first-half={post_trend.first_mean:.1f} ms"
        f"  second-half={post_trend.second_mean:.1f} ms  ratio={post_trend.ratio:.3f}\n"
        f"  [informational] approx scheduling (uid seen→Running) "
        f"p50={sched_p50:.0f} ms  p99={sched_p99:.0f} ms\n"
        f"  [informational] full dispatch cycle p99={cycle_p99:.0f} ms\n"
    )
    print(diag, flush=True)  # noqa: T201 — intentional CI baseline output (run with -s)

    # Evaluate every KPI before failing so one breach does not hide the others.
    failures: list[str] = []
    if pre_p99 > TARGET_P99_PRE_POD_OVERHEAD_MS:
        failures.append(
            f"p99 pre-pod overhead {pre_p99:.1f} ms exceeds {TARGET_P99_PRE_POD_OVERHEAD_MS:.0f} ms limit (test 1.1)"
        )
    if post_p99 > TARGET_P99_POST_POD_OVERHEAD_MS:
        failures.append(
            f"p99 post-pod overhead {post_p99:.1f} ms exceeds {TARGET_P99_POST_POD_OVERHEAD_MS:.0f} ms limit (test 1.2)"
        )
    if pre_trend.ratio > TARGET_TREND_RATIO_MAX:
        failures.append(
            f"Pre-pod overhead trend ratio {pre_trend.ratio:.3f} exceeds "
            f"{TARGET_TREND_RATIO_MAX} — possible carry-over latency"
        )
    if post_trend.ratio > TARGET_TREND_RATIO_MAX:
        failures.append(
            f"Post-pod overhead trend ratio {post_trend.ratio:.3f} exceeds "
            f"{TARGET_TREND_RATIO_MAX} — possible carry-over latency"
        )
    if failures:
        raise AssertionError("\n".join(failures) + diag)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDispatchOverhead:
    """EP dispatch overhead: API accept → Job created; server listening → Execute entered.

    Validates:
        - Test 1.1: p99 pre-pod overhead ≤ 500 ms (created_at → backend_resource_uid)
        - Test 1.2: p99 post-pod overhead ≤ 300 ms (worker listening → Execute entered)
        - No upward latency trend across the 100-run batch for either interval
        - Approx scheduling and full cycle recorded informally (not KPI thresholds)
    """

    @pytest.mark.asyncio
    async def test_dispatch_overhead(
        self,
        perf_database_url: str,
        perf_api_url: str,
        perf_token: str,
        perf_k8s_core_api: CoreV1Api,
        perf_ep_namespace: str,
    ) -> None:
        """Test 1.1: p99 pre-pod ≤ 500 ms and test 1.2: p99 post-pod ≤ 300 ms."""
        pre_pod_ms, pod_scheduling_ms, post_pod_ms, full_cycle_ms = await run_sequential_batch(
            perf_database_url,
            perf_api_url,
            perf_token,
            perf_k8s_core_api,
            perf_ep_namespace,
            batch_size=BATCH_SIZE,
            scenario="default",
        )
        assert_dispatch_overhead(pre_pod_ms, pod_scheduling_ms, post_pod_ms, full_cycle_ms, scenario="default")

    @pytest.mark.asyncio
    @pytest.mark.skip(
        reason=(
            "TODO: cluster_load_80pct fixture not yet implemented. "
            "Implement a fixture that pre-fills 80 % of node capacity with "
            "pause pods before enabling this test."
        )
    )
    async def test_dispatch_overhead_cluster_load(
        self,
        perf_database_url: str,
        perf_api_url: str,
        perf_token: str,
        perf_k8s_core_api: CoreV1Api,
        perf_ep_namespace: str,
        cluster_load_80pct,
    ) -> None:
        """Test 1.1 / 1.2 under 80 % cluster load — skipped until load fixture exists."""
        pre_pod_ms, pod_scheduling_ms, post_pod_ms, full_cycle_ms = await run_sequential_batch(
            perf_database_url,
            perf_api_url,
            perf_token,
            perf_k8s_core_api,
            perf_ep_namespace,
            batch_size=BATCH_SIZE,
            scenario="loaded",
        )
        assert_dispatch_overhead(pre_pod_ms, pod_scheduling_ms, post_pod_ms, full_cycle_ms, scenario="loaded")
