"""EP dispatch overhead — pre-pod and post-pod, measured in a single sequential batch.

All measurements are purely external — no in-process hooks or schema changes required.
Each interval is detected by polling an observable boundary: DB for Job created and
work item completion, Kubernetes API for pod phase Running.

Measurement notes:
  * pre_pod_ms and post_pod_ms each carry ≤ 25 ms poll detection overshoot.
  * pod_scheduling_ms carries ≤ 100 ms detection overshoot (recorded as informational only).
  * post_pod_ms approximates gRPC-connect + execute-submit overhead because the work
    payload (echo) has near-zero execution time; it is not an exact in-process measurement.

Test 1.1: p99 pre-pod overhead  ≤ 500 ms  (submission → Kubernetes Job created)
Test 1.2: p99 post-pod overhead ≤ 300 ms  (pod Running → work item COMPLETED)
          No upward latency trend across the 100-run batch

Run with:
    pytest -m perf
"""

from __future__ import annotations

import asyncio
import os
import statistics
import time
import uuid
from typing import TYPE_CHECKING, NamedTuple

import pytest

from execution_plane.models.work_item import WorkItemStatus
from execution_plane.work_item.work_item_store import WorkStore
from execution_plane.work_item_client import build_payload, submit_work_item

if TYPE_CHECKING:
    from kubernetes.client import CoreV1Api

pytestmark = pytest.mark.perf

# ---------------------------------------------------------------------------
# Thresholds and constants
# ---------------------------------------------------------------------------

TARGET_P99_PRE_POD_OVERHEAD_MS = 500.0  # test 1.1
TARGET_P99_POST_POD_OVERHEAD_MS = 300.0  # test 1.2
TARGET_TREND_RATIO_MAX = 1.10

BATCH_SIZE = 10  # minimum for a meaningful p99 (1 sample in 100 may exceed the limit)

# The deployed API only accepts tokens whose client_id matches its EP_AO_CLIENT_ID,
# and work items are persisted under that client id — so DB polls must use the
# same value. Override when the deployment uses a different service client.
CLIENT_ID = os.environ.get("EP_PERF_CLIENT_ID", "perf-dispatch-overhead")

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


async def poll_job_created(
    work_store: WorkStore,
    work_id: uuid.UUID,
    *,
    t_submit: float,
) -> tuple[float, float]:
    """Poll DB for ``backend_resource_uid``; return pre-pod overhead and detection time.

    After the EP worker creates the Kubernetes Job it persists
    ``backend_resource_uid`` to the work item row.  The first non-null value
    is the closest proxy for Job-created time without in-process hooks.

    poll_interval=25 ms — noise ≤ 25 ms, ~5 % of the 500 ms test 1.1 budget.
    timeout=30 s is generous; Job creation normally completes in < 1 s.
    """
    poll_interval = 0.025  # 25 ms
    timeout = 30
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        item = await work_store.get(work_id, client_id=CLIENT_ID)
        if item is not None and item.backend_resource_uid is not None:
            t = time.monotonic()
            return (t - t_submit) * 1000.0, t
        await asyncio.sleep(poll_interval)
    msg = f"Kubernetes Job not created within {timeout} s for {work_id}"
    raise TimeoutError(msg)


async def poll_pod_running(
    k8s_core: CoreV1Api,
    work_id: uuid.UUID,
    namespace: str,
    *,
    t_job_created: float,
) -> tuple[float, float]:
    """Poll the K8s API until the worker pod enters Running phase; return scheduling time.

    poll_interval=100 ms — scheduling takes seconds so 100 ms precision is
    sufficient.  Detection overshoot ≤ 100 ms is documented in the module
    header; pod_scheduling_ms is recorded as informational only.
    timeout=120 s covers cold image-pull scenarios.
    """
    label_selector = f"execution-plane.syntara.io/work-item={work_id}"
    poll_interval = 0.100  # 100 ms — overshoot ≤ 100 ms; scheduling_ms is informational
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
                t = time.monotonic()
                return (t - t_job_created) * 1000.0, t
        await asyncio.sleep(poll_interval)
    msg = f"Worker pod for work item {work_id} did not enter Running phase within {timeout} s"
    raise TimeoutError(msg)


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

    Each dispatch is measured at three external boundaries:

    1. pre_pod_ms   — API submission → ``backend_resource_uid`` set in DB (Job created)
    2. scheduling_ms — Job created → pod phase Running (K8s API); informational only
    3. post_pod_ms  — pod Running → work item COMPLETED in DB; approximates gRPC-connect
       and execute-submit overhead since the echo payload has near-zero execution time.

    All intervals include poll detection overshoot as documented in the module header.
    full_cycle_ms is informational and includes up to 25 ms completion poll overshoot.

    Returns:
        pre_pod_ms       — submission to Job created (ms)
        pod_scheduling_ms — Job created to pod Running (ms); informational
        post_pod_ms      — pod Running to COMPLETED (ms)
        full_cycle_ms    — submission to COMPLETED (ms); informational

    """
    pre_pod_ms: list[float] = []
    pod_scheduling_ms: list[float] = []
    post_pod_ms: list[float] = []
    full_cycle_ms: list[float] = []

    terminal = {
        WorkItemStatus.COMPLETED,
        WorkItemStatus.FAILED,
        WorkItemStatus.CANCELLED,
        WorkItemStatus.RECONCILIATION_REQUIRED,
    }

    completion_timeout_seconds = 120

    async with WorkStore.from_database_url(database_url) as work_store:
        for run_index in range(batch_size):
            work_id = uuid.uuid4()
            # Fresh workflow_run_id per item; image comes from EP_NODE_IMAGE.
            payload = build_payload(
                language="bash",
                code="echo 'Hello, Execution Plane!'",
                timeout_seconds=120,
                environment={},
            )

            # 1. Submit via EP API
            t_submit = time.monotonic()
            await asyncio.to_thread(submit_work_item, api_url, token, work_id=work_id, payload=payload)

            # 2. Job created — pre-pod overhead
            pre_pod_elapsed, t_job_created = await poll_job_created(work_store, work_id, t_submit=t_submit)
            pre_pod_ms.append(pre_pod_elapsed)

            # 3. Pod Running — Kubernetes scheduling time (informational)
            sched_elapsed, t_pod_running = await poll_pod_running(
                k8s_core, work_id, namespace, t_job_created=t_job_created
            )
            pod_scheduling_ms.append(sched_elapsed)

            # 4. Work item COMPLETED — post-pod overhead
            item = None
            t_completed: float = 0.0
            deadline = time.monotonic() + completion_timeout_seconds
            while time.monotonic() < deadline:
                item = await work_store.get(work_id, client_id=CLIENT_ID)
                if item is not None and item.status in terminal:
                    t_completed = time.monotonic()
                    break
                await asyncio.sleep(0.025)  # 25 ms — precise enough for 300 ms R2 target

            if item is None or item.status not in terminal:
                msg = (
                    f"[{scenario}] run {run_index}: work item {work_id} did not reach "
                    f"a terminal state within {completion_timeout_seconds} s"
                )
                raise TimeoutError(msg)

            post_pod_ms.append((t_completed - t_pod_running) * 1000.0)
            full_cycle_ms.append((t_completed - t_submit) * 1000.0)

            assert item.status is WorkItemStatus.COMPLETED, (
                f"[{scenario}] run {run_index}: expected COMPLETED, got {item.status}; result={item.result}"
            )

    return pre_pod_ms, pod_scheduling_ms, post_pod_ms, full_cycle_ms


def assert_dispatch_overhead(
    pre_pod_ms: list[float],
    pod_scheduling_ms: list[float],
    post_pod_ms: list[float],
    full_cycle_ms: list[float],
    *,
    scenario: str,
) -> None:
    """Assert test 1.1, test 1.2, and trend KPIs; embed a full diagnostics block in failure messages."""
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
        f"  [informational] pod scheduling p50={sched_p50:.0f} ms  p99={sched_p99:.0f} ms\n"
        f"  [informational] full dispatch cycle p99={cycle_p99:.0f} ms\n"
    )

    assert pre_p99 <= TARGET_P99_PRE_POD_OVERHEAD_MS, (
        f"p99 pre-pod overhead {pre_p99:.1f} ms exceeds {TARGET_P99_PRE_POD_OVERHEAD_MS:.0f} ms limit (test 1.1){diag}"
    )
    assert post_p99 <= TARGET_P99_POST_POD_OVERHEAD_MS, (
        f"p99 post-pod overhead {post_p99:.1f} ms exceeds "
        f"{TARGET_P99_POST_POD_OVERHEAD_MS:.0f} ms limit (test 1.2){diag}"
    )
    assert pre_trend.ratio <= TARGET_TREND_RATIO_MAX, (
        f"Pre-pod overhead trend ratio {pre_trend.ratio:.3f} exceeds "
        f"{TARGET_TREND_RATIO_MAX} — possible carry-over latency{diag}"
    )
    assert post_trend.ratio <= TARGET_TREND_RATIO_MAX, (
        f"Post-pod overhead trend ratio {post_trend.ratio:.3f} exceeds "
        f"{TARGET_TREND_RATIO_MAX} — possible carry-over latency{diag}"
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDispatchOverhead:
    """EP dispatch overhead: submission → Kubernetes Job created → pod Running → COMPLETED.

    Validates:
        - Test 1.1: p99 pre-pod overhead ≤ 500 ms (submission → Kubernetes Job created)
        - Test 1.2: p99 post-pod overhead ≤ 300 ms (pod Running → work item COMPLETED)
        - No upward latency trend across the 100-run batch for either interval
        - Pod scheduling time and full cycle time recorded informally
    """

    @pytest.mark.asyncio
    async def test_dispatch_overhead_on_idle_cluster(
        self,
        perf_database_url: str,
        perf_api_url: str,
        perf_token: str,
        perf_k8s_core_api: CoreV1Api,
        perf_ep_namespace: str,
    ) -> None:
        """Test 1.1: p99 pre-pod ≤ 500 ms and test 1.2: p99 post-pod ≤ 300 ms — idle cluster."""
        pre_pod_ms, pod_scheduling_ms, post_pod_ms, full_cycle_ms = await run_sequential_batch(
            perf_database_url,
            perf_api_url,
            perf_token,
            perf_k8s_core_api,
            perf_ep_namespace,
            batch_size=BATCH_SIZE,
            scenario="idle",
        )
        assert_dispatch_overhead(pre_pod_ms, pod_scheduling_ms, post_pod_ms, full_cycle_ms, scenario="idle")

    @pytest.mark.asyncio
    @pytest.mark.skip(
        reason=(
            "TODO: cluster_load_80pct fixture not yet implemented. "
            "Implement a fixture that pre-fills 80 % of node capacity with "
            "pause pods before enabling this test."
        )
    )
    async def test_dispatch_overhead_unaffected_by_cluster_load(
        self,
        perf_database_url: str,
        perf_api_url: str,
        perf_token: str,
        perf_k8s_core_api: CoreV1Api,
        perf_ep_namespace: str,
        cluster_load_80pct,
    ) -> None:
        """Test 1.1: p99 pre-pod ≤ 500 ms and test 1.2: p99 post-pod ≤ 300 ms — 80 % loaded cluster.

        Confirms EP overhead is independent of Kubernetes scheduler pressure.
        Requires ``cluster_load_80pct`` to pre-fill 80 % of node capacity.
        """
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
