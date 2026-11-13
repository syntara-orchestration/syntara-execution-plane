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
  Each batch tags ``workflow_run_id`` as ``perf-dispatch-overhead:<run_id>:<index>``
  and deletes those work-item rows from the shared DB in a ``finally`` block,
  after a 40 s settle so the last item's Job teardown can finish
  (runs are sequential, so only that pod can still be in grace).

Run with:
    pytest -m perf -s                  # -s keeps p50/p95/p99 diagnostics on stdout
    EP_PERF_REQUIRED=1 pytest -m perf -s
"""

from __future__ import annotations

import os
import statistics
from typing import TYPE_CHECKING

import pytest

from .runner import percentile, run_sequential_batch, trend_ratio

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

WORKFLOW_PREFIX = "perf-dispatch-overhead"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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
        batch = await run_sequential_batch(
            perf_database_url,
            perf_api_url,
            perf_token,
            perf_k8s_core_api,
            perf_ep_namespace,
            batch_size=BATCH_SIZE,
            scenario="default",
            workflow_prefix=WORKFLOW_PREFIX,
            measure_pre_pod=True,
            measure_scheduling=True,
            measure_post_pod=True,
            measure_pod_lifecycle=False,
        )
        assert_dispatch_overhead(
            batch.pre_pod_ms,
            batch.pod_scheduling_ms,
            batch.post_pod_ms,
            batch.full_cycle_ms,
            scenario="default",
        )

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
        batch = await run_sequential_batch(
            perf_database_url,
            perf_api_url,
            perf_token,
            perf_k8s_core_api,
            perf_ep_namespace,
            batch_size=BATCH_SIZE,
            scenario="loaded",
            workflow_prefix=WORKFLOW_PREFIX,
            measure_pre_pod=True,
            measure_scheduling=True,
            measure_post_pod=True,
            measure_pod_lifecycle=False,
        )
        pre_pod_ms = batch.pre_pod_ms
        pod_scheduling_ms = batch.pod_scheduling_ms
        post_pod_ms = batch.post_pod_ms
        full_cycle_ms = batch.full_cycle_ms
        assert_dispatch_overhead(pre_pod_ms, pod_scheduling_ms, post_pod_ms, full_cycle_ms, scenario="loaded")
