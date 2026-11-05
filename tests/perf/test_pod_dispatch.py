"""PERF-B-1 / PERF-B-2: sequential and concurrent pod dispatch.

PERF-B-1 (R3): one-by-one pod create → COMPLETED stays flat (half-means ±10 %).
  pod_lifecycle_ms = completed_at - creationTimestamp (excludes Job teardown).

PERF-B-2 (R4/R5): submit the whole batch together; wall(N) ≤ wall(1) + 20 %;
  throughput scales within 20 % of linear from 20 → 50.
  Requires EP_MAX_CONCURRENT_DISPATCHES ≥ batch size.

Run: pytest -m perf -s tests/perf/test_pod_dispatch.py
"""

from __future__ import annotations

import asyncio
import functools
import statistics
from typing import TYPE_CHECKING, NamedTuple

import pytest
from kubernetes.stream import stream

from .conftest import missing_prerequisite
from .runner import (
    ParallelBatchResult,
    percentile,
    run_parallel_batch,
    run_sequential_batch,
    trend_ratio,
)

if TYPE_CHECKING:
    from kubernetes.client import CoreV1Api

pytestmark = pytest.mark.perf

TARGET_HALF_MEAN_DEVIATION_MAX = 0.10
TARGET_TREND_RATIO_MAX = 1.10
# Upper bound only — faster than baseline is OK.
TARGET_BATCH_TO_SINGLE_DEVIATION_MAX = 0.20
# Median of warm runs after one discarded cold start.
BASELINE_SAMPLES = 3
TARGET_THROUGHPUT_SCALE_TOLERANCE = 0.20
TARGET_FD_VARIANCE_MAX = 0.10
TARGET_RSS_GROWTH_MAX = 0.20
MIN_SAMPLES_FOR_FLATNESS = 10

BATCH_SIZE_SMALL = 20
BATCH_SIZE_LARGE = 50
RESOURCE_WARMUP_BATCH_SIZE = 1

WORKFLOW_PREFIX_SEQUENTIAL = "perf-sequential-dispatch"
WORKFLOW_PREFIX_PARALLEL = "perf-parallel-dispatch"
WORKER_LABEL_SELECTOR = "app.kubernetes.io/name=execution-plane,app.kubernetes.io/component=worker"


class WorkerResources(NamedTuple):
    """Worker process RSS (kB) and open FD count."""

    rss_kb: int
    open_fds: int
    pod_name: str


def pod_exec(k8s_core: CoreV1Api, namespace: str, pod_name: str, command: list[str]) -> str:
    """Exec *command* in the worker container.

    Args:
        k8s_core: Kubernetes CoreV1Api client.
        namespace: Namespace of the worker pod.
        pod_name: Worker pod name.
        command: argv to run in the container.

    Returns:
        Combined stdout/stderr from the exec stream.

    """
    try:
        return stream(
            k8s_core.connect_get_namespaced_pod_exec,
            pod_name,
            namespace,
            command=command,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            _request_timeout=30,
        )
    except Exception as exc:
        msg = (
            f"exec into worker pod {pod_name!r} failed for {command!r}; "
            "image must provide cat/sh/ls/wc and allow pod exec"
        )
        raise RuntimeError(msg) from exc


def _parse_vmrss_kb(status_out: str, pod_name: str) -> int:
    """Parse VmRSS (kB) from /proc/1/status.

    Args:
        status_out: Contents of ``/proc/1/status``.
        pod_name: Pod name for error messages.

    Returns:
        Resident set size in kilobytes.

    """
    rss_kb: int | None = None
    for line in status_out.splitlines():
        if line.startswith("VmRSS:"):
            parts = line.split()
            if len(parts) < 2:
                break
            rss_kb = int(parts[1])
            break
    if rss_kb is None or rss_kb <= 0:
        msg = (
            f"VmRSS not found in /proc/1/status on worker pod {pod_name!r} (cat/proc readable?). status={status_out!r}"
        )
        raise RuntimeError(msg)
    return rss_kb


def _parse_open_fds(fd_out: str, pod_name: str) -> int:
    """Parse open-FD count from ``ls /proc/1/fd | wc -l``.

    Args:
        fd_out: Command output containing the FD count.
        pod_name: Pod name for error messages.

    Returns:
        Number of open file descriptors.

    """
    try:
        open_fds = int(fd_out.strip().splitlines()[-1])
    except (ValueError, IndexError) as exc:
        msg = f"open FD count not parseable on worker pod {pod_name!r} (need sh/ls/wc); output={fd_out!r}"
        raise RuntimeError(msg) from exc
    if open_fds <= 0:
        msg = f"open FD count is {open_fds} on worker pod {pod_name!r}; expected > 0"
        raise RuntimeError(msg)
    return open_fds


def sample_worker_resources(
    k8s_core: CoreV1Api,
    namespace: str,
    *,
    pod_name: str | None = None,
) -> WorkerResources | None:
    """Sample RSS/FDs from a Running EP worker pod.

    Args:
        k8s_core: Kubernetes CoreV1Api client.
        namespace: Namespace to search for the worker.
        pod_name: Exact pod to sample; when omitted, pick the first Running worker.

    Returns:
        RSS/FD sample, or ``None`` only when *pod_name* is omitted and no Running
        worker matches the label selector. When *pod_name* is set, raises if that
        pod is not Running.

    """
    pods = k8s_core.list_namespaced_pod(namespace, label_selector=WORKER_LABEL_SELECTOR, _request_timeout=15)
    running = sorted(
        (pod for pod in pods.items if pod.status and pod.status.phase == "Running" and pod.metadata),
        key=lambda pod: pod.metadata.name or "",
    )
    if pod_name is not None:
        # Exact pod was requested — never return None (callers pass it to RSS/FD asserts).
        if not any(pod.metadata.name == pod_name for pod in running):
            msg = (
                f"EP worker pod {pod_name!r} is no longer Running (restarted or rescheduled); "
                "RSS/FD comparison requires the same pod"
            )
            raise RuntimeError(msg)
    else:
        if not running:
            return None
        pod_name = running[0].metadata.name
    if not pod_name:
        msg = "Running EP worker pod has no metadata.name"
        raise RuntimeError(msg)

    rss_kb = _parse_vmrss_kb(pod_exec(k8s_core, namespace, pod_name, ["cat", "/proc/1/status"]), pod_name)
    open_fds = _parse_open_fds(pod_exec(k8s_core, namespace, pod_name, ["sh", "-c", "ls /proc/1/fd | wc -l"]), pod_name)

    return WorkerResources(rss_kb=rss_kb, open_fds=open_fds, pod_name=pod_name)


def mean_deviations(data: list[float], mean: float) -> list[tuple[int, float]]:
    """Return ``(1-based index, |x - mean| / mean)`` for diagnostics.

    Args:
        data: Sample values.
        mean: Series mean used as the deviation baseline.

    Returns:
        Per-sample relative deviations; empty when ``mean <= 0``.

    """
    if mean <= 0:
        return []
    return [(index + 1, abs(value - mean) / mean) for index, value in enumerate(data)]


def series_sketch(values: list[float]) -> str:
    """Compact first/quartiles/last sketch of a series.

    Args:
        values: Sample series in run order.

    Returns:
        Human-readable sketch string for diagnostics.

    """
    n = len(values)
    if n == 0:
        return "(empty)"
    if n <= 4:
        return "  ".join(f"run{index + 1}={values[index]:.1f}" for index in range(n))
    indexes = sorted({0, n // 4, n // 2, (3 * n) // 4, n - 1})
    return "  ".join(f"run{index + 1}={values[index]:.1f}" for index in indexes)


def assert_worker_resources_stable(
    before: WorkerResources,
    after: WorkerResources,
    *,
    scenario: str,
    label: str,
) -> None:
    """Fail if RSS growth or FD variance exceeds limits on the same worker pod.

    Args:
        before: Resource sample taken before the series.
        after: Resource sample taken after the series (same pod).
        scenario: Label for diagnostics.
        label: Short description of the series (e.g. PERF-B-1).

    """
    fd_base = max(before.open_fds, 1)
    fd_variance = abs(after.open_fds - before.open_fds) / fd_base
    rss_base = max(before.rss_kb, 1)
    rss_growth = (after.rss_kb - before.rss_kb) / rss_base

    diag = (
        f"\n--- Worker resources ({scenario}) ---\n"
        f"  pod:  before={before.pod_name}  after={after.pod_name}\n"
        f"  RSS:  before={before.rss_kb} kB  after={after.rss_kb} kB"
        f"  growth={rss_growth:.3f}\n"
        f"  FDs:  before={before.open_fds}  after={after.open_fds}"
        f"  variance={fd_variance:.3f}\n"
    )
    print(diag, flush=True)  # noqa: T201 — CI baseline (run with -s)

    failures: list[str] = []
    if before.pod_name != after.pod_name:
        failures.append(
            f"EP worker pod changed during series: before={before.pod_name!r} after={after.pod_name!r} "
            "(RSS/FD comparison requires the same pod)"
        )
    if fd_variance > TARGET_FD_VARIANCE_MAX:
        failures.append(
            f"Open FD variance {fd_variance:.3f} exceeds ±{TARGET_FD_VARIANCE_MAX:.0%} after {label} ({scenario})"
        )
    if rss_growth > TARGET_RSS_GROWTH_MAX:
        failures.append(
            f"Worker RSS grew by {rss_growth:.3f} — exceeds {TARGET_RSS_GROWTH_MAX:.0%} growth limit ({scenario})"
        )
    if failures:
        raise AssertionError("\n".join(failures) + diag)


def assert_pod_lifecycle_flat(pod_lifecycle_ms: list[float], *, scenario: str) -> None:
    """PERF-B-1: half-series means within ±10 % of series mean; no upward trend.

    Per-sample ±10 % is not used — mixed clocks / second-truncation are too noisy.

    Args:
        pod_lifecycle_ms: Per-run create→COMPLETED durations in milliseconds.
        scenario: Label for diagnostics.

    """
    n = len(pod_lifecycle_ms)
    if n < MIN_SAMPLES_FOR_FLATNESS:
        msg = (
            f"[{scenario}] need at least {MIN_SAMPLES_FOR_FLATNESS} samples for half-series flatness asserts, got n={n}"
        )
        raise AssertionError(msg)

    mean = statistics.mean(pod_lifecycle_ms)
    stdev = statistics.stdev(pod_lifecycle_ms) if n > 1 else 0.0
    p50 = percentile(pod_lifecycle_ms, 50)
    p95 = percentile(pod_lifecycle_ms, 95)
    p99 = percentile(pod_lifecycle_ms, 99)
    trend = trend_ratio(pod_lifecycle_ms)
    deviations = mean_deviations(pod_lifecycle_ms, mean)
    max_sample_dev = max((delta for _, delta in deviations), default=0.0)
    worst_run = next((run for run, delta in deviations if delta == max_sample_dev), n)
    first = pod_lifecycle_ms[0]
    last = pod_lifecycle_ms[-1]
    first_last_ratio = abs(last - first) / first if first > 0 else 0.0
    half_dev_first = abs(trend.first_mean - mean) / mean if mean > 0 else 0.0
    half_dev_second = abs(trend.second_mean - mean) / mean if mean > 0 else 0.0

    series = series_sketch(pod_lifecycle_ms)
    band_lo = mean * (1.0 - TARGET_HALF_MEAN_DEVIATION_MAX)
    band_hi = mean * (1.0 + TARGET_HALF_MEAN_DEVIATION_MAX)

    total = sum(pod_lifecycle_ms)
    diag = (
        f"\n--- Sequential pod lifecycle ({scenario}, n={n}) ---\n"
        f"  clocks: start=pod creationTimestamp (K8s, second-truncated);"
        f" end=work_item.completed_at (DB) — not a single-clock span\n"
        f"  per-run samples (ms): {', '.join(f'{v:.0f}' for v in pod_lifecycle_ms)}\n"
        f"  n={n}  sum={total:.1f} ms  mean={mean:.1f} ms  stdev={stdev:.1f} ms\n"
        f"  half-mean ±{TARGET_HALF_MEAN_DEVIATION_MAX:.0%} of series mean "
        f"band=[{band_lo:.1f}, {band_hi:.1f}] ms\n"
        f"  half-means: first={trend.first_mean:.1f} ms (dev={half_dev_first:.3f})"
        f"  second={trend.second_mean:.1f} ms (dev={half_dev_second:.3f})\n"
        f"  p50={p50:.1f} ms  p95={p95:.1f} ms  p99={p99:.1f} ms\n"
        f"  [informational] max per-sample |x-mean|/mean={max_sample_dev:.3f}"
        f" at run {worst_run} (value={pod_lifecycle_ms[worst_run - 1]:.1f} ms)\n"
        f"  [informational] first={first:.1f} ms  last={last:.1f} ms"
        f"  |last-first|/first={first_last_ratio:.3f}\n"
        f"  trend: first-half={trend.first_mean:.1f} ms"
        f"  second-half={trend.second_mean:.1f} ms  ratio={trend.ratio:.3f}\n"
        f"  series: {series}\n"
    )
    print(diag, flush=True)  # noqa: T201 — CI baseline (run with -s)

    failures: list[str] = []
    if half_dev_first > TARGET_HALF_MEAN_DEVIATION_MAX:
        failures.append(
            f"Pod lifecycle first-half mean {trend.first_mean:.1f} ms deviates "
            f"{half_dev_first:.3f} from series mean {mean:.1f} ms "
            f"(limit ±{TARGET_HALF_MEAN_DEVIATION_MAX:.0%}) (PERF-B-1)"
        )
    if half_dev_second > TARGET_HALF_MEAN_DEVIATION_MAX:
        failures.append(
            f"Pod lifecycle second-half mean {trend.second_mean:.1f} ms deviates "
            f"{half_dev_second:.3f} from series mean {mean:.1f} ms "
            f"(limit ±{TARGET_HALF_MEAN_DEVIATION_MAX:.0%}) (PERF-B-1)"
        )
    if trend.ratio > TARGET_TREND_RATIO_MAX:
        failures.append(
            f"Pod lifecycle trend ratio {trend.ratio:.3f} exceeds "
            f"{TARGET_TREND_RATIO_MAX} — possible carry-over latency (PERF-B-1)"
        )
    if failures:
        raise AssertionError("\n".join(failures) + diag)


def assert_batch_fully_parallel(
    batch: ParallelBatchResult,
    *,
    batch_size: int,
    single_wall_ms: float,
    scenario: str,
) -> None:
    """PERF-B-2: batch wall at most 20 % above single-pod wall (faster is OK).

    Args:
        batch: Concurrent batch result (wall-clock and per-item samples).
        batch_size: Number of items submitted together.
        single_wall_ms: Single-pod baseline wall-clock in milliseconds.
        scenario: Label for diagnostics.

    """
    if single_wall_ms <= 0:
        msg = f"[{scenario}] single-pod wall is {single_wall_ms}; cannot evaluate parallelism"
        raise AssertionError(msg)

    deviation = (batch.wall_clock_ms - single_wall_ms) / single_wall_ms
    serial_bound_ms = batch_size * single_wall_ms
    speedup = serial_bound_ms / batch.wall_clock_ms if batch.wall_clock_ms > 0 else float("inf")
    throughput = (batch_size / batch.wall_clock_ms) * 1000.0 if batch.wall_clock_ms > 0 else 0.0

    life = batch.pod_lifecycle_ms
    full = batch.full_cycle_ms
    life_mean = statistics.mean(life) if life else 0.0
    full_mean = statistics.mean(full) if full else 0.0
    life_p50 = percentile(life, 50) if len(life) >= 2 else (life[0] if life else 0.0)
    life_p95 = percentile(life, 95) if len(life) >= 2 else life_p50
    full_p50 = percentile(full, 50) if len(full) >= 2 else (full[0] if full else 0.0)
    full_p95 = percentile(full, 95) if len(full) >= 2 else full_p50
    band_lo = single_wall_ms * (1.0 - TARGET_BATCH_TO_SINGLE_DEVIATION_MAX)
    band_hi = single_wall_ms * (1.0 + TARGET_BATCH_TO_SINGLE_DEVIATION_MAX)

    diag = (
        f"\n--- Concurrent batch ({scenario}, n={batch_size}) ---\n"
        f"  single-pod wall={single_wall_ms:.1f} ms\n"
        f"  batch wall={batch.wall_clock_ms:.1f} ms"
        f"  (batch-single)/single={deviation:+.3f}"
        f"  allowed <= +{TARGET_BATCH_TO_SINGLE_DEVIATION_MAX:.0%}"
        f"  band=[{band_lo:.1f}, {band_hi:.1f}] ms (lower edge informational)\n"
        f"  serial-bound N*single={serial_bound_ms:.1f} ms  speedup={speedup:.2f}x\n"
        f"  throughput={throughput:.3f} items/s\n"
        f"  per-item full_cycle: mean={full_mean:.1f} ms  p50={full_p50:.1f} ms"
        f"  p95={full_p95:.1f} ms  n={len(full)}\n"
        f"  per-item pod_lifecycle: mean={life_mean:.1f} ms  p50={life_p50:.1f} ms"
        f"  p95={life_p95:.1f} ms  n={len(life)}\n"
    )
    print(diag, flush=True)  # noqa: T201 — CI baseline (run with -s)

    if deviation > TARGET_BATCH_TO_SINGLE_DEVIATION_MAX:
        raise AssertionError(
            f"Batch wall-clock {batch.wall_clock_ms:.1f} ms is {deviation:+.3f} vs "
            f"single-pod wall {single_wall_ms:.1f} ms "
            f"(limit +{TARGET_BATCH_TO_SINGLE_DEVIATION_MAX:.0%}) — "
            f"expected full parallelism (PERF-B-2). "
            f"Check EP_MAX_CONCURRENT_DISPATCHES >= {batch_size}." + diag
        )


def assert_throughput_scales(
    throughput_small: float,
    throughput_large: float,
    *,
    small_n: int,
    large_n: int,
) -> None:
    """PERF-B-2 R5: throughput at least 80 % of linear (upper side informational).

    Args:
        throughput_small: Items/s at the smaller concurrency.
        throughput_large: Items/s at the larger concurrency.
        small_n: Smaller batch size (e.g. 20).
        large_n: Larger batch size (e.g. 50).

    """
    if throughput_small <= 0:
        msg = f"n{small_n} throughput is {throughput_small}; cannot evaluate scaling"
        raise AssertionError(msg)
    if small_n <= 0:
        msg = f"small_n must be > 0, got {small_n}"
        raise AssertionError(msg)

    ideal_ratio = large_n / small_n
    ratio = throughput_large / throughput_small
    lo = ideal_ratio * (1.0 - TARGET_THROUGHPUT_SCALE_TOLERANCE)
    hi = ideal_ratio * (1.0 + TARGET_THROUGHPUT_SCALE_TOLERANCE)
    diag = (
        f"\n--- Concurrent throughput scaling (n{small_n} → n{large_n}) ---\n"
        f"  throughput n{small_n}={throughput_small:.3f} items/s\n"
        f"  throughput n{large_n}={throughput_large:.3f} items/s\n"
        f"  ratio={ratio:.3f}  ideal={ideal_ratio:.3f}"
        f"  required >= {lo:.2f}  (upper edge {hi:.2f} informational)\n"
    )
    print(diag, flush=True)  # noqa: T201 — CI baseline (run with -s)

    if ratio < lo:
        raise AssertionError(
            f"Throughput ratio n{large_n}/n{small_n}={ratio:.3f} is "
            f"below the {lo:.3f} floor "
            f"(linear {ideal_ratio:.3f} - {TARGET_THROUGHPUT_SCALE_TOLERANCE:.0%}) (PERF-B-2 R5)" + diag
        )


async def _measure_single_pod_wall_ms(
    database_url: str,
    api_url: str,
    token: str,
    k8s_core: CoreV1Api,
    namespace: str,
    scenario: str,
) -> float:
    """Single-pod baseline: discard 1 cold run, median of BASELINE_SAMPLES warm runs.

    Args:
        database_url: Perf PostgreSQL URL.
        api_url: Deployed EP API base URL.
        token: Service JWT for submissions.
        k8s_core: Kubernetes CoreV1Api client.
        namespace: Namespace where worker pods run.
        scenario: Label prefix for baseline runs.

    Returns:
        Median warm single-pod wall-clock in milliseconds.

    """
    walls: list[float] = []
    for index in range(1 + BASELINE_SAMPLES):
        baseline = await run_parallel_batch(
            database_url,
            api_url,
            token,
            k8s_core,
            namespace,
            batch_size=RESOURCE_WARMUP_BATCH_SIZE,
            scenario=f"{scenario}-baseline{index}",
            workflow_prefix=f"{WORKFLOW_PREFIX_PARALLEL}-baseline",
            measure_pod_lifecycle=True,
        )
        if index > 0:
            walls.append(baseline.wall_clock_ms)
    return statistics.median(walls)


class TestSequentialDispatchVariance:
    """PERF-B-1: sequential pod lifecycle flat at n20 / n50; RSS/FD stable on n50."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "batch_size", [BATCH_SIZE_SMALL, BATCH_SIZE_LARGE], ids=[f"n{BATCH_SIZE_SMALL}", f"n{BATCH_SIZE_LARGE}"]
    )
    async def test_sequential_pod_lifecycle_stays_flat(
        self,
        batch_size: int,
        perf_database_url: str,
        perf_api_url: str,
        perf_token: str,
        perf_k8s_core_api: CoreV1Api,
        perf_ep_namespace: str,
    ) -> None:
        """PERF-B-1: pod create→COMPLETED stays flat across one-by-one runs."""
        scenario = f"sequential-n{batch_size}"
        sample_resources = batch_size == BATCH_SIZE_LARGE
        resources_before: WorkerResources | None = None

        if sample_resources:
            await run_sequential_batch(
                perf_database_url,
                perf_api_url,
                perf_token,
                perf_k8s_core_api,
                perf_ep_namespace,
                batch_size=RESOURCE_WARMUP_BATCH_SIZE,
                scenario=f"{scenario}-warmup",
                workflow_prefix=f"{WORKFLOW_PREFIX_SEQUENTIAL}-warmup",
                measure_pre_pod=False,
                measure_scheduling=False,
                measure_post_pod=False,
                measure_pod_lifecycle=True,
            )
            resources_before = await asyncio.to_thread(sample_worker_resources, perf_k8s_core_api, perf_ep_namespace)
            if resources_before is None:
                missing_prerequisite(
                    f"EP worker pod not found for RSS/FD baseline (label_selector={WORKER_LABEL_SELECTOR!r})"
                )
            assert resources_before is not None

        sequential = await run_sequential_batch(
            perf_database_url,
            perf_api_url,
            perf_token,
            perf_k8s_core_api,
            perf_ep_namespace,
            batch_size=batch_size,
            scenario=scenario,
            workflow_prefix=WORKFLOW_PREFIX_SEQUENTIAL,
            measure_pre_pod=False,
            measure_scheduling=False,
            measure_post_pod=False,
            measure_pod_lifecycle=True,
        )
        assert len(sequential.pod_lifecycle_ms) == batch_size
        assert_pod_lifecycle_flat(sequential.pod_lifecycle_ms, scenario=scenario)

        if not sample_resources:
            return

        assert resources_before is not None
        resources_after = await asyncio.to_thread(
            functools.partial(
                sample_worker_resources,
                perf_k8s_core_api,
                perf_ep_namespace,
                pod_name=resources_before.pod_name,
            )
        )
        assert_worker_resources_stable(
            resources_before,
            resources_after,
            scenario=scenario,
            label="sequential series (PERF-B-1)",
        )


def _throughput(batch_size: int, wall_clock_ms: float) -> float:
    """Items/s from measured batch wall (not clamped to baseline).

    R5 compares n20→n50 scaling independently of the single-pod wall assert.
    Clamping to baseline made R5 nearly unreachable once both batches passed
    wall ≤ +20 % of single.
    """
    if wall_clock_ms <= 0:
        return 0.0
    return (batch_size / wall_clock_ms) * 1000.0


class TestParallelDispatchThroughput:
    """PERF-B-2: baseline → n20 → n50; wall ≤ +20 % of single; R5 + RSS/FD on n50."""

    @pytest.mark.asyncio
    async def test_concurrent_batches_match_single_pod_wall(
        self,
        perf_database_url: str,
        perf_api_url: str,
        perf_token: str,
        perf_k8s_core_api: CoreV1Api,
        perf_ep_namespace: str,
    ) -> None:
        """PERF-B-2: n20/n50 wall within +20 % of single-pod; throughput scales."""
        single_wall_ms = await _measure_single_pod_wall_ms(
            perf_database_url,
            perf_api_url,
            perf_token,
            perf_k8s_core_api,
            perf_ep_namespace,
            "parallel-baseline",
        )
        assert single_wall_ms > 0

        throughput_by_n: dict[int, float] = {}
        resources_before: WorkerResources | None = None
        for batch_size in (BATCH_SIZE_SMALL, BATCH_SIZE_LARGE):
            scenario = f"parallel-n{batch_size}"
            if batch_size == BATCH_SIZE_LARGE:
                resources_before = await asyncio.to_thread(
                    sample_worker_resources, perf_k8s_core_api, perf_ep_namespace
                )
                if resources_before is None:
                    missing_prerequisite(
                        f"EP worker pod not found for RSS/FD baseline (label_selector={WORKER_LABEL_SELECTOR!r})"
                    )
                assert resources_before is not None

            batch = await run_parallel_batch(
                perf_database_url,
                perf_api_url,
                perf_token,
                perf_k8s_core_api,
                perf_ep_namespace,
                batch_size=batch_size,
                scenario=scenario,
                workflow_prefix=WORKFLOW_PREFIX_PARALLEL,
                measure_pod_lifecycle=True,
            )
            assert len(batch.full_cycle_ms) == batch_size
            assert len(batch.pod_lifecycle_ms) == batch_size

            throughput_by_n[batch_size] = _throughput(batch_size, batch.wall_clock_ms)
            assert_batch_fully_parallel(
                batch,
                batch_size=batch_size,
                single_wall_ms=single_wall_ms,
                scenario=scenario,
            )

        assert_throughput_scales(
            throughput_by_n[BATCH_SIZE_SMALL],
            throughput_by_n[BATCH_SIZE_LARGE],
            small_n=BATCH_SIZE_SMALL,
            large_n=BATCH_SIZE_LARGE,
        )

        assert resources_before is not None
        resources_after = await asyncio.to_thread(
            functools.partial(
                sample_worker_resources,
                perf_k8s_core_api,
                perf_ep_namespace,
                pod_name=resources_before.pod_name,
            )
        )
        assert_worker_resources_stable(
            resources_before,
            resources_after,
            scenario=f"parallel-n{BATCH_SIZE_LARGE}",
            label="concurrent batch (PERF-B-2)",
        )
