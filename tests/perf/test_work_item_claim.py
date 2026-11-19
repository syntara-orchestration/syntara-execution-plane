"""Test 3.1 / R6: claim uniqueness and reserve_one latency under 1→2 workers.

DB-only: seed PENDING rows, drain with separate WorkStore pools (no API/K8s).
``reserve_one`` timing is full call wall time (not claim-query SQL).
Lock wait is a sampled ``pg_stat_activity`` fraction (often ~0 with SKIP LOCKED).

Run:
    pytest -m perf tests/perf/test_work_item_claim.py -s
    EP_PERF_REQUIRED=1 pytest -m perf tests/perf/test_work_item_claim.py -s
"""

from __future__ import annotations

import asyncio
import statistics
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlmodel import col

from execution_plane.database.engine import create_unpooled_engine
from execution_plane.database.ssl import ssl_connect_args_from_env, strip_unsupported_asyncpg_ssl_query
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.work_item.work_item_store import WorkStore

from .conftest import perf_required
from .runner import cleanup_work_items, percentile

pytestmark = [
    pytest.mark.perf,
    pytest.mark.timeout(900),
]

TARGET_MEAN_RESERVE_ONE_MS = 20.0
TARGET_CONTENTION_RATIO_MAX = 2.0
TARGET_LOCK_WAIT_FRACTION_MAX = 0.10
MIN_LOCK_WAIT_ACTIVITY_SAMPLES = 10
LOCK_WAIT_POLL_SECONDS = 0.02

BATCH_SIZE = 1000
REPLICA_COUNTS = (1, 2)
# ~1 s of empty polls while peers finish CLAIMED→COMPLETED.
EMPTY_CLAIM_STREAK_LIMIT = 1000

# client_id uses ":"; application_name uses "-" for pg_stat_activity.
CLIENT_ID_PREFIX = "perf-claim"
APP_NAME_PREFIX = "perf-claim"
SEED_PAYLOAD: dict[str, Any] = {"perf": "d3.1", "invocation": {"version": 1}}


@dataclass
class ClaimRecord:
    """One successful claim attributed to a replica."""

    work_id: uuid.UUID
    replica_id: int
    claim_owner_id: uuid.UUID
    claim_generation: int
    reserve_one_ms: float
    claimed_at_mono: float


@dataclass
class DrainPassResult:
    """Correctness, latency, and lock-wait samples for one replica-count pass."""

    replica_count: int
    seeded_ids: list[uuid.UUID]
    claims: list[ClaimRecord] = field(default_factory=list)
    reserve_one_ms: list[float] = field(default_factory=list)
    wall_clock_ms: float = 0.0
    duplicate_item_count: int = 0
    duplicate_extra_claims: int = 0
    missing_count: int = 0
    unexpected_count: int = 0
    non_terminal_count: int = 0
    activity_samples: int = 0
    lock_wait_samples: int = 0
    poll_samples: int = 0
    sampler_error: str | None = None
    cleanup_error: str | None = None

    @property
    def lock_wait_fraction(self) -> float:
        if self.activity_samples <= 0:
            return 0.0
        return self.lock_wait_samples / self.activity_samples


def _replica_application_name(run_id: uuid.UUID, replica_id: int) -> str:
    return f"{APP_NAME_PREFIX}-{run_id}-r{replica_id}"


def _create_claim_engine(database_url: str, *, application_name: str) -> AsyncEngine:
    """Pooled engine tagged for lock-wait sampling."""
    connect_args = dict(ssl_connect_args_from_env())
    server_settings = dict(connect_args.get("server_settings") or {})
    server_settings["application_name"] = application_name
    connect_args["server_settings"] = server_settings
    return create_async_engine(
        strip_unsupported_asyncpg_ssl_query(database_url),
        connect_args=connect_args,
        pool_size=2,
        max_overflow=2,
        pool_pre_ping=True,
    )


async def _sample_lock_waits(
    database_url: str,
    *,
    application_name_prefix: str,
    stop: asyncio.Event,
    result: DrainPassResult,
) -> None:
    """Poll tagged backends; record errors on *result* without raising."""
    engine = create_unpooled_engine(database_url)
    try:
        try:
            async with engine.connect() as conn:
                while not stop.is_set():
                    rows = await conn.execute(
                        text(
                            """
                            SELECT wait_event_type
                            FROM pg_stat_activity
                            WHERE datname = current_database()
                              AND pid <> pg_backend_pid()
                              AND application_name LIKE :prefix
                              AND state <> 'idle'
                            """
                        ),
                        {"prefix": f"{application_name_prefix}%"},
                    )
                    wait_types = [row[0] for row in rows]
                    result.poll_samples += 1
                    result.activity_samples += len(wait_types)
                    result.lock_wait_samples += sum(1 for wait_type in wait_types if wait_type == "Lock")
                    with suppress(TimeoutError):
                        await asyncio.wait_for(stop.wait(), timeout=LOCK_WAIT_POLL_SECONDS)
        except Exception as exc:
            result.sampler_error = f"{type(exc).__name__}: {exc}"
    finally:
        await engine.dispose()


async def _count_pending_for_client(work_store: WorkStore, client_id: str) -> int:
    async with work_store._session_context() as session:
        result = await session.execute(
            select(func.count())
            .select_from(WorkItem)
            .where(col(WorkItem.client_id) == client_id, col(WorkItem.status) == WorkItemStatus.PENDING)
        )
        return int(result.scalar_one())


async def _count_global_pending(work_store: WorkStore) -> int:
    async with work_store._session_context() as session:
        result = await session.execute(
            select(func.count()).select_from(WorkItem).where(col(WorkItem.status) == WorkItemStatus.PENDING)
        )
        return int(result.scalar_one())


async def _require_clean_pending_queue(work_store: WorkStore) -> None:
    """reserve_one is table-global — refuse leftover PENDING."""
    pending = await _count_global_pending(work_store)
    if not pending:
        return
    message = (
        f"perf DB has {pending} leftover PENDING work item(s); "
        "test 3.1 requires a clean claim queue (reserve_one is table-global)"
    )
    if perf_required():
        pytest.fail(f"EP_PERF_REQUIRED=1 and prerequisite missing: {message}")
    pytest.skip(message)


async def _replica_drain(
    database_url: str,
    *,
    replica_id: int,
    run_id: uuid.UUID,
    client_id: str,
    seeded_ids: set[uuid.UUID],
    expected_total: int,
    claims_out: list[ClaimRecord],
    reserve_one_ms_out: list[float],
    claims_lock: asyncio.Lock,
) -> None:
    """Drain until seeded claims hit *expected_total* (foreign claims excluded from latency)."""
    empty_pending_streak = 0
    none_attempts = 0
    max_none_attempts = EMPTY_CLAIM_STREAK_LIMIT * 10
    engine = _create_claim_engine(
        database_url,
        application_name=_replica_application_name(run_id, replica_id),
    )
    try:
        work_store = WorkStore.from_engine(engine)
        while True:
            async with claims_lock:
                seeded_claimed = sum(1 for claim in claims_out if claim.work_id in seeded_ids)
                if seeded_claimed >= expected_total:
                    break

            t0 = time.perf_counter()
            item = await work_store.reserve_one()
            elapsed_ms = (time.perf_counter() - t0) * 1000.0

            if item is None:
                none_attempts += 1
                pending = await _count_pending_for_client(work_store, client_id)
                if pending == 0:
                    empty_pending_streak += 1
                    if empty_pending_streak >= EMPTY_CLAIM_STREAK_LIMIT:
                        break
                else:
                    empty_pending_streak = 0
                if none_attempts >= max_none_attempts:
                    break
                await asyncio.sleep(0.001)
                continue

            empty_pending_streak = 0
            none_attempts = 0
            if item.status is not WorkItemStatus.CLAIMED:
                continue
            claim_owner_id = item.claim_owner_id
            if claim_owner_id is None:
                msg = f"replica {replica_id}: reserve_one returned item {item.id} without claim_owner_id"
                raise RuntimeError(msg)

            await work_store.set_result(
                item.id,
                {"perf": "d3.1", "replica_id": replica_id},
                WorkItemStatus.COMPLETED,
                claim_owner_id=claim_owner_id,
                claim_generation=item.claim_generation,
            )

            is_seeded = item.id in seeded_ids
            record = ClaimRecord(
                work_id=item.id,
                replica_id=replica_id,
                claim_owner_id=claim_owner_id,
                claim_generation=item.claim_generation,
                reserve_one_ms=elapsed_ms,
                claimed_at_mono=time.monotonic(),
            )

            async with claims_lock:
                claims_out.append(record)
                if is_seeded:
                    reserve_one_ms_out.append(elapsed_ms)
    finally:
        await engine.dispose()


async def run_claim_pass(
    database_url: str,
    *,
    replica_count: int,
    batch_size: int,
) -> DrainPassResult:
    """Seed *batch_size* items and drain with *replica_count* claim clients."""
    run_id = uuid.uuid4()
    client_id = f"{CLIENT_ID_PREFIX}:{run_id}"
    app_prefix = f"{APP_NAME_PREFIX}-{run_id}"
    seeded_ids: list[uuid.UUID] = []
    result = DrainPassResult(replica_count=replica_count, seeded_ids=seeded_ids)

    async with WorkStore.from_database_url(database_url) as seeder:
        try:
            for _ in range(batch_size):
                work_id = uuid.uuid4()
                await seeder.dispatch(client_id, work_id, SEED_PAYLOAD)
                seeded_ids.append(work_id)
            seeded_set = set(seeded_ids)

            claims_lock = asyncio.Lock()
            stop_sampler = asyncio.Event()
            sampler_task = asyncio.create_task(
                _sample_lock_waits(
                    database_url,
                    application_name_prefix=app_prefix,
                    stop=stop_sampler,
                    result=result,
                ),
                name=f"perf-claim-lock-sampler-{run_id}",
            )
            wall_t0 = time.perf_counter()
            try:
                await asyncio.gather(
                    *[
                        _replica_drain(
                            database_url,
                            replica_id=replica_id,
                            run_id=run_id,
                            client_id=client_id,
                            seeded_ids=seeded_set,
                            expected_total=batch_size,
                            claims_out=result.claims,
                            reserve_one_ms_out=result.reserve_one_ms,
                            claims_lock=claims_lock,
                        )
                        for replica_id in range(replica_count)
                    ]
                )
            finally:
                result.wall_clock_ms = (time.perf_counter() - wall_t0) * 1000.0
                stop_sampler.set()
                try:
                    await sampler_task
                except Exception as exc:
                    if result.sampler_error is None:
                        result.sampler_error = f"{type(exc).__name__}: {exc}"

            async with seeder._session_context() as session:
                rows = await session.execute(
                    select(WorkItem.id, WorkItem.status).where(col(WorkItem.client_id) == client_id)
                )
                db_rows = list(rows.all())

            result.claims.sort(key=lambda c: c.claimed_at_mono)
            _record_pass_correctness(result, db_rows=db_rows)
            return result
        finally:
            try:
                await cleanup_work_items(seeder, seeded_ids)
            except Exception as exc:
                result.cleanup_error = f"{type(exc).__name__}: {exc}"
                print(  # noqa: T201 — CI baseline (run with -s)
                    f"perf claim cleanup failed for {client_id}: {result.cleanup_error}",
                    flush=True,
                )


def _record_pass_correctness(
    result: DrainPassResult,
    *,
    db_rows: list[Any],
) -> None:
    """Tally duplicate / gap / non-terminal counts (no raise)."""
    seeded = set(result.seeded_ids)
    claimed_ids = [claim.work_id for claim in result.claims]
    claimed_set = set(claimed_ids)
    claim_counts: dict[uuid.UUID, int] = {}
    for work_id in claimed_ids:
        claim_counts[work_id] = claim_counts.get(work_id, 0) + 1

    duplicate_items = {work_id: count for work_id, count in claim_counts.items() if count > 1}
    result.duplicate_item_count = len(duplicate_items)
    result.duplicate_extra_claims = sum(count - 1 for count in duplicate_items.values())
    result.missing_count = len(seeded - claimed_set)
    result.unexpected_count = len(claimed_set - seeded)
    result.non_terminal_count = sum(
        1
        for _row_id, status in db_rows
        if status
        not in {
            WorkItemStatus.COMPLETED,
            WorkItemStatus.FAILED,
            WorkItemStatus.CANCELLED,
            WorkItemStatus.RECONCILIATION_REQUIRED,
        }
    )


def _latency_sketch(samples: list[float]) -> tuple[float, float, float, float]:
    """Return ``(mean, p50, p95, p99)``."""
    if not samples:
        return 0.0, 0.0, 0.0, 0.0
    mean = statistics.mean(samples)
    if len(samples) < 2:
        return mean, samples[0], samples[0], samples[0]
    return mean, percentile(samples, 50), percentile(samples, 95), percentile(samples, 99)


def _lock_wait_observability_failure(pass_result: DrainPassResult, *, label: str) -> str | None:
    if pass_result.sampler_error:
        if perf_required():
            return f"{label}: lock-wait sampler error: {pass_result.sampler_error}"
        return None
    if pass_result.poll_samples == 0:
        if perf_required():
            return f"{label}: lock-wait sampler recorded 0 polls (observer did not run)"
        return None
    if pass_result.activity_samples == 0:
        return (
            f"{label}: lock-wait sampler saw {pass_result.poll_samples} poll(s) but 0 tagged "
            f"non-idle backends (application_name tagging miss?)"
        )
    return None


def _collect_claim_pass_failures(
    pass_1: DrainPassResult,
    pass_2: DrainPassResult,
    *,
    mean_1: float,
    mean_2: float,
    contention_ratio: float,
    lock_frac_1: float,
    lock_frac_2: float,
    lock_wait_limit: float,
    lock_wait_sample_ok: bool,
) -> list[str]:
    failures: list[str] = []
    total_duplicate_items = pass_1.duplicate_item_count + pass_2.duplicate_item_count
    if total_duplicate_items:
        failures.append(
            f"duplicate claims: {pass_1.duplicate_item_count} item(s) at 1 worker, "
            f"{pass_2.duplicate_item_count} item(s) at 2 workers "
            f"(extra claim events={pass_1.duplicate_extra_claims + pass_2.duplicate_extra_claims})"
        )
    total_missing = pass_1.missing_count + pass_2.missing_count
    if total_missing:
        failures.append(f"missing claims: {pass_1.missing_count} at 1 worker, {pass_2.missing_count} at 2 workers")
    total_unexpected = pass_1.unexpected_count + pass_2.unexpected_count
    if total_unexpected:
        failures.append(
            f"unexpected claims: {pass_1.unexpected_count} at 1 worker, {pass_2.unexpected_count} at 2 workers"
        )
    total_non_terminal = pass_1.non_terminal_count + pass_2.non_terminal_count
    if total_non_terminal:
        failures.append(
            f"non-terminal items: {pass_1.non_terminal_count} at 1 worker, {pass_2.non_terminal_count} at 2 workers"
        )
    if mean_2 > TARGET_MEAN_RESERVE_ONE_MS:
        failures.append(
            f"avg claim wall time at 2 workers {mean_2:.3f} ms exceeds "
            f"{TARGET_MEAN_RESERVE_ONE_MS:.0f} ms limit "
            f"(full reserve_one wall, not claim-query SQL) (test 3.1)"
        )
    if contention_ratio > TARGET_CONTENTION_RATIO_MAX:
        failures.append(
            f"avg claim wall contention ratio {contention_ratio:.3f} exceeds "
            f"{TARGET_CONTENTION_RATIO_MAX} from 1 → 2 workers (test 3.1)"
        )
    for pass_result, label in ((pass_1, "replicas=1"), (pass_2, "replicas=2")):
        obs_failure = _lock_wait_observability_failure(pass_result, label=label)
        if obs_failure:
            failures.append(obs_failure)
    if lock_wait_sample_ok and lock_frac_2 > lock_wait_limit:
        failures.append(
            f"sampled lock-wait fraction at 2 workers {lock_frac_2:.4f} exceeds "
            f"{lock_wait_limit:.4f} (1-worker={lock_frac_1:.4f}) (test 3.1)"
        )
    return failures


def assert_claim_pass(
    pass_1: DrainPassResult,
    pass_2: DrainPassResult,
) -> None:
    """Assert R6 KPIs; always print diagnostics."""
    mean_1, p50_1, p95_1, p99_1 = _latency_sketch(pass_1.reserve_one_ms)
    mean_2, p50_2, p95_2, p99_2 = _latency_sketch(pass_2.reserve_one_ms)
    n = len(pass_2.reserve_one_ms)

    seeded_2 = set(pass_2.seeded_ids)
    claims_by_replica: dict[int, int] = {}
    for claim in pass_2.claims:
        if claim.work_id in seeded_2:
            claims_by_replica[claim.replica_id] = claims_by_replica.get(claim.replica_id, 0) + 1

    contention_ratio = (mean_2 / mean_1) if mean_1 > 0 else 1.0
    lock_frac_1 = pass_1.lock_wait_fraction
    lock_frac_2 = pass_2.lock_wait_fraction
    lock_wait_limit = max(TARGET_LOCK_WAIT_FRACTION_MAX, lock_frac_1 * TARGET_CONTENTION_RATIO_MAX)
    lock_wait_sample_ok = (
        pass_1.sampler_error is None
        and pass_2.sampler_error is None
        and pass_1.activity_samples >= MIN_LOCK_WAIT_ACTIVITY_SAMPLES
        and pass_2.activity_samples >= MIN_LOCK_WAIT_ACTIVITY_SAMPLES
    )

    diag = (
        f"\n--- Work item claim (test 3.1, n={n}) ---\n"
        f"  note: avg_claim_wall_ms = full reserve_one wall "
        f"(not claim-query SQL / lock-wait ms)\n"
        f"  note: lock_wait = sampled pg_stat_activity Lock fraction "
        f"(poll={LOCK_WAIT_POLL_SECONDS:.3f}s)\n"
        f"  replicas=1: avg_claim_wall_ms={mean_1:.3f}  p50={p50_1:.3f}  p95={p95_1:.3f}"
        f"  p99={p99_1:.3f}  wall={pass_1.wall_clock_ms:.0f} ms"
        f"  duplicates={pass_1.duplicate_item_count}"
        f" (extra_claims={pass_1.duplicate_extra_claims})"
        f"  missing={pass_1.missing_count}"
        f"  unexpected={pass_1.unexpected_count}"
        f"  non_terminal={pass_1.non_terminal_count}"
        f"  lock_wait={lock_frac_1:.4f}"
        f" (lock={pass_1.lock_wait_samples}/{pass_1.activity_samples}"
        f" polls={pass_1.poll_samples}"
        f" sampler_error={pass_1.sampler_error!r})\n"
        f"  replicas=2: avg_claim_wall_ms={mean_2:.3f}  p50={p50_2:.3f}  p95={p95_2:.3f}"
        f"  p99={p99_2:.3f}  wall={pass_2.wall_clock_ms:.0f} ms"
        f"  duplicates={pass_2.duplicate_item_count}"
        f" (extra_claims={pass_2.duplicate_extra_claims})"
        f"  missing={pass_2.missing_count}"
        f"  unexpected={pass_2.unexpected_count}"
        f"  non_terminal={pass_2.non_terminal_count}"
        f"  lock_wait={lock_frac_2:.4f}"
        f" (lock={pass_2.lock_wait_samples}/{pass_2.activity_samples}"
        f" polls={pass_2.poll_samples}"
        f" sampler_error={pass_2.sampler_error!r})\n"
        f"  claim distribution (replicas=2, seeded only): {claims_by_replica}\n"
        f"  contention ratio avg_claim_wall 2/1={contention_ratio:.3f}\n"
        f"  lock_wait limit at 2 workers={lock_wait_limit:.4f}"
        f" (sample_ok={lock_wait_sample_ok})\n"
    )
    print(diag, flush=True)  # noqa: T201 — CI baseline (run with -s)

    failures = _collect_claim_pass_failures(
        pass_1,
        pass_2,
        mean_1=mean_1,
        mean_2=mean_2,
        contention_ratio=contention_ratio,
        lock_frac_1=lock_frac_1,
        lock_frac_2=lock_frac_2,
        lock_wait_limit=lock_wait_limit,
        lock_wait_sample_ok=lock_wait_sample_ok,
    )
    if failures:
        raise AssertionError("\n".join(failures) + diag)


class TestWorkItemClaimCorrectness:
    """Test 3.1: unique claims and ≤ 20 ms mean reserve_one under 2 workers."""

    @pytest.mark.asyncio
    async def test_claim_correctness_under_concurrent_workers(
        self,
        perf_database_url: str,
    ) -> None:
        async with WorkStore.from_database_url(perf_database_url) as probe:
            await probe.check_ready()
            await _require_clean_pending_queue(probe)

        passes: dict[int, DrainPassResult] = {}
        for replica_count in REPLICA_COUNTS:
            passes[replica_count] = await run_claim_pass(
                perf_database_url,
                replica_count=replica_count,
                batch_size=BATCH_SIZE,
            )

        assert_claim_pass(passes[1], passes[2])
