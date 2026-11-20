"""Test 3.2: completion delivery after a callback outage (R7 / R8).

Seed pending completion notifications, fail a mock AO callback for
``EP_PERF_OUTAGE_SECONDS``, then recover and drain. Asserts no loss, jittered
exponential backoff, no busy-poll, and ≥50 events/s due-backlog drain after
force-due (not natural backoff wait). Mock stays failing until the notifier is
stopped so sleep cannot race a 202.

Run:
    pytest -m perf tests/perf/test_callback_outage_delivery.py -s
    EP_PERF_REQUIRED=1 pytest -m perf tests/perf/test_callback_outage_delivery.py -s

Overrides: EP_PERF_OUTBOX_BATCH_SIZE (500), EP_PERF_OUTAGE_SECONDS (120).
"""

from __future__ import annotations

import asyncio
import json
import os
import statistics
import threading
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from itertools import pairwise
from typing import Any

import pytest
from sqlalchemy import func, select, update
from sqlmodel import col

from execution_plane.completion_notifier.notifier import (
    DELIVERY_POLL_SECONDS,
    MAX_RETRY_SECONDS,
    CompletionNotifier,
)
from execution_plane.config.settings import Settings
from execution_plane.models.work_item import CompletionNotificationState, WorkItem, WorkItemStatus
from execution_plane.work_item.work_item_store import WorkStore

from .runner import cleanup_work_items

pytestmark = [
    pytest.mark.perf,
    pytest.mark.timeout(900),
]

BATCH_SIZE = int(os.environ.get("EP_PERF_OUTBOX_BATCH_SIZE", "500"))
OUTAGE_SECONDS = float(os.environ.get("EP_PERF_OUTAGE_SECONDS", "120"))
TARGET_DELIVERY_THROUGHPUT_PER_SEC = 50.0
MIN_EMPTY_POLL_GAP_SECONDS = DELIVERY_POLL_SECONDS * 0.75
MIN_EMPTY_POLL_GAPS = 3
# Matches CompletionNotifier jitter (observed delay may exceed MAX_RETRY_SECONDS).
RETRY_JITTER_MIN = 0.8
RETRY_JITTER_MAX = 1.2
MAX_OBSERVED_RETRY_SECONDS = int(MAX_RETRY_SECONDS * RETRY_JITTER_MAX)
# Hold mock failing past the measured window until notifier stop.
OUTAGE_HOLD_SECONDS = 3600.0
CLIENT_ID_PREFIX = "perf-outbox"
SEED_PAYLOAD: dict[str, Any] = {"perf": "3.2"}
SEED_RESULT: dict[str, Any] = {"perf": "3.2", "output": "ok"}


@dataclass
class CallbackAttempt:
    """One HTTP POST observed by the mock AO callback."""

    work_item_id: uuid.UUID | None
    message_id: uuid.UUID | None
    mono: float
    status_code: int


@dataclass
class MockCallbackState:
    """Shared state for the threaded mock callback server."""

    outage_until_mono: float
    attempts: list[CallbackAttempt] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)
    success_count: int = 0
    failure_count: int = 0


def _start_mock_callback(outage_seconds: float) -> tuple[ThreadingHTTPServer, MockCallbackState, str]:
    """Bind a localhost mock callback; return server, state, and callback URL."""
    state = MockCallbackState(outage_until_mono=time.monotonic() + outage_seconds)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: object) -> None:
            return

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                body = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                body = {}

            message_id: uuid.UUID | None = None
            work_item_id: uuid.UUID | None = None
            try:
                if body.get("message_id"):
                    message_id = uuid.UUID(str(body["message_id"]))
                parent = (body.get("command") or {}).get("parent_command_id")
                if parent:
                    work_item_id = uuid.UUID(str(parent))
            except (ValueError, TypeError, AttributeError):
                pass

            if time.monotonic() < state.outage_until_mono:
                status = 503
                payload: dict[str, Any] = {"error": "callback outage"}
                with state.lock:
                    state.failure_count += 1
            else:
                status = 202
                payload = {
                    "received": True,
                    "message_id": str(message_id) if message_id is not None else str(uuid.uuid4()),
                }
                with state.lock:
                    state.success_count += 1

            with state.lock:
                state.attempts.append(
                    CallbackAttempt(
                        work_item_id=work_item_id,
                        message_id=message_id,
                        mono=time.monotonic(),
                        status_code=status,
                    )
                )

            encoded = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, name="perf-3.2-mock-callback", daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    return server, state, f"http://{host}:{port}/v1/commands"


@dataclass
class RetryRecord:
    """One failed delivery attempt persisted by the notifier."""

    work_item_id: uuid.UUID
    retry_after_seconds: int
    mono: float
    attempt_number: int


@dataclass
class PollSample:
    """One claim_work_items_that_need_completion_notification call."""

    mono: float
    claimed: int


class InstrumentingWorkStore:
    """WorkStore wrapper that records claim polls and retry delays."""

    def __init__(self, store: WorkStore) -> None:
        """Wrap *store* and retain retry / poll samples on this instance."""
        self._store = store
        self.retries: list[RetryRecord] = []
        self.polls: list[PollSample] = []
        # DB completion_notification_attempts at the time each item was last claimed.
        # The notifier computes retry base as 2^min(db_attempts+1, 8) using the DB
        # value at claim time, so we must track the same value — not a local counter
        # that diverges when worker-pod notifiers also increment the DB column.
        self._db_attempts_at_claim: dict[uuid.UUID, int] = {}

    def __getattr__(self, name: str) -> object:
        """Delegate unknown attributes to the underlying WorkStore."""
        return getattr(self._store, name)

    async def claim_work_items_that_need_completion_notification(self, *, limit: int = BATCH_SIZE) -> list[WorkItem]:
        items = await self._store.claim_work_items_that_need_completion_notification(limit=limit)
        self.polls.append(PollSample(mono=time.monotonic(), claimed=len(items)))
        for item in items:
            self._db_attempts_at_claim[item.id] = item.completion_notification_attempts
        return items

    async def record_completion_notification_attempt(
        self,
        item_id: uuid.UUID,
        *,
        error: str,
        retry_after_seconds: int = 10,
    ) -> bool:
        changed = await self._store.record_completion_notification_attempt(
            item_id, error=error, retry_after_seconds=retry_after_seconds
        )
        if changed:
            # Mirrors notifier formula: base uses db_attempts_at_claim + 1.
            # Increment stored value so back-to-back retries within the same claim
            # cycle advance correctly (re-claim will refresh from DB anyway).
            db_base = self._db_attempts_at_claim.get(item_id, 0)
            attempt_number = db_base + 1
            self._db_attempts_at_claim[item_id] = db_base + 1
            self.retries.append(
                RetryRecord(
                    work_item_id=item_id,
                    retry_after_seconds=retry_after_seconds,
                    mono=time.monotonic(),
                    attempt_number=attempt_number,
                )
            )
        return changed


async def _seed_completion_outbox(
    work_store: WorkStore,
    *,
    client_id: str,
    batch_size: int,
) -> list[uuid.UUID]:
    """Bulk-insert COMPLETED work items with pending completion notifications."""
    now = datetime.now(UTC)
    seeded = [uuid.uuid4() for _ in range(batch_size)]
    rows = [
        WorkItem(
            id=work_id,
            client_id=client_id,
            status=WorkItemStatus.COMPLETED,
            payload=dict(SEED_PAYLOAD),
            result=dict(SEED_RESULT),
            created_at=now,
            completed_at=now,
            completion_notification_state=CompletionNotificationState.PENDING,
            completion_notification_attempts=0,
            completion_notification_next_attempt_at=now,
            completion_notification_last_error=None,
            completion_notification_delivered_at=None,
            completion_notification_claim_expires_at=None,
        )
        for work_id in seeded
    ]
    async with work_store._session_context() as session:
        session.add_all(rows)
        await session.commit()
    return seeded


async def _make_pending_notifications_due(work_store: WorkStore, client_id: str) -> None:
    """Make PENDING/DELIVERING rows immediately claimable."""
    async with work_store._session_context() as session:
        await session.execute(
            update(WorkItem)
            .where(col(WorkItem.client_id) == client_id)
            .where(
                col(WorkItem.completion_notification_state).in_(
                    [
                        CompletionNotificationState.PENDING,
                        CompletionNotificationState.DELIVERING,
                    ]
                )
            )
            .values(
                completion_notification_state=CompletionNotificationState.PENDING,
                completion_notification_next_attempt_at=datetime.now(UTC),
                completion_notification_claim_expires_at=None,
            )
        )
        await session.commit()


async def _count_delivered(work_store: WorkStore, client_id: str) -> int:
    """Return how many rows for *client_id* are DELIVERED."""
    async with work_store._session_context() as session:
        result = await session.execute(
            select(func.count()).where(
                col(WorkItem.client_id) == client_id,
                col(WorkItem.completion_notification_state) == CompletionNotificationState.DELIVERED,
            )
        )
        return result.scalar_one()


async def _wait_until_all_delivered(
    work_store: WorkStore,
    *,
    client_id: str,
    expected: int,
    timeout_seconds: float,
) -> float:
    """Poll until *expected* rows are DELIVERED; return elapsed seconds from call start."""
    t0 = time.perf_counter()
    deadline = t0 + timeout_seconds
    while time.perf_counter() < deadline:
        if await _count_delivered(work_store, client_id) >= expected:
            return time.perf_counter() - t0
        await asyncio.sleep(0.05)
    msg = f"only waited out {timeout_seconds:.0f}s; not all {expected} notifications delivered"
    raise TimeoutError(msg)


async def _stop_notifier(task: asyncio.Task[None] | None) -> None:
    """Cancel the notifier task and wait for it to finish."""
    if task is None:
        return
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


def _build_notifier(database_url: str, callback_url: str) -> CompletionNotifier:
    """Build a notifier aimed at the plain-HTTP mock (httpx ignores SSL for http://)."""
    settings = Settings(
        EP_DATABASE_URL=database_url,
        EP_COMPLETION_CALLBACK_URL=callback_url,
        EP_CALLBACK_TIMEOUT_SECONDS=5.0,
    )
    return CompletionNotifier(settings)


def _empty_poll_gaps_during_outage(polls: list[PollSample], outage_until_mono: float) -> list[float]:
    """Gaps between consecutive empty polls inside the outage window."""
    empty = [p.mono for p in polls if p.claimed == 0 and p.mono < outage_until_mono]
    return [b - a for a, b in pairwise(empty)]


def _retry_records_by_item(retries: list[RetryRecord]) -> dict[uuid.UUID, list[RetryRecord]]:
    by_item: dict[uuid.UUID, list[RetryRecord]] = {}
    for record in sorted(retries, key=lambda r: (r.work_item_id, r.attempt_number, r.mono)):
        by_item.setdefault(record.work_item_id, []).append(record)
    return by_item


def _expected_retry_base(attempt_number: int) -> int:
    """Pre-jitter base for post-increment attempt_number (mirrors CompletionNotifier)."""
    return min(MAX_RETRY_SECONDS, 2 ** min(attempt_number, 8))


def _retry_out_of_jitter_band(retries: list[RetryRecord]) -> int:
    """Count retries whose delay falls outside [0.8, 1.2] * base."""
    bad = 0
    for record in retries:
        base = _expected_retry_base(record.attempt_number)
        low = int(base * RETRY_JITTER_MIN)
        high = int(base * RETRY_JITTER_MAX)
        if record.retry_after_seconds < low or record.retry_after_seconds > high:
            bad += 1
    return bad


def _retry_shrank_while_base_grew(retries: list[RetryRecord]) -> int:
    """Count items where delay shrank while base still grew (plateau jitter shrink is OK)."""
    bad_items = 0
    for records in _retry_records_by_item(retries).values():
        if any(
            _expected_retry_base(later.attempt_number) > _expected_retry_base(earlier.attempt_number)
            and later.retry_after_seconds < earlier.retry_after_seconds
            for earlier, later in pairwise(records)
        ):
            bad_items += 1
    return bad_items


def _empty_poll_failure(empty_gaps: list[float], min_empty_gap: float) -> str | None:
    """Return an empty-poll KPI failure message, or None if the check passes."""
    require_sample = float(DELIVERY_POLL_SECONDS * (MIN_EMPTY_POLL_GAPS + 1)) <= OUTAGE_SECONDS
    if require_sample and len(empty_gaps) < MIN_EMPTY_POLL_GAPS:
        return (
            f"only {len(empty_gaps)} empty-poll gap(s) during outage; need >= {MIN_EMPTY_POLL_GAPS} "
            f"to validate poll cadence (test 3.2)"
        )
    if empty_gaps and min_empty_gap < MIN_EMPTY_POLL_GAP_SECONDS:
        return (
            f"empty-poll gap min {min_empty_gap:.3f}s < {MIN_EMPTY_POLL_GAP_SECONDS:.3f}s "
            f"— poll loop appears to busy-spin during outage (test 3.2)"
        )
    return None


def _collect_callback_outage_failures(
    *,
    batch_size: int,
    failure_count: int,
    retries: list[RetryRecord],
    delivered_count: int,
    delivered_before_drain: int,
    successes_during_outage: int,
    max_retry: int,
    out_of_band: int,
    shrank_while_growing: int,
    empty_gaps: list[float],
    min_empty_gap: float,
    throughput: float,
) -> list[str]:
    """Return KPI failure messages for test 3.2 (empty means pass)."""
    checks: list[tuple[bool, str]] = [
        (failure_count < 1, "no callback failures during outage — notifier did not exercise retries (test 3.2)"),
        (not retries, "no persisted retry records during outage — backoff path not observed (test 3.2)"),
        (
            delivered_count < batch_size,
            f"delivered {delivered_count}/{batch_size} — events permanently lost (test 3.2)",
        ),
        (
            delivered_before_drain > 0,
            f"{delivered_before_drain} notification(s) already DELIVERED before recovery drain "
            f"— outage boundary leaked successes (test 3.2)",
        ),
        (
            successes_during_outage > 0,
            f"{successes_during_outage} successful callback(s) during outage window (test 3.2)",
        ),
        (
            max_retry > MAX_OBSERVED_RETRY_SECONDS,
            f"retry_after max {max_retry}s exceeds jittered cap {MAX_OBSERVED_RETRY_SECONDS}s "
            f"(MAX_RETRY_SECONDS={MAX_RETRY_SECONDS} * {RETRY_JITTER_MAX}) (test 3.2)",
        ),
        (
            out_of_band > 0,
            f"{out_of_band} retry_after value(s) outside [{RETRY_JITTER_MIN}, {RETRY_JITTER_MAX}] * base (test 3.2)",
        ),
        (
            shrank_while_growing > 0,
            f"retry_after shrank while exponential base still grew for {shrank_while_growing} item(s) (test 3.2)",
        ),
        (
            throughput < TARGET_DELIVERY_THROUGHPUT_PER_SEC,
            f"post-recovery throughput {throughput:.1f} events/s < "
            f"{TARGET_DELIVERY_THROUGHPUT_PER_SEC:.0f} events/s (test 3.2)",
        ),
    ]
    failures_out = [msg for failed, msg in checks if failed]
    empty_poll_msg = _empty_poll_failure(empty_gaps, min_empty_gap)
    if empty_poll_msg is not None:
        failures_out.append(empty_poll_msg)
    return failures_out


def assert_callback_outage_delivery(
    *,
    batch_size: int,
    mock: MockCallbackState,
    retries: list[RetryRecord],
    polls: list[PollSample],
    outage_until_mono: float,
    recovery_drain_seconds: float,
    delivered_count: int,
    delivered_before_drain: int,
) -> None:
    """Assert test 3.2 / R7 / R8 KPIs; always print diagnostics."""
    with mock.lock:
        successes_during_outage = sum(1 for a in mock.attempts if a.mono < outage_until_mono and a.status_code < 400)
        failure_count = mock.failure_count
        success_count = mock.success_count
        total_attempts = len(mock.attempts)

    retry_delays = [r.retry_after_seconds for r in retries]
    max_retry = max(retry_delays) if retry_delays else 0
    out_of_band = _retry_out_of_jitter_band(retries)
    shrank_while_growing = _retry_shrank_while_base_grew(retries)

    empty_gaps = _empty_poll_gaps_during_outage(polls, outage_until_mono)
    median_empty_gap = statistics.median(empty_gaps) if empty_gaps else float("nan")
    min_empty_gap = min(empty_gaps) if empty_gaps else float("nan")
    drained = max(0, delivered_count - delivered_before_drain)
    throughput = drained / recovery_drain_seconds if recovery_drain_seconds > 0 else 0.0

    diag = (
        f"\n--- Callback outage delivery (test 3.2, n={batch_size}) ---\n"
        f"  outage={OUTAGE_SECONDS:.0f}s  delivered={delivered_count}"
        f"  delivered_before_drain={delivered_before_drain}"
        f"  mock_attempts={total_attempts}  failures={failure_count}  successes={success_count}\n"
        f"  successes_during_outage={successes_during_outage}\n"
        f"  retry_after: count={len(retry_delays)}  max={max_retry}s"
        f"  out_of_band={out_of_band}  shrank_while_base_grew={shrank_while_growing}\n"
        f"  empty polls in outage: gaps={len(empty_gaps)}"
        f"  median_gap={median_empty_gap:.3f}s  min_gap={min_empty_gap:.3f}s"
        f"  (expected ~{DELIVERY_POLL_SECONDS}s)\n"
        f"  recovery drain: {recovery_drain_seconds:.3f}s  drained={drained}"
        f"  throughput={throughput:.1f} events/s"
        f"  (target >= {TARGET_DELIVERY_THROUGHPUT_PER_SEC:.0f}/s; due-backlog after force-due)\n"
    )
    print(diag, flush=True)  # noqa: T201 — CI baseline (run with -s)

    failures_out = _collect_callback_outage_failures(
        batch_size=batch_size,
        failure_count=failure_count,
        retries=retries,
        delivered_count=delivered_count,
        delivered_before_drain=delivered_before_drain,
        successes_during_outage=successes_during_outage,
        max_retry=max_retry,
        out_of_band=out_of_band,
        shrank_while_growing=shrank_while_growing,
        empty_gaps=empty_gaps,
        min_empty_gap=min_empty_gap,
        throughput=throughput,
    )
    if failures_out:
        raise AssertionError("\n".join(failures_out) + diag)


class TestCallbackOutageDelivery:
    """Test 3.2: completion event delivery after callback outage; ≥ 50 events/s drain."""

    @pytest.mark.asyncio
    async def test_delivery_after_callback_outage(
        self,
        perf_database_url: str,
    ) -> None:
        """Test 3.2: no loss, bounded retries, fast drain after recovery."""
        run_id = uuid.uuid4()
        client_id = f"{CLIENT_ID_PREFIX}:{run_id}"
        seeded_ids: list[uuid.UUID] = []
        server, mock, callback_url = _start_mock_callback(OUTAGE_HOLD_SECONDS)
        notifier: CompletionNotifier | None = None
        notifier_task: asyncio.Task[None] | None = None
        outage_until_mono = 0.0

        try:
            async with WorkStore.from_database_url(perf_database_url) as raw_store:
                await raw_store.check_ready()
                seeded_ids.extend(await _seed_completion_outbox(raw_store, client_id=client_id, batch_size=BATCH_SIZE))

                instrumented = InstrumentingWorkStore(raw_store)
                notifier = _build_notifier(perf_database_url, callback_url)

                # Hold mock failing until after notifier stop (avoid sleep/202 race).
                outage_until_mono = time.monotonic() + OUTAGE_SECONDS
                with mock.lock:
                    mock.outage_until_mono = outage_until_mono + OUTAGE_HOLD_SECONDS
                notifier_task = asyncio.create_task(notifier.run(instrumented), name="perf-3.2-notifier-outage")
                await asyncio.sleep(OUTAGE_SECONDS)

                await _stop_notifier(notifier_task)
                notifier_task = None
                with mock.lock:
                    mock.outage_until_mono = time.monotonic()
                await _make_pending_notifications_due(raw_store, client_id)
                delivered_before_drain = await _count_delivered(raw_store, client_id)

                notifier_task = asyncio.create_task(notifier.run(instrumented), name="perf-3.2-notifier-drain")
                drain_timeout = max(60.0, BATCH_SIZE / TARGET_DELIVERY_THROUGHPUT_PER_SEC * 4.0)
                recovery_drain_seconds = await _wait_until_all_delivered(
                    raw_store,
                    client_id=client_id,
                    expected=BATCH_SIZE,
                    timeout_seconds=drain_timeout,
                )
                delivered_count = await _count_delivered(raw_store, client_id)

                assert_callback_outage_delivery(
                    batch_size=BATCH_SIZE,
                    mock=mock,
                    retries=instrumented.retries,
                    polls=instrumented.polls,
                    outage_until_mono=outage_until_mono,
                    recovery_drain_seconds=recovery_drain_seconds,
                    delivered_count=delivered_count,
                    delivered_before_drain=delivered_before_drain,
                )
        finally:
            await _stop_notifier(notifier_task)
            if notifier is not None:
                await notifier.close()
            server.shutdown()
            server.server_close()
            if seeded_ids:
                async with WorkStore.from_database_url(perf_database_url) as cleanup:
                    await cleanup_work_items(cleanup, seeded_ids)
