"""Tests for the Execution Plane worker lifecycle."""

from __future__ import annotations

import asyncio
import os
import signal
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, Self
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from execution_plane.models.execution_target import BackendType
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.store_errors import StaleClaimError
from execution_plane.work_item.errors import WorkItemNotFoundError
from execution_plane.work_item.work_item_store import WorkStore
from execution_plane.worker_manager.vanilla_k8s.manager import WorkloadCancelledError

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from execution_plane.execution_target_reconciler.placement import WorkerManagerRegistry


@pytest.fixture
def worker_engine(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Use an inert process-owned engine in worker wiring tests."""
    from execution_plane import worker

    engine = SimpleNamespace(dispose=AsyncMock())
    monkeypatch.setattr(worker, "create_pooled_engine", Mock(return_value=engine))
    return engine


class _StoreContext:
    def __init__(self, name: str, events: list[str]) -> None:
        self.name = name
        self.events = events

    async def __aenter__(self) -> Self:
        self.events.append(f"{self.name}:start")
        return self

    async def __aexit__(self, *_: object) -> None:
        self.events.append(f"{self.name}:stop")


class _CredentialResolver:
    def __init__(self, events: list[str] | None = None) -> None:
        self.events = events

    async def __aenter__(self) -> Self:
        if self.events is not None:
            self.events.append("resolver:start")
        return self

    async def __aexit__(self, *_: object) -> None:
        if self.events is not None:
            self.events.append("resolver:stop")

    async def close(self) -> None:
        if self.events is not None:
            self.events.append("resolver:close")


class _DrainMonitor:
    def __init__(
        self,
        _target_store: object,
        _cluster_store: object,
        _work_item_store: object,
        events: list[str],
    ) -> None:
        self.events = events

    async def run(self, _task_group: asyncio.TaskGroup) -> None:
        self.events.append("monitor:start")
        try:
            await asyncio.sleep(0)
        finally:
            self.events.append("monitor:stop")


class _ResourceMonitor:
    def __init__(self, _target_store: object, _cluster_store: object, _settings: object, events: list[str]) -> None:
        self.events = events

    async def run(self) -> None:
        self.events.append("resource-monitor:start")
        try:
            await asyncio.sleep(0)
        finally:
            self.events.append("resource-monitor:stop")


class _WorkStore:
    def __init__(self) -> None:
        self.results: list[tuple[object, dict[str, object], object]] = []

    async def set_result(self, item_id: object, result: dict[str, object], status: object, **_kwargs: object) -> None:
        self.results.append((item_id, result, status))


class _TargetStore:
    def __init__(self, target: object) -> None:
        self.target = target

    async def get(self, _target_id: object) -> object:
        return self.target


class _Registry:
    def __init__(self, manager: object) -> None:
        self.manager = manager

    def get(self, _backend_type: object) -> object:
        return self.manager


class _Manager:
    def __init__(self) -> None:
        self.dispatched: list[object] = []

    async def dispatch(self, work_item: object) -> dict[str, object]:
        self.dispatched.append(work_item)
        raise WorkloadCancelledError(execution_started=True)


@pytest.mark.asyncio
async def test_process_item_preserves_execution_started_for_cancelled_running_work() -> None:
    from execution_plane import worker

    item = WorkItem(
        id=uuid4(),
        client_id="test-client",
        status=WorkItemStatus.CLAIMED,
        execution_target_id=uuid4(),
        created_at=datetime.now(UTC),
        claim_owner_id=uuid4(),
        claim_generation=1,
    )
    target = SimpleNamespace(backend_type=BackendType.KUBERNETES)
    manager = _Manager()
    work_store = _WorkStore()

    await worker._process_item(
        item,
        work_store,
        _TargetStore(target),
        _Registry(manager),
        SimpleNamespace(dispatch_retry_backoff_seconds=0),
    )

    assert manager.dispatched == [item]
    assert work_store.results == [
        (
            item.id,
            {
                "cancelled": True,
                "execution_started": True,
                "reason": "Node workload termination was confirmed after execution started",
            },
            WorkItemStatus.CANCELLED,
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("deleted", [False, True])
async def test_retired_dispatch_claim_does_not_stop_worker(*, deleted: bool) -> None:
    from execution_plane import worker

    item = WorkItem(id=uuid4(), client_id="test-client", claim_owner_id=uuid4(), claim_generation=1)
    store = AsyncMock(spec=WorkStore)
    store.set_result.side_effect = WorkItemNotFoundError(item.id) if deleted else StaleClaimError("Retired claim")
    slots = asyncio.BoundedSemaphore(1)
    await slots.acquire()
    dispatch_event = asyncio.Event()

    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(worker._dispatch_item(item, store, Mock(), Mock(), Mock(), slots, dispatch_event))

    store.set_result.assert_awaited_once()
    assert not slots.locked()
    assert dispatch_event.is_set()


class _CompletionDelivery:
    def __init__(self, _settings: object, events: list[str]) -> None:
        self.events = events

    async def run(self, _store: object) -> None:
        self.events.append("delivery:run")

    async def close(self) -> None:
        self.events.append("delivery:stop")


@pytest.fixture
def worker_events(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    from execution_plane import worker

    events: list[str] = []
    monkeypatch.setattr(worker, "get_settings", lambda: SimpleNamespace(max_concurrent_dispatches=2))
    monkeypatch.setattr(WorkStore, "from_engine", lambda _engine: _StoreContext("work", events))
    monkeypatch.setattr(worker.ClusterStore, "from_engine", lambda _engine: _StoreContext("cluster", events))
    monkeypatch.setattr(worker.ExecutionTargetStore, "from_engine", lambda _engine: _StoreContext("target", events))
    monkeypatch.setattr(
        worker, "DrainMonitor", lambda target, cluster, work: _DrainMonitor(target, cluster, work, events)
    )
    monkeypatch.setattr(
        worker,
        "ResourceMonitor",
        lambda target, cluster, settings, **_kwargs: _ResourceMonitor(target, cluster, settings, events),
    )
    monkeypatch.setattr(worker, "CompletionNotifier", lambda settings: _CompletionDelivery(settings, events))
    monkeypatch.setattr(worker, "CredentialResolver", lambda _settings: _CredentialResolver(events))
    monkeypatch.setattr(worker, "VanillaK8sWorkerManager", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(worker, "build_placement_resolver", lambda *_args: object())
    monkeypatch.setattr(worker, "_listen_loop", _finished_listener)
    monkeypatch.setattr(worker, "run_orphan_policy_reconciler", _finished_policy_reconciler)
    return events


@pytest.mark.asyncio
async def test_run_worker_wires_placement_resolver_and_one_task_group(
    monkeypatch: pytest.MonkeyPatch,
    worker_events: list[str],
) -> None:
    from execution_plane import worker

    placement_resolver = object()
    built_with: list[tuple[object, object, WorkerManagerRegistry]] = []
    groups: list[asyncio.TaskGroup] = []

    def build_resolver(cluster, target, managers) -> object:
        built_with.append((cluster, target, managers))
        return placement_resolver

    async def record_poller(scheduler, _progress, _target, managers, _settings, _event, task_group) -> None:
        assert scheduler._placement_resolver is placement_resolver
        assert managers is built_with[0][2]
        groups.append(task_group)

    async def record_drainer(_self, task_group) -> None:
        groups.append(task_group)

    monkeypatch.setattr(worker, "build_placement_resolver", build_resolver)
    monkeypatch.setattr(worker, "_poll_loop", record_poller)
    monkeypatch.setattr(_DrainMonitor, "run", record_drainer)
    await worker.run_worker("postgresql+asyncpg://localhost/syntara")

    assert len(groups) == 2
    assert groups[0] is groups[1]
    assert built_with[0][0].name == "cluster"
    assert built_with[0][1].name == "target"
    assert "resolver:stop" in worker_events


@pytest.mark.asyncio
async def test_run_worker_completes_cleanup_after_shutdown_signal(
    monkeypatch: pytest.MonkeyPatch,
    worker_events: list[str],
) -> None:
    from execution_plane import worker
    from execution_plane.runtime import run_until_shutdown

    listener_started = asyncio.Event()

    async def blocked_listener(_database_url, _event) -> None:
        listener_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            worker_events.append("listener:stop")

    monkeypatch.setattr(worker, "_listen_loop", blocked_listener)
    monkeypatch.setattr(worker, "_poll_loop", _finished_poller)
    runner = asyncio.create_task(run_until_shutdown(worker.run_worker("postgresql+asyncpg://localhost/syntara")))
    await asyncio.wait_for(listener_started.wait(), timeout=1)
    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(runner, timeout=1)

    for service in ("resource-monitor", "monitor", "listener", "delivery", "resolver"):
        assert worker_events.index(f"{service}:stop") < worker_events.index("target:stop")
    assert "work:stop" in worker_events


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_dispatch", [False, True])
async def test_worker_awaits_dispatch_cleanup_before_closing_resources(
    monkeypatch: pytest.MonkeyPatch,
    worker_events: list[str],
    *,
    fail_dispatch: bool,
) -> None:
    from execution_plane import worker

    started = asyncio.Event()
    cleaning = asyncio.Event()
    release_cleanup = asyncio.Event()

    class Scheduler:
        async def claim_and_place_work(self) -> SimpleNamespace | None:
            return SimpleNamespace(id=uuid4(), status=WorkItemStatus.CLAIMED)

    async def process_item(item, *_args: object) -> None:
        started.set()
        try:
            if fail_dispatch:
                message = "dispatch failed"
                raise ValueError(message)
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release_cleanup.wait()
            worker_events.append(f"dispatch:{item.id}:stop")

    monkeypatch.setattr(worker, "WorkScheduler", lambda *_args: Scheduler())
    monkeypatch.setattr(worker, "_process_item", process_item)
    runner = asyncio.create_task(worker.run_worker("postgresql+asyncpg://localhost/syntara"))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        if not fail_dispatch:
            runner.cancel()
        await asyncio.wait_for(cleaning.wait(), timeout=1)
        assert not runner.done()
        assert not any(
            event.endswith(":stop")
            for event in worker_events
            if event.split(":")[0]
            in {
                "work",
                "cluster",
                "target",
                "resolver",
                "delivery",
            }
        )
    finally:
        release_cleanup.set()
        if fail_dispatch:
            with pytest.raises(ExceptionGroup) as error:
                await asyncio.wait_for(runner, timeout=1)
            assert all(isinstance(exc, ValueError) for exc in error.value.exceptions)
        else:
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(runner, timeout=1)

    stopped = [i for i, event in enumerate(worker_events) if event.startswith("dispatch:")]
    assert stopped
    assert max(stopped) < worker_events.index("delivery:stop")
    assert worker_events.index("delivery:stop") < worker_events.index("resolver:stop")
    assert worker_events.index("resolver:stop") < worker_events.index("target:stop")


@pytest.mark.asyncio
async def test_worker_cleans_up_tasks_when_drain_startup_fails(
    monkeypatch: pytest.MonkeyPatch,
    worker_events: list[str],
) -> None:
    from execution_plane import worker

    drain_started = asyncio.Event()

    async def drain() -> None:
        drain_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            worker_events.append("drain:stop")

    async def failing_start(_self, task_group) -> None:
        task_group.create_task(drain())
        await drain_started.wait()
        message = "initial scan failed"
        raise RuntimeError(message)

    monkeypatch.setattr(_DrainMonitor, "run", failing_start)
    monkeypatch.setattr(worker, "_poll_loop", _finished_poller)
    with pytest.raises(ExceptionGroup) as error:
        await worker.run_worker("postgresql+asyncpg://localhost/syntara")

    assert isinstance(error.value.exceptions[0], RuntimeError)
    assert worker_events.index("drain:stop") < worker_events.index("delivery:stop")
    assert worker_events.index("delivery:stop") < worker_events.index("work:stop")


@pytest.mark.asyncio
async def test_poll_loop_bounds_claims_and_reuses_slots_without_waiting_for_poll_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from execution_plane import worker

    claims: list[int] = []
    started: asyncio.Queue[int] = asyncio.Queue()
    gates = [asyncio.Event() for _ in range(3)]
    active = 0
    peak = 0

    class Scheduler:
        returned_cancelled = False

        async def claim_and_place_work(self) -> SimpleNamespace | None:
            if not self.returned_cancelled:
                self.returned_cancelled = True
                return SimpleNamespace(id=-1, status=WorkItemStatus.CANCELLED)
            if len(claims) == 3:
                return None
            index = len(claims)
            claims.append(index)
            return SimpleNamespace(id=index, status=WorkItemStatus.CLAIMED)

    async def process_item(item, *_args: object) -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        started.put_nowait(item.id)
        try:
            await gates[item.id].wait()
        finally:
            active -= 1

    async def run() -> None:
        async with asyncio.TaskGroup() as group:
            group.create_task(
                worker._poll_loop(
                    Scheduler(),
                    None,
                    None,
                    None,
                    SimpleNamespace(max_concurrent_dispatches=2),
                    asyncio.Event(),
                    group,
                )
            )

    monkeypatch.setattr(worker, "_process_item", process_item)
    monkeypatch.setattr(worker, "POLL_INTERVAL_SECONDS", 3600)
    runner = asyncio.create_task(run())
    try:
        assert await asyncio.wait_for(started.get(), timeout=1) == 0
        assert await asyncio.wait_for(started.get(), timeout=1) == 1
        assert claims == [0, 1]
        gates[0].set()
        assert await asyncio.wait_for(started.get(), timeout=1) == 2
        assert peak == 2
    finally:
        runner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await runner
    assert active == 0


async def _finished_listener(_database_url: str, _wakeup_event: object) -> None:
    return None


async def _finished_poller(*_args: object) -> None:
    return None


async def _finished_policy_reconciler(*_args: object) -> None:
    return None


@pytest.mark.asyncio
async def test_run_worker_disposes_engine_if_store_setup_fails(
    monkeypatch: pytest.MonkeyPatch, worker_engine: SimpleNamespace
) -> None:
    from execution_plane import worker

    monkeypatch.setattr(WorkStore, "from_engine", Mock(side_effect=RuntimeError("store setup failed")))
    with pytest.raises(RuntimeError, match="store setup failed"):
        await worker.run_worker("postgresql+asyncpg://localhost/syntara")
    worker_engine.dispose.assert_awaited_once()


def test_worker_main_starts_and_stops_logging(monkeypatch: pytest.MonkeyPatch) -> None:
    from execution_plane import worker

    events: list[str] = []
    monkeypatch.setattr(worker, "start_loggers", lambda: events.append("start"))
    monkeypatch.setattr(worker, "stop_loggers", lambda: events.append("stop"))

    async def fake_run() -> None:
        return None

    async def fake_run_until_shutdown(target: Coroutine[None, None, None]) -> None:
        events.append("run")
        await target

    monkeypatch.setattr(worker, "_run", fake_run)

    monkeypatch.setattr(worker, "run_until_shutdown", fake_run_until_shutdown)

    worker.main()

    assert events == ["start", "run", "stop"]
