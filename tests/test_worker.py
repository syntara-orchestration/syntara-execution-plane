"""Tests for the Execution Plane worker lifecycle."""

from __future__ import annotations

import asyncio
import os
import signal
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, Self, cast
from uuid import uuid4

import pytest

from execution_plane.models.execution_target import BackendType
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.work_item.work_item_store import WorkStore
from execution_plane.worker_manager.vanilla_k8s.manager import WorkloadCancelledError

if TYPE_CHECKING:
    from execution_plane.execution_target_reconciler.placement import WorkerManagerRegistry


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
    async def close(self) -> None:
        return None


class _DrainMonitor:
    def __init__(
        self,
        _target_store: object,
        _cluster_store: object,
        _work_item_store: object,
        events: list[str],
    ) -> None:
        self.events = events

    async def start(self) -> None:
        self.events.append("monitor:start")

    async def stop(self) -> None:
        self.events.append("monitor:stop")


class _ResourceMonitor:
    def __init__(self, _target_store: object, _cluster_store: object, _settings: object, events: list[str]) -> None:
        self.events = events

    async def start(self) -> None:
        self.events.append("resource-monitor:start")

    async def stop(self) -> None:
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
    target = SimpleNamespace(backend_type=BackendType.VANILLA_K8S)
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


class _CompletionDelivery:
    def __init__(self, _settings: object, events: list[str]) -> None:
        self.events = events

    async def run(self, _store: object) -> None:
        self.events.append("delivery:run")

    async def close(self) -> None:
        self.events.append("delivery:stop")


@pytest.mark.asyncio
async def test_run_worker_wires_placement_resolver_to_the_poll_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    from execution_plane import worker

    events: list[str] = []
    work_service = _StoreContext("work", events)
    cluster_store = _StoreContext("cluster", events)
    target_store = _StoreContext("target", events)
    workload_manager = object()
    placement_resolver = object()
    built_with: list[tuple[object, object, WorkerManagerRegistry]] = []
    poll_calls: list[WorkerManagerRegistry] = []

    monkeypatch.setattr(worker, "get_ep_settings", lambda: SimpleNamespace())
    monkeypatch.setattr(WorkStore, "from_database_url", lambda _url: work_service)
    monkeypatch.setattr(
        "execution_plane.worker.ClusterStore.from_database_url",
        lambda _url: cluster_store,
    )
    monkeypatch.setattr(
        "execution_plane.worker.ExecutionTargetStore.from_database_url",
        lambda _url: target_store,
    )
    monkeypatch.setattr(
        worker,
        "DrainMonitor",
        lambda target, cluster, work: _DrainMonitor(target, cluster, work, events),
    )
    monkeypatch.setattr(
        worker,
        "ResourceMonitor",
        lambda target, cluster, settings, **_kwargs: _ResourceMonitor(target, cluster, settings, events),
    )
    monkeypatch.setattr(worker, "CompletionNotifier", lambda _settings: _CompletionDelivery(SimpleNamespace(), events))
    monkeypatch.setattr(worker, "CredentialResolver", lambda _settings: _CredentialResolver())
    monkeypatch.setattr(worker, "VanillaK8sWorkerManager", lambda *_args, **_kwargs: workload_manager)

    def build_resolver(cluster: object, target: object, managers: WorkerManagerRegistry) -> object:
        built_with.append((cluster, target, managers))
        return placement_resolver

    async def record_poller(
        _scheduler: object,
        _progress: object,
        _target_store: object,
        managers: WorkerManagerRegistry,
        _settings: object,
        _wakeup_event: object,
    ) -> None:
        poll_calls.append(managers)

    monkeypatch.setattr(worker, "build_placement_resolver", build_resolver)
    monkeypatch.setattr(worker, "_listen_loop", _finished_listener)
    monkeypatch.setattr(worker, "_poll_loop", record_poller)
    monkeypatch.setattr(worker, "run_orphan_policy_reconciler", _finished_policy_reconciler)

    await worker.run_worker("postgresql+asyncpg://localhost/syntara")

    assert built_with == [(cluster_store, target_store, poll_calls[0])]
    assert poll_calls[0].get(BackendType.VANILLA_K8S) is workload_manager


@pytest.mark.asyncio
async def test_run_worker_owns_drain_monitor_for_the_worker_lifetime(monkeypatch: pytest.MonkeyPatch) -> None:
    from execution_plane import worker

    events: list[str] = []
    monkeypatch.setattr(worker, "get_ep_settings", lambda: SimpleNamespace())

    monkeypatch.setattr(WorkStore, "from_database_url", lambda _url: _StoreContext("work", events))
    monkeypatch.setattr(
        "execution_plane.worker.ClusterStore.from_database_url",
        lambda _url: _StoreContext("cluster", events),
    )
    monkeypatch.setattr(
        "execution_plane.worker.ExecutionTargetStore.from_database_url",
        lambda _url: _StoreContext("target", events),
    )
    monkeypatch.setattr(
        worker,
        "DrainMonitor",
        lambda target, cluster, work: _DrainMonitor(target, cluster, work, events),
    )
    monkeypatch.setattr(
        worker,
        "ResourceMonitor",
        lambda target, cluster, settings, **_kwargs: _ResourceMonitor(target, cluster, settings, events),
    )
    monkeypatch.setattr(worker, "CompletionNotifier", lambda settings: _CompletionDelivery(settings, events))
    monkeypatch.setattr(worker, "CredentialResolver", lambda _settings: _CredentialResolver())
    monkeypatch.setattr(worker, "VanillaK8sWorkerManager", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(worker, "build_placement_resolver", lambda *_args: object())
    monkeypatch.setattr(worker, "_listen_loop", _finished_listener)
    monkeypatch.setattr(worker, "_poll_loop", _finished_poller)
    monkeypatch.setattr(worker, "run_orphan_policy_reconciler", _finished_policy_reconciler)

    await worker.run_worker("postgresql+asyncpg://localhost/syntara")

    assert events.index("monitor:start") < events.index("monitor:stop")
    assert events.index("resource-monitor:start") < events.index("resource-monitor:stop")
    assert events.index("resource-monitor:stop") < events.index("work:stop")
    assert events.index("monitor:stop") < events.index("work:stop")


@pytest.mark.asyncio
async def test_run_worker_completes_cleanup_after_shutdown_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    from execution_plane import worker
    from execution_plane.runtime import run_until_shutdown

    events: list[str] = []
    listener_started = asyncio.Event()
    monkeypatch.setattr(worker, "get_ep_settings", lambda: SimpleNamespace())
    monkeypatch.setattr(WorkStore, "from_database_url", lambda _url: _StoreContext("work", events))
    monkeypatch.setattr(
        "execution_plane.worker.ClusterStore.from_database_url",
        lambda _url: _StoreContext("cluster", events),
    )
    monkeypatch.setattr(
        "execution_plane.worker.ExecutionTargetStore.from_database_url",
        lambda _url: _StoreContext("target", events),
    )
    monkeypatch.setattr(
        worker,
        "DrainMonitor",
        lambda target, cluster, work: _DrainMonitor(target, cluster, work, events),
    )
    monkeypatch.setattr(
        worker,
        "ResourceMonitor",
        lambda target, cluster, settings, **_kwargs: _ResourceMonitor(target, cluster, settings, events),
    )
    monkeypatch.setattr(worker, "CompletionNotifier", lambda _settings: _CompletionDelivery(SimpleNamespace(), events))
    monkeypatch.setattr(worker, "CredentialResolver", lambda _settings: _CredentialResolver())
    monkeypatch.setattr(worker, "VanillaK8sWorkerManager", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(worker, "build_placement_resolver", lambda *_args: object())

    async def blocked_listener(_database_url: str, _wakeup_event: object) -> None:
        listener_started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(worker, "_listen_loop", blocked_listener)
    monkeypatch.setattr(worker, "_poll_loop", _finished_poller)
    monkeypatch.setattr(worker, "run_orphan_policy_reconciler", _finished_policy_reconciler)

    runner = asyncio.create_task(run_until_shutdown(worker.run_worker("postgresql+asyncpg://localhost/syntara")))
    await asyncio.wait_for(listener_started.wait(), timeout=1)
    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(runner, timeout=1)

    assert "resource-monitor:stop" in events
    assert "monitor:stop" in events
    assert "delivery:stop" in events
    assert "work:stop" in events


@pytest.mark.asyncio
async def test_dispatch_task_is_owned_and_released(monkeypatch: pytest.MonkeyPatch) -> None:
    from execution_plane import worker

    snapshots: list[int] = []
    inflight: dict[asyncio.Task[None], None] = {}
    dispatch_event = asyncio.Event()
    params = cast(
        "worker._ProcessItemKwargs",
        {
            "item": object(),
            "work_progress": object(),
            "target_queries": object(),
            "worker_managers": object(),
            "settings": object(),
        },
    )

    async def fake_process_item(**_kwargs: object) -> None:
        snapshots.append(len(inflight))

    monkeypatch.setattr(worker, "_process_item", fake_process_item)

    task = worker._start_dispatch_task(params, inflight, dispatch_event, name="test-dispatch")

    assert task in inflight
    await task

    assert snapshots == [1], "task should be in inflight while _process_item runs"
    assert len(inflight) == 0, "task should remove itself on completion"
    assert dispatch_event.is_set()


@pytest.mark.asyncio
async def test_dispatch_task_is_released_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    from execution_plane import worker

    inflight: dict[asyncio.Task[None], None] = {}
    dispatch_event = asyncio.Event()
    params = cast(
        "worker._ProcessItemKwargs",
        {
            "item": object(),
            "work_progress": object(),
            "target_queries": object(),
            "worker_managers": object(),
            "settings": object(),
        },
    )

    async def fake_process_item(**_kwargs: object) -> None:
        msg = "boom"
        raise ValueError(msg)

    monkeypatch.setattr(worker, "_process_item", fake_process_item)

    task = worker._start_dispatch_task(params, inflight, dispatch_event, name="test-dispatch-error")

    with pytest.raises(ValueError, match="boom"):
        await task

    assert len(inflight) == 0, "task should remove itself even after an error"
    assert dispatch_event.is_set()


async def _finished_listener(_database_url: str, _wakeup_event: object) -> None:
    return None


async def _finished_poller(*_args: object) -> None:
    return None


async def _finished_policy_reconciler(*_args: object) -> None:
    return None
