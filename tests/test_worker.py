"""Tests for the Execution Plane worker lifecycle."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Self

import pytest


class _StoreContext:
    def __init__(self, name: str, events: list[str]) -> None:
        self.name = name
        self.events = events

    async def __aenter__(self) -> Self:
        self.events.append(f"{self.name}:start")
        return self

    async def __aexit__(self, *_: object) -> None:
        self.events.append(f"{self.name}:stop")


class _DrainMonitor:
    def __init__(self, _target_store: object, _cluster_store: object, _work_store: object, events: list[str]) -> None:
        self.events = events

    async def start(self) -> None:
        self.events.append("monitor:start")

    async def stop(self) -> None:
        self.events.append("monitor:stop")


class _CompletionDelivery:
    def __init__(self, _settings: object, events: list[str]) -> None:
        self.events = events

    async def run(self, _store: object) -> None:
        self.events.append("delivery:run")

    async def close(self) -> None:
        self.events.append("delivery:stop")


@pytest.mark.asyncio
async def test_run_worker_owns_drain_monitor_for_the_worker_lifetime(monkeypatch: pytest.MonkeyPatch) -> None:
    from execution_plane import worker

    events: list[str] = []
    monkeypatch.setattr(worker, "get_ep_settings", lambda: SimpleNamespace())

    monkeypatch.setattr(
        "execution_plane.worker.WorkStore.from_database_url",
        lambda _url: _StoreContext("work", events),
    )
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
    monkeypatch.setattr(worker, "CompletionEventDelivery", lambda settings: _CompletionDelivery(settings, events))
    monkeypatch.setattr(worker, "KubernetesJobManager", lambda _settings: object())
    monkeypatch.setattr(worker, "build_placement_resolver", lambda _cluster, _target: object())
    monkeypatch.setattr(worker, "_listen_loop", _finished_listener)
    monkeypatch.setattr(worker, "_poll_loop", _finished_poller)
    monkeypatch.setattr(worker, "run_cluster_binding_reconciler", _finished_reconciler)

    await worker.run_worker("postgresql+asyncpg://localhost/syntara")

    assert events.index("monitor:start") < events.index("monitor:stop")
    assert events.index("monitor:stop") < events.index("work:stop")


async def _finished_listener(_database_url: str, _wakeup_event: object) -> None:
    return None


async def _finished_poller(*_args: object) -> None:
    return None


async def _finished_reconciler(_database_url: str) -> None:
    return None
