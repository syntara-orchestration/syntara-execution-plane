"""Tests for process-owned drain monitoring."""

from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from execution_plane.drain_monitor import DrainMonitor
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import ExecutionTarget, TargetStatus
from execution_plane_client.models.execution_target import KubernetesPlacement

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@asynccontextmanager
async def _worker_tasks() -> AsyncIterator[asyncio.TaskGroup]:
    with suppress(asyncio.CancelledError):
        async with asyncio.TaskGroup() as group:
            yield group
            raise asyncio.CancelledError


_DATABASE_UNAVAILABLE = "database unavailable"
_TEMPORARY_FAILURE = "temporary failure"


def _target(*, cluster_id: uuid.UUID | None = None, is_default: bool = False) -> ExecutionTarget:
    now = datetime.now(UTC)
    return ExecutionTarget(
        id=uuid.uuid4(),
        cluster_id=cluster_id or uuid.uuid4(),
        name="target-a",
        backend_type="kubernetes",
        endpoint="https://target.example",
        placement=KubernetesPlacement(namespace="execution"),
        credential_ref=uuid.uuid4(),
        status=TargetStatus.DRAINING,
        enabled=False,
        is_default=is_default,
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


def _cluster() -> Cluster:
    now = datetime.now(UTC)
    return Cluster(
        id=uuid.uuid4(),
        name="cluster-a",
        endpoint="https://cluster.example",
        credential_ref=uuid.uuid4(),
        status=ClusterStatus.DRAINING,
        enabled=False,
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


class _TargetStore:
    def __init__(self, targets: list[ExecutionTarget]) -> None:
        self.targets = targets
        self.finalized: list[uuid.UUID] = []
        self.failed: list[uuid.UUID] = []

    async def list(self, **_: object) -> list[ExecutionTarget]:
        return self.targets

    async def get(self, target_id: uuid.UUID) -> ExecutionTarget | None:
        return next((target for target in self.targets if target.id == target_id), None)

    async def finalize_delete(self, target_id: uuid.UUID) -> None:
        self.finalized.append(target_id)
        self.targets[:] = [target for target in self.targets if target.id != target_id]

    async def mark_failed(self, target_id: uuid.UUID, _updated_by: uuid.UUID) -> None:
        self.failed.append(target_id)


class _ClusterStore:
    def __init__(self, clusters: list[Cluster]) -> None:
        self.clusters = clusters
        self.finalized: list[uuid.UUID] = []
        self.failed: list[uuid.UUID] = []
        self.recorded_discovery_failures = 0
        self.recorded_drain_failures = 0

    async def list(self, **_: object) -> list[Cluster]:
        return self.clusters

    async def finalize_delete(self, cluster_id: uuid.UUID) -> None:
        self.finalized.append(cluster_id)

    async def record_discovery_state(
        self, cluster_id: uuid.UUID, _status: ClusterStatus, _message: str, _updated_by: uuid.UUID
    ) -> Cluster:
        self.failed.append(cluster_id)
        self.recorded_discovery_failures += 1
        return self.clusters[0]

    async def mark_drain_failed(self, cluster_id: uuid.UUID, _message: str, _updated_by: uuid.UUID) -> Cluster:
        self.failed.append(cluster_id)
        self.recorded_drain_failures += 1
        return self.clusters[0]


class _WorkStore:
    def __init__(self, *, drained: bool) -> None:
        self.drained = drained

    async def is_target_drained(self, _target_id: uuid.UUID) -> bool:
        return self.drained


@pytest.mark.asyncio
async def test_monitor_recovers_and_finalizes_persisted_target_and_cluster_drains() -> None:
    cluster = _cluster()
    targets = [_target(cluster_id=cluster.id, is_default=True), _target()]
    target_ids = [target.id for target in targets]
    target_store = _TargetStore(targets)
    cluster_store = _ClusterStore([cluster])
    monitor = DrainMonitor(target_store, cluster_store, _WorkStore(drained=True), poll_interval=0)  # type: ignore[arg-type]

    async with _worker_tasks() as group:
        await monitor._scan_draining(group)
        await asyncio.gather(*monitor._tasks.values())

    assert set(target_store.finalized) == set(target_ids)
    assert cluster_store.finalized == [cluster.id]


@pytest.mark.asyncio
async def test_worker_group_cancels_monitor_and_waiting_drains() -> None:
    target = _target()
    target_store = _TargetStore([target])
    started = asyncio.Event()
    stopped = asyncio.Event()

    class WaitingWorkStore:
        async def is_target_drained(self, _target_id) -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

    monitor = DrainMonitor(
        target_store,  # type: ignore[arg-type]
        _ClusterStore([]),  # type: ignore[arg-type]
        WaitingWorkStore(),  # type: ignore[arg-type]
        monitor_interval=3600,
    )
    async with _worker_tasks() as group:
        task = group.create_task(monitor.run(group))
        await asyncio.wait_for(started.wait(), timeout=1)

    assert task.cancelled()
    assert stopped.is_set()
    assert target_store.finalized == []
    assert all(task.done() for task in monitor._tasks.values())


@pytest.mark.asyncio
async def test_monitor_polls_for_newly_persisted_target_drains() -> None:
    target = _target()
    target_store = _TargetStore([])
    monitor = DrainMonitor(
        target_store,  # type: ignore[arg-type]
        _ClusterStore([]),  # type: ignore[arg-type]
        _WorkStore(drained=True),  # type: ignore[arg-type]
        poll_interval=0,
        monitor_interval=0,
    )
    async with _worker_tasks() as group:
        await monitor._scan_draining(group)
        group.create_task(monitor._monitor_loop(group))
        target_store.targets.append(target)
        for _ in range(100):
            if target.id in target_store.finalized:
                break
            await asyncio.sleep(0)

    assert target_store.finalized == [target.id]


@pytest.mark.asyncio
async def test_initial_scan_failure_cancels_already_scheduled_drains() -> None:
    target = _target()
    started = asyncio.Event()
    stopped = asyncio.Event()

    class WaitingWorkStore:
        async def is_target_drained(self, _target_id) -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

    monitor = DrainMonitor(
        _TargetStore([target]),  # type: ignore[arg-type]
        _ClusterStore([]),  # type: ignore[arg-type]
        WaitingWorkStore(),  # type: ignore[arg-type]
    )

    async def failing_scan(group) -> None:
        monitor._schedule_target(target.id, target.updated_by, group)
        await started.wait()
        raise RuntimeError(_DATABASE_UNAVAILABLE)

    monitor._scan_draining = failing_scan  # type: ignore[method-assign]
    with pytest.raises(ExceptionGroup) as error:
        async with asyncio.TaskGroup() as group:
            group.create_task(monitor.run(group))

    assert isinstance(error.value.exceptions[0], RuntimeError)
    assert stopped.is_set()


@pytest.mark.asyncio
async def test_monitor_cancels_individual_target_drain_when_cluster_drain_is_scheduled() -> None:
    cluster = _cluster()
    target = _target(cluster_id=cluster.id)
    monitor = DrainMonitor(
        _TargetStore([target]),  # type: ignore[arg-type]
        _ClusterStore([cluster]),  # type: ignore[arg-type]
        _WorkStore(drained=False),  # type: ignore[arg-type]
        monitor_interval=60,
    )
    async with _worker_tasks() as group:
        monitor._schedule_target(target.id, uuid.uuid4(), group)
        target_task = monitor._tasks[target.id]
        await monitor._schedule_cluster(cluster.id, uuid.uuid4(), group)
        assert target_task.cancelled()
        assert cluster.id in monitor._tasks


@pytest.mark.asyncio
@pytest.mark.parametrize("cluster_drain", [False, True])
async def test_monitor_ignores_duplicate_schedule(*, cluster_drain: bool) -> None:
    cluster = _cluster()
    target = _target(cluster_id=cluster.id)
    monitor = DrainMonitor(
        _TargetStore([target]),  # type: ignore[arg-type]
        _ClusterStore([cluster]),  # type: ignore[arg-type]
        _WorkStore(drained=False),  # type: ignore[arg-type]
    )
    async with _worker_tasks() as group:
        if cluster_drain:
            await monitor._schedule_cluster(cluster.id, uuid.uuid4(), group)
            first = monitor._tasks[cluster.id]
            await monitor._schedule_cluster(cluster.id, uuid.uuid4(), group)
            assert monitor._tasks[cluster.id] is first
        else:
            monitor._schedule_target(target.id, uuid.uuid4(), group)
            first = monitor._tasks[target.id]
            monitor._schedule_target(target.id, uuid.uuid4(), group)
            assert monitor._tasks[target.id] is first


@pytest.mark.asyncio
async def test_monitor_logs_and_continues_after_a_scan_failure() -> None:
    monitor = DrainMonitor(
        _TargetStore([]),  # type: ignore[arg-type]
        _ClusterStore([]),  # type: ignore[arg-type]
        _WorkStore(drained=True),  # type: ignore[arg-type]
        monitor_interval=0,
    )
    retried = asyncio.Event()
    calls = 0

    async def failing_scan(_group) -> None:
        nonlocal calls
        calls += 1
        if calls > 1:
            retried.set()
        raise RuntimeError(_TEMPORARY_FAILURE)

    monitor._scan_draining = failing_scan  # type: ignore[method-assign]
    async with _worker_tasks() as group:
        group.create_task(monitor._monitor_loop(group))
        await asyncio.wait_for(retried.wait(), timeout=1)

    assert calls > 1


@pytest.mark.asyncio
async def test_monitor_propagates_scan_cancellation() -> None:
    monitor = DrainMonitor(
        _TargetStore([]),  # type: ignore[arg-type]
        _ClusterStore([]),  # type: ignore[arg-type]
        _WorkStore(drained=True),  # type: ignore[arg-type]
        monitor_interval=0,
    )

    async def cancelled_scan(_group) -> None:
        raise asyncio.CancelledError

    monitor._scan_draining = cancelled_scan  # type: ignore[method-assign]
    async with asyncio.TaskGroup() as group:
        task = group.create_task(monitor._monitor_loop(group))
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_monitor_silences_failure_when_target_failure_recording_also_fails() -> None:
    target = _target()

    class FailingTargetStore(_TargetStore):
        async def get(self, _target_id: uuid.UUID) -> ExecutionTarget | None:
            raise RuntimeError(_DATABASE_UNAVAILABLE)

        async def mark_failed(self, _target_id: uuid.UUID, _updated_by: uuid.UUID) -> None:
            raise RuntimeError(_DATABASE_UNAVAILABLE)

    monitor = DrainMonitor(
        FailingTargetStore([target]),  # type: ignore[arg-type]
        _ClusterStore([]),  # type: ignore[arg-type]
        _WorkStore(drained=True),  # type: ignore[arg-type]
    )

    await monitor._drain_target(target.id, uuid.uuid4())


@pytest.mark.asyncio
async def test_monitor_marks_a_target_failed_when_drain_finalization_raises() -> None:
    target = _target()

    class FailingTargetStore(_TargetStore):
        async def get(self, _target_id: uuid.UUID) -> ExecutionTarget | None:
            raise RuntimeError(_DATABASE_UNAVAILABLE)

    target_store = FailingTargetStore([target])
    monitor = DrainMonitor(
        target_store,  # type: ignore[arg-type]
        _ClusterStore([]),  # type: ignore[arg-type]
        _WorkStore(drained=True),  # type: ignore[arg-type]
    )

    await monitor._drain_target(target.id, uuid.uuid4())

    assert target_store.failed == [target.id]


@pytest.mark.asyncio
async def test_monitor_marks_a_cluster_failed_when_cluster_drain_raises() -> None:
    cluster = _cluster()

    class FailingTargetStore(_TargetStore):
        async def list(self, **_: object) -> list[ExecutionTarget]:
            raise RuntimeError(_DATABASE_UNAVAILABLE)

    cluster_store = _ClusterStore([cluster])
    monitor = DrainMonitor(
        FailingTargetStore([]),  # type: ignore[arg-type]
        cluster_store,  # type: ignore[arg-type]
        _WorkStore(drained=True),  # type: ignore[arg-type]
    )

    await monitor._drain_cluster(cluster.id, uuid.uuid4())

    assert cluster_store.failed == [cluster.id]
    assert cluster_store.recorded_discovery_failures == 0
    assert cluster_store.recorded_drain_failures == 1


@pytest.mark.asyncio
async def test_monitor_silences_failure_when_cluster_failure_recording_also_fails() -> None:
    cluster = _cluster()

    class FailingTargetStore(_TargetStore):
        async def list(self, **_: object) -> list[ExecutionTarget]:
            raise RuntimeError(_DATABASE_UNAVAILABLE)

    class FailingClusterStore(_ClusterStore):
        async def mark_drain_failed(self, _cluster_id: uuid.UUID, _message: str, _updated_by: uuid.UUID) -> Cluster:
            raise RuntimeError(_DATABASE_UNAVAILABLE)

    monitor = DrainMonitor(
        FailingTargetStore([]),  # type: ignore[arg-type]
        FailingClusterStore([cluster]),  # type: ignore[arg-type]
        _WorkStore(drained=True),  # type: ignore[arg-type]
    )

    await monitor._drain_cluster(cluster.id, uuid.uuid4())


@pytest.mark.asyncio
async def test_monitor_propagates_cluster_drain_cancellation() -> None:
    cluster = _cluster()
    gate = asyncio.Event()

    class WaitingTargetStore(_TargetStore):
        async def list(self, **_: object) -> list[ExecutionTarget]:
            await gate.wait()
            return []

    monitor = DrainMonitor(
        WaitingTargetStore([]),  # type: ignore[arg-type]
        _ClusterStore([cluster]),  # type: ignore[arg-type]
        _WorkStore(drained=True),  # type: ignore[arg-type]
    )
    task = asyncio.create_task(monitor._drain_cluster(cluster.id, uuid.uuid4()))
    await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_monitor_polls_until_work_for_a_target_is_drained() -> None:
    target = _target()

    class EventuallyDrainedWorkStore:
        def __init__(self) -> None:
            self.calls = 0

        async def is_target_drained(self, _target_id: uuid.UUID) -> bool:
            self.calls += 1
            return self.calls > 1

    work_item_store = EventuallyDrainedWorkStore()
    monitor = DrainMonitor(
        _TargetStore([target]),  # type: ignore[arg-type]
        _ClusterStore([]),  # type: ignore[arg-type]
        work_item_store,  # type: ignore[arg-type]
        poll_interval=0,
    )

    await monitor._wait_until_drained(target.id)

    assert work_item_store.calls == 2
