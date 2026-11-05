"""Hysteresis, per-target isolation, and clean shutdown."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    import uuid

from execution_plane.execution_target.execution_target_store import ExecutionTargetNotFoundError
from execution_plane.models.execution_target import TargetStatus
from execution_plane.resource_monitor.monitor import RESOURCE_MONITOR_ACTOR_ID, ResourceMonitor
from execution_plane.resource_monitor.probes import ProbeResult
from tests.resource_monitor.conftest import make_cluster, make_target, settings


class _ClusterStore:
    def __init__(self, clusters: dict[uuid.UUID, object]) -> None:
        self.clusters = clusters
        self.credential_reads: list[uuid.UUID] = []

    async def get_with_credentials(self, cluster_id: uuid.UUID) -> object:
        self.credential_reads.append(cluster_id)
        return self.clusters.get(cluster_id)


class _TargetStore:
    def __init__(self, targets: list[object]) -> None:
        self.targets = targets
        self.health: list[tuple[uuid.UUID, bool, str | None, uuid.UUID]] = []
        self.fail_record = False

    async def list(self, **_: object) -> list[object]:
        return self.targets

    async def record_health(
        self,
        target_id: uuid.UUID,
        *,
        healthy: bool,
        message: str | None,
        updated_by: uuid.UUID,
    ) -> object:
        if self.fail_record:
            raise ExecutionTargetNotFoundError(target_id)
        self.health.append((target_id, healthy, message, updated_by))
        target = next(item for item in self.targets if item.id == target_id)  # type: ignore[attr-defined]
        target.status = TargetStatus.ACTIVE if healthy else TargetStatus.DEGRADED
        target.status_message = None if healthy else message
        return target


class _ScriptedProbe:
    def __init__(self, results: dict[str, ProbeResult] | ProbeResult | Exception) -> None:
        self.results = results
        self.calls: list[str] = []

    async def probe(self, cluster: object, _credential: str, target: object) -> ProbeResult:
        del cluster
        self.calls.append(target.name)  # type: ignore[attr-defined]
        if isinstance(self.results, Exception):
            raise self.results
        if isinstance(self.results, ProbeResult):
            return self.results
        return self.results[target.name]  # type: ignore[attr-defined]


class _Resolver:
    def __init__(self, credential: str) -> None:
        self.credential = credential
        self.calls: list[tuple[object, str]] = []

    async def resolve(self, credential_ref: object, *, client_id: str) -> str:
        self.calls.append((credential_ref, client_id))
        return self.credential


def _monitor(
    targets: list[object],
    clusters: dict[uuid.UUID, object],
    probe: object,
    *,
    failure_threshold: int | None = None,
    success_threshold: int | None = None,
) -> tuple[ResourceMonitor, _TargetStore, _ClusterStore]:
    overrides: dict[str, float] = {}
    if failure_threshold is not None:
        overrides["resource_monitor_failure_threshold"] = failure_threshold
    if success_threshold is not None:
        overrides["resource_monitor_success_threshold"] = success_threshold
    ep_settings = settings(**overrides)
    target_store = _TargetStore(targets)
    cluster_store = _ClusterStore(clusters)
    resolver = _Resolver("resolved-token")
    monitor = ResourceMonitor(
        target_store,  # type: ignore[arg-type]
        cluster_store,  # type: ignore[arg-type]
        ep_settings,
        probe=probe,  # type: ignore[arg-type]
        credential_resolver=resolver,  # type: ignore[arg-type]
        interval=0.01,
    )
    return monitor, target_store, cluster_store


@pytest.mark.asyncio
async def test_hysteresis_requires_consecutive_failures_before_degraded() -> None:
    cluster = make_cluster()
    target = make_target(cluster)
    probe = _ScriptedProbe(ProbeResult(healthy=False, message="Kubernetes API timed out"))
    monitor, target_store, cluster_store = _monitor([target], {cluster.id: cluster}, probe)

    await monitor.scan()
    await monitor.scan()
    assert target_store.health == []

    await monitor.scan()
    assert target_store.health == [(target.id, False, "Kubernetes API timed out", RESOURCE_MONITOR_ACTOR_ID)]
    assert cluster_store.credential_reads == [cluster.id, cluster.id, cluster.id]


@pytest.mark.asyncio
async def test_monitor_resolves_the_cluster_reference_before_probing() -> None:
    cluster = make_cluster()
    cluster.source_client_id = "syntara-orchestration"
    target = make_target(cluster)
    resolver = _Resolver("resolved-token")

    class RecordingProbe:
        def __init__(self) -> None:
            self.credentials: list[str] = []

        async def probe(self, _cluster: object, credential: str, _target: object) -> ProbeResult:
            self.credentials.append(credential)
            return ProbeResult(healthy=True)

    probe = RecordingProbe()
    monitor = ResourceMonitor(
        _TargetStore([target]),  # type: ignore[arg-type]
        _ClusterStore({cluster.id: cluster}),  # type: ignore[arg-type]
        settings(),
        probe=probe,  # type: ignore[arg-type]
        interval=0.01,
        credential_resolver=resolver,  # type: ignore[arg-type]
    )

    await monitor.scan()

    assert resolver.calls == [(cluster.credential_ref, "syntara-orchestration")]
    assert probe.credentials == ["resolved-token"]


@pytest.mark.asyncio
async def test_single_success_does_not_clear_degraded() -> None:
    cluster = make_cluster()
    target = make_target(cluster, status=TargetStatus.DEGRADED, status_message="Kubernetes API timed out")
    probe = _ScriptedProbe(ProbeResult(healthy=True))
    monitor, target_store, _cluster_store = _monitor([target], {cluster.id: cluster}, probe)

    await monitor.scan()
    assert target_store.health == []

    await monitor.scan()
    assert target_store.health == [(target.id, True, None, RESOURCE_MONITOR_ACTOR_ID)]
    assert target.status is TargetStatus.ACTIVE


@pytest.mark.asyncio
async def test_success_resets_failure_streak() -> None:
    cluster = make_cluster()
    target = make_target(cluster)
    results = iter(
        [
            ProbeResult(healthy=False, message="Kubernetes API timed out"),
            ProbeResult(healthy=False, message="Kubernetes API timed out"),
            ProbeResult(healthy=True),
            ProbeResult(healthy=False, message="Kubernetes API timed out"),
        ]
    )

    class Alternating:
        async def probe(self, cluster: object, _credential: str, target: object) -> ProbeResult:
            del cluster, target
            return next(results)

    monitor, target_store, _cluster_store = _monitor([target], {cluster.id: cluster}, Alternating())
    for _ in range(4):
        await monitor.scan()
    assert target_store.health == []


@pytest.mark.asyncio
async def test_one_failing_target_does_not_prevent_probing_the_next() -> None:
    cluster = make_cluster()
    bad = make_target(cluster, name="bad")
    good = make_target(cluster, name="good")

    class MixedProbe:
        async def probe(self, cluster: object, _credential: str, target: object) -> ProbeResult:
            del cluster
            if target.name == "bad":  # type: ignore[attr-defined]
                message = "boom"
                raise RuntimeError(message)
            return ProbeResult(healthy=False, message="Namespace gone")

    monitor, target_store, _cluster_store = _monitor(
        [bad, good],
        {cluster.id: cluster},
        MixedProbe(),
        failure_threshold=1,
    )
    await monitor.scan()
    assert target_store.health == [(good.id, False, "Namespace gone", RESOURCE_MONITOR_ACTOR_ID)]


@pytest.mark.asyncio
async def test_skipped_targets_are_not_written() -> None:
    cluster = make_cluster()
    target = make_target(cluster)
    probe = _ScriptedProbe(ProbeResult(healthy=False, message="ignored", skipped=True))
    monitor, target_store, _cluster_store = _monitor(
        [target],
        {cluster.id: cluster},
        probe,
        failure_threshold=1,
    )
    await monitor.scan()
    assert target_store.health == []


@pytest.mark.asyncio
async def test_missing_cluster_skips_the_target() -> None:
    cluster = make_cluster()
    target = make_target(cluster)
    probe = _ScriptedProbe(ProbeResult(healthy=False, message="should not run"))
    monitor, target_store, _cluster_store = _monitor([target], {}, probe, failure_threshold=1)
    await monitor.scan()
    assert probe.calls == []
    assert target_store.health == []


@pytest.mark.asyncio
async def test_disappeared_target_does_not_abort_scan() -> None:
    cluster = make_cluster()
    target = make_target(cluster)
    probe = _ScriptedProbe(ProbeResult(healthy=False, message="gone"))
    monitor, target_store, _cluster_store = _monitor(
        [target],
        {cluster.id: cluster},
        probe,
        failure_threshold=1,
    )
    target_store.fail_record = True
    await monitor.scan()
    assert target_store.health == []


@pytest.mark.asyncio
async def test_worker_group_cancels_the_monitor_without_waiting_for_the_interval() -> None:
    cluster = make_cluster()
    target = make_target(cluster)
    scanned = asyncio.Event()

    class Probe:
        async def probe(self, *_args: object) -> ProbeResult:
            scanned.set()
            return ProbeResult(healthy=True, skipped=True)

    monitor, _target_store, _cluster_store = _monitor([target], {cluster.id: cluster}, Probe())
    monitor._interval = 3600
    with pytest.raises(asyncio.CancelledError):
        async with asyncio.TaskGroup() as group:
            task = group.create_task(monitor.run())
            await asyncio.wait_for(scanned.wait(), timeout=1)
            raise asyncio.CancelledError

    assert task.cancelled()
