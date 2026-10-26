"""Backend-independent lifecycle ordering and cancellation races."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from execution_plane.config import EPSettings
from execution_plane.models.work_item import WorkItemStatus
from execution_plane.store_errors import StaleClaimError
from execution_plane.worker_manager.base import WorkerManager
from execution_plane.worker_manager.lifecycle import (
    BackendError,
    BackendResource,
    ExecutionResult,
    Invocation,
    ReleaseResult,
    RetryableDispatchError,
    WorkAttempt,
    WorkerAllocation,
)
from tests.worker_manager.factories import WorkStoreStub, vanilla_k8s_resources

if TYPE_CHECKING:
    from execution_plane_client.commands.work import WorkExecute


class _Invocation(Invocation):
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.cancelled = False
        self.wait_for_cancel = False
        self.cancel_observed = asyncio.Event()
        self.error: Exception | None = None

    async def result(self) -> ExecutionResult:
        self.events.append("result")
        if self.wait_for_cancel:
            await self.cancel_observed.wait()
        if self.error is not None:
            raise self.error
        return ExecutionResult(WorkItemStatus.COMPLETED, {"output": {"stdout": "done"}})

    async def cancel(self) -> None:
        self.events.append("cancel")
        self.cancelled = True
        self.cancel_observed.set()


class _Allocation(WorkerAllocation):
    def __init__(self, store: WorkStoreStub) -> None:
        self.store = store
        self.invocation = _Invocation(store.events)
        self.error: Exception | None = None
        self.release_result = ReleaseResult("complete", execution_stopped=True)
        self.cancel_before_submit = False
        self.lose_claim_before_submit = False
        self.resource_uid: str | None = None

    @property
    def resource(self) -> BackendResource:
        return BackendResource("worker", self.resource_uid)

    async def allocate(self) -> None:
        self.store.events.append("allocate")
        self.resource_uid = "worker-uid"

    async def wait_ready(self) -> None:
        self.store.events.append("ready")
        if self.cancel_before_submit:
            self.store.status = WorkItemStatus.CANCEL_REQUESTED
        if self.lose_claim_before_submit:
            self.store.lost = True
        if self.error is not None:
            raise self.error

    async def submit(self) -> Invocation:
        self.store.events.append("submit")
        return self.invocation

    async def release(self) -> ReleaseResult:
        self.store.events.append("release")
        return self.release_result


class _Manager(WorkerManager):
    def __init__(self, store: WorkStoreStub, allocation: _Allocation) -> None:
        super().__init__(
            store,
            EPSettings().model_copy(
                update={"dispatch_retry_backoff_seconds": 0},
            ),
        )
        self.allocation = allocation

    async def prepare(self, attempt: WorkAttempt, command: WorkExecute) -> WorkerAllocation:
        self.allocation.store.events.append("prepare")
        return self.allocation


@pytest.mark.asyncio
async def test_submission_precedes_running_and_terminal_persistence_precedes_release() -> None:
    store = WorkStoreStub()
    allocation = _Allocation(store)
    item = vanilla_k8s_resources()[2]

    result = await _Manager(store, allocation).dispatch(item)

    assert result == {"output": {"stdout": "done"}}
    assert store.events == [
        "dispatched",
        "prepare",
        "resource_pending",
        "allocate",
        "resource_pending",
        "ready",
        "submit",
        "running",
        "result",
        "completed",
        "release",
        "resource_complete",
    ]
    assert store.running_calls == [(item.id, item.claim_owner_id, item.claim_generation)]


@pytest.mark.asyncio
async def test_cancel_before_dispatch_prevents_backend_work() -> None:
    store = WorkStoreStub(dispatch_result=False)
    allocation = _Allocation(store)

    assert await _Manager(store, allocation).dispatch(vanilla_k8s_resources()[2]) is None
    assert store.events == ["dispatched"]


@pytest.mark.asyncio
async def test_cancellation_before_submission_requires_release_confirmation() -> None:
    store = WorkStoreStub()
    allocation = _Allocation(store)
    allocation.cancel_before_submit = True

    await _Manager(store, allocation).dispatch(vanilla_k8s_resources()[2])

    assert "submit" not in store.events
    assert store.results == [(WorkItemStatus.CANCELLED, {"cancelled": True, "execution_started": False})]
    assert store.events.index("release") < store.events.index("cancelled")


@pytest.mark.asyncio
async def test_cancellation_winning_running_write_still_observes_terminal_result() -> None:
    store = WorkStoreStub(running_result=False)
    allocation = _Allocation(store)
    allocation.invocation.wait_for_cancel = True

    await _Manager(store, allocation).dispatch(vanilla_k8s_resources()[2])

    assert allocation.invocation.cancelled
    assert store.status is WorkItemStatus.COMPLETED
    assert store.requeues == 0


@pytest.mark.asyncio
async def test_missing_result_after_cancellation_preserves_submission_fact() -> None:
    store = WorkStoreStub(running_result=False)
    allocation = _Allocation(store)
    allocation.invocation.wait_for_cancel = True
    allocation.invocation.error = BackendError("Stream stopped")

    await _Manager(store, allocation).dispatch(vanilla_k8s_resources()[2])

    assert store.results == [(WorkItemStatus.CANCELLED, {"cancelled": True, "execution_started": True})]
    assert store.events.index("release") < store.events.index("cancelled")
    assert store.requeues == 0


@pytest.mark.asyncio
async def test_start_persistence_failure_keeps_the_invocation_result() -> None:
    store = WorkStoreStub()
    store.running_error = RuntimeError("Database unavailable")
    allocation = _Allocation(store)

    await _Manager(store, allocation).dispatch(vanilla_k8s_resources()[2])

    assert store.status is WorkItemStatus.COMPLETED
    assert store.requeues == 0


@pytest.mark.asyncio
async def test_retryable_failure_after_submission_never_requeues() -> None:
    store = WorkStoreStub()
    allocation = _Allocation(store)
    allocation.invocation.error = RetryableDispatchError("Misclassified result failure")

    await _Manager(store, allocation).dispatch(vanilla_k8s_resources()[2])

    assert store.status is WorkItemStatus.RECONCILIATION_REQUIRED
    assert store.requeues == 0


@pytest.mark.asyncio
async def test_pre_submission_retry_releases_resources_first() -> None:
    store = WorkStoreStub()
    allocation = _Allocation(store)
    allocation.error = RetryableDispatchError("Node unavailable")

    await _Manager(store, allocation).dispatch(vanilla_k8s_resources()[2])

    assert store.requeues == 1
    assert store.events.index("release") < store.events.index("requeue")
    assert "submit" not in store.events


@pytest.mark.asyncio
async def test_unconfirmed_cancellation_requires_reconciliation() -> None:
    store = WorkStoreStub()
    allocation = _Allocation(store)
    allocation.cancel_before_submit = True
    allocation.release_result = ReleaseResult("failed", execution_stopped=False)

    await _Manager(store, allocation).dispatch(vanilla_k8s_resources()[2])

    assert store.status is WorkItemStatus.RECONCILIATION_REQUIRED
    assert not store.results


@pytest.mark.asyncio
async def test_claim_loss_prevents_submission_and_terminal_mutations() -> None:
    store = WorkStoreStub()
    allocation = _Allocation(store)
    allocation.lose_claim_before_submit = True

    with pytest.raises(StaleClaimError):
        await _Manager(store, allocation).dispatch(vanilla_k8s_resources()[2])

    assert "submit" not in store.events
    assert "release" in store.events
    assert not store.results


@pytest.mark.asyncio
async def test_shutdown_before_submission_releases_and_requeues() -> None:
    store = WorkStoreStub()
    entered = asyncio.Event()

    class AllocatingWorker(_Allocation):
        async def allocate(self) -> None:
            entered.set()
            await asyncio.Event().wait()

    allocation = AllocatingWorker(store)
    task = asyncio.create_task(_Manager(store, allocation).dispatch(vanilla_k8s_resources()[2]))
    await entered.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert store.status is WorkItemStatus.PENDING
    assert store.events.index("release") < store.events.index("requeue")
    assert "submit" not in store.events


@pytest.mark.asyncio
async def test_shutdown_after_submission_releases_and_requires_reconciliation() -> None:
    store = WorkStoreStub()
    entered = asyncio.Event()

    class RunningInvocation(_Invocation):
        async def result(self) -> ExecutionResult:
            entered.set()
            await asyncio.Event().wait()
            raise AssertionError

    allocation = _Allocation(store)
    allocation.invocation = RunningInvocation(store.events)
    task = asyncio.create_task(_Manager(store, allocation).dispatch(vanilla_k8s_resources()[2]))
    await entered.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert allocation.invocation.cancelled
    assert store.status is WorkItemStatus.RECONCILIATION_REQUIRED
    assert "release" in store.events
    assert store.requeues == 0


@pytest.mark.asyncio
async def test_cleanup_failure_does_not_replace_a_persisted_execution_result() -> None:
    store = WorkStoreStub()
    allocation = _Allocation(store)
    allocation.release_result = ReleaseResult("failed", execution_stopped=False, error="Cleanup unconfirmed")

    await _Manager(store, allocation).dispatch(vanilla_k8s_resources()[2])

    assert store.status is WorkItemStatus.COMPLETED
    assert "resource_failed" in store.events
    assert not store.reconciliations
