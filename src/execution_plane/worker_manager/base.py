"""Shared coordinator for staged backend execution and WorkItem persistence."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, final

import structlog
from pydantic import ValidationError

from execution_plane.models.work_item import WorkItemStatus
from execution_plane.store_errors import StaleClaimError
from execution_plane.worker_manager.claim import CancellationRequestedError, ClaimLease
from execution_plane.worker_manager.lifecycle import (
    BackendError,
    ExecutionResult,
    Invocation,
    ReleaseResult,
    RetryableDispatchError,
    WorkAttempt,
    WorkerAllocation,
    WorkItemPayloadError,
    WorkloadOutcomeUnknownError,
)
from execution_plane_client.commands.work import WorkExecute

if TYPE_CHECKING:
    from execution_plane.config import EPSettings
    from execution_plane.models.work_item import WorkItem
    from execution_plane.work_item.work_item_store import WorkStore

logger = structlog.stdlib.get_logger(__name__)


@dataclass
class _Dispatch:
    attempt: WorkAttempt
    lease: ClaimLease
    allocation: WorkerAllocation | None = None
    invocation: Invocation | None = None
    submission_attempted: bool = False
    persisted: bool = False
    result: dict[str, Any] | None = None
    error: Exception | None = None


class WorkerManager(ABC):
    """Own the claim, lifecycle transitions, cancellation, and terminal outbox.

    Implementations prepare allocations; the coordinator persists WorkItems.
    The final dispatch method provides the same ordering for every backend.
    """

    def __init__(self, work_progress: WorkStore, settings: EPSettings) -> None:
        """Bind lifecycle persistence and its timing policy."""
        self._work_progress = work_progress
        self._settings = settings

    @abstractmethod
    async def prepare(self, attempt: WorkAttempt, command: WorkExecute) -> WorkerAllocation:
        """Resolve backend configuration without allocating external resources."""

    @final
    async def dispatch(self, work_item: WorkItem) -> dict[str, Any] | None:
        """Run one fenced attempt, persisting observed results before normal release."""
        if work_item.claim_owner_id is None or work_item.execution_target_id is None:
            message = "Dispatch requires a claimed work item with an assigned target"
            raise ValueError(message)
        attempt = WorkAttempt(
            work_item.id, work_item.claim_owner_id, work_item.claim_generation, work_item.execution_target_id
        )
        if not await self._work_progress.mark_dispatched(
            attempt.work_item_id,
            claim_owner_id=attempt.claim_owner_id,
            claim_generation=attempt.claim_generation,
        ):
            return None
        state = _Dispatch(attempt, ClaimLease(self._work_progress, attempt))
        state.lease.start()
        shutting_down = False
        try:
            try:
                await self._execute(state, work_item.payload)
            except asyncio.CancelledError:
                shutting_down = True
                state.error = (
                    WorkloadOutcomeUnknownError("Controller stopped after invocation submission may have begun")
                    if state.submission_attempted
                    else RetryableDispatchError("Controller stopped before invocation submission")
                )
            except Exception as exc:  # noqa: BLE001 - every backend failure needs a durable outcome
                state.error = exc
                logger.warning(
                    "Worker lifecycle stage failed", work_item_id=attempt.identity, error_type=type(exc).__name__
                )
            finally:
                released = await self._release(state)
            if not state.persisted:
                await self._settle(state, released, shutting_down=shutting_down)
        finally:
            await state.lease.stop()
        if shutting_down:
            raise asyncio.CancelledError
        return state.result

    async def _execute(self, state: _Dispatch, payload: dict[str, Any]) -> None:
        try:
            command = WorkExecute.model_validate(payload)
        except ValidationError as exc:
            message = "Work item payload is not a valid work.execute command"
            raise WorkItemPayloadError(message) from exc
        state.allocation = await state.lease.run(self.prepare(state.attempt, command))
        await self._record_resource(state, "pending")
        await state.lease.run(state.allocation.allocate())
        await self._record_resource(state, "pending")
        await state.lease.run(state.allocation.wait_ready())
        # This read avoids submission when cancellation is already known. The
        # database and remote submission cannot be one atomic transaction.
        await state.lease.refresh()
        state.lease.check()
        state.submission_attempted = True
        state.invocation = await state.allocation.submit()
        try:
            running = await self._work_progress.mark_running(
                state.attempt.work_item_id,
                claim_owner_id=state.attempt.claim_owner_id,
                claim_generation=state.attempt.claim_generation,
            )
        except StaleClaimError:
            raise
        except Exception:  # Keep the live handle and try to retain its result.
            logger.exception("Could not persist execution start", work_item_id=state.attempt.identity)
        else:
            if not running:
                await state.lease.refresh()
                if state.lease.error is not None:
                    raise state.lease.error
                if not state.lease.cancel_requested:
                    message = "WorkItem no longer accepts execution progress"
                    raise WorkloadOutcomeUnknownError(message)
                # Execute was issued. Cancellation now operates on the live
                # invocation, even though RUNNING could not replace CANCEL_REQUESTED.
        outcome = await self._wait_result(state.invocation, state.lease)
        await self._persist(state, outcome)

    async def _wait_result(self, invocation: Invocation, lease: ClaimLease) -> ExecutionResult:
        result = asyncio.create_task(invocation.result())
        interrupted = asyncio.create_task(lease.interrupted.wait())
        try:
            done, _ = await asyncio.wait({result, interrupted}, return_when=asyncio.FIRST_COMPLETED)
            if result in done:
                return result.result()
            if lease.error is not None:
                raise lease.error
            await invocation.cancel()
            try:
                return await asyncio.wait_for(asyncio.shield(result), timeout=self._settings.node_grace_seconds)
            except TimeoutError:
                raise CancellationRequestedError from None
        finally:
            interrupted.cancel()
            result.cancel()
            with suppress(asyncio.CancelledError):
                await interrupted
            with suppress(asyncio.CancelledError, Exception):
                await result

    async def _persist(self, state: _Dispatch, outcome: ExecutionResult) -> None:
        await self._work_progress.set_result(
            state.attempt.work_item_id,
            outcome.result,
            outcome.status,
            claim_owner_id=state.attempt.claim_owner_id,
            claim_generation=state.attempt.claim_generation,
        )
        state.persisted = True
        state.result = outcome.result

    async def _release(self, state: _Dispatch) -> ReleaseResult:
        if state.invocation is not None and not state.persisted:
            with suppress(Exception):
                await state.invocation.cancel()
        if state.allocation is None:
            return ReleaseResult("not_required", execution_stopped=True)
        try:
            released = await state.allocation.release()
        except Exception:  # Cleanup cannot replace an observed terminal result.
            logger.exception("Worker release failed", work_item_id=state.attempt.identity)
            released = ReleaseResult("failed", execution_stopped=False, error="Worker release failed")
        try:
            await self._record_resource(state, released.status, released.error)
        except Exception:
            logger.exception("Could not persist worker cleanup state", work_item_id=state.attempt.identity)
        return released

    async def _settle(self, state: _Dispatch, released: ReleaseResult, *, shutting_down: bool) -> None:
        if isinstance(state.error, StaleClaimError):
            raise state.error
        await state.lease.refresh()
        if isinstance(state.lease.error, StaleClaimError):
            raise state.lease.error
        if (
            state.lease.cancel_requested
            and released.execution_stopped
            and (not state.submission_attempted or state.invocation is not None)
            and not isinstance(state.error, WorkloadOutcomeUnknownError)
        ):
            await self._persist(
                state,
                ExecutionResult(
                    WorkItemStatus.CANCELLED,
                    {"cancelled": True, "execution_started": state.invocation is not None},
                ),
            )
        elif (
            isinstance(state.error, RetryableDispatchError)
            and not state.submission_attempted
            and released.execution_stopped
        ):
            interrupted_backoff = False
            if not shutting_down:
                try:
                    await asyncio.sleep(self._settings.dispatch_retry_backoff_seconds)
                except asyncio.CancelledError:
                    interrupted_backoff = True
            item = await self._work_progress.requeue_pre_execute_failure(
                state.attempt.work_item_id,
                claim_owner_id=state.attempt.claim_owner_id,
                claim_generation=state.attempt.claim_generation,
            )
            if item.status is WorkItemStatus.CANCEL_REQUESTED:
                # Settle cancellation using evidence that submission never
                # occurred and release completed.
                await self._persist(
                    state,
                    ExecutionResult(WorkItemStatus.CANCELLED, {"cancelled": True, "execution_started": False}),
                )
            if interrupted_backoff:
                raise asyncio.CancelledError
        elif isinstance(state.error, (WorkItemPayloadError, BackendError)) and not state.submission_attempted:
            await self._persist(
                state,
                ExecutionResult(
                    WorkItemStatus.FAILED,
                    {
                        "error": str(state.error),
                        "error_type": "WorkItemPayloadError"
                        if isinstance(state.error, WorkItemPayloadError)
                        else "NodeTransportError",
                    },
                ),
            )
        else:
            reason = (
                str(state.error)
                if isinstance(state.error, (BackendError, WorkloadOutcomeUnknownError))
                else "Worker did not produce a durable terminal result; execution outcome is unknown"
            )
            await self._work_progress.mark_reconciliation_required(
                state.attempt.work_item_id,
                reason,
                claim_owner_id=state.attempt.claim_owner_id,
                claim_generation=state.attempt.claim_generation,
            )
            logger.warning("Work item requires reconciliation", work_item_id=state.attempt.identity)

    async def _record_resource(self, state: _Dispatch, status: str, error: str | None = None) -> None:
        resource = state.allocation.resource if state.allocation is not None else None
        if resource is not None:
            await self._work_progress.update_backend_resource(
                state.attempt.work_item_id,
                resource_name=resource.name,
                resource_uid=resource.uid,
                cleanup_status=status,
                cleanup_error=error,
                claim_owner_id=state.attempt.claim_owner_id,
                claim_generation=state.attempt.claim_generation,
            )
