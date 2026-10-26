"""Lease maintenance and cancellation observation for a dispatch attempt."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from execution_plane.store_errors import StaleClaimError

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from execution_plane.work_item.work_item_store import WorkStore
    from execution_plane.worker_manager.lifecycle import WorkAttempt


class CancellationRequestedError(Exception):
    """The caller requested cancellation before a stage could finish."""


class ClaimLease:
    """Renew one claim until persistence and resource release are finished."""

    def __init__(self, store: WorkStore, attempt: WorkAttempt) -> None:
        """Bind immutable attempt identity to its lifecycle store."""
        self.store = store
        self.attempt = attempt
        self.interrupted = asyncio.Event()
        self.cancel_requested = False
        self.error: Exception | None = None
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Start renewal before credential resolution or external allocation."""
        self._task = asyncio.create_task(self._renew(), name=f"lease-{self.attempt.attempt_id}")

    async def stop(self) -> None:
        """Join the lease task before its store is disposed."""
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def refresh(self) -> None:
        """Renew and remember cancellation or ownership loss."""
        try:
            requested = await self.store.refresh_claim(
                self.attempt.work_item_id,
                claim_owner_id=self.attempt.claim_owner_id,
                claim_generation=self.attempt.claim_generation,
            )
            if requested is None:
                self.error = StaleClaimError("Worker claim is no longer current")
            elif requested:
                self.cancel_requested = True
        except Exception as exc:  # noqa: BLE001 - stop dispatch when lease maintenance fails
            self.error = exc
        if self.error is not None or self.cancel_requested:
            self.interrupted.set()

    def check(self) -> None:
        """Prevent a new external stage after cancellation or claim loss."""
        if self.error is not None:
            raise self.error
        if self.cancel_requested:
            raise CancellationRequestedError

    async def run[T](self, operation: Coroutine[Any, Any, T]) -> T:
        """Interrupt a pre-submission stage and join it before resource release."""
        if self.interrupted.is_set():
            operation.close()
            self.check()
        task = asyncio.create_task(operation)
        interrupted = asyncio.create_task(self.interrupted.wait())
        try:
            done, _ = await asyncio.wait({task, interrupted}, return_when=asyncio.FIRST_COMPLETED)
            if task in done:
                return task.result()
            self.check()
            message = "Claim monitoring stopped without a reason"
            raise RuntimeError(message)
        finally:
            interrupted.cancel()
            task.cancel()
            with suppress(asyncio.CancelledError):
                await interrupted
            # Retrieve failures even if interruption and completion raced.
            with suppress(asyncio.CancelledError, Exception):
                await task

    async def _renew(self) -> None:
        while True:
            await self.refresh()
            if self.error is not None:
                return
            await asyncio.sleep(5)
