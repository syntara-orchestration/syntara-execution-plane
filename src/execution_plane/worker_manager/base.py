"""WorkerManager protocol — the swappable backend interface.

Concrete implementations (VanillaK8sWorkerManager, OpenShellWorkerManager) live
in submodules. The Task Executor selects an implementation at startup based on the
pool's backend_type.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable

    from execution_plane.models.work_item import WorkItem
    from execution_plane.work_item.work_item_store import WorkStore


class ExecutionStartRejectedError(Exception):
    """Cancellation won before the backend could submit Execute."""


class ExecutionStartPersistenceError(Exception):
    """EP could not durably record the execution-start boundary."""


class WorkerManager(Protocol):
    """Obtains a backend worker, dispatches one logical work item, and returns its result."""

    async def dispatch(self, work_item: WorkItem) -> dict[str, Any] | None:
        """Dispatch through the selected backend; application data moves over its node protocol."""
        ...


class WorkerManagerBase(ABC):
    """Coordinate WorkItem dispatch boundaries for concrete WorkerManagers."""

    def __init__(self, work_progress: WorkStore) -> None:
        """Bind the backend to the WorkItem state boundary."""
        self._work_progress = work_progress

    async def dispatch(self, work_item: WorkItem) -> dict[str, Any] | None:
        """Cross the claim-fenced external-dispatch boundary before backend work."""
        claim_owner_id = work_item.claim_owner_id
        if claim_owner_id is None:
            raise ValueError("Work item has no claim owner")  # noqa: TRY003, EM101
        if not await self._work_progress.mark_dispatched(
            work_item.id,
            claim_owner_id=claim_owner_id,
            claim_generation=work_item.claim_generation,
        ):
            return None
        return await self._dispatch(work_item)

    @abstractmethod
    async def _dispatch(self, work_item: WorkItem) -> dict[str, Any]:
        """Run backend-specific dispatch after DISPATCHED is durable."""

    def execution_start_callback(self, work_item: WorkItem) -> Callable[[], None]:
        """Return a thread-safe hook for the backend's Execute-submission boundary."""
        claim_owner_id = work_item.claim_owner_id
        if claim_owner_id is None:
            raise ValueError("Work item has no claim owner")  # noqa: TRY003, EM101
        loop = asyncio.get_running_loop()

        def mark_execution_started() -> None:
            future = asyncio.run_coroutine_threadsafe(
                self._work_progress.mark_running(
                    work_item.id,
                    claim_owner_id=claim_owner_id,
                    claim_generation=work_item.claim_generation,
                ),
                loop,
            )
            try:
                running = future.result(timeout=30)
            except Exception as exc:
                future.cancel()
                raise ExecutionStartPersistenceError from exc
            if not running:
                raise ExecutionStartRejectedError

        return mark_execution_started
