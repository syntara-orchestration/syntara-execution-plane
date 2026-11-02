"""Shared WorkerManager dispatch and execution-start coordination."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest

from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.worker_manager.base import (
    ExecutionStartRejectedError,
    WorkerManagerBase,
)


class _WorkStore:
    def __init__(self, *, dispatch_result: bool = True, running_result: bool = True) -> None:
        self.dispatch_result = dispatch_result
        self.running_result = running_result
        self.dispatch_calls: list[tuple[UUID, UUID, int]] = []
        self.running_calls: list[tuple[UUID, UUID, int]] = []

    async def mark_dispatched(
        self,
        item_id: UUID,
        *,
        claim_owner_id: UUID | None,
        claim_generation: int | None,
    ) -> bool:
        assert claim_owner_id is not None
        assert claim_generation is not None
        self.dispatch_calls.append((item_id, claim_owner_id, claim_generation))
        return self.dispatch_result

    async def mark_running(
        self,
        item_id: UUID,
        *,
        claim_owner_id: UUID,
        claim_generation: int,
    ) -> bool:
        self.running_calls.append((item_id, claim_owner_id, claim_generation))
        return self.running_result


class _Manager(WorkerManagerBase):
    async def _dispatch(self, work_item: WorkItem) -> dict[str, Any]:
        return {"work_item_id": work_item.id}


def _work_item() -> WorkItem:
    return WorkItem(
        id=UUID(int=1),
        client_id="test-client",
        status=WorkItemStatus.CLAIMED,
        created_at=datetime.now(UTC),
        claim_owner_id=UUID(int=2),
        claim_generation=3,
    )


@pytest.mark.asyncio
async def test_base_dispatch_marks_dispatched_before_backend_work() -> None:
    store = _WorkStore()
    manager = _Manager(store)
    item = _work_item()

    result = await manager.dispatch(item)

    assert result == {"work_item_id": item.id}
    assert store.dispatch_calls == [(item.id, item.claim_owner_id, item.claim_generation)]


@pytest.mark.asyncio
async def test_base_dispatch_returns_none_when_cancellation_wins() -> None:
    store = _WorkStore(dispatch_result=False)
    manager = _Manager(store)
    item = _work_item()

    assert await manager.dispatch(item) is None


@pytest.mark.asyncio
async def test_execution_start_callback_rejects_cancellation_race() -> None:
    store = _WorkStore(running_result=False)
    manager = _Manager(store)
    item = _work_item()

    callback = manager.execution_start_callback(item)

    with pytest.raises(ExecutionStartRejectedError):
        await asyncio.to_thread(callback)
    assert store.running_calls == [(item.id, item.claim_owner_id, item.claim_generation)]
