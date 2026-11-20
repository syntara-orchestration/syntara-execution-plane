"""Tests for terminal Kubernetes pre-execution failures."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from execution_plane.models.work_item import WorkItem
from execution_plane.work_item.work_item_store import WorkStore
from execution_plane.worker_manager.vanilla_k8s.manager import NodeExecutionError
from execution_plane.worker_manager.vanilla_k8s.transport import pre_execution_failure_reason
from execution_plane_client.models.work_item import WorkItemStatus
from execution_plane_client.types import BackendType

FAILURE_MESSAGE = "Kubernetes worker Pod could not be scheduled (Unschedulable)"


def _pod(*, conditions: list[SimpleNamespace], container_statuses: list[SimpleNamespace]) -> SimpleNamespace:
    return SimpleNamespace(
        status=SimpleNamespace(
            conditions=conditions,
            container_statuses=container_statuses,
        )
    )


def test_unschedulable_pod_is_a_pre_execution_failure() -> None:
    pod = _pod(
        conditions=[
            SimpleNamespace(
                type="PodScheduled",
                status="False",
                reason="Unschedulable",
            )
        ],
        container_statuses=[],
    )

    assert pre_execution_failure_reason(pod) == "Kubernetes worker Pod could not be scheduled (Unschedulable)"


def test_image_pull_backoff_is_a_pre_execution_failure() -> None:
    pod = _pod(
        conditions=[],
        container_statuses=[
            SimpleNamespace(
                state=SimpleNamespace(
                    waiting=SimpleNamespace(reason="ImagePullBackOff"),
                )
            )
        ],
    )

    assert pre_execution_failure_reason(pod) == "Kubernetes worker image could not be pulled (ImagePullBackOff)"


@pytest.mark.asyncio
async def test_terminal_pre_execution_error_fails_work_item() -> None:
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
    manager = Mock()
    manager.dispatch = AsyncMock(side_effect=NodeExecutionError(FAILURE_MESSAGE, error_type="NodeTransportError"))
    work_store = AsyncMock(spec=WorkStore)
    target_store = Mock()
    target_store.get = AsyncMock(return_value=target)
    worker_managers = Mock()
    worker_managers.get = Mock(return_value=manager)

    await worker._process_item(
        item,
        work_store,
        target_store,
        worker_managers,
        Mock(),
    )

    work_store.set_result.assert_awaited_once_with(
        item.id,
        {"error": FAILURE_MESSAGE, "error_type": "NodeTransportError"},
        WorkItemStatus.FAILED,
        claim_owner_id=item.claim_owner_id,
        claim_generation=item.claim_generation,
    )
