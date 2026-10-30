"""Vanilla Kubernetes WorkerManager execution-start lifecycle."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable
    from uuid import UUID

import pytest
from syntara_plugin.runtime import ContainerResult

from execution_plane.config import EPSettings
from execution_plane.credential_resolution.resolver import CredentialResolver
from execution_plane.worker_manager.vanilla_k8s import manager as manager_module
from execution_plane.worker_manager.vanilla_k8s.manager import WorkloadCancelledError
from execution_plane.worker_manager.vanilla_k8s.transport import TransportError
from tests.worker_manager.factories import (
    WorkStoreStub,
    vanilla_k8s_manager,
    vanilla_k8s_resources,
)


@pytest.mark.asyncio
async def test_dispatch_marks_running_at_the_execute_submission_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = EPSettings()
    resolver = CredentialResolver(settings)
    resources = vanilla_k8s_resources()
    work_store = WorkStoreStub()
    work_item = resources[2]

    async def resolve(_credential_ref: UUID, **_kwargs: object) -> str:
        return "resolved-token"

    monkeypatch.setattr(resolver, "resolve", resolve)

    def run_job(*, on_execute_submitted: Callable[[], None], **_kwargs: object) -> dict[str, Any]:
        on_execute_submitted()
        return {
            "kind": "result",
            "result": ContainerResult(
                "sha256:" + "a" * 64,
                output={},
                status_code=0,
                error_code="",
                error_message="",
            ),
        }

    monkeypatch.setattr(manager_module, "run_job", run_job)
    manager = vanilla_k8s_manager(settings, resolver, resources, work_store)

    result = await manager.dispatch(work_item)

    assert result["output"] == {}
    assert result["status_code"] == 0
    assert work_store.running_calls == [(work_item.id, work_item.claim_owner_id, work_item.claim_generation)]


@pytest.mark.asyncio
async def test_execute_submission_aborts_when_cancellation_wins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = EPSettings()
    resolver = CredentialResolver(settings)
    resources = vanilla_k8s_resources()
    work_store = WorkStoreStub(running_result=False)
    work_item = resources[2]

    async def resolve(_credential_ref: UUID, **_kwargs: object) -> str:
        return "resolved-token"

    monkeypatch.setattr(resolver, "resolve", resolve)

    def run_job(*, on_execute_submitted: Callable[[], None], **_kwargs: object) -> dict[str, Any]:
        try:
            on_execute_submitted()
        except TransportError as exc:
            exc.cancelled_confirmed = True
            raise
        pytest.fail("Execute must not be submitted after cancellation wins", pytrace=False)

    monkeypatch.setattr(manager_module, "run_job", run_job)
    manager = vanilla_k8s_manager(settings, resolver, resources, work_store)

    with pytest.raises(WorkloadCancelledError) as error:
        await manager.dispatch(work_item)

    assert error.value.execution_started is False
