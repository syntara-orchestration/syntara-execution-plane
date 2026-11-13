"""Vanilla Kubernetes WorkerManager execution-start lifecycle."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable
    from uuid import UUID

import pytest
from syntara_plugin.runtime import ContainerResult

from execution_plane.config.settings import Settings
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
    settings = Settings()
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
    settings = Settings()
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


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["shutdown", "callback_in_progress", "claim_refresh"])
async def test_dispatch_joins_transport_and_database_callbacks_before_exiting(  # noqa: C901, PLR0915 - Shutdown races cross the thread and event loop.
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    import asyncio
    import threading

    settings = Settings()
    resolver = CredentialResolver(settings)
    resources = vanilla_k8s_resources()
    transport_started = asyncio.Event()
    callback_started = asyncio.Event()
    release_callback = asyncio.Event()
    transport_finished = threading.Event()
    events: list[str] = []
    loop = asyncio.get_running_loop()
    message = "claim refresh failed"

    class WorkStore(WorkStoreStub):
        async def refresh_claim(self, *_args: object, **_kwargs: object) -> bool:
            raise RuntimeError(message)

        async def update_backend_resource(self, *_args: object, **_kwargs: object) -> None:
            callback_started.set()
            await release_callback.wait()
            events.append("database callback finished")

    async def resolve(_credential_ref: UUID, **_kwargs: object) -> str:
        return "resolved-token"

    def run_job(
        *,
        cancelled: threading.Event,
        on_resource: Callable[[str, str | None, str, str | None], None],
        **_kwargs: object,
    ) -> dict[str, Any]:
        try:
            loop.call_soon_threadsafe(transport_started.set)
            if failure != "callback_in_progress":
                assert cancelled.wait(timeout=2), "dispatch must signal transport cancellation"
            on_resource("attempt", "uid", "deleted", None)
            assert cancelled.wait(timeout=2)
            events.append("transport finished")
            transport_error = "transport stopped"
            raise TransportError(transport_error, submitted=True)
        finally:
            transport_finished.set()

    monkeypatch.setattr(resolver, "resolve", resolve)
    monkeypatch.setattr(manager_module, "run_job", run_job)
    if failure == "claim_refresh":
        monkeypatch.setattr(manager_module, "CLAIM_REFRESH_SECONDS", 0.01)
    manager = vanilla_k8s_manager(settings, resolver, resources, WorkStore())
    dispatch = asyncio.create_task(manager.dispatch(resources[2]))
    try:
        await asyncio.wait_for(transport_started.wait(), timeout=1)
        if failure == "callback_in_progress":
            await asyncio.wait_for(callback_started.wait(), timeout=1)
        if failure != "claim_refresh":
            dispatch.cancel()
        await asyncio.wait_for(callback_started.wait(), timeout=1)
        assert not dispatch.done()
        assert not transport_finished.is_set()
        if failure != "claim_refresh":
            # Repeated shutdown must not interrupt the join or cancel the DB callback.
            dispatch.cancel()
            await asyncio.sleep(0)
            assert not dispatch.done()
    finally:
        release_callback.set()
        if failure == "claim_refresh":
            with pytest.raises(RuntimeError, match=message):
                await asyncio.wait_for(dispatch, timeout=2)
        else:
            dispatch.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(dispatch, timeout=2)

    assert transport_finished.is_set()
    assert events == ["database callback finished", "transport finished"]


@pytest.mark.parametrize("stored_identity", ["absent", "matching", "different"])
async def test_stored_work_uses_row_identity_and_keeps_metadata_out_of_invocation(monkeypatch, stored_identity):
    """Pre-upgrade payloads execute; descriptive metadata cannot address another work item."""
    from uuid import uuid4

    settings = Settings()
    resolver = CredentialResolver(settings)
    resources = vanilla_k8s_resources()
    work_item = resources[2]
    assert "work_item_id" not in work_item.payload
    payload = {**work_item.payload, "metadata": {"work_item_id": str(uuid4()), "project_id": "project"}}
    if stored_identity != "absent":
        payload["work_item_id"] = str(work_item.id if stored_identity == "matching" else uuid4())
    work_item.payload = payload

    async def resolve(_credential_ref: UUID, **_kwargs: object) -> str:
        return "resolved-token"

    monkeypatch.setattr(resolver, "resolve", resolve)
    manager = vanilla_k8s_manager(settings, resolver, resources, WorkStoreStub())
    try:
        context = await manager._dispatch_context(work_item)
        assert context.identity == str(work_item.id)
        assert context.attempt_id == f"{work_item.id}-{work_item.claim_generation}"
        assert "metadata" not in context.invocation.model_dump()
        assert "project_id" not in context.invocation.runtime_context
    finally:
        await resolver.close()
