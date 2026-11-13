"""Vanilla Kubernetes WorkerManager execution-start lifecycle."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable
    from uuid import UUID

import pytest
from syntara_plugin.runtime import ContainerResult

from execution_plane.config.settings import Settings
from execution_plane.worker_manager.vanilla_k8s import manager as manager_module
from execution_plane.worker_manager.vanilla_k8s.manager import WorkloadCancelledError
from execution_plane.worker_manager.vanilla_k8s.transport import TransportError
from tests.worker_manager.factories import (
    WorkStoreStub,
    vanilla_k8s_credential_resolvers,
    vanilla_k8s_manager,
    vanilla_k8s_resources,
)


@pytest.mark.asyncio
async def test_dispatch_marks_running_at_the_execute_submission_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings()
    async with vanilla_k8s_credential_resolvers(settings) as (cluster_resolver, workload_resolver):
        resources = vanilla_k8s_resources()
        work_store = WorkStoreStub()
        work_item = resources[2]

        async def resolve(_credential_ref: UUID, **_kwargs: object) -> str:
            return "resolved-token"

        monkeypatch.setattr(cluster_resolver, "resolve", resolve)

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
        manager = vanilla_k8s_manager(settings, cluster_resolver, workload_resolver, resources, work_store)

        result = await manager.dispatch(work_item)

    assert result["output"] == {}
    assert result["status_code"] == 0
    assert work_store.running_calls == [(work_item.id, work_item.claim_owner_id, work_item.claim_generation)]


@pytest.mark.asyncio
async def test_dispatch_context_binds_credential_resolution_to_the_work_item_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    settings = Settings()
    async with vanilla_k8s_credential_resolvers(settings) as (cluster_resolver, workload_resolver):
        resources = vanilla_k8s_resources()
        work_store = WorkStoreStub()
        work_item = resources[2]
        bindings: list[object] = []
        constructor_calls: list[dict[str, object]] = []

        async def resolve(_credential_ref: UUID, **_kwargs: object) -> str:
            return "resolved-token"

        async def resolve_workload(*_args: object, **_kwargs: object) -> dict[str, str]:
            return {}

        monkeypatch.setattr(cluster_resolver, "resolve", resolve)
        monkeypatch.setattr(workload_resolver, "resolve_workload", resolve_workload)

        class RecordingSDKResolver:
            def __init__(
                self,
                credential_bindings: list[object],
                credential_resolver: object,
                **kwargs: object,
            ) -> None:
                bindings.extend(credential_bindings)
                constructor_calls.append(kwargs)
                self.credential_resolver = credential_resolver

            def __call__(self, _context: object) -> dict[str, str]:
                return {}

        monkeypatch.setattr(manager_module, "SDKCredentialResolver", RecordingSDKResolver)
        manager = vanilla_k8s_manager(settings, cluster_resolver, workload_resolver, resources, work_store)

        context = await manager._dispatch_context(work_item)

        assert isinstance(context.credential_handler, RecordingSDKResolver)
        assert context.credential_handler.credential_resolver is workload_resolver
        assert bindings == []
        assert constructor_calls == [
            {
                "loop": asyncio.get_running_loop(),
                "parent_command_id": work_item.id,
                "attempt_id": f"{work_item.id}-{work_item.claim_generation}",
                "invocation_id": f"{work_item.id}-{work_item.claim_generation}",
                "workflow_run_id": "workflow-1",
                "step_revision_id": "script.v1",
            }
        ]


@pytest.mark.asyncio
async def test_execute_submission_aborts_when_cancellation_wins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings()
    async with vanilla_k8s_credential_resolvers(settings) as (cluster_resolver, workload_resolver):
        resources = vanilla_k8s_resources()
        work_store = WorkStoreStub(running_result=False)
        work_item = resources[2]

        async def resolve(_credential_ref: UUID, **_kwargs: object) -> str:
            return "resolved-token"

        monkeypatch.setattr(cluster_resolver, "resolve", resolve)

        def run_job(*, on_execute_submitted: Callable[[], None], **_kwargs: object) -> dict[str, Any]:
            try:
                on_execute_submitted()
            except TransportError as exc:
                exc.cancelled_confirmed = True
                raise
            pytest.fail("Execute must not be submitted after cancellation wins", pytrace=False)

        monkeypatch.setattr(manager_module, "run_job", run_job)
        manager = vanilla_k8s_manager(settings, cluster_resolver, workload_resolver, resources, work_store)

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
    async with vanilla_k8s_credential_resolvers(settings) as (cluster_resolver, workload_resolver):
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

        monkeypatch.setattr(cluster_resolver, "resolve", resolve)
        monkeypatch.setattr(manager_module, "run_job", run_job)
        if failure == "claim_refresh":
            monkeypatch.setattr(manager_module, "CLAIM_REFRESH_SECONDS", 0.01)
        manager = vanilla_k8s_manager(settings, cluster_resolver, workload_resolver, resources, WorkStore())
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
