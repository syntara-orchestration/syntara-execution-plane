"""Tests for the SDK credential resolver."""

from __future__ import annotations

import asyncio
import threading
import time
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID, uuid4

import pytest
from syntara_plugin.runtime import CredentialAccessError, CredentialRequestContext

from execution_plane.credential_resolution.errors import CredentialResolutionError
from execution_plane.credential_resolution.sdk_resolver import SDKCredentialResolver
from execution_plane.credential_resolution.workload import WorkloadCredentialResolver
from execution_plane_client.commands.work import WorkCredentialBinding

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Mapping


def _context(
    name: str = "acme.echo.api-token", *, cancellation: threading.Event | None = None
) -> CredentialRequestContext:
    return CredentialRequestContext(
        invocation_id="work-1",
        name=name,
        cancellation=cancellation or threading.Event(),
        deadline=time.monotonic() + 5,
    )


def _binding(recipe_id: str = "acme.echo.api-token", credential_ref: UUID | None = None) -> WorkCredentialBinding:
    return WorkCredentialBinding(
        credential_recipe_id=recipe_id,
        credential_ref=credential_ref or uuid4(),
    )


async def _fail(*_args: object, **_kwargs: object) -> dict[str, str]:
    message = "resolution failed"
    raise CredentialResolutionError(message)


class _FakeWorkloadResolver(WorkloadCredentialResolver):
    def __init__(self, resolve: Callable[..., Coroutine[Any, Any, Mapping[str, str]]]) -> None:
        self._resolve = resolve

    async def resolve_workload(
        self,
        credential_ref: UUID,
        credential_recipe_id: str,
        **kwargs: object,
    ) -> Mapping[str, str]:
        return await self._resolve(credential_ref, credential_recipe_id, **kwargs)


def _resolver(
    bindings: list[WorkCredentialBinding],
    resolve: Callable[..., Coroutine[Any, Any, Mapping[str, str]]],
    loop: asyncio.AbstractEventLoop,
    *,
    parent_command_id: UUID | None = None,
    max_concurrent_reads: int = 2,
) -> SDKCredentialResolver:
    return SDKCredentialResolver(
        bindings,
        _FakeWorkloadResolver(resolve),
        loop=loop,
        parent_command_id=parent_command_id or uuid4(),
        attempt_id="work-1-attempt-2",
        invocation_id="work-1",
        workflow_run_id="workflow-1",
        step_revision_id="script.v1",
        max_concurrent_reads=max_concurrent_reads,
    )


def _run_on_loop[T](body: Callable[[asyncio.AbstractEventLoop], T]) -> T:
    loop = asyncio.new_event_loop()
    outcome: dict[str, object] = {}
    finished = threading.Event()

    def run_body() -> None:
        try:
            outcome["value"] = body(loop)
        except BaseException as error:
            outcome["error"] = error
        finally:
            finished.set()

    worker = threading.Thread(target=run_body, name="sdk-resolver-test-body", daemon=True)
    worker.start()

    def stop_when_finished() -> None:
        if finished.is_set():
            loop.stop()
        else:
            loop.call_later(0.01, stop_when_finished)

    loop.call_soon(stop_when_finished)
    loop.run_forever()
    worker.join(timeout=5)
    loop.close()

    error = cast("BaseException | None", outcome.get("error"))
    if error is not None:
        raise error
    return cast("T", outcome.get("value"))


def test_credential_resolver_rejects_unbound_recipe_ids_as_missing() -> None:
    def body(loop: asyncio.AbstractEventLoop) -> str:
        resolver = _resolver([_binding()], _fail, loop)
        with pytest.raises(CredentialAccessError) as error:
            resolver(_context("acme.echo.workspace-token"))
        return error.value.reason

    assert _run_on_loop(body) == "missing"


def test_credential_resolver_rejects_bound_recipes_when_resolution_fails() -> None:
    def body(loop: asyncio.AbstractEventLoop) -> str:
        resolver = _resolver([_binding()], _fail, loop)
        with pytest.raises(CredentialAccessError) as error:
            resolver(_context())
        return error.value.reason

    assert _run_on_loop(body) == "unavailable"


def test_credential_resolver_dispatches_an_invocation_scoped_workload_command() -> None:
    parent_command_id = uuid4()
    credential_ref = uuid4()
    calls: list[dict[str, object]] = []

    async def resolve(credential_ref: UUID, recipe_id: str, **kwargs: object) -> dict[str, str]:
        calls.append({"credential_ref": credential_ref, "recipe_id": recipe_id, **kwargs})
        return {"token": "resolved-token"}

    def body(loop: asyncio.AbstractEventLoop) -> Mapping[str, str]:
        resolver = _resolver(
            [_binding(credential_ref=credential_ref)],
            resolve,
            loop,
            parent_command_id=parent_command_id,
        )
        return resolver(_context())

    credential = _run_on_loop(body)
    assert credential == {"token": "resolved-token"}
    assert calls == [
        {
            "credential_ref": credential_ref,
            "recipe_id": "acme.echo.api-token",
            "parent_command_id": parent_command_id,
            "attempt_id": "work-1-attempt-2",
            "workflow_run_id": "workflow-1",
            "step_revision_id": "script.v1",
        }
    ]


def test_credential_resolver_discards_a_late_response_after_cancellation() -> None:
    cancellation = threading.Event()
    response_ready = threading.Event()
    request_started = threading.Event()

    async def resolve(*_args: object, **_kwargs: object) -> dict[str, str]:
        request_started.set()
        for _ in range(500):
            if response_ready.is_set():
                break
            await asyncio.sleep(0.01)
        return {"token": "must-not-be-delivered"}

    def body(loop: asyncio.AbstractEventLoop) -> str:
        resolver = _resolver([_binding()], resolve, loop)
        results: list[CredentialAccessError] = []
        reader = threading.Thread(
            target=lambda: results.append(_capture_error(resolver, _context(cancellation=cancellation))),
            name="sdk-resolver-late-read",
        )
        reader.start()
        assert request_started.wait(timeout=5)
        cancellation.set()
        response_ready.set()
        reader.join(timeout=5)
        assert not reader.is_alive()
        return results[0].reason

    assert _run_on_loop(body) == "expired"


def test_credential_resolver_bounds_concurrent_reads() -> None:
    active = 0
    peak = 0
    lock = threading.Lock()

    async def resolve(*_args: object, **_kwargs: object) -> dict[str, str]:
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        await asyncio.sleep(0.02)
        with lock:
            active -= 1
        return {"token": "resolved-token"}

    def body(loop: asyncio.AbstractEventLoop) -> list[Mapping[str, str]]:
        resolver = _resolver(
            [_binding(f"acme.echo.token-{index}") for index in range(4)],
            resolve,
            loop,
            max_concurrent_reads=2,
        )
        results: list[Mapping[str, str]] = []
        threads = [
            threading.Thread(
                target=lambda index=index: results.append(resolver(_context(f"acme.echo.token-{index}"))),
                name=f"sdk-resolver-read-{index}",
            )
            for index in range(4)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
            assert not thread.is_alive()
        return results

    results = _run_on_loop(body)
    assert results == [{"token": "resolved-token"}] * 4
    assert peak == 2


def _capture_error(resolver: SDKCredentialResolver, context: CredentialRequestContext) -> CredentialAccessError:
    try:
        resolver(context)
    except CredentialAccessError as error:
        return error
    message = "credential resolution should have failed"
    raise AssertionError(message)
