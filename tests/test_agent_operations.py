"""Tests for invocation-scoped AO agent operation callbacks."""

from __future__ import annotations

import asyncio
import threading
import time
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID, uuid4

import httpx
import pytest
from syntara_plugin.runtime import HostOperationError
from syntara_plugin.runtime.container import HostOperationRequestContext

from execution_plane.agent_operations import AgentOperationResolver
from execution_plane_client.commands.agent import AgentOperation, AgentOperationCommandResult
from execution_plane_client.commands.results import TargetHealthCommandResult, TransportReceipt

if TYPE_CHECKING:
    from collections.abc import Callable

    from execution_plane_client.protocol import MessageEnvelope


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        service_jwt_audience="execution-plane",
        ao_client_id="automation-orchestrator",
        credential_resolution_callback_url="https://ao.example/callback",
        credential_resolution_callback_ca_cert_path="/certs/ca.pem",
        credential_resolution_callback_cert_path="/certs/client.pem",
        credential_resolution_callback_key_path="/certs/client-key.pem",
    )


def _context(
    *,
    operation: str = "agent.model.turn",
    cancellation: threading.Event | None = None,
    deadline: float | None = None,
) -> HostOperationRequestContext:
    return HostOperationRequestContext(
        invocation_id="invocation-context",
        request_id="request-1",
        operation=operation,
        payload={"turn_index": 1},
        cancellation=cancellation or threading.Event(),
        deadline=deadline or time.monotonic() + 5,
    )


def _resolver(
    loop: asyncio.AbstractEventLoop,
    *,
    invocation_id: UUID | None = None,
    allowed_operations: frozenset[str] = frozenset({"agent.model.turn", "agent.mcp.call"}),
    resolver_type: type[AgentOperationResolver] = AgentOperationResolver,
) -> AgentOperationResolver:
    return resolver_type(
        _settings(),
        loop=loop,
        work_item_id=UUID("00000000-0000-0000-0000-000000000101"),
        attempt_id="attempt-2",
        step_revision_id="syntara.agent-runtime.agent-runtime:v1",
        invocation_id=invocation_id or UUID("00000000-0000-0000-0000-000000000202"),
        allowed_operations=allowed_operations,
    )


def _run_with_loop[T](callback: Callable[[asyncio.AbstractEventLoop], T]) -> T:
    loop = asyncio.new_event_loop()
    started = threading.Event()

    def run() -> None:
        asyncio.set_event_loop(loop)
        started.set()
        loop.run_forever()

    thread = threading.Thread(target=run, name="agent-operation-test-loop", daemon=True)
    thread.start()
    assert started.wait(timeout=5)
    try:
        return callback(loop)
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        loop.close()


def test_resolver_requires_the_operation_grant() -> None:
    loop = asyncio.new_event_loop()
    try:
        resolver = _resolver(loop, allowed_operations=frozenset())
        with pytest.raises(HostOperationError) as error:
            resolver(_context())
        assert error.value.code == "permission_denied"
    finally:
        loop.close()


def test_resolver_requires_an_agent_invocation_id() -> None:
    loop = asyncio.new_event_loop()
    try:
        resolver = _resolver(loop, invocation_id=None)
        resolver._invocation_id = None
        with pytest.raises(HostOperationError) as error:
            resolver(_context())
        assert error.value.code == "invalid_request"
    finally:
        loop.close()


def test_resolver_rejects_an_expired_request_before_posting() -> None:
    loop = asyncio.new_event_loop()
    try:
        resolver = _resolver(loop)
        with pytest.raises(HostOperationError) as error:
            resolver(_context(deadline=time.monotonic() - 1))
        assert error.value.code == "deadline_exceeded"
    finally:
        loop.close()


def test_resolver_correlates_a_host_operation_and_returns_ao_result() -> None:
    observed: dict[str, object] = {}

    class RecordingResolver(AgentOperationResolver):
        async def _post(self, envelope: MessageEnvelope, timeout_seconds: float, request_id: str) -> dict[str, Any]:
            observed["envelope"] = envelope
            observed["timeout_seconds"] = timeout_seconds
            observed["request_id"] = request_id
            return {"content": "answer"}

    def invoke(loop: asyncio.AbstractEventLoop) -> dict[str, Any]:
        return _resolver(loop, resolver_type=RecordingResolver)(_context(operation="agent.mcp.call"))

    assert _run_with_loop(invoke) == {"content": "answer"}
    envelope = cast("MessageEnvelope", observed["envelope"])
    assert envelope.source_id == "execution-plane"
    assert envelope.destination_id == "automation-orchestrator"
    assert envelope.command.command_type == "agent.operation"
    assert envelope.command.payload == {
        "work_item_id": "00000000-0000-0000-0000-000000000101",
        "agent_invocation_id": "00000000-0000-0000-0000-000000000202",
        "attempt_id": "attempt-2",
        "step_revision_id": "syntara.agent-runtime.agent-runtime:v1",
        "operation": "agent.mcp.call",
        "request_id": "request-1",
        "payload": {"turn_index": 1},
    }
    assert observed["request_id"] == "request-1"
    assert 0 < cast("float", observed["timeout_seconds"]) <= 5


def test_resolver_translates_callback_exceptions_to_unknown_outcome() -> None:
    class FailingResolver(AgentOperationResolver):
        async def _post(self, _envelope: MessageEnvelope, _timeout_seconds: float, _request_id: str) -> dict[str, Any]:
            message = "private callback details"
            raise RuntimeError(message)

    def invoke(loop: asyncio.AbstractEventLoop) -> str:
        resolver = _resolver(loop, resolver_type=FailingResolver)
        with pytest.raises(HostOperationError) as error:
            resolver(_context())
        assert error.value.outcome_unknown
        return str(error.value)

    assert _run_with_loop(invoke) == "AO host operation outcome is unknown."


def test_resolver_cancels_the_pending_callback_when_the_workload_ends() -> None:
    callback_started = threading.Event()
    cancellation = threading.Event()

    class PendingResolver(AgentOperationResolver):
        async def _post(self, _envelope: MessageEnvelope, _timeout_seconds: float, _request_id: str) -> dict[str, Any]:
            callback_started.set()
            await asyncio.Event().wait()

    def invoke(loop: asyncio.AbstractEventLoop) -> str:
        resolver = _resolver(loop, resolver_type=PendingResolver)
        outcome: list[str] = []

        def request() -> None:
            with pytest.raises(HostOperationError) as error:
                resolver(_context(cancellation=cancellation))
            assert error.value.outcome_unknown
            outcome.append(str(error.value))

        request_thread = threading.Thread(target=request, name="agent-operation-request")
        request_thread.start()
        assert callback_started.wait(timeout=5)
        cancellation.set()
        request_thread.join(timeout=5)
        assert not request_thread.is_alive()
        return outcome[0]

    assert _run_with_loop(invoke) == "The workload ended before AO confirmed the host operation."


class _FakeResponse:
    def __init__(self, body: dict[str, object], status_code: int = 200) -> None:
        self._body = body
        self.status_code = status_code
        self.request = httpx.Request("POST", "https://ao.example/callback")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            response = httpx.Response(self.status_code, request=self.request)
            message = "callback failed"
            raise httpx.HTTPStatusError(message, request=self.request, response=response)

    def json(self) -> dict[str, object]:
        return self._body


class _FakeCallbackClient:
    def __init__(self, response: _FakeResponse | None = None, *, configured: bool = True) -> None:
        self.response = response
        self.configured = configured
        self.posted: dict[str, object] | None = None
        self.timeout_seconds: float | None = None
        self.closed = False

    async def post_json(self, body: dict[str, object], *, timeout_seconds: float | None = None) -> _FakeResponse:
        self.posted = body
        self.timeout_seconds = timeout_seconds
        assert self.response is not None
        return self.response

    async def close(self) -> None:
        self.closed = True


def _envelope(request_id: str = "request-1") -> MessageEnvelope:
    return AgentOperation(
        work_item_id=UUID("00000000-0000-0000-0000-000000000101"),
        agent_invocation_id=UUID("00000000-0000-0000-0000-000000000202"),
        attempt_id="attempt-2",
        step_revision_id="syntara.agent-runtime.agent-runtime:v1",
        operation="agent.model.turn",
        request_id=request_id,
        payload={"turn_index": 1},
    ).to_message(
        source_id="execution-plane",
        destination_id="automation-orchestrator",
        client_id="automation-orchestrator",
    )


def _install_client(monkeypatch: pytest.MonkeyPatch, client: _FakeCallbackClient) -> None:
    monkeypatch.setattr(
        "execution_plane.agent_operations.CallbackHTTPClient",
        lambda **_kwargs: client,
    )


def test_post_validates_the_correlated_ao_receipt_and_closes_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    envelope = _envelope()
    receipt = TransportReceipt(
        message_id=envelope.message_id,
        result=AgentOperationCommandResult(request_id="request-1", result={"content": "answer"}),
    )
    client = _FakeCallbackClient(_FakeResponse(receipt.model_dump(mode="json")))
    _install_client(monkeypatch, client)
    resolver = _resolver(asyncio.new_event_loop())
    try:
        result = asyncio.run(resolver._post(envelope, 2.0, "request-1"))
    finally:
        resolver._loop.close()

    assert result == {"content": "answer"}
    assert client.posted is not None
    assert client.posted["command"] == envelope.command.model_dump(mode="json")
    assert client.timeout_seconds == 2.0
    assert client.closed


@pytest.mark.parametrize(
    ("status_code", "expected_code", "unknown"),
    [(400, "invalid_request", False), (403, "permission_denied", False), (500, "outcome_unknown", True)],
)
def test_post_maps_http_statuses_to_safe_host_errors(
    monkeypatch: pytest.MonkeyPatch,
    *,
    status_code: int,
    expected_code: str,
    unknown: bool,
) -> None:
    client = _FakeCallbackClient(_FakeResponse({}, status_code))
    _install_client(monkeypatch, client)
    resolver = _resolver(asyncio.new_event_loop())
    try:
        with pytest.raises(HostOperationError) as error:
            asyncio.run(resolver._post(_envelope(), 2.0, "request-1"))
    finally:
        resolver._loop.close()

    assert error.value.code == expected_code
    assert error.value.outcome_unknown is unknown
    assert client.closed


def test_post_rejects_unmatched_message_id_or_request_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    envelope = _envelope()
    wrong_receipt = TransportReceipt(
        message_id=uuid4(),
        result=AgentOperationCommandResult(request_id="request-1", result={}),
    )
    client = _FakeCallbackClient(_FakeResponse(wrong_receipt.model_dump(mode="json")))
    _install_client(monkeypatch, client)
    resolver = _resolver(asyncio.new_event_loop())
    try:
        with pytest.raises(ValueError, match="receipt did not match"):
            asyncio.run(resolver._post(envelope, 2.0, "request-1"))
    finally:
        resolver._loop.close()
    assert client.closed


def test_post_rejects_a_receipt_for_a_different_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    envelope = _envelope()
    receipt = TransportReceipt(
        message_id=envelope.message_id,
        result=TargetHealthCommandResult(healthy=True),
    )
    client = _FakeCallbackClient(_FakeResponse(receipt.model_dump(mode="json")))
    _install_client(monkeypatch, client)
    resolver = _resolver(asyncio.new_event_loop())
    try:
        with pytest.raises(ValueError, match="receipt did not match"):
            asyncio.run(resolver._post(envelope, 2.0, "request-1"))
    finally:
        resolver._loop.close()
    assert client.closed


def test_post_rejects_an_unconfigured_ao_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeCallbackClient(configured=False)
    _install_client(monkeypatch, client)
    resolver = _resolver(asyncio.new_event_loop())
    try:
        with pytest.raises(HostOperationError) as error:
            asyncio.run(resolver._post(_envelope(), 2.0, "request-1"))
    finally:
        resolver._loop.close()
    assert error.value.code == "upstream_failed"
    assert not client.closed
