"""The node client issues Execute separately from readiness and result observation."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest

from execution_plane.models.work_item import WorkItemStatus
from execution_plane.node_protocol import node_pb2 as pb
from execution_plane.worker_manager import node_client as client_module
from execution_plane.worker_manager.node_client import NodeClient

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class _Call:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.stopped = False

    async def __aiter__(self) -> AsyncIterator[pb.ExecutionEvent]:
        self.events.append("result")
        yield pb.ExecutionEvent(invocation_id="attempt", result=pb.Result(status_code=0, output_json=b"{}"))
        self.stopped = True

    def done(self) -> bool:
        return self.stopped

    def cancel(self) -> None:
        self.stopped = True


class _Stub:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.Health = AsyncMock(return_value=pb.HealthResponse(ready=True, protocol_version=1))
        self.Cancel = AsyncMock(return_value=pb.CancelResponse(accepted=True))

    def Execute(self, *_args: object, **_kwargs: object) -> _Call:  # noqa: N802 - gRPC method name
        self.events.append("execute")
        return _Call(self.events)


@pytest.mark.asyncio
async def test_execute_is_issued_before_its_handle_returns(monkeypatch):
    stub = _Stub()
    monkeypatch.setattr(client_module.rpc, "NodeServiceStub", lambda _channel: stub)
    node = NodeClient(None, pb.ExecuteRequest(invocation_id="attempt", timeout_seconds=30), None)

    await node.wait_ready(1)
    assert stub.events == []
    invocation = node.submit(5)
    assert stub.events == ["execute"]
    outcome = await invocation.result()
    assert stub.events == ["execute", "result"]
    assert outcome.status is WorkItemStatus.COMPLETED
    assert outcome.result == {"output": {}}


@pytest.mark.asyncio
async def test_submission_cannot_be_repeated(monkeypatch):
    stub = _Stub()
    monkeypatch.setattr(client_module.rpc, "NodeServiceStub", lambda _channel: stub)
    node = NodeClient(None, pb.ExecuteRequest(invocation_id="attempt", timeout_seconds=30), None)
    node.submit(5)

    with pytest.raises(RuntimeError, match="already been issued"):
        node.submit(5)

    assert stub.events == ["execute"]
