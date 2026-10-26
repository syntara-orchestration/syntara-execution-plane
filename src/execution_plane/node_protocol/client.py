"""Separate node readiness, Execute submission, result observation, and cancellation."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import grpc
import structlog
from grpc import aio

from execution_plane.node_protocol import node_pb2 as pb
from execution_plane.node_protocol import node_pb2_grpc as rpc
from execution_plane.node_protocol.codec import decode_event
from execution_plane.node_protocol.result import execution_result
from execution_plane.worker_manager.lifecycle import BackendError, ExecutionResult, Invocation, RetryableDispatchError

if TYPE_CHECKING:
    from typing import Any

logger = structlog.stdlib.get_logger(__name__)


class NodeClient:
    """A connection to a ready node; constructing it never submits execution."""

    def __init__(
        self,
        channel: aio.Channel,
        request: pb.ExecuteRequest,
        output_config: dict[str, str] | None,
    ) -> None:
        """Bind the request to its channel without starting Execute."""
        self._stub = rpc.NodeServiceStub(channel)
        self._request = request
        self._output_config = output_config
        self._submitted = False

    async def wait_ready(self, readiness_seconds: float) -> None:
        """Health-check until the service is compatible or the connection deadline expires."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + readiness_seconds
        while True:
            try:
                health = await self._stub.Health(pb.HealthRequest(), timeout=min(1, max(0.1, deadline - loop.time())))
            except grpc.RpcError:
                if loop.time() >= deadline:
                    message = "Node gRPC server did not become ready"
                    raise RetryableDispatchError(message) from None
                await asyncio.sleep(0.1)
            else:
                if not health.ready or health.protocol_version != 1:
                    message = "Node server is unavailable or incompatible"
                    raise BackendError(message)
                return

    def submit(self, grace: int) -> NodeInvocation:
        """Initiate Execute once, returning its live call before observing results."""
        if self._submitted:
            message = "Execute has already been issued on this node connection"
            raise RuntimeError(message)
        self._submitted = True
        call = self._stub.Execute(self._request, timeout=self._request.timeout_seconds + grace)
        return NodeInvocation(call, self._stub, self._request.invocation_id, self._output_config)


class NodeInvocation(Invocation):
    """Own an issued Execute stream while EP persists progress and observes cancellation."""

    def __init__(
        self,
        call: aio.UnaryStreamCall[pb.ExecuteRequest, pb.ExecutionEvent],
        stub: rpc.NodeServiceStub[
            aio.UnaryStreamMultiCallable[pb.ExecuteRequest, pb.ExecutionEvent],
            aio.UnaryUnaryMultiCallable[pb.CancelRequest, pb.CancelResponse],
            aio.UnaryUnaryMultiCallable[pb.HealthRequest, pb.HealthResponse],
        ],
        identity: str,
        output_config: dict[str, str] | None,
    ) -> None:
        """Retain the stream so status persistence never controls whether Execute is issued."""
        self._call = call
        self._stub = stub
        self._identity = identity
        self._output_config = output_config

    async def result(self) -> ExecutionResult:
        """Require exactly one terminal result and a clean end to its stream."""
        result: dict[str, Any] | None = None
        try:
            async for event in self._call:
                frame = decode_event(event, self._identity)
                if result is not None:
                    message = "Node returned events after its terminal result"
                    raise BackendError(message)
                if frame["kind"] == "progress":
                    logger.debug("Node progress", attempt_id=self._identity, node_event=frame.get("event"))
                else:
                    result = frame
        except asyncio.CancelledError:
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                raise
            message = "Node invocation stopped without a terminal result"
            raise BackendError(message) from None
        except grpc.RpcError as exc:
            # Remote diagnostic text can include credentials. Only retain the code.
            message = f"Node gRPC call failed ({exc.code().name})"
            raise BackendError(message) from None
        except (ValueError, KeyError, TypeError, AttributeError):
            message = "Invalid node gRPC result"
            raise BackendError(message) from None
        if result is None:
            message = "Node gRPC stream ended without a result"
            raise BackendError(message)
        return execution_result(result, self._output_config)

    async def cancel(self) -> None:
        """Keep Execute open for a terminal result when cooperative cancellation is accepted."""
        if self._call.done():
            return
        try:
            response = await self._stub.Cancel(pb.CancelRequest(invocation_id=self._identity), timeout=5)
            if response.accepted:
                return
        except grpc.RpcError:
            pass
        self._call.cancel()
