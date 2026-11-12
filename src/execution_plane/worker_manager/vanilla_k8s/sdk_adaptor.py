"""Connect the SDK container client to EP's attempt and cancellation lifecycle.

The SDK owns the protobuf messages, gRPC stubs, JSON codec, and stream
validation. EP owns its dedicated channel, submission fencing, and cleanup.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Any

import structlog
from syntara_plugin.runtime import (
    ContainerClient,
    ContainerInvocation,
    ContainerProgress,
    ContainerRemoteError,
    ContainerResult,
)
from syntara_plugin.runtime.container import ANY_STEP_REVISION

from execution_plane.credential_resolution.sdk_resolver import CredentialResolver

logger = structlog.stdlib.get_logger(__name__)

if TYPE_CHECKING:
    from collections.abc import Callable

    import grpc

    from execution_plane_client.commands.work import WorkloadInvocation

PORT = 50051
MAX_MESSAGE_BYTES = 2_097_152
CHANNEL_OPTIONS = (
    ("grpc.max_receive_message_length", MAX_MESSAGE_BYTES),
    ("grpc.max_send_message_length", MAX_MESSAGE_BYTES),
    ("grpc.enable_retries", 0),
)


class WorkloadRpcError(Exception):
    """A safe protocol error with explicit pre-submission retry classification."""

    def __init__(self, message: str, *, retryable: bool = False, cancelled: bool = False) -> None:
        """Keep retries and cancellation separate from a remote step failure."""
        super().__init__(message)
        self.retryable = retryable
        self.cancelled = cancelled


def _wait_for_health(
    client: ContainerClient,
    step_revision_id: str,
    cancelled: threading.Event,
    startup: float,
) -> None:
    deadline = time.monotonic() + startup
    while True:
        if cancelled.is_set():
            message = "Workload execution cancelled before submission"
            raise WorkloadRpcError(message, cancelled=True)
        try:
            supported = client.health(timeout_seconds=min(1, max(0.1, deadline - time.monotonic())))
        except ContainerRemoteError as error:
            if error.source != "transport" or time.monotonic() >= deadline:
                message = "Workload gRPC server is unavailable or incompatible"
                raise WorkloadRpcError(message, retryable=error.source == "transport") from None
            cancelled.wait(0.1)
            continue
        if step_revision_id not in supported and ANY_STEP_REVISION not in supported:
            message = "Workload does not advertise the selected step revision"
            raise WorkloadRpcError(message)
        return


def _monitor_cancellation(
    client: ContainerClient,
    channel: grpc.Channel,
    identity: str,
    cancelled: threading.Event,
    stopped: threading.Event,
    grace: int,
) -> None:
    """Cancel an in-flight invocation, force-closing the channel if it won't stop.

    Polls ``cancelled`` until ``invoke`` signals ``stopped``. On a cancellation
    request it sends a Cancel RPC, then waits up to ``grace`` for a graceful
    terminal. If the step ignores it, closing the channel interrupts the stream
    so ``invoke`` unwinds and ``run_job`` can delete the Job. Worst-case cancel
    latency is therefore ~``grace`` plus the Pod's terminationGracePeriodSeconds,
    backstopped by the Job's activeDeadlineSeconds.
    """
    while not stopped.wait(0.05):
        if not cancelled.is_set():
            continue
        try:
            accepted = client.cancel(identity, timeout_seconds=min(5, max(0.1, grace)))
        except (ContainerRemoteError, ValueError):
            accepted = False
        if not accepted or not stopped.wait(grace):
            # Closing the channel only interrupts EP's own stream iterator; it
            # does not reach into the Pod. Unblocking the iterator lets invoke
            # unwind so run_job's cleanup can delete the Job, which kills the Pod.
            channel.close()
        return


def _failure_frame(error: ContainerRemoteError, *, cancelled: bool = False) -> dict[str, Any]:
    if error.outcome == "unknown":
        # If the workload was cancelled, report it as a cancellation even if outcome is unknown
        is_cancelled = error.code.value == "CANCELLED" or cancelled
        message = "Workload reported an unknown execution outcome"
        raise WorkloadRpcError(message, cancelled=is_cancelled) from None
    return {"kind": "failure", "code": error.code.value, "retryable": error.retryable, "outcome": error.outcome}


def _is_log_batch(event: object) -> bool:
    """Recognize an SDK operational log batch without hard-importing its type.

    Duck-typed so the adaptor works with any SDK version that yields log
    batches (the pinned SDK release predates the contract; the stream simply
    never yields them there) and with test doubles.
    """
    if isinstance(event, ContainerProgress | ContainerResult | str):
        return False
    return hasattr(event, "records") and hasattr(event, "invocation_id")


def _deliver_log_batch(event: object, sink: Callable[[object], None] | None) -> None:
    """Hand one batch to the sink; telemetry never fails the invocation."""
    if sink is None:
        return
    try:
        sink(event)
    except Exception:  # noqa: BLE001 - Forwarding failures must not fail a work item.
        logger.warning("Operational log batch forwarding failed", exc_info=True)


def invoke(  # noqa: C901, PLR0912 - stream consumption plus safe telemetry delivery
    channel: grpc.Channel,
    invocation: WorkloadInvocation,
    *,
    identity: str,
    progress: Callable[[dict[str, Any]], None],
    cancelled: threading.Event,
    startup: float = 120,
    grace: int = 30,
    on_execute_submitted: Callable[[], None] | None = None,
    log_sink: Callable[[object], None] | None = None,
) -> dict[str, Any]:
    """Negotiate the SDK service, execute once, and consume its entire stream."""
    request = ContainerInvocation(
        invocation_id=identity,
        workflow_run_id=invocation.workflow_run_id,
        step_revision_id=invocation.step_revision_id,
        inputs=invocation.inputs,
        runtime_context=invocation.runtime_context,
        timeout_seconds=invocation.timeout_seconds,
    )
    client = ContainerClient(channel)
    _wait_for_health(client, invocation.step_revision_id, cancelled, startup)
    if cancelled.is_set():
        message = "Workload execution cancelled before submission"
        raise WorkloadRpcError(message, cancelled=True)
    if on_execute_submitted is not None:
        on_execute_submitted()
    stopped = threading.Event()
    watcher = threading.Thread(
        target=_monitor_cancellation,
        args=(client, channel, identity, cancelled, stopped, grace),
        daemon=True,
        name="workload-grpc-cancel",
    )
    watcher.start()
    result: ContainerResult | None = None
    try:
        # The SDK clamps the Execute deadline to the invocation's own
        # timeout_seconds, so transport cannot outlive the step's contract
        # budget. The cancellation grace window is enforced by the watcher
        # (Cancel + channel close) and run_job's Job deletion, not the wire.
        for event in client.stream(
            request,
            timeout_seconds=invocation.timeout_seconds,
            credential_handler=CredentialResolver(invocation.credential_bindings),
        ):
            if _is_log_batch(event):
                # Best-effort operational telemetry: batches after the terminal
                # result are dropped, never fatal.
                if result is None:
                    _deliver_log_batch(event, log_sink)
                continue
            if result is not None:
                message = "Workload returned events after its terminal result"
                raise WorkloadRpcError(message)
            if isinstance(event, ContainerProgress):
                progress(
                    {
                        "kind": "progress",
                        "event": event.state,
                        "data": {"sequence": event.sequence, "message": event.message, "percent": event.percent},
                    }
                )
            else:
                result = event
        if result is None:
            message = "Workload stream ended without a terminal result"
            raise WorkloadRpcError(message)
        return {"kind": "result", "result": result}
    except ContainerRemoteError as error:
        if error.source == "workload" and result is None:
            return _failure_frame(error, cancelled=cancelled.is_set())
        message = "Workload did not return a valid terminal outcome"
        raise WorkloadRpcError(message, cancelled=cancelled.is_set()) from None
    except ValueError:
        # A cancellation watcher can close the channel before Execute starts.
        message = "Workload gRPC channel stopped"
        raise WorkloadRpcError(message, cancelled=cancelled.is_set()) from None
    finally:
        stopped.set()
        watcher.join(timeout=6)
        # The caller (run_job) owns the channel via its `with` block and always
        # closes it on exit. We deliberately do not close it here; only the
        # cancellation watcher force-closes early, and only to interrupt a stuck
        # stream.
