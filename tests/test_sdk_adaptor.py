"""Exercise EP against the installed SDK's actual protobuf service."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event, Thread
from typing import Any
from unittest.mock import Mock, patch

import grpc
import pytest
from syntara_plugin.runtime import (
    ContainerInvocationError,
    ContainerResult,
    CredentialAccessError,
    FailureCode,
    StepRegistry,
    add_container_runtime_service,
)
from syntara_plugin.runtime.protocol import container_pb2 as pb
from syntara_plugin.runtime.protocol import container_pb2_grpc as rpc

from execution_plane.worker_manager.vanilla_k8s.manager import (
    NodeExecutionError,
    VanillaK8sWorkerManager,
    _map_result,
)
from execution_plane.worker_manager.vanilla_k8s.sdk_adaptor import WorkloadRpcError, invoke
from execution_plane_client.commands.work import WorkloadInvocation
from execution_plane_client.models.work_item import WorkItemStatus

_DIGEST = "sha256:" + "a" * 64


@contextmanager
def _server(*, handler=None, service=None, step="script.v1") -> Iterator[grpc.Channel]:
    with ThreadPoolExecutor(max_workers=4) as pool:
        server = grpc.server(pool)
        if service is not None:
            rpc.add_ContainerServiceServicer_to_server(service, server)
        else:
            registry = StepRegistry({step: handler})
            add_container_runtime_service(server, registry)
        port = server.add_insecure_port("127.0.0.1:0")
        assert port
        server.start()
        try:
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                yield channel
        finally:
            server.stop(grace=0).wait()


def _invoke(
    channel,
    *,
    credential_bindings: list[dict[str, object]] | None = None,
    **overrides: object,
) -> dict[str, Any]:
    kwargs = {
        "identity": "work-1-attempt-2",
        "progress": lambda _event: None,
        "cancelled": Event(),
        "startup": 0.2,
        "grace": 1,
    }
    kwargs.update(overrides)
    invocation = {
        "workflow_run_id": "workflow-1",
        "step_revision_id": "script.v1",
        "inputs": {"environment": {"GREETING": "hello"}, "large_integer": 2**60, "null": None},
        "runtime_context": {"driver": "test"},
        "timeout_seconds": 2,
    }
    if credential_bindings is not None:
        invocation["credential_bindings"] = credential_bindings
    return invoke(
        channel,
        WorkloadInvocation.model_validate(invocation),
        **kwargs,
    )


@pytest.mark.parametrize(
    ("credential_bindings", "expected_error"),
    [
        ([], "missing"),
        (
            [{"credential_recipe_id": "acme.echo.database", "credential_ref": "00000000-0000-0000-0000-000000000001"}],
            "unavailable",
        ),
    ],
)
def test_sdk_service_receives_identity_inputs_and_rejects_credential_requests(
    credential_bindings: list[dict[str, object]], expected_error: str
):
    calls = []

    def handler(invocation) -> ContainerResult:
        assert invocation.invocation_id == "work-1-attempt-2"
        assert invocation.workflow_run_id == "workflow-1"
        assert invocation.step_revision_id == "script.v1"
        assert invocation.inputs["environment"] == {"GREETING": "hello"}
        assert invocation.inputs["large_integer"] == 2**60
        assert invocation.inputs["null"] is None
        assert invocation.runtime_context == {"driver": "test"}
        calls.append("execute")
        credentials = invocation.credential_provider.restrict(
            frozenset({"acme.echo.database"}),
            lambda _document: None,
        )
        with pytest.raises(CredentialAccessError) as error:
            credentials.read("acme.echo.database")
        return ContainerResult(_DIGEST, output={"credential_error": error.value.reason})

    with _server(handler=handler) as channel:
        result = _invoke(
            channel,
            credential_bindings=credential_bindings,
            on_execute_submitted=lambda: calls.append("submitted"),
        )
    assert calls == ["submitted", "execute"]
    assert _map_result(result, None)["output"] == {"credential_error": expected_error}


def test_selected_step_must_be_advertised_before_submission():
    submitted = Event()
    with _server(handler=lambda _: pytest.fail("Execute must not be called"), step="different.v1") as channel:
        with pytest.raises(WorkloadRpcError, match="advertise") as error:
            _invoke(channel, on_execute_submitted=submitted.set)
    assert not submitted.is_set()
    assert not error.value.retryable


def test_cancellation_before_health_does_not_submit():
    cancelled, submitted = Event(), Event()
    cancelled.set()
    with _server(handler=lambda _: pytest.fail("Execute must not be called")) as channel:
        with pytest.raises(WorkloadRpcError) as error:
            _invoke(channel, cancelled=cancelled, on_execute_submitted=submitted.set)
    assert error.value.cancelled
    assert not submitted.is_set()


def test_cancel_is_relayed_to_the_sdk_service():
    cancelled, started = Event(), Event()

    def handler(invocation) -> ContainerResult:
        started.set()
        assert invocation.cancellation.wait(3)
        return ContainerResult(_DIGEST, output={"stdout": "partial"})

    def request_cancel() -> None:
        assert started.wait(3)
        cancelled.set()

    watcher = Thread(target=request_cancel)
    with _server(handler=handler) as channel:
        watcher.start()
        frame = _invoke(channel, cancelled=cancelled)
        watcher.join(timeout=3)
    assert frame["kind"] == "failure"
    assert frame["code"] == "CANCELLED"
    with pytest.raises(NodeExecutionError) as error:
        _map_result(frame, None)
    assert error.value.error_type == "CancelledError"


def test_deliberate_failure_retains_partial_output_and_status_metadata():
    terminal = ContainerResult(
        _DIGEST,
        output={"stdout": "partial", "exit_code": 17},
        status_code=17,
        status_message="script exited",
        error_result={
            "code": "SCRIPT_FAILED",
            "message": "Script returned nonzero status",
            "retryable": True,
            "category": "business",
        },
        error_schema_digest="sha256:" + "b" * 64,
    )
    with _server(handler=lambda _: terminal) as channel:
        frame = _invoke(channel)
    with pytest.raises(NodeExecutionError) as error:
        _map_result(frame, {"stdout": "stdout"})
    assert error.value.output == {"stdout": "partial"}
    assert error.value.details == {
        "output_schema_digest": _DIGEST,
        "status_code": 17,
        "status_message": "script exited",
        "error_code": "SCRIPT_FAILED",
        "error_message": "Script returned nonzero status",
        "retryable": True,
    }


def test_deliberate_failure_without_output_is_still_a_terminal_failure():
    terminal = ContainerResult(
        _DIGEST,
        status_code=300,
        error_result={"code": "FAILED", "message": "Failed", "retryable": False, "category": "business"},
        error_schema_digest="sha256:" + "b" * 64,
    )
    with _server(handler=lambda _: terminal) as channel:
        frame = _invoke(channel)
    with pytest.raises(NodeExecutionError) as error:
        _map_result(frame, None)
    assert error.value.output is None
    assert error.value.details["status_code"] == 300


def test_object_reference_survives_result_mapping():
    with _server(handler=lambda _: ContainerResult(_DIGEST, object_reference="output-1")) as channel:
        frame = _invoke(channel)
    mapped = _map_result(frame, None)
    assert mapped["object_reference"] == "output-1"
    assert mapped["output_schema_digest"] == _DIGEST
    assert "output" not in mapped


def test_runtime_failure_is_distinct_from_a_deliberate_failed_result():
    def handler(_) -> ContainerResult:
        raise ContainerInvocationError(FailureCode.UPSTREAM_TRANSIENT, "Safe failure", retryable=True)

    with _server(handler=handler) as channel:
        frame = _invoke(channel)
    with pytest.raises(NodeExecutionError) as error:
        _map_result(frame, None)
    assert error.value.details == {
        "failure_code": "UPSTREAM_TRANSIENT",
        "retryable": True,
        "outcome": "not_completed",
    }


def test_runtime_reported_unknown_outcome_requires_reconciliation():
    def handler(_) -> ContainerResult:
        raise ContainerInvocationError(FailureCode.OUTCOME_UNKNOWN, "Unknown", outcome="unknown")

    with _server(handler=handler) as channel:
        with pytest.raises(WorkloadRpcError, match="unknown execution outcome") as error:
            _invoke(channel)
    assert not error.value.retryable
    assert not error.value.cancelled


class _Service(rpc.ContainerServiceServicer):
    def __init__(self, events, *, abi="syntara.container/v1alpha1") -> None:
        self.events = events
        self.abi = abi

    def Health(self, request, context) -> pb.HealthResponse:  # noqa: N802
        return pb.HealthResponse(ready=True, runtime_abi=self.abi, step_revision_ids=["script.v1"])

    def Execute(self, requests, context) -> Iterator[pb.ExecuteServerMessage]:  # noqa: N802
        request = next(requests)
        assert request.WhichOneof("message") == "start"
        for event in self.events(request.start, context):
            yield pb.ExecuteServerMessage(event=event)


def _terminal(identity="work-1-attempt-2") -> pb.ExecutionEvent:
    return pb.ExecutionEvent(
        invocation_id=identity,
        result=pb.Result(output_schema_digest=_DIGEST, output_json=b'{"stdout":"done"}'),
    )


def test_progress_is_forwarded_before_the_result():
    progress = []

    def events(request, _context) -> Iterator[pb.ExecutionEvent]:
        yield pb.ExecutionEvent(invocation_id=request.invocation_id, progress=pb.Progress(sequence=1, state="started"))
        yield _terminal(request.invocation_id)

    with _server(service=_Service(events)) as channel:
        frame = _invoke(channel, progress=progress.append)
    assert progress == [
        {"kind": "progress", "event": "started", "data": {"sequence": 1, "message": "", "percent": None}}
    ]
    assert _map_result(frame, None)["output"] == {"stdout": "done"}


@pytest.mark.parametrize("mode", ["wrong_identity", "missing_result", "duplicate_result", "progress_after_result"])
def test_invalid_stream_is_never_retried_after_submission(mode):
    submitted = Event()

    def events(request, _context) -> Iterator[pb.ExecutionEvent]:
        if mode == "missing_result":
            return
        yield _terminal("wrong-attempt" if mode == "wrong_identity" else request.invocation_id)
        if mode == "duplicate_result":
            yield _terminal(request.invocation_id)
        if mode == "progress_after_result":
            yield pb.ExecutionEvent(
                invocation_id=request.invocation_id, progress=pb.Progress(sequence=1, state="progress")
            )

    with _server(service=_Service(events)) as channel:
        with pytest.raises(WorkloadRpcError) as error:
            _invoke(channel, on_execute_submitted=submitted.set)
    assert submitted.is_set()
    assert not error.value.retryable


def test_transport_failure_after_submission_does_not_retry_or_expose_remote_text():
    calls = []

    def events(_request, context) -> Iterator[pb.ExecutionEvent]:
        calls.append("execute")
        context.abort(grpc.StatusCode.UNAVAILABLE, "untrusted diagnostic")
        yield  # make this a streaming handler

    with _server(service=_Service(events)) as channel:
        with pytest.raises(WorkloadRpcError) as error:
            _invoke(channel)
    assert calls == ["execute"]
    assert not error.value.retryable
    assert "untrusted diagnostic" not in str(error.value)


def test_abi_mismatch_fails_before_submission():
    submitted = Event()
    with _server(service=_Service(lambda *_: (), abi="wrong")) as channel:
        with pytest.raises(WorkloadRpcError) as error:
            _invoke(channel, on_execute_submitted=submitted.set)
    assert not submitted.is_set()
    assert not error.value.retryable


def test_health_transport_failure_is_retryable_before_submission():
    class Unavailable(_Service):
        def Health(self, request, context) -> pb.HealthResponse:  # noqa: N802
            context.abort(grpc.StatusCode.UNAVAILABLE, "not listening yet")

    submitted = Event()
    with _server(service=Unavailable(lambda *_: ())) as channel:
        with pytest.raises(WorkloadRpcError) as error:
            _invoke(channel, startup=0.01, on_execute_submitted=submitted.set)
    assert not submitted.is_set()
    assert error.value.retryable


def test_cancellation_closes_the_channel_when_the_workload_ignores_cancel():
    started, stopped, cancelled = Event(), Event(), Event()

    def events(request, context) -> Iterator[pb.ExecutionEvent]:
        context.add_callback(stopped.set)
        started.set()
        assert stopped.wait(3)
        yield _terminal(request.invocation_id)

    class IgnoresCancel(_Service):
        def Cancel(self, request, context) -> pb.CancelResponse:  # noqa: N802
            return pb.CancelResponse(accepted=True)

    def request_cancel() -> None:
        assert started.wait(3)
        cancelled.set()

    watcher = Thread(target=request_cancel)
    with _server(service=IgnoresCancel(events)) as channel:
        watcher.start()
        with pytest.raises(WorkloadRpcError) as error:
            _invoke(channel, cancelled=cancelled, grace=0)
        watcher.join(timeout=3)
    assert error.value.cancelled
    assert not error.value.retryable


def test_workload_timeout_reaches_failed_without_cancellation():
    """Receive the SDK timeout over real gRPC and map it to EP's FAILED state."""
    stopped, cancelled = Event(), Event()

    def handler(invocation) -> ContainerResult:
        assert invocation.cancellation.wait(5)
        stopped.set()
        return ContainerResult(_DIGEST, output={"late": True})

    with _server(handler=handler) as channel:
        frame = _invoke(channel, cancelled=cancelled)
    assert stopped.wait(1)
    assert not cancelled.is_set()
    assert frame["kind"] == "failure"
    assert frame["code"] == "TIMEOUT"
    result, status = VanillaK8sWorkerManager._result_and_status(frame, None, cancelled)
    assert status is WorkItemStatus.FAILED
    assert result["failure_code"] == "TIMEOUT"
    assert result["outcome"] == "not_completed"


def test_channel_should_not_be_closed_in_invoke_finally_block():
    """Verify that invoke's finally block doesn't close the channel.

    Channel ownership:
        - run_job's `with` block owns the channel and always closes it on exit
        - Only the cancellation watcher force-closes early (to interrupt stuck streams)
        - invoke itself should NOT close the channel in its finally block

    The old code did `with suppress(Exception): channel.close()` which:
        1. Causes potential double-close (watcher + finally + run_job)
        2. Hides errors that should be visible
        3. Makes ownership unclear

    This test FAILS on devel (which closes the channel in finally).
    This test PASSES on refactor/sdk-workload-protocol (which doesn't).
    """
    channel = Mock()

    mock_client = Mock()
    mock_client.stream = Mock(return_value=iter([ContainerResult("sha256:" + "a" * 64, output={"done": True})]))
    mock_client.health = Mock(return_value=["script.v1"])

    with patch("execution_plane.worker_manager.vanilla_k8s.sdk_adaptor.ContainerClient", return_value=mock_client):
        _invoke(
            channel,
            identity="work-1",
            progress=lambda _: None,
            cancelled=Event(),
            startup=0.1,
            grace=1,
        )

    # invoke should NOT close the channel - that's run_job's responsibility
    assert not channel.close.called, (
        "invoke's finally block should NOT close the channel. "
        "The channel is owned by run_job's `with` block, which always closes it. "
        "Only the cancellation watcher force-closes early (to interrupt stuck streams)."
    )
