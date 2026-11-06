"""Small SDK script workload used only by the cluster integration tests.

When the invoke payload sets ``environment.EP_PERF_WORKER_TIMESTAMPS=1``, the
worker appends a single ``EP_PERF_TIMINGS=...`` JSON line to stderr with
pod-local monotonic stamps:

* ``server_listening_mono`` — set once in ``main()`` right after ``server.start()``
* ``execute_entered_mono`` — set at the top of ``execute()``
* ``post_pod_overhead_ms`` — execute_entered - server_listening, in milliseconds

That interval approximates EP post-pod overhead (gRPC connect + health + Execute
submit) on one clock, without K8s or runner clock skew.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress

import grpc
from syntara_plugin.runtime import ContainerInvocation, ContainerResult, StepRegistry, add_container_runtime_service

OUTPUT_SCHEMA_DIGEST = "sha256:" + "a" * 64

# Set in main() after the gRPC server is listening; read from execute().
_server_listening_mono: float | None = None

_PERF_TIMINGS_ENV = "EP_PERF_WORKER_TIMESTAMPS"
_PERF_TIMINGS_PREFIX = "EP_PERF_TIMINGS="


def _stdout_json(language: str, stdout: str) -> object:
    if language != "python" or not stdout.strip():
        return None
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        try:
            return json.loads([line for line in stdout.splitlines() if line.strip()][-1])
        except json.JSONDecodeError:
            return None


def _perf_timings_line(*, execute_entered_mono: float) -> str:
    """Return an stderr marker line, or empty when listening was never stamped."""
    listening = _server_listening_mono
    if listening is None:
        return ""
    payload = {
        "server_listening_mono": listening,
        "execute_entered_mono": execute_entered_mono,
        "post_pod_overhead_ms": (execute_entered_mono - listening) * 1000.0,
    }
    return f"{_PERF_TIMINGS_PREFIX}{json.dumps(payload)}\n"


def execute(invocation: ContainerInvocation) -> ContainerResult:
    """Run the test's script with its ordinary environment inputs."""
    execute_entered_mono = time.monotonic()
    language = invocation.inputs["language"]
    command = [sys.executable, "-c"] if language == "python" else ["/bin/bash", "-c"]
    environment = dict(os.environ)
    environment.update(invocation.inputs.get("environment", {}))
    emit_perf = environment.get(_PERF_TIMINGS_ENV) == "1"
    deadline = time.monotonic() + invocation.timeout_seconds
    with subprocess.Popen(
        [*command, invocation.inputs["code"]],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=environment,
        start_new_session=True,
    ) as process:
        while True:
            if invocation.cancelled or time.monotonic() >= deadline:
                with suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
            try:
                stdout, stderr = process.communicate(timeout=0.1)
                break
            except subprocess.TimeoutExpired:
                continue
    if emit_perf:
        stderr = (stderr or "") + _perf_timings_line(execute_entered_mono=execute_entered_mono)
    status = 0 if process.returncode == 0 else 1
    if status == 0:
        return ContainerResult(
            OUTPUT_SCHEMA_DIGEST,
            # AO consumes script.v1 results verbatim, so this workload must provide
            # the ScriptOutput contract rather than only generic process metadata.
            output={
                "return_code": process.returncode,
                "stdout": stdout[:10_000],
                "stderr": stderr[:10_000],
                "stdout_json": _stdout_json(language, stdout),
            },
            status_code=status,
            status_message="completed",
        )
    return ContainerResult(
        OUTPUT_SCHEMA_DIGEST,
        output={
            "return_code": process.returncode,
            "stdout": stdout[:10_000],
            "stderr": stderr[:10_000],
            "stdout_json": _stdout_json(language, stdout),
        },
        status_code=status,
        status_message="failed",
        error_result={
            "code": "SCRIPT_FAILED",
            "message": "Script returned nonzero status",
            "retryable": False,
            "category": "business",
        },
        error_schema_digest="sha256:" + "b" * 64,
    )


def main() -> None:
    """Serve the same SDK service consumed by the EP worker."""
    global _server_listening_mono  # noqa: PLW0603 — stamped once for execute() readers
    with ThreadPoolExecutor(max_workers=4) as pool:
        server = grpc.server(pool)
        add_container_runtime_service(server, StepRegistry({"script.v1": execute}))
        server.add_insecure_port("[::]:50051")
        server.start()
        _server_listening_mono = time.monotonic()
        server.wait_for_termination()


if __name__ == "__main__":
    main()
