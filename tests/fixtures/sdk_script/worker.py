"""Small SDK script workload used only by the cluster integration tests."""

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


def execute(invocation: ContainerInvocation) -> ContainerResult:
    """Run the test's script with its ordinary environment inputs."""
    language = invocation.inputs["language"]
    command = [sys.executable, "-c"] if language == "python" else ["/bin/bash", "-c"]
    environment = dict(os.environ)
    environment.update(invocation.inputs.get("environment", {}))
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
    status = 0 if process.returncode == 0 else 1
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
        status_message="completed" if status == 0 else "failed",
        error_code="SCRIPT_FAILED" if status else "",
        error_message="Script returned nonzero status" if status else "",
    )


def main() -> None:
    """Serve the same SDK service consumed by the EP worker."""
    with ThreadPoolExecutor(max_workers=4) as pool:
        server = grpc.server(pool)
        add_container_runtime_service(server, StepRegistry({"script.v1": execute}))
        server.add_insecure_port("[::]:50051")
        server.start()
        server.wait_for_termination()


if __name__ == "__main__":
    main()
