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

import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import replace
from typing import Any

import grpc
from pydantic import BaseModel
from syntara_plugin.contracts import canonical_json_bytes
from syntara_plugin.runtime import (
    ActionStep,
    ContainerInvocation,
    ContainerResult,
    ExecutionContext,
    InstalledStepManifest,
    StepBinding,
    StepDescriptor,
    StepRegistry,
    add_container_runtime_service,
    container_step_for_binding,
)

ERROR_SCHEMA_DIGEST = "sha256:" + "b" * 64
_STEP_REVISION_ID = "script.v1"
_INTEGRATION_ID = "syntara.e2e.script"
CREDENTIAL_RECIPE_ID = "syntara.e2e.bearer-token"
_USE_CREDENTIAL_ENV = "EP_E2E_USE_CREDENTIAL"
_CREDENTIAL_DIGEST_PREFIX = "EP_E2E_CREDENTIAL_SHA256="

# Set in main() after the gRPC server is listening; read from execute().
_server_listening_mono: float | None = None

_PERF_TIMINGS_ENV = "EP_PERF_WORKER_TIMESTAMPS"
_PERF_TIMINGS_PREFIX = "EP_PERF_TIMINGS="


class ScriptInput(BaseModel):
    """Validated inputs for the SDK-backed script fixture."""

    language: str
    code: str
    environment: dict[str, str] = {}
    timeout_seconds: int = 60


class ScriptOutput(BaseModel):
    """AO-compatible output from the SDK-backed script fixture."""

    return_code: int
    stdout: str
    stderr: str
    stdout_json: Any | None = None


class ScriptStep(ActionStep[ScriptInput, ScriptOutput]):
    """Run the test script and optionally exercise credential resolution."""

    def run(self, inputs: ScriptInput, context: ExecutionContext) -> ScriptOutput:
        execute_entered_mono = time.monotonic()
        command = [sys.executable, "-c"] if inputs.language == "python" else ["/bin/bash", "-c"]
        environment = dict(os.environ)
        environment.update(inputs.environment)
        emit_perf = environment.get(_PERF_TIMINGS_ENV) == "1"
        deadline = time.monotonic() + inputs.timeout_seconds
        with subprocess.Popen(
            [*command, inputs.code],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=environment,
            start_new_session=True,
        ) as process:
            while True:
                if context.cancelled or time.monotonic() >= deadline:
                    with suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGKILL)
                try:
                    stdout, stderr = process.communicate(timeout=0.1)
                    break
                except subprocess.TimeoutExpired:
                    continue
        if emit_perf:
            stderr = (stderr or "") + _perf_timings_line(execute_entered_mono=execute_entered_mono)
        stdout_json = _stdout_json(inputs.language, stdout)
        if inputs.environment.get(_USE_CREDENTIAL_ENV) == "1":
            document = context.credential(CREDENTIAL_RECIPE_ID).read()
            digest = hashlib.sha256(canonical_json_bytes(document)).hexdigest()
            stdout += f"{_CREDENTIAL_DIGEST_PREFIX}{digest}\n"
        return ScriptOutput(
            return_code=process.returncode,
            stdout=stdout[:10_000],
            stderr=stderr[:10_000],
            stdout_json=stdout_json,
        )


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


def script_binding() -> StepBinding:
    """Return the trusted public binding for the fixture script step."""
    step = ScriptStep()
    installed_step = InstalledStepManifest.from_manifest(
        _STEP_REVISION_ID,
        {
            "apiVersion": "syntara.io/v1alpha1",
            "kind": "Action",
            "metadata": {
                "name": "script",
                "displayName": "SDK Script",
                "description": "Runs an E2E test script and can read a test credential.",
            },
            "spec": {
                "integrationType": _INTEGRATION_ID,
                "runtime": {"kind": "custom-runner", "entryPoint": f"{__name__}:ScriptStep"},
                "input": {"kind": "inline", "value": ScriptInput.model_json_schema()},
                "output": {"kind": "inline", "value": ScriptOutput.model_json_schema()},
            },
        },
        {},
        credential_targets=_credential_targets(),
    )
    return StepBinding(StepDescriptor.for_installed_step(step, installed_step), step)


def _credential_targets() -> list[dict[str, object]]:
    return [
        {
            "apiVersion": "syntara.io/v1alpha1",
            "kind": "IntegrationType",
            "metadata": {
                "name": "e2e-script",
                "displayName": "E2E Script",
                "description": "Fixture integration for SDK credential callbacks.",
            },
            "spec": {
                "id": _INTEGRATION_ID,
                "endpointKinds": ["https"],
                "credentialTypes": [CREDENTIAL_RECIPE_ID],
                "configuration": {"kind": "inline", "value": {}},
            },
        },
        {
            "apiVersion": "syntara.io/v1alpha1",
            "kind": "CredentialRecipe",
            "metadata": {
                "name": "e2e-bearer-token",
                "displayName": "E2E Bearer Token",
                "description": "Fixture bearer credential used by the SDK script.",
            },
            "spec": {
                "id": CREDENTIAL_RECIPE_ID,
                "integrationType": _INTEGRATION_ID,
                "credentialSchema": {"kind": "inline", "value": {"type": "object"}},
                "secretFields": ["bearer_token"],
                "issuance": {"kind": "static-bearer.v1"},
            },
        },
    ]


def execute(invocation: ContainerInvocation) -> ContainerResult:
    """Run the test's script through its public SDK step binding."""
    invocation = replace(
        invocation,
        inputs={**invocation.inputs, "timeout_seconds": invocation.timeout_seconds},
    )
    result = container_step_for_binding(script_binding())(invocation)
    output = result.output
    if result.status_code != 0 or output is None or output.get("return_code") == 0:
        return result
    return ContainerResult(
        result.output_schema_digest,
        output=output,
        object_reference=result.object_reference,
        status_code=1,
        status_message="failed",
        error_result={
            "code": "SCRIPT_FAILED",
            "message": "Script returned nonzero status",
            "retryable": False,
            "category": "business",
        },
        error_schema_digest=ERROR_SCHEMA_DIGEST,
    )


def main() -> None:
    """Serve the same SDK service consumed by the EP worker."""
    global _server_listening_mono  # noqa: PLW0603 — stamped once for execute() readers
    with ThreadPoolExecutor(max_workers=4) as pool:
        server = grpc.server(pool)
        add_container_runtime_service(server, StepRegistry({_STEP_REVISION_ID: execute}))
        server.add_insecure_port("[::]:50051")
        server.start()
        _server_listening_mono = time.monotonic()
        server.wait_for_termination()


if __name__ == "__main__":
    main()
