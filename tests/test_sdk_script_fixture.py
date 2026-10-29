"""Tests for the SDK script workload used by cluster integration tests."""

from __future__ import annotations

import importlib.util
import json
import shlex
import sys
import time
import tomllib
from pathlib import Path
from threading import Event
from typing import TYPE_CHECKING, Any

import pytest
from syntara_plugin.contracts import canonical_json_bytes
from syntara_plugin.runtime import ContainerInvocation, ExecutionContext, invoke

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


def _load_worker() -> Any:  # noqa: ANN401
    path = Path(__file__).parent / "fixtures" / "sdk_script" / "worker.py"
    spec = importlib.util.spec_from_file_location("sdk_script_worker", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class _ScopedRecordingCredentialProvider:
    def __init__(self, documents: dict[str, dict[str, str]], reads: list[str]) -> None:
        self._documents = documents
        self._reads = reads

    def read(self, name: str) -> dict[str, str]:
        self._reads.append(name)
        return self._documents[name]

    def close(self) -> None:
        return None


class _RecordingCredentialProvider:
    def __init__(self, documents: dict[str, dict[str, str]]) -> None:
        self._documents = documents
        self.reads: list[str] = []

    def restrict(
        self,
        names: frozenset[str],
        on_read: Callable[[Mapping[str, Any]], None],
    ) -> _ScopedRecordingCredentialProvider:
        assert names == {"syntara.e2e.bearer-token"}
        return _ScopedRecordingCredentialProvider(self._documents, self.reads)


def test_sdk_script_image_matches_ep_contracts_source() -> None:
    project = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())
    dependency = next(
        dependency
        for dependency in project["project"]["dependencies"]
        if dependency.startswith("syntara-plugin-contracts @ ")
    )
    containerfile = (Path(__file__).parent / "fixtures" / "sdk_script" / "Containerfile").read_text()
    install = next(line for line in containerfile.splitlines() if line.startswith("RUN pip install "))

    assert shlex.split(install) == ["RUN", "pip", "install", "--no-cache-dir", dependency]


def test_python_script_result_matches_ao_output_contract() -> None:
    worker = _load_worker()
    invocation = ContainerInvocation(
        invocation_id="test-invocation",
        step_revision_id="script.v1",
        inputs={"language": "python", "code": "import json; print(json.dumps({'answer': 42}))"},
        timeout_seconds=10,
        cancellation=Event(),
    )

    result = worker.execute(invocation)

    assert result.output == {
        "return_code": 0,
        "stdout": '{"answer": 42}\n',
        "stderr": "",
        "stdout_json": {"answer": 42},
    }


def test_python_script_json_after_logs_is_parsed() -> None:
    worker = _load_worker()
    invocation = ContainerInvocation(
        invocation_id="test-invocation",
        step_revision_id="script.v1",
        inputs={"language": "python", "code": "import json; print('running'); print(json.dumps({'answer': 42}))"},
        timeout_seconds=10,
        cancellation=Event(),
    )

    result = worker.execute(invocation)

    assert result.output["stdout_json"] == {"answer": 42}


def test_perf_worker_timestamps_appended_to_stderr_when_enabled() -> None:
    worker = _load_worker()
    # Must be on the same mono clock as execute(); a fixed large stub can sit
    # ahead of the process mono and make execute_entered < listening.
    listening = time.monotonic() - 1.0
    worker._server_listening_mono = listening
    invocation = ContainerInvocation(
        invocation_id="test-invocation",
        step_revision_id="script.v1",
        inputs={
            "language": "bash",
            "code": "echo hi",
            "environment": {"EP_PERF_WORKER_TIMESTAMPS": "1"},
        },
        timeout_seconds=10,
        cancellation=Event(),
    )

    result = worker.execute(invocation)

    stderr = result.output["stderr"]
    assert "EP_PERF_TIMINGS=" in stderr
    line = next(line for line in stderr.splitlines() if line.startswith("EP_PERF_TIMINGS="))
    payload = json.loads(line.removeprefix("EP_PERF_TIMINGS="))
    assert payload["server_listening_mono"] == listening
    assert payload["execute_entered_mono"] >= listening
    assert payload["post_pod_overhead_ms"] == pytest.approx((payload["execute_entered_mono"] - listening) * 1000.0)
    assert payload["post_pod_overhead_ms"] >= 0.0


def test_perf_worker_timestamps_omitted_by_default() -> None:
    worker = _load_worker()
    worker._server_listening_mono = 1000.0
    invocation = ContainerInvocation(
        invocation_id="test-invocation",
        step_revision_id="script.v1",
        inputs={"language": "bash", "code": "echo hi"},
        timeout_seconds=10,
        cancellation=Event(),
    )

    result = worker.execute(invocation)

    assert "EP_PERF_TIMINGS=" not in result.output["stderr"]


def test_script_timeout_is_enforced_locally() -> None:
    worker = _load_worker()
    invocation = ContainerInvocation(
        invocation_id="test-invocation",
        step_revision_id="script.v1",
        inputs={"language": "bash", "code": "sleep 2"},
        timeout_seconds=1,
        cancellation=Event(),
    )

    result = worker.execute(invocation)

    assert result.output["return_code"] != 0


def test_failed_script_returns_terminal_failure() -> None:
    worker = _load_worker()
    invocation = ContainerInvocation(
        invocation_id="test-invocation",
        step_revision_id="script.v1",
        inputs={"language": "bash", "code": "exit 7"},
        timeout_seconds=10,
        cancellation=Event(),
    )

    result = worker.execute(invocation)

    assert result.status_code == 1
    assert result.output["return_code"] == 7
    assert result.error_result is not None
    assert result.error_result["code"] == "SCRIPT_FAILED"


def test_credential_is_not_requested_without_opt_in() -> None:
    worker = _load_worker()
    provider = _RecordingCredentialProvider({"syntara.e2e.bearer-token": {"bearer_token": "secret"}})
    context = ExecutionContext(
        invocation_id="test-invocation",
        credential_provider=provider,
    )

    outcome = invoke(
        worker.script_binding(),
        {"language": "bash", "code": "echo hi"},
        context,
    )

    assert outcome.status_code == 0
    assert provider.reads == []


def test_opt_in_credential_read_emits_digest_without_secret() -> None:
    worker = _load_worker()
    document = {"auth_type": "bearer", "bearer_token": "e2e-secret-token"}
    provider = _RecordingCredentialProvider({"syntara.e2e.bearer-token": document})
    context = ExecutionContext(
        invocation_id="test-invocation",
        credential_provider=provider,
    )

    outcome = invoke(
        worker.script_binding(),
        {
            "language": "bash",
            "code": "echo hi",
            "environment": {"EP_E2E_USE_CREDENTIAL": "1"},
        },
        context,
    )

    assert outcome.status_code == 0
    assert provider.reads == ["syntara.e2e.bearer-token"]
    result = outcome.result
    assert result is not None
    assert result["stdout"] == f"hi\nEP_E2E_CREDENTIAL_SHA256={_sha256(document)}\n"
    assert "e2e-secret-token" not in result["stderr"]


def test_opt_in_credential_read_preserves_python_stdout_json() -> None:
    worker = _load_worker()
    document = {"auth_type": "bearer", "bearer_token": "e2e-secret-token"}
    provider = _RecordingCredentialProvider({"syntara.e2e.bearer-token": document})
    context = ExecutionContext(
        invocation_id="test-invocation",
        credential_provider=provider,
    )

    outcome = invoke(
        worker.script_binding(),
        {
            "language": "python",
            "code": "import json; print(json.dumps({'answer': 42}))",
            "environment": {"EP_E2E_USE_CREDENTIAL": "1"},
        },
        context,
    )

    assert outcome.status_code == 0
    result = outcome.result
    assert result is not None
    assert result["stdout_json"] == {"answer": 42}


def _sha256(document: dict[str, str]) -> str:
    import hashlib

    return hashlib.sha256(canonical_json_bytes(document)).hexdigest()
