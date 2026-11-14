"""Tests for the SDK script workload used by cluster integration tests."""

from __future__ import annotations

import importlib.util
import json
import re
import time
import tomllib
from pathlib import Path
from threading import Event
from typing import TYPE_CHECKING

import pytest
from syntara_plugin.runtime import ContainerInvocation

if TYPE_CHECKING:
    from types import ModuleType


def _load_worker() -> ModuleType:
    path = Path(__file__).parent / "fixtures" / "sdk_script" / "worker.py"
    spec = importlib.util.spec_from_file_location("sdk_script_worker", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_sdk_script_image_matches_ep_contracts_source() -> None:
    project = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())
    source = project["tool"]["uv"]["sources"]["syntara-plugin-contracts"]
    repository = source["git"].removesuffix(".git")
    expected = f"{repository}/archive/{source['rev']}.tar.gz#subdirectory={source['subdirectory']}"
    containerfile = (Path(__file__).parent / "fixtures" / "sdk_script" / "Containerfile").read_text()
    match = re.search(
        r'^RUN pip install --no-cache-dir "syntara-plugin-contracts @ ([^"\n]+)"$', containerfile, re.MULTILINE
    )

    assert match is not None
    assert match.group(1) == expected


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
