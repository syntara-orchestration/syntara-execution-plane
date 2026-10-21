"""Tests for the SDK script workload used by cluster integration tests."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from threading import Event
from typing import TYPE_CHECKING

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


def test_python_script_result_matches_ao_output_contract() -> None:
    worker = _load_worker()
    invocation = ContainerInvocation(
        invocation_id="test-invocation",
        workflow_run_id="test-workflow",
        step_revision_id="script.v1",
        inputs={"language": "python", "code": "import json; print(json.dumps({'answer': 42}))"},
        credential_file_paths={},
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
        workflow_run_id="test-workflow",
        step_revision_id="script.v1",
        inputs={"language": "python", "code": "import json; print('running'); print(json.dumps({'answer': 42}))"},
        credential_file_paths={},
        timeout_seconds=10,
        cancellation=Event(),
    )

    result = worker.execute(invocation)

    assert result.output["stdout_json"] == {"answer": 42}
