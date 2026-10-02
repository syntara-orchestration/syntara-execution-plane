"""Entrypoint run inside a short-lived, isolated script workload pod."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from execution_plane.script_executor import ScriptExecutionError, execute_script

_EXPECTED_ARGUMENT_COUNT = 2


async def _run(input_path: Path) -> int:
    """Execute one JSON payload and emit one bounded JSON result on stdout."""
    payload: dict[str, Any] = json.loads(input_path.read_text(encoding="utf-8"))
    input_config = payload.get("input_config")
    if not isinstance(input_config, dict):
        result = {"status": "failed", "result": {"error": "Invalid workload input", "error_type": "ConfigError"}}
    else:
        output_config = payload.get("output_config")
        try:
            result = {"status": "completed", "result": await execute_script(input_config, output_config)}
        except ScriptExecutionError as exc:
            result = {
                "status": "failed",
                "result": {
                    "error": str(exc),
                    "error_type": "ScriptExecutionError",
                    "exit_code": exc.exit_code,
                    "stdout": exc.stdout,
                    "stderr": exc.stderr,
                },
            }
        except TimeoutError as exc:
            result = {"status": "failed", "result": {"error": str(exc), "error_type": "TimeoutError"}}
    sys.stdout.write(json.dumps(result, separators=(",", ":")) + "\n")
    return 0


def main() -> None:
    """Run the workload payload file mounted by the Execution Plane controller."""
    if len(sys.argv) != _EXPECTED_ARGUMENT_COUNT:
        msg = "Usage: python -m execution_plane.workload_runner <payload.json>"
        raise SystemExit(msg)
    raise SystemExit(asyncio.run(_run(Path(sys.argv[1]))))


if __name__ == "__main__":
    main()
