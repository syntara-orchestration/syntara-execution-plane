"""Map node results to the backend-neutral execution outcome."""

from __future__ import annotations

from typing import Any

from execution_plane.models.work_item import WorkItemStatus
from execution_plane.worker_manager.lifecycle import BackendError, ExecutionResult

MAX_STATUS_CODE = 255


def execution_result(frame: dict[str, Any], output_config: dict[str, str] | None) -> ExecutionResult:
    """Retain terminal errors and partial output without interpreting output expressions."""
    result = frame.get("result")
    if not isinstance(result, dict) or type(result.get("StatusCode")) is not int:
        message = "Node returned an invalid result"
        raise BackendError(message)
    status_code = result["StatusCode"]
    if not 0 <= status_code <= MAX_STATUS_CODE:
        message = "Node returned an out-of-range status code"
        raise BackendError(message)
    raw = result.get("Result")
    if raw is not None and not isinstance(raw, dict):
        message = "Node returned invalid output"
        raise BackendError(message)
    raw = raw or {}
    # Select output fields by name. AO evaluates template expressions.
    output = raw if output_config is None else {key: raw[key] for key in output_config if key in raw}
    if status_code == 0:
        return ExecutionResult(WorkItemStatus.COMPLETED, {"output": output})
    failure = frame.get("error") or {}
    message = result.get("ErrorMessage") or result.get("StatusMessage") or "Node execution failed"
    error_type = failure.get("type") or "NodeExecutionError"
    if error_type == "CancelledError":
        return ExecutionResult(
            WorkItemStatus.CANCELLED,
            {"cancelled": True, "execution_started": True, "reason": message, "output": output},
        )
    return ExecutionResult(WorkItemStatus.FAILED, {"error": message, "error_type": error_type, "output": output})
