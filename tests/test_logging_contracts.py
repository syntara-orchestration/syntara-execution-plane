"""Source contracts for application logging."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

_ROOT = Path(__file__).resolve().parents[1]
_LOG_METHODS = frozenset({"debug", "info", "warning", "error", "critical", "exception"})


def _source_files() -> Iterator[Path]:
    for package in (_ROOT / "src/execution_plane", _ROOT / "src/execution_plane_client"):
        yield from package.rglob("*.py")


def _is_structlog_logger_factory(call: ast.Call) -> bool:
    function = call.func
    return (
        isinstance(function, ast.Attribute)
        and function.attr == "get_logger"
        and isinstance(function.value, ast.Attribute)
        and function.value.attr == "stdlib"
        and isinstance(function.value.value, ast.Name)
        and function.value.value.id == "structlog"
        and len(call.args) == 1
        and isinstance(call.args[0], ast.Name)
        and call.args[0].id == "__name__"
        and not call.keywords
    )


def _is_logger_call(call: ast.Call) -> bool:
    function = call.func
    return (
        isinstance(function, ast.Attribute)
        and function.attr in _LOG_METHODS
        and isinstance(function.value, ast.Name)
        and function.value.id == "logger"
    )


def _is_direct_logging_call(call: ast.Call) -> bool:
    function = call.func
    return (
        isinstance(function, ast.Attribute)
        and function.attr in {*_LOG_METHODS, "basicConfig"}
        and isinstance(function.value, ast.Name)
        and function.value.id == "logging"
    )


def _call_errors(path: Path, node: ast.Call) -> list[str]:
    errors: list[str] = []
    if _is_direct_logging_call(node):
        errors.append(f"{path}: direct logging module calls are forbidden")
    if isinstance(node.func, ast.Name) and node.func.id == "fileConfig":
        errors.append(f"{path}: fileConfig is forbidden")
    if isinstance(node.func, ast.Name) and node.func.id == "print":
        errors.append(f"{path}: print is forbidden")
    if not _is_logger_call(node):
        return errors

    if len(node.args) != 1 or not isinstance(node.args[0], ast.Constant) or not isinstance(node.args[0].value, str):
        errors.append(f"{path}:{node.lineno}: logger calls must pass one literal event string")
        return errors

    event = node.args[0].value
    if not event or "." in event:
        errors.append(f"{path}:{node.lineno}: event must be non-empty human-readable text")
    if any(keyword.arg == "extra" for keyword in node.keywords):
        errors.append(f"{path}:{node.lineno}: structured fields must be keyword arguments, not extra")
    return errors


def _file_errors(path: Path) -> list[str]:
    errors: list[str] = []

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "logger" for target in node.targets
        ):
            if not (isinstance(node.value, ast.Call) and _is_structlog_logger_factory(node.value)):
                errors.append(f"{path}: logger must be created by structlog.stdlib.get_logger(__name__)")
        elif isinstance(node, ast.Call):
            errors.extend(_call_errors(path, node))
    return errors


def test_all_package_logging_uses_structlog_events_and_fields() -> None:
    errors = [error for path in _source_files() for error in _file_errors(path)]
    assert not errors, "\n".join(errors)
