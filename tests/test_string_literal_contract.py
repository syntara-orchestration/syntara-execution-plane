"""Source contract: multi-value string Literals are reserved for wire tags.

Domain vocabularies (status sets, reason sets, backend families, ...) belong
in ``StrEnum`` classes — they give iteration, membership, exhaustiveness and
typo-proof comparisons. ``Literal`` stays the right tool for pydantic
discriminated-union tags and single-value markers, which is what the
remaining uses are.

This test fails any *new* ``Literal`` holding two or more string values.
If a future change genuinely needs one (e.g. a closed tag union), add it to
the allowlist below with a reason.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

_ROOT = Path(__file__).resolve().parents[1]

# (relative path, sorted tuple of the literal's string values) -> reason
ALLOWLIST: dict[tuple[str, tuple[str, ...]], str] = {
    # Exact operation tags in the SDK host-operation protocol.
    (
        "src/execution_plane_client/commands/agent.py",
        ("agent.mcp.call", "agent.model.turn"),
    ): "The SDK host operation field is a closed wire-tag union.",
    (
        "src/execution_plane_client/commands/work.py",
        ("agent.mcp.call", "agent.model.turn"),
    ): "The work invocation grants these exact host operation tags.",
}


def _source_files() -> Iterator[Path]:
    for package in (_ROOT / "src/execution_plane", _ROOT / "src/execution_plane_client"):
        yield from package.rglob("*.py")


def _string_literal_values(node: ast.Subscript) -> tuple[str, ...] | None:
    """Return the string members of one Literal[...] subscript, if any."""
    if not isinstance(node.slice, ast.Tuple):
        return None
    values = [elt.value for elt in node.slice.elts if isinstance(elt, ast.Constant) and isinstance(elt.value, str)]
    return tuple(values) if values else None


def test_no_multi_value_string_literals_outside_the_allowlist() -> None:
    errors: list[str] = []
    for path in _source_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Subscript):
                continue
            function = node.value
            if not (isinstance(function, ast.Name) and function.id == "Literal"):
                continue
            values = _string_literal_values(node)
            if values is None or len(values) < 2:
                continue
            relative = str(path.relative_to(_ROOT))
            if (relative, tuple(sorted(values))) in ALLOWLIST:
                continue
            errors.append(
                f"{relative}:{node.lineno}: Literal[{', '.join(map(repr, values))}] is a "
                "multi-value string set — model it as a StrEnum (add to the allowlist "
                "with a reason only if it is a genuine wire-tag union)"
            )
    assert not errors, "\n".join(errors)
