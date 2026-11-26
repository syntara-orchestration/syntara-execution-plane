"""Prepare PEP 517 dependency input for the hashed build requirements export.

pybuild-deps resolves public package artifacts, but cannot inspect a PEP 508
Git dependency. For the pinned SDK contracts source, keep its backend pinned
here so offline builds can install that source archive from Hermeto's cache.
"""

from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_REQUIREMENTS = _ROOT / "requirements.txt"
_OUTPUT = _ROOT / ".requirements-build-input.txt"
_CONTRACTS_VCS_PREFIX = "syntara-plugin-contracts @ git+"
_CONTRACTS_BUILD_REQUIREMENTS = ("hatchling==1.32.4",)


def main() -> int:
    """Write public requirements and VCS PEP 517 backends to one input file."""
    lines = _REQUIREMENTS.read_text(encoding="utf-8").splitlines()
    has_contracts_vcs = any(line.startswith(_CONTRACTS_VCS_PREFIX) for line in lines)
    if has_contracts_vcs:
        lines = [line for line in lines if not line.startswith(_CONTRACTS_VCS_PREFIX)]
        lines.extend(_CONTRACTS_BUILD_REQUIREMENTS)
    _OUTPUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
