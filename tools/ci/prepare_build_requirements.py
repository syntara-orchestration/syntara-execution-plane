"""Prepare PEP 517 dependency input for the hashed build requirements export.

pybuild-deps resolves public package artifacts, but cannot inspect a PEP 508
Git dependency. For the pinned SDK contracts source, keep its backend pinned
here so offline builds can install that source archive from Hermeto's cache.
"""

from __future__ import annotations

import sys
from pathlib import Path

_CONTRACTS_VCS_PREFIX = "syntara-plugin-contracts @ git+"
_CONTRACTS_BUILD_REQUIREMENTS = ("hatchling==1.32.4",)


def main() -> int:
    """Write public requirements and VCS PEP 517 backends to one input file."""
    if len(sys.argv) != 3:
        message = "usage: prepare_build_requirements.py REQUIREMENTS OUTPUT"
        raise SystemExit(message)
    requirements = Path(sys.argv[1])
    output = Path(sys.argv[2])
    lines = requirements.read_text(encoding="utf-8").splitlines()
    has_contracts_vcs = any(line.startswith(_CONTRACTS_VCS_PREFIX) for line in lines)
    if has_contracts_vcs:
        lines = [line for line in lines if not line.startswith(_CONTRACTS_VCS_PREFIX)]
        lines.extend(_CONTRACTS_BUILD_REQUIREMENTS)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
