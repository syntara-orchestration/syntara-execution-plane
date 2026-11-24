"""Regenerate build requirements using only versions in the committed lock.

Run with the pip, pip-tools and pybuild-deps versions pinned in the Makefile.
Build isolation can require several versions of the same backend, so preserve
an allowed set of versions rather than imposing conflicting == constraints.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

from packaging.utils import canonicalize_name
from packaging.version import Version
from pip._internal.network.session import PipSession
from pip._internal.req.constructors import install_req_from_parsed_requirement
from pip._internal.req.req_file import parse_requirements
from pybuild_deps.scripts import compile as compile_cli

if TYPE_CHECKING:
    from pip._internal.models.candidate import InstallationCandidate


def main() -> None:
    """Restrict the resolver's candidate pool unless explicitly upgrading."""
    upgrade = "--upgrade" in sys.argv
    if upgrade:
        sys.argv.remove("--upgrade")
    else:
        lock = Path("requirements-build.txt")
        if not lock.is_file():
            message = "Missing requirements-build.txt; run make upgrade-build-requirements"
            raise SystemExit(message)
        allowed: dict[str, set[Version]] = {}
        with PipSession() as session:
            for parsed in parse_requirements(str(lock), session=session):
                req = install_req_from_parsed_requirement(parsed)
                for spec in req.specifier:
                    if spec.operator != "==" or "*" in spec.version:
                        message = f"Expected exact build pin: {req}"
                        raise SystemExit(message)
                    allowed.setdefault(canonicalize_name(req.name), set()).add(Version(spec.version))

        class LockedRepository(compile_cli.PyPIRepository):
            def __init__(self, pip_args: list[str], cache_dir: str) -> None:
                super().__init__(pip_args, cache_dir)
                find_candidates = self.finder.find_all_candidates

                def locked_candidates(project_name: str) -> list[InstallationCandidate]:
                    versions = allowed.get(canonicalize_name(project_name), set())
                    return [c for c in find_candidates(project_name) if Version(str(c.version)) in versions]

                # Every resolution, including the compiler's fallback without
                # runtime constraints, uses this same restricted finder.
                self.finder.find_all_candidates = locked_candidates

        compile_cli.PyPIRepository = LockedRepository

    compile_cli.compile()


if __name__ == "__main__":
    main()
