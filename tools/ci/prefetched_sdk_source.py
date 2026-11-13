"""Locate SDK contracts in Hermeto's pinned Git archive for an offline build."""

from __future__ import annotations

import os
import re
import tarfile
import tempfile
import tomllib
from pathlib import Path
from urllib.parse import urlsplit


def prefetched_sdk_source(project_root: Path, archives: Path, destination: Path) -> Path:
    """Extract the contracts project and canonical schemas without network access."""
    metadata = tomllib.loads((project_root / "pyproject.toml").read_text())
    source = metadata["tool"]["uv"]["sources"]["syntara-plugin-contracts"]
    lock = tomllib.loads((project_root / "uv.lock").read_text())
    package = next(package for package in lock["package"] if package["name"] == "syntara-plugin-contracts")
    commit = urlsplit(package["source"]["git"]).fragment
    if not re.fullmatch(r"[0-9a-f]{40}", commit) or commit != source["rev"]:
        msg = "SDK source must match the immutable Git commit in uv.lock"
        raise ValueError(msg)
    subdirectory = Path(source["subdirectory"])
    if subdirectory.is_absolute() or ".." in subdirectory.parts:
        msg = "SDK subdirectory must remain inside its repository"
        raise ValueError(msg)
    repository = Path(urlsplit(source["git"]).path).stem
    archive_path = archives / f"{repository}-gitcommit-{commit}.tar.gz"
    prefixes = (f"app/{subdirectory.as_posix()}/", "app/contracts/")
    with tarfile.open(archive_path, "r:gz") as archive:
        archive.extractall(
            destination,
            members=[entry for entry in archive.getmembers() if entry.name.startswith(prefixes)],
            filter="data",
        )
    project = destination / "app" / subdirectory
    if not (project / "pyproject.toml").is_file() or not (destination / "app/contracts/schemas").is_dir():
        msg = "Prefetched SDK archive is missing its project or canonical schemas"
        raise ValueError(msg)
    return project


if __name__ == "__main__":
    print(
        prefetched_sdk_source(Path.cwd(), Path(os.environ["PIP_FIND_LINKS"]), Path(tempfile.mkdtemp(prefix="ep-sdk-")))
    )
