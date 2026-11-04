#!/usr/bin/env python3
"""Keep only Linux x86_64/aarch64 hashes in a uv-exported requirements.txt.

Hermeto downloads every artifact whose hash appears in requirements.txt.
uv export includes macOS, Windows, and PyPy wheels; those must be dropped
so Cachi2 only prefetches what Konflux actually builds.

Usage:
    python3 tools/ci/filter_requirements_hashes.py [--lock uv.lock] [requirements.txt]
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ALLOWED_PLATFORM_PATTERNS = re.compile(
    r"("
    r"py3-none-any|py2\.py3-none-any"
    r"|none-any"
    r"|cp31[23]-cp31[23]-manylinux[_0-9]*_(x86_64|aarch64)"
    r"|cp31[23]-cp31[23]-musllinux[_0-9]*_(x86_64|aarch64)"
    r"|cp3[0-9]+-abi3-manylinux[_0-9]*_(x86_64|aarch64)"
    r"|cp3[0-9]+-abi3-musllinux[_0-9]*_(x86_64|aarch64)"
    r")"
)

_URL_HASH = re.compile(r'url\s*=\s*"([^"]+)",\s*hash\s*=\s*"sha256:([a-f0-9]+)"')
_HASH_LINE = re.compile(r"--hash=sha256:([a-f0-9]+)")
SDIST_EXTENSIONS = (".tar.gz", ".zip")
MIN_WHEEL_PARTS = 4


def parse_uv_lock(lock_path: Path) -> dict[str, str]:
    """Map sha256 digest to artifact filename from uv.lock URLs."""
    hash_to_filename: dict[str, str] = {}
    for match in _URL_HASH.finditer(lock_path.read_text(encoding="utf-8")):
        url, digest = match.group(1), match.group(2)
        hash_to_filename[digest] = url.rsplit("/", 1)[-1]
    return hash_to_filename


def is_allowed(filename: str) -> bool:
    """Return True when the artifact is an sdist or a Linux CPython wheel we build."""
    if any(filename.endswith(ext) for ext in SDIST_EXTENSIONS):
        return True
    if not filename.endswith(".whl"):
        return True
    parts = filename.rsplit("-", 3)
    if len(parts) < MIN_WHEEL_PARTS:
        return True
    platform_tag = "-".join(parts[-3:]).removesuffix(".whl")
    return bool(ALLOWED_PLATFORM_PATTERNS.search(platform_tag))


def _collect_hash_block(
    lines: list[str], start: int, hash_to_filename: dict[str, str]
) -> tuple[list[tuple[str, str | None]], int]:
    """Collect consecutive --hash= lines and the filenames those digests map to."""
    block: list[tuple[str, str | None]] = []
    index = start
    while index < len(lines) and lines[index].strip().startswith("--hash="):
        hash_line = lines[index]
        sha_match = _HASH_LINE.search(hash_line)
        filename = hash_to_filename.get(sha_match.group(1)) if sha_match else None
        block.append((hash_line, filename))
        index += 1
    return block, index


def _filter_hash_block(hash_block: list[tuple[str, str | None]]) -> tuple[list[str], int, int]:
    """Keep allowed-platform hashes; retain unknowns so we never drop a needed sdist."""
    kept: list[str] = []
    unknown = 0
    for hash_line, filename in hash_block:
        if filename is None:
            kept.append(hash_line)
            unknown += 1
        elif is_allowed(filename):
            kept.append(hash_line)
    if not kept and hash_block:
        kept.append(hash_block[0][0])
    if len(kept) == 1:
        last = kept[0].rstrip()
        if last.endswith(" \\"):
            kept[0] = last[:-2] + "\n"
    return kept, len(kept), unknown


def filter_requirements(req_path: Path, hash_to_filename: dict[str, str]) -> str:
    """Rewrite requirements.txt so only Konflux platform hashes remain."""
    lines = req_path.read_text(encoding="utf-8").splitlines(keepends=True)
    output: list[str] = []
    total_hashes = 0
    kept_hashes = 0
    unknown_hashes = 0
    index = 0
    while index < len(lines):
        if not lines[index].strip().startswith("--hash="):
            output.append(lines[index])
            index += 1
            continue
        hash_block, index = _collect_hash_block(lines, index, hash_to_filename)
        total_hashes += len(hash_block)
        kept, count, unknown = _filter_hash_block(hash_block)
        kept_hashes += count
        unknown_hashes += unknown
        output.extend(kept)
    print(
        f"Filtered {req_path.name}: {kept_hashes}/{total_hashes} hashes kept ({unknown_hashes} unresolved)",
        file=sys.stderr,
    )
    return "".join(output)


def main() -> int:
    """Filter a uv-exported requirements file in place."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("requirements", nargs="?", default="requirements.txt")
    parser.add_argument("--lock", default="uv.lock")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    lock_path = Path(args.lock)
    req_path = Path(args.requirements)
    if not lock_path.is_file():
        print(f"Error: {lock_path} not found", file=sys.stderr)
        return 1
    if not req_path.is_file():
        print(f"Error: {req_path} not found", file=sys.stderr)
        return 1

    filtered = filter_requirements(req_path, parse_uv_lock(lock_path))
    if args.dry_run:
        sys.stdout.write(filtered)
    else:
        req_path.write_text(filtered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
