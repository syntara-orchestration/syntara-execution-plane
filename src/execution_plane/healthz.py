"""Readiness probe CLI for the Execution Plane worker.

Exit code 0: the database is reachable and the work_items table is
readable. Exit code 1: anything else.

Used as the container readiness probe by the operator (AAP-93327): a
failing probe marks the pod not-ready — pausing rolling updates and
removing it from dispatch — without restarting it, which is the right
behavior for a transient database blip. The command resolves its
configuration through the same settings system as the worker, so env var
names and defaults stay in sync automatically.
"""

from __future__ import annotations

import asyncio
import sys

from execution_plane.config.settings import get_settings
from execution_plane.work_item.work_item_store import WorkStore


async def check() -> None:
    """Resolve settings like the worker and verify WorkStore readability."""
    settings = get_settings()
    async with WorkStore.from_database_url(settings.database_url) as store:
        await store.check_ready()


def main() -> int:
    """Run the readiness check; report failures on stderr with exit 1."""
    try:
        asyncio.run(check())
    except Exception as exc:  # noqa: BLE001 - any failure means not ready
        # A raw write, not a logger: the kubelet captures probe stderr, and
        # structlog is not configured in the probe's short-lived process.
        sys.stderr.write(f"execution-plane-healthz: {exc}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
