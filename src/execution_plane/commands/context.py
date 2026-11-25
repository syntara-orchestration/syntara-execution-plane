"""EP infrastructure supplied to installed command handlers."""

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncEngine


@dataclass(frozen=True)
class EPCommandContext:
    """Authenticated client and application-owned domain database."""

    client_id: str
    engine: AsyncEngine
    workload_image_policy_path: str | None = None
