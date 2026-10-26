"""Join bounded blocking backend operations before releasing their resources."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


async def run_blocking[**P, T](operation: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    """Do not abandon a resource mutation when its async caller is cancelled."""
    task = asyncio.create_task(asyncio.to_thread(operation, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # Cancelling to_thread does not stop its thread. The bounded API call
        # must finish before release can inspect ownership and delete resources.
        with suppress(Exception):
            await task
        raise
