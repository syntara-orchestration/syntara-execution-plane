"""Runtime helpers shared by Execution Plane entrypoints."""

from __future__ import annotations

import asyncio
import contextlib
import signal
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from collections.abc import Coroutine

logger = structlog.stdlib.get_logger(__name__)
_SHUTDOWN_SIGNALS = (signal.SIGINT, signal.SIGTERM)


async def run_until_shutdown(target: Coroutine[Any, Any, None]) -> None:
    """Run a coroutine until SIGINT or SIGTERM requests a clean cancellation."""
    target_task = asyncio.create_task(target)
    loop = asyncio.get_running_loop()
    installed_signals: list[signal.Signals] = []

    def request_shutdown() -> None:
        logger.info("Shutdown signal received, cancelling runtime tasks")
        target_task.cancel()

    for shutdown_signal in _SHUTDOWN_SIGNALS:
        try:
            loop.add_signal_handler(shutdown_signal, request_shutdown)
        except NotImplementedError:
            continue
        installed_signals.append(shutdown_signal)

    try:
        await target_task
    except asyncio.CancelledError:
        if not target_task.cancelled():
            target_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await target_task
            raise
        logger.info("Runtime shutdown complete")
    finally:
        for shutdown_signal in installed_signals:
            loop.remove_signal_handler(shutdown_signal)
