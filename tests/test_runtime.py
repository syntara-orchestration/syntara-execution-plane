"""Tests for Execution Plane entrypoint runtime behavior."""

from __future__ import annotations

import asyncio
import os
import signal

import pytest

from execution_plane.runtime import run_until_shutdown


@pytest.mark.asyncio
@pytest.mark.parametrize("shutdown_signal", [signal.SIGINT, signal.SIGTERM])
async def test_run_until_shutdown_cancels_target_and_runs_cleanup(shutdown_signal: signal.Signals) -> None:
    started = asyncio.Event()
    cleanup_ran = False

    async def target() -> None:
        nonlocal cleanup_ran
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_ran = True

    runner = asyncio.create_task(run_until_shutdown(target()))
    await asyncio.wait_for(started.wait(), timeout=1)
    os.kill(os.getpid(), shutdown_signal)

    await asyncio.wait_for(runner, timeout=1)

    assert cleanup_ran


@pytest.mark.asyncio
async def test_run_until_shutdown_propagates_target_errors() -> None:
    async def target() -> None:
        msg = "target failed"
        raise RuntimeError(msg)

    with pytest.raises(RuntimeError, match="target failed"):
        await run_until_shutdown(target())
