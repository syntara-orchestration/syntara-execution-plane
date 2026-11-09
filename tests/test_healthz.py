"""Tests for the execution-plane-healthz readiness probe CLI."""

from __future__ import annotations

import io
from typing import Self
from unittest.mock import patch

import pytest

from execution_plane import healthz


def test_main_returns_zero_when_check_succeeds() -> None:
    with (
        patch.object(healthz, "check") as check,
        patch("sys.stderr", new=io.StringIO()) as stderr,
    ):
        check.side_effect = None
        assert healthz.main() == 0
    check.assert_awaited_once()
    assert stderr.getvalue() == ""


def test_main_returns_one_and_reports_error_when_check_fails() -> None:
    with (
        patch.object(healthz, "check", side_effect=RuntimeError("connection refused")),
        patch("sys.stderr", new=io.StringIO()) as stderr,
    ):
        assert healthz.main() == 1
    assert "connection refused" in stderr.getvalue()


@pytest.mark.parametrize(
    "url",
    ["postgresql+asyncpg://user:pass@host:5432/db"],
)
def test_check_resolves_database_url_through_settings(url: str) -> None:
    """check() builds the WorkStore from the settings-resolved database URL."""
    from execution_plane.config.settings import Settings

    with patch.dict("os.environ", {"EP_DATABASE_URL": url}):
        settings = Settings()
    captured: dict[str, str] = {}

    class _FakeStore:
        def __init__(self, database_url: str) -> None:
            captured["url"] = database_url

        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def check_ready(self) -> None:
            captured["ready"] = "yes"

    with (
        patch.object(healthz, "get_settings", return_value=settings),
        patch.object(healthz, "WorkStore") as store_cls,
    ):
        store_cls.from_database_url = _FakeStore
        import asyncio

        asyncio.run(healthz.check())

    assert captured["url"] == url
    assert captured["ready"] == "yes"
