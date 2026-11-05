"""Tests for the Execution Plane API entrypoint runtime."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import uvicorn

if TYPE_CHECKING:
    import pytest

from execution_plane.api import main as api_main


async def test_api_lifespan_owns_one_engine() -> None:
    from unittest.mock import AsyncMock, patch

    from sqlalchemy.ext.asyncio import AsyncEngine

    engine = AsyncMock(spec=AsyncEngine)
    app = api_main.create_app()
    with patch.object(api_main, "create_pooled_engine", return_value=engine) as create_engine:
        async with api_main.lifespan(app):
            assert app.state.ep_engine is engine
            assert app.state.ep_session_factory.kw["bind"] is engine
            engine.dispose.assert_not_awaited()
        create_engine.assert_called_once_with(app.state.ep_settings.database_url)
    engine.dispose.assert_awaited_once()


def test_api_main_configures_a_bounded_graceful_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = SimpleNamespace(
        api_host="127.0.0.1",
        api_port=8001,
        api_tls_cert_path=None,
        api_tls_key_path=None,
        api_tls_client_ca_path=None,
        api_graceful_shutdown_seconds=7,
    )
    captured_options: dict[str, Any] = {}

    monkeypatch.setattr(api_main, "get_ep_settings", lambda: settings)
    monkeypatch.setattr(uvicorn, "run", lambda _app, **options: captured_options.update(options))

    api_main.main()

    assert captured_options["timeout_graceful_shutdown"] == 7
