"""Shared fixtures for execution-plane unit tests."""

from collections.abc import Generator

import pytest

from execution_plane.config.settings import get_settings


@pytest.fixture(autouse=True)
def ep_settings_env(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Set the minimum required env vars for Settings and clear lru_caches."""
    monkeypatch.setenv("EP_DATABASE_URL", "postgresql+asyncpg://user:password@localhost/test")
    monkeypatch.setenv("EP_CREDENTIAL_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
