"""Shared fixtures for execution-plane integration tests.

Every test under ``tests/integration`` requires a real PostgreSQL database. Set
``EP_TEST_DATABASE_URL`` to a disposable asyncpg-compatible URL before running;
tests skip when it is absent.

Cluster-dispatch tests have an additional Kubernetes dependency and live under
``tests/integration/kind`` with their own ``conftest.py`` (the ``ep_cluster``
fixture). Postgres-only tests live under ``tests/integration/postgres``.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from execution_plane.config.settings import get_settings

if TYPE_CHECKING:
    from collections.abc import Generator

_REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _integration_database_env(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Override EP_DATABASE_URL with the real test DB for integration tests.

    The root conftest sets EP_DATABASE_URL to a dummy value for unit tests.
    This fixture runs after it (closer-conftest fixtures take priority) and
    re-points the setting at the disposable integration database so that
    get_settings() returns usable credentials during each test function.
    """
    database_url = os.environ.get("EP_TEST_DATABASE_URL")
    if database_url:
        monkeypatch.setenv("EP_DATABASE_URL", database_url)
        get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(scope="session")
def integration_database_url() -> str:
    """Return the disposable PostgreSQL URL, or skip the session if absent."""
    url = os.environ.get("EP_TEST_DATABASE_URL")
    if not url:
        pytest.skip("EP_TEST_DATABASE_URL must be set for integration tests")
    return url


@pytest.fixture(scope="session")
def migrated_database(integration_database_url: str) -> str:
    """Run alembic migrations against the test database and return the URL."""
    env = {**os.environ, "DATABASE_URL": integration_database_url}
    subprocess.run(
        ["uv", "run", "alembic", "-c", "alembic.ini", "upgrade", "head"],
        check=True,
        env=env,
        cwd=_REPO_ROOT,
    )
    return integration_database_url
