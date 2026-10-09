"""Shared fixtures for execution-plane integration tests.

Every test under ``tests/integration`` requires a real PostgreSQL database.
Set ``EP_TEST_DATABASE_URL`` to a disposable asyncpg-compatible URL before
running; tests fail immediately if it is absent.

Cluster-dispatch tests have an additional Kubernetes dependency and live under
``tests/integration/kind`` with their own ``conftest.py`` (the ``ep_cluster``
fixture). Postgres-only tests live under ``tests/integration/postgres``.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def integration_database_url() -> str:
    """Return the disposable PostgreSQL URL; fail immediately if absent."""
    url = os.environ.get("EP_TEST_DATABASE_URL")
    if not url:
        pytest.fail("EP_TEST_DATABASE_URL must be set to run integration tests")
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
