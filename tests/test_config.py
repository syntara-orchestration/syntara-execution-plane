"""Tests for execution-plane configuration."""

import pytest
from pydantic import ValidationError

from execution_plane.config import EPSettings, to_asyncpg_url


def test_to_asyncpg_url_normalizes_postgresql_driver_variants() -> None:
    """Listener URLs use the plain PostgreSQL scheme for every input variant."""
    assert (
        to_asyncpg_url("postgresql+asyncpg://user:password@localhost/syntara")
        == "postgresql://user:password@localhost/syntara"
    )
    assert (
        to_asyncpg_url("postgresql+psycopg://user:password@localhost/syntara")
        == "postgresql://user:password@localhost/syntara"
    )
    assert (
        to_asyncpg_url("postgresql://user:password@localhost/syntara") == "postgresql://user:password@localhost/syntara"
    )


def test_ep_settings_has_cold_start_and_network_defaults() -> None:
    settings = EPSettings()
    assert settings.node_startup_seconds == 120
    assert settings.node_grace_seconds == 30
    assert settings.api_graceful_shutdown_seconds == 20
    assert settings.workload_allowed_egress_cidrs == []
    assert settings.resource_monitor_interval_seconds == 30.0
    assert settings.resource_monitor_probe_timeout_seconds == 10.0
    assert settings.resource_monitor_failure_threshold == 3
    assert settings.resource_monitor_success_threshold == 2
    assert settings.db_ssl_mode == "prefer"


def test_resource_monitor_timeout_must_be_shorter_than_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EP_RESOURCE_MONITOR_INTERVAL_SECONDS", "5")
    monkeypatch.setenv("EP_RESOURCE_MONITOR_PROBE_TIMEOUT_SECONDS", "5")
    with pytest.raises(ValidationError, match="shorter than"):
        EPSettings()
