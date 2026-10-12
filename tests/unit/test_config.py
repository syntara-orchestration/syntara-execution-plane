"""Tests for execution-plane configuration."""

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
    assert settings.workload_allowed_egress_cidrs == []
