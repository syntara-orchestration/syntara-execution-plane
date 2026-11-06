"""Tests for execution-plane configuration."""

import pytest
from pydantic import Field, ValidationError, computed_field
from pydantic_settings import BaseSettings

from execution_plane.config import settings as settings_module
from execution_plane.config.database import DatabaseSettings, to_asyncpg_url
from execution_plane.config.settings import Settings


def test_to_asyncpg_url_normalizes_postgresql_driver_variants() -> None:
    """Listener URLs use the plain PostgreSQL scheme for every input variant."""
    assert (
        to_asyncpg_url("postgresql+asyncpg://user:password@localhost/syntara")
        == "postgresql://user:password@localhost/syntara"
    )


def test_database_settings_expose_asyncpg_url() -> None:
    settings = DatabaseSettings(EP_DATABASE_URL="postgresql+asyncpg://user:password@localhost/syntara")  # type: ignore[call-arg]

    assert settings.database_url_asyncpg == "postgresql://user:password@localhost/syntara"
    assert (
        to_asyncpg_url("postgresql+psycopg://user:password@localhost/syntara")
        == "postgresql://user:password@localhost/syntara"
    )
    assert (
        to_asyncpg_url("postgresql://user:password@localhost/syntara") == "postgresql://user:password@localhost/syntara"
    )


def test_ep_settings_has_cold_start_and_network_defaults() -> None:
    settings = Settings()  # type: ignore[call-arg]
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
        Settings()  # type: ignore[call-arg]


def test_database_pool_defaults() -> None:
    settings = Settings()  # type: ignore[call-arg]
    assert settings.db_pool_size == 5
    assert settings.db_pool_max_overflow == 0
    assert settings.db_pool_timeout == 30.0


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("EP_DB_POOL_SIZE", "0"),
        ("EP_DB_POOL_SIZE", "-1"),
        ("EP_DB_POOL_MAX_OVERFLOW", "-1"),
        ("EP_DB_POOL_TIMEOUT", "0"),
        ("EP_DB_POOL_TIMEOUT", "-1"),
        ("EP_DB_POOL_TIMEOUT", "inf"),
        ("EP_DB_POOL_TIMEOUT", "nan"),
    ],
)
def test_database_pool_requires_finite_bounds(monkeypatch: pytest.MonkeyPatch, variable: str, value: str) -> None:
    monkeypatch.setenv(variable, value)
    with pytest.raises(ValidationError):
        Settings()  # type: ignore[call-arg]


def test_settings_reject_duplicate_component_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    class FirstSettings(BaseSettings):
        duplicate: int = Field(default=1, validation_alias="EP_FIRST")

    class SecondSettings(BaseSettings):
        duplicate: int = Field(default=2, validation_alias="EP_SECOND")

    monkeypatch.setattr(settings_module, "_SETTING_CLASSES", (FirstSettings, SecondSettings))
    with pytest.raises(ValidationError, match="Duplicate setting field 'duplicate'"):
        Settings()  # type: ignore[call-arg]


def test_settings_reject_duplicate_environment_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    class FirstSettings(BaseSettings):
        first: int = Field(default=1, validation_alias="EP_DUPLICATE")

    class SecondSettings(BaseSettings):
        second: int = Field(default=2, validation_alias="EP_DUPLICATE")

    monkeypatch.setattr(settings_module, "_SETTING_CLASSES", (FirstSettings, SecondSettings))
    with pytest.raises(ValidationError, match="Duplicate setting alias 'EP_DUPLICATE'"):
        Settings()  # type: ignore[call-arg]


def test_settings_reject_duplicate_computed_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    class FirstSettings(BaseSettings):
        @computed_field  # type: ignore[prop-decorator]
        @property
        def duplicate(self) -> int:
            return 1

    class SecondSettings(BaseSettings):
        @computed_field  # type: ignore[prop-decorator]
        @property
        def duplicate(self) -> int:
            return 2

    monkeypatch.setattr(settings_module, "_SETTING_CLASSES", (FirstSettings, SecondSettings))
    with pytest.raises(ValidationError, match="Duplicate computed setting field 'duplicate'"):
        Settings()  # type: ignore[call-arg]
