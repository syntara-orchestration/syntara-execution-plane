"""Combined Execution Plane settings."""

from __future__ import annotations

from functools import lru_cache

from pydantic import computed_field
from pydantic_settings import SettingsConfigDict

from execution_plane.config.api import APISettings
from execution_plane.config.callbacks import CallbackSettings
from execution_plane.config.database import DatabaseSettings, to_asyncpg_url
from execution_plane.config.dispatch import DispatchSettings
from execution_plane.config.identity import IdentitySettings
from execution_plane.config.logging import LoggingSettings
from execution_plane.config.monitoring import ResourceMonitorSettings
from execution_plane.config.otel import OpenTelemetrySettings
from execution_plane.config.security import CredentialEncryptionSettings
from execution_plane.config.workload import NodeLifecycleSettings, WorkloadEgressSettings


class EPSettings(
    WorkloadEgressSettings,
    CallbackSettings,
    ResourceMonitorSettings,
    DispatchSettings,
    CredentialEncryptionSettings,
    APISettings,
    OpenTelemetrySettings,
    LoggingSettings,
    IdentitySettings,
    NodeLifecycleSettings,
    DatabaseSettings,
):
    """Settings for the standalone Execution Plane service and worker."""

    model_config = SettingsConfigDict(extra="ignore")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url_asyncpg(self) -> str:
        """asyncpg-compatible URL (strips the +asyncpg SQLAlchemy driver suffix)."""
        return to_asyncpg_url(self.database_url)


@lru_cache
def get_ep_settings() -> EPSettings:
    """Load and cache execution-plane settings from the environment."""
    # BaseSettings loads the required database_url from the environment.
    return EPSettings()  # type: ignore[call-arg]
