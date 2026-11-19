"""Combined Execution Plane settings."""

from __future__ import annotations

from functools import lru_cache

from pydantic import AliasChoices, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from execution_plane.config.api import APISettings
from execution_plane.config.callbacks import CallbackSettings
from execution_plane.config.database import DatabaseSettings
from execution_plane.config.dispatch import DispatchSettings
from execution_plane.config.identity import IdentitySettings
from execution_plane.config.logging import LoggingSettings
from execution_plane.config.monitoring import ResourceMonitorSettings
from execution_plane.config.otel import OpenTelemetrySettings
from execution_plane.config.security import CredentialEncryptionSettings
from execution_plane.config.workload import NodeLifecycleSettings, WorkloadEgressSettings

_SETTING_CLASSES = (
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
)


def _register_identifier(
    identifier_owners: dict[str, tuple[str, str]],
    field_name: str,
    owner: str,
    kind: str,
) -> None:
    if field_name not in identifier_owners:
        identifier_owners[field_name] = (owner, kind)
        return

    previous_owner, previous_kind = identifier_owners[field_name]
    if previous_kind == kind:
        label = "Duplicate computed setting field" if kind == "computed" else "Duplicate setting field"
    else:
        label = "Duplicate setting identifier"
    msg = f"{label} '{field_name}' in {previous_owner} and {owner}"
    raise ValueError(msg)


def _validation_aliases(field: object) -> tuple[str, ...]:
    validation_alias = getattr(field, "validation_alias", None)
    if isinstance(validation_alias, AliasChoices):
        return tuple(str(alias) for alias in validation_alias.choices)
    if validation_alias is not None:
        return (str(validation_alias),)
    return ()


def _validate_unique_setting_identifiers(setting_classes: tuple[type[BaseSettings], ...]) -> None:
    """Reject fields or environment aliases inherited from multiple settings bases."""
    identifier_owners: dict[str, tuple[str, str]] = {}
    alias_owners: dict[str, str] = {}
    for setting_class in setting_classes:
        owner = setting_class.__name__
        for field_name, field in setting_class.model_fields.items():
            _register_identifier(identifier_owners, field_name, owner, "field")
            for alias in _validation_aliases(field):
                if alias in alias_owners:
                    msg = f"Duplicate setting alias '{alias}' in {alias_owners[alias]} and {owner}"
                    raise ValueError(msg)
                alias_owners[alias] = owner

        for field_name in setting_class.model_computed_fields:
            _register_identifier(identifier_owners, field_name, owner, "computed")


class Settings(
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

    @model_validator(mode="after")
    def validate_unique_setting_identifiers(self) -> Settings:
        """Ensure component settings do not silently override one another."""
        _validate_unique_setting_identifiers(_SETTING_CLASSES)
        return self


@lru_cache
def get_settings() -> Settings:
    """Load and cache execution-plane settings from the environment."""
    # BaseSettings loads the required database_url from the environment.
    return Settings()  # type: ignore[call-arg]
