"""Execution target resource monitoring settings."""

from __future__ import annotations

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings


class ResourceMonitorSettings(BaseSettings):
    """Resource monitor interval, timeout, and threshold settings."""

    resource_monitor_interval_seconds: float = Field(
        default=30.0,
        ge=1.0,
        validation_alias="EP_RESOURCE_MONITOR_INTERVAL_SECONDS",
    )
    resource_monitor_probe_timeout_seconds: float = Field(
        default=10.0,
        gt=0.0,
        validation_alias="EP_RESOURCE_MONITOR_PROBE_TIMEOUT_SECONDS",
    )
    resource_monitor_failure_threshold: int = Field(
        default=3,
        ge=1,
        validation_alias="EP_RESOURCE_MONITOR_FAILURE_THRESHOLD",
    )
    resource_monitor_success_threshold: int = Field(
        default=2,
        ge=1,
        validation_alias="EP_RESOURCE_MONITOR_SUCCESS_THRESHOLD",
    )

    @model_validator(mode="after")
    def validate_resource_monitor_timeout(self) -> ResourceMonitorSettings:
        """Keep each probe shorter than the scan interval so scans cannot overlap on a timeout."""
        if self.resource_monitor_probe_timeout_seconds >= self.resource_monitor_interval_seconds:
            msg = "EP_RESOURCE_MONITOR_PROBE_TIMEOUT_SECONDS must be shorter than EP_RESOURCE_MONITOR_INTERVAL_SECONDS"
            raise ValueError(msg)
        return self
