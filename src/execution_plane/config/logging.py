"""Local structured logging settings."""

from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings

_VALID_LOG_LEVELS = frozenset({"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"})


class LoggingSettings(BaseSettings):
    """Structured stdout logging settings."""

    log_level: str = Field(default="INFO", validation_alias="EP_LOG_LEVEL")
    log_output_format: str = Field(default="json", validation_alias="EP_LOG_OUTPUT_FORMAT")

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        """Accept only standard Python logging level names."""
        normalized = value.upper()
        if normalized not in _VALID_LOG_LEVELS:
            msg = f"Invalid EP_LOG_LEVEL '{value}'. Must be one of: {', '.join(sorted(_VALID_LOG_LEVELS))}"
            raise ValueError(msg)
        return normalized

    @field_validator("log_output_format")
    @classmethod
    def validate_log_output_format(cls, value: str) -> str:
        """Accept only structured JSON or plain local text output."""
        normalized = value.lower()
        if normalized not in {"json", "text"}:
            msg = "EP_LOG_OUTPUT_FORMAT must be 'json' or 'text'"
            raise ValueError(msg)
        return normalized
