"""Work dispatch settings."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings


class DispatchSettings(BaseSettings):
    """Dispatch retry and concurrency settings."""

    dispatch_retry_backoff_seconds: float = Field(default=5.0, validation_alias="EP_DISPATCH_RETRY_BACKOFF_SECONDS")
    max_concurrent_dispatches: int = Field(
        default=8,
        ge=1,
        validation_alias="EP_MAX_CONCURRENT_DISPATCHES",
        description=(
            "Defensive cap on the number of cold-start dispatches in flight simultaneously. "
            "Limits resource consumption when many work items arrive at once."
        ),
    )
