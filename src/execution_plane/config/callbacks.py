"""Outbound callback settings."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings


class CallbackSettings(BaseSettings):
    """Completion and credential-resolution callback settings."""

    completion_callback_url: str | None = Field(default=None, validation_alias="EP_COMPLETION_CALLBACK_URL")
    completion_callback_ca_cert_path: str | None = Field(default=None, validation_alias="EP_CALLBACK_CA_CERT_PATH")
    completion_callback_cert_path: str | None = Field(default=None, validation_alias="EP_CALLBACK_CERT_PATH")
    completion_callback_key_path: str | None = Field(default=None, validation_alias="EP_CALLBACK_KEY_PATH")
    completion_callback_timeout_seconds: float = Field(
        default=10.0, gt=0.0, validation_alias="EP_CALLBACK_TIMEOUT_SECONDS"
    )
    credential_resolution_callback_url: str | None = Field(
        default=None,
        validation_alias="EP_CREDENTIAL_RESOLUTION_CALLBACK_URL",
    )
    credential_resolution_callback_ca_cert_path: str | None = Field(
        default=None,
        validation_alias="EP_CREDENTIAL_RESOLUTION_CALLBACK_CA_CERT_PATH",
    )
    credential_resolution_callback_cert_path: str | None = Field(
        default=None,
        validation_alias="EP_CREDENTIAL_RESOLUTION_CALLBACK_CERT_PATH",
    )
    credential_resolution_callback_key_path: str | None = Field(
        default=None,
        validation_alias="EP_CREDENTIAL_RESOLUTION_CALLBACK_KEY_PATH",
    )
    credential_resolution_callback_timeout_seconds: float = Field(
        default=10.0,
        gt=0.0,
        validation_alias="EP_CREDENTIAL_RESOLUTION_CALLBACK_TIMEOUT_SECONDS",
    )
