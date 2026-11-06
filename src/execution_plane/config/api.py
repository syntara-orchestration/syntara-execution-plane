"""Standalone API server settings."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings


class APISettings(BaseSettings):
    """HTTP listener, TLS, and graceful shutdown settings."""

    api_host: str = Field(default="127.0.0.1", validation_alias="EP_API_HOST")
    api_port: int = Field(default=8001, validation_alias="EP_API_PORT")
    api_graceful_shutdown_seconds: int = Field(
        default=20,
        ge=1,
        validation_alias="EP_API_GRACEFUL_SHUTDOWN_SECONDS",
    )
    api_tls_cert_path: str | None = Field(default=None, validation_alias="EP_API_TLS_CERT_PATH")
    api_tls_key_path: str | None = Field(default=None, validation_alias="EP_API_TLS_KEY_PATH")
    api_tls_client_ca_path: str | None = Field(default=None, validation_alias="EP_API_TLS_CLIENT_CA_PATH")
