"""Service identity settings shared by EP and AO."""

from __future__ import annotations

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings


class IdentitySettings(BaseSettings):
    """JWT and service identity settings."""

    ao_jwt_public_key_path: str | None = Field(
        default=None,
        validation_alias=AliasChoices("EP_AO_JWT_PUBLIC_KEY_PATH", "AO_JWT_PUBLIC_KEY_PATH"),
    )
    ao_jwt_issuer: str | None = Field(
        default=None,
        validation_alias=AliasChoices("EP_AO_JWT_ISSUER", "AO_JWT_ISSUER"),
    )
    service_jwt_audience: str = Field(
        default="execution-plane",
        validation_alias=AliasChoices("EP_SERVICE_JWT_AUDIENCE", "SERVICE_JWT_AUDIENCE"),
    )
    ao_client_id: str = Field(default="syntara-orchestration", validation_alias="EP_AO_CLIENT_ID")
