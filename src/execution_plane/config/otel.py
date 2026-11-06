"""OTLP log emission settings."""

from __future__ import annotations

import re
import socket
from urllib.parse import urlparse

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings

_SVC_DNS_RE = re.compile(
    r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?\.svc(?:\.cluster\.local)?$"
)


class OpenTelemetrySettings(BaseSettings):
    """OTLP HTTP exporter and authentication settings."""

    otel_enabled: bool = Field(default=False, validation_alias="EP_OTEL_ENABLED")
    otel_service_name: str = Field(default="execution-plane", validation_alias="EP_OTEL_SERVICE_NAME", min_length=1)
    otel_service_instance_id: str = Field(
        default_factory=socket.gethostname,
        validation_alias="EP_OTEL_SERVICE_INSTANCE_ID",
        min_length=1,
    )
    otel_endpoint: str = Field(
        default="http://localhost:4318/v1/logs",
        validation_alias="EP_OTEL_ENDPOINT",
    )
    otel_api_key: SecretStr | None = Field(default=None, validation_alias="EP_OTEL_API_KEY")
    otel_auth_header_name: str = Field(default="Authorization", validation_alias="EP_OTEL_AUTH_HEADER_NAME")
    otel_ca_cert_file: str | None = Field(default=None, validation_alias="EP_OTEL_CA_CERT_FILE")
    otel_client_cert_file: str | None = Field(default=None, validation_alias="EP_OTEL_CLIENT_CERT_FILE")
    otel_client_key_file: str | None = Field(default=None, validation_alias="EP_OTEL_CLIENT_KEY_FILE")

    @field_validator("otel_endpoint")
    @classmethod
    def validate_otel_endpoint(cls, value: str) -> str:
        """Require HTTPS except for loopback or Kubernetes service collectors."""
        parsed = urlparse(value)
        if parsed.scheme == "https":
            return value
        if parsed.scheme == "http" and (
            parsed.hostname in {"localhost", "127.0.0.1", "::1"} or _SVC_DNS_RE.match(parsed.hostname or "")
        ):
            return value
        msg = (
            "Remote OTLP endpoints must use HTTPS; HTTP is allowed only for loopback "
            "or <service>.<namespace>.svc[.cluster.local] collectors"
        )
        raise ValueError(msg)

    @model_validator(mode="after")
    def validate_otel_fields(self) -> OpenTelemetrySettings:
        """Reject incomplete OTEL credentials and certificate material."""
        if (self.otel_client_cert_file is None) != (self.otel_client_key_file is None):
            missing = "EP_OTEL_CLIENT_KEY_FILE" if self.otel_client_cert_file else "EP_OTEL_CLIENT_CERT_FILE"
            configured = "EP_OTEL_CLIENT_CERT_FILE" if self.otel_client_cert_file else "EP_OTEL_CLIENT_KEY_FILE"
            msg = f"{configured} requires {missing}"
            raise ValueError(msg)
        return self
