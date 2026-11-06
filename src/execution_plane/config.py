"""Execution Plane configuration — reads from EP-owned environment variables."""

from __future__ import annotations

import base64
import ipaddress
import re
import socket
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse

from pydantic import AliasChoices, Field, SecretStr, computed_field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url

from execution_plane.database.ssl import strip_unsupported_asyncpg_ssl_query

_AES_256_KEY_BYTES = 32
_VALID_SSL_MODES = frozenset({"disable", "allow", "prefer", "require", "verify-ca", "verify-full"})
_SSL_MODES_REQUIRING_CLIENT_CERTS = frozenset({"require", "verify-ca", "verify-full"})
_VALID_LOG_LEVELS = frozenset({"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"})
_SVC_DNS_RE = re.compile(
    r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?\.svc(?:\.cluster\.local)?$"
)


def to_asyncpg_url(database_url: str) -> str:
    """Return a PostgreSQL URL without a SQLAlchemy DBAPI driver suffix."""
    rendered = make_url(database_url).set(drivername="postgresql").render_as_string(hide_password=False)
    return strip_unsupported_asyncpg_ssl_query(rendered)


class EPSettings(BaseSettings):
    """Settings for the standalone Execution Plane service and worker."""

    model_config = SettingsConfigDict(extra="ignore")

    # Runtime configuration accepts only the EP-owned database variable. Alembic
    # has a separate DATABASE_URL input for the one-off migration job.
    database_url: str = Field(validation_alias="EP_DATABASE_URL")
    db_pool_size: int = Field(default=5, ge=1, validation_alias="EP_DB_POOL_SIZE")
    db_pool_max_overflow: int = Field(default=0, ge=0, validation_alias="EP_DB_POOL_MAX_OVERFLOW")
    db_pool_timeout: float = Field(default=30.0, gt=0.0, allow_inf_nan=False, validation_alias="EP_DB_POOL_TIMEOUT")
    db_ssl_mode: str = Field(default="prefer", validation_alias="EP_DB_SSL_MODE")
    db_ssl_root_cert: str | None = Field(default=None, validation_alias="EP_DB_SSL_ROOT_CERT")
    db_ssl_cert: str | None = Field(default=None, validation_alias="EP_DB_SSL_CERT")
    db_ssl_key: str | None = Field(default=None, validation_alias="EP_DB_SSL_KEY")

    node_startup_seconds: int = Field(default=120, validation_alias="EP_NODE_STARTUP_SECONDS", ge=1)
    node_grace_seconds: int = Field(default=30, validation_alias="EP_NODE_GRACE_SECONDS", ge=1)

    # Namespace for the default execution target created when a cluster is first
    # provisioned. Namespace is otherwise a per-target placement concern, editable
    # via execution_target.update.
    default_workload_namespace: str = Field(
        default="default",
        validation_alias="EP_DEFAULT_WORKLOAD_NAMESPACE",
        min_length=1,
        max_length=63,
    )

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
    log_level: str = Field(default="INFO", validation_alias="EP_LOG_LEVEL")
    log_output_format: str = Field(default="json", validation_alias="EP_LOG_OUTPUT_FORMAT")
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
    credential_encryption_key: str | None = Field(default=None, validation_alias="EP_CREDENTIAL_ENCRYPTION_KEY")
    credential_encryption_key_path: str | None = Field(
        default=None,
        validation_alias="EP_CREDENTIAL_ENCRYPTION_KEY_PATH",
    )
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
    completion_callback_url: str | None = Field(default=None, validation_alias="EP_COMPLETION_CALLBACK_URL")
    completion_callback_ca_cert_path: str | None = Field(default=None, validation_alias="EP_CALLBACK_CA_CERT_PATH")
    completion_callback_cert_path: str | None = Field(default=None, validation_alias="EP_CALLBACK_CERT_PATH")
    completion_callback_key_path: str | None = Field(default=None, validation_alias="EP_CALLBACK_KEY_PATH")
    completion_callback_timeout_seconds: float = Field(default=10.0, validation_alias="EP_CALLBACK_TIMEOUT_SECONDS")
    credential_resolution_callback_url: str | None = Field(
        default=None, validation_alias="EP_CREDENTIAL_RESOLUTION_CALLBACK_URL"
    )
    credential_resolution_callback_ca_cert_path: str | None = Field(
        default=None, validation_alias="EP_CREDENTIAL_RESOLUTION_CALLBACK_CA_CERT_PATH"
    )
    credential_resolution_callback_cert_path: str | None = Field(
        default=None, validation_alias="EP_CREDENTIAL_RESOLUTION_CALLBACK_CERT_PATH"
    )
    credential_resolution_callback_key_path: str | None = Field(
        default=None, validation_alias="EP_CREDENTIAL_RESOLUTION_CALLBACK_KEY_PATH"
    )
    credential_resolution_callback_timeout_seconds: float = Field(
        default=10.0, validation_alias="EP_CREDENTIAL_RESOLUTION_CALLBACK_TIMEOUT_SECONDS"
    )
    workload_allowed_egress_cidrs: list[str] = Field(
        default_factory=list,
        validation_alias="EP_WORKLOAD_ALLOWED_EGRESS_CIDRS",
    )
    workload_forbidden_egress_cidrs: list[str] = Field(
        default_factory=list,
        validation_alias="EP_WORKLOAD_FORBIDDEN_EGRESS_CIDRS",
    )

    @field_validator("db_ssl_mode")
    @classmethod
    def validate_ssl_mode(cls, value: str) -> str:
        """Accept only PostgreSQL sslmode values."""
        lowered = value.lower()
        if lowered not in _VALID_SSL_MODES:
            msg = f"Invalid SSL mode '{value}'. Must be one of: {', '.join(sorted(_VALID_SSL_MODES))}"
            raise ValueError(msg)
        return lowered

    @field_validator("db_ssl_root_cert", "db_ssl_cert", "db_ssl_key", mode="before")
    @classmethod
    def empty_ssl_path_to_none(cls, value: str | None) -> str | None:
        """Treat blank certificate paths as unset."""
        if value == "":
            return None
        return value

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
    def validate_ssl_fields(self) -> EPSettings:
        """Reject incomplete TLS combinations before the first connection."""
        if self.db_ssl_key is not None and self.db_ssl_cert is None:
            msg = "EP_DB_SSL_KEY requires EP_DB_SSL_CERT"
            raise ValueError(msg)
        if (self.db_ssl_cert is not None or self.db_ssl_key is not None) and (
            self.db_ssl_mode not in _SSL_MODES_REQUIRING_CLIENT_CERTS
        ):
            msg = (
                "Client certificates are only supported with SSL modes "
                f"{', '.join(sorted(_SSL_MODES_REQUIRING_CLIENT_CERTS))}; "
                f"current mode is '{self.db_ssl_mode}'"
            )
            raise ValueError(msg)
        if self.db_ssl_mode in ("verify-ca", "verify-full") and self.db_ssl_root_cert is None:
            msg = "EP_DB_SSL_ROOT_CERT is required when EP_DB_SSL_MODE is verify-ca or verify-full"
            raise ValueError(msg)
        return self

    @model_validator(mode="after")
    def validate_resource_monitor_timeout(self) -> EPSettings:
        """Keep each probe shorter than the scan interval so scans cannot overlap on a timeout."""
        if self.resource_monitor_probe_timeout_seconds >= self.resource_monitor_interval_seconds:
            msg = "EP_RESOURCE_MONITOR_PROBE_TIMEOUT_SECONDS must be shorter than EP_RESOURCE_MONITOR_INTERVAL_SECONDS"
            raise ValueError(msg)
        return self

    @model_validator(mode="after")
    def validate_otel_fields(self) -> EPSettings:
        """Reject incomplete OTEL credentials and certificate material."""
        if (self.otel_client_cert_file is None) != (self.otel_client_key_file is None):
            missing = "EP_OTEL_CLIENT_KEY_FILE" if self.otel_client_cert_file else "EP_OTEL_CLIENT_CERT_FILE"
            configured = "EP_OTEL_CLIENT_CERT_FILE" if self.otel_client_cert_file else "EP_OTEL_CLIENT_KEY_FILE"
            msg = f"{configured} requires {missing}"
            raise ValueError(msg)
        return self

    @model_validator(mode="after")
    def validate_workload_network_ranges(self) -> EPSettings:
        """Require explicit denies when a broad workload egress range is allowed."""
        allowed_networks = [ipaddress.ip_network(value, strict=False) for value in self.workload_allowed_egress_cidrs]
        forbidden_networks = [
            ipaddress.ip_network(value, strict=False) for value in self.workload_forbidden_egress_cidrs
        ]
        if allowed_networks and not forbidden_networks:
            msg = "EP_WORKLOAD_FORBIDDEN_EGRESS_CIDRS is required when workload egress is allowed"
            raise ValueError(msg)
        for network in allowed_networks:
            for denied in forbidden_networks:
                if isinstance(network, ipaddress.IPv4Network):
                    if not isinstance(denied, ipaddress.IPv4Network):
                        continue
                    partial_overlap = network.overlaps(denied) and not denied.subnet_of(network)
                else:
                    if not isinstance(denied, ipaddress.IPv6Network):
                        continue
                    partial_overlap = network.overlaps(denied) and not denied.subnet_of(network)
                if partial_overlap:
                    msg = f"Forbidden egress range {denied} must be contained by allowed range {network}"
                    raise ValueError(msg)
        return self

    @model_validator(mode="after")
    def load_credential_encryption_key(self) -> EPSettings:
        """Load EP's data key from a secret file and validate its AES-256 size."""
        if self.credential_encryption_key is None and self.credential_encryption_key_path:
            self.credential_encryption_key = (
                Path(self.credential_encryption_key_path).read_text(encoding="ascii").strip()
            )
        if self.credential_encryption_key is None:
            msg = "EP_CREDENTIAL_ENCRYPTION_KEY or EP_CREDENTIAL_ENCRYPTION_KEY_PATH must be configured"
            raise ValueError(msg)
        decoded_key = base64.urlsafe_b64decode(self.credential_encryption_key)
        if len(decoded_key) != _AES_256_KEY_BYTES:
            msg = "The Execution Plane credential encryption key must decode to 32 bytes"
            raise ValueError(msg)
        return self

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url_asyncpg(self) -> str:
        """asyncpg-compatible URL (strips the +asyncpg SQLAlchemy driver prefix)."""
        return to_asyncpg_url(self.database_url)


@lru_cache
def get_ep_settings() -> EPSettings:
    """Load and cache execution-plane settings from the environment."""
    # BaseSettings loads the required database_url from the environment.
    return EPSettings()  # type: ignore[call-arg]
