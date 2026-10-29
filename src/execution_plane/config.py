"""Execution Plane configuration — reads from EP-owned environment variables."""

from __future__ import annotations

import base64
import ipaddress
from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, Field, computed_field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url

from execution_plane.database.ssl import strip_unsupported_asyncpg_ssl_query

_AES_256_KEY_BYTES = 32
_VALID_SSL_MODES = frozenset({"disable", "allow", "prefer", "require", "verify-ca", "verify-full"})
_SSL_MODES_REQUIRING_CLIENT_CERTS = frozenset({"require", "verify-ca", "verify-full"})


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
