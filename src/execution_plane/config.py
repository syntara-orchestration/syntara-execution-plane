"""Execution Plane configuration — reads from EP-owned environment variables."""

from __future__ import annotations

import base64
import ipaddress
from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, Field, computed_field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url

_AES_256_KEY_BYTES = 32


def to_asyncpg_url(database_url: str) -> str:
    """Return a PostgreSQL URL without a SQLAlchemy DBAPI driver suffix."""
    return make_url(database_url).set(drivername="postgresql").render_as_string(hide_password=False)


class ScriptExecutorSettings(BaseSettings):
    """Script execution settings — no database connection required.

    Used directly by script_executor.py so it can be imported and tested
    without a database URL in the environment.
    """

    model_config = SettingsConfigDict(extra="ignore")

    # Process cleanup timing
    script_cleanup_terminate_timeout: float = 1.0
    script_cleanup_kill_timeout: float = 0.5

    # Per-env-var size cap (bytes)
    max_env_var_length: int = 32768  # 32 KB

    max_completion_result_bytes: int = Field(default=1_887_436, validation_alias="EP_MAX_COMPLETION_RESULT_BYTES")


class EPSettings(ScriptExecutorSettings):
    """Full settings for the execution-plane worker, including database."""

    # Runtime configuration accepts only the EP-owned database variable. Alembic
    # has a separate DATABASE_URL input for the one-off migration job.
    database_url: str = Field(validation_alias="EP_DATABASE_URL")

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
    api_tls_cert_path: str | None = Field(default=None, validation_alias="EP_API_TLS_CERT_PATH")
    api_tls_key_path: str | None = Field(default=None, validation_alias="EP_API_TLS_KEY_PATH")
    api_tls_client_ca_path: str | None = Field(default=None, validation_alias="EP_API_TLS_CLIENT_CA_PATH")
    credential_encryption_key: str | None = Field(default=None, validation_alias="EP_CREDENTIAL_ENCRYPTION_KEY")
    credential_encryption_key_path: str | None = Field(
        default=None,
        validation_alias="EP_CREDENTIAL_ENCRYPTION_KEY_PATH",
    )
    completion_callback_url: str | None = Field(default=None, validation_alias="EP_COMPLETION_CALLBACK_URL")
    completion_callback_ca_cert_path: str | None = Field(default=None, validation_alias="EP_CALLBACK_CA_CERT_PATH")
    completion_callback_cert_path: str | None = Field(default=None, validation_alias="EP_CALLBACK_CERT_PATH")
    completion_callback_key_path: str | None = Field(default=None, validation_alias="EP_CALLBACK_KEY_PATH")
    completion_callback_timeout_seconds: float = Field(default=10.0, validation_alias="EP_CALLBACK_TIMEOUT_SECONDS")
    workload_runner_image: str | None = Field(default=None, validation_alias="EP_WORKLOAD_RUNNER_IMAGE")
    workload_runner_cpu_request: str = Field(default="100m", validation_alias="EP_WORKLOAD_CPU_REQUEST")
    workload_runner_memory_request: str = Field(default="128Mi", validation_alias="EP_WORKLOAD_MEMORY_REQUEST")
    workload_runner_cpu_limit: str = Field(default="1", validation_alias="EP_WORKLOAD_CPU_LIMIT")
    workload_runner_memory_limit: str = Field(default="512Mi", validation_alias="EP_WORKLOAD_MEMORY_LIMIT")
    workload_cluster_ca_bundle_path: str | None = Field(default=None, validation_alias="EP_CLUSTER_CA_BUNDLE_PATH")
    workload_allowed_egress_cidrs: list[str] = Field(
        default_factory=list,
        validation_alias="EP_WORKLOAD_ALLOWED_EGRESS_CIDRS",
    )
    workload_forbidden_egress_cidrs: list[str] = Field(
        default_factory=list,
        validation_alias="EP_WORKLOAD_FORBIDDEN_EGRESS_CIDRS",
    )

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
def get_script_executor_settings() -> ScriptExecutorSettings:
    """Load and cache script executor settings from the environment."""
    return ScriptExecutorSettings()


@lru_cache
def get_ep_settings() -> EPSettings:
    """Load and cache execution-plane settings from the environment."""
    # BaseSettings loads the required database_url from the environment.
    return EPSettings()  # type: ignore[call-arg]
