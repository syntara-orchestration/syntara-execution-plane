"""Database connection settings."""

from __future__ import annotations

from pydantic import Field, computed_field, field_validator, model_validator
from pydantic_settings import BaseSettings
from sqlalchemy.engine import make_url

from execution_plane.database.ssl import strip_unsupported_asyncpg_ssl_query

_VALID_SSL_MODES = frozenset({"disable", "allow", "prefer", "require", "verify-ca", "verify-full"})
_SSL_MODES_REQUIRING_CLIENT_CERTS = frozenset({"require", "verify-ca", "verify-full"})


def to_asyncpg_url(database_url: str) -> str:
    """Return a PostgreSQL URL without a SQLAlchemy DBAPI driver suffix."""
    rendered = make_url(database_url).set(drivername="postgresql").render_as_string(hide_password=False)
    return strip_unsupported_asyncpg_ssl_query(rendered)


class DatabaseSettings(BaseSettings):
    """PostgreSQL connection and TLS settings."""

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

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url_asyncpg(self) -> str:
        """asyncpg-compatible URL (strips the +asyncpg SQLAlchemy driver suffix)."""
        return to_asyncpg_url(self.database_url)

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
    def validate_ssl_fields(self) -> DatabaseSettings:
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
