"""Unit tests for asyncpg SSL connect_args construction."""

from __future__ import annotations

import ssl
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from execution_plane.config.database import to_asyncpg_url
from execution_plane.config.settings import Settings
from execution_plane.database.ssl import (
    build_ssl_connect_args,
    ssl_connect_args_from_env,
    strip_unsupported_asyncpg_ssl_query,
)

if TYPE_CHECKING:
    from pathlib import Path


def _write_self_signed(certs_dir: Path, filename: str) -> tuple[Path, Path]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC))
        .not_valid_after(datetime.now(UTC) + timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    cert_path = certs_dir / f"{filename}.pem"
    key_path = certs_dir / f"{filename}.key"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    return cert_path, key_path


def test_disable_returns_passthrough() -> None:
    assert build_ssl_connect_args(ssl_mode="disable") == {"ssl": "disable"}


def test_allow_returns_passthrough() -> None:
    assert build_ssl_connect_args(ssl_mode="allow") == {"ssl": "allow"}


def test_prefer_returns_empty() -> None:
    assert build_ssl_connect_args(ssl_mode="prefer") == {}


def test_require_without_certs_returns_passthrough() -> None:
    assert build_ssl_connect_args(ssl_mode="require") == {"ssl": "require"}


def test_verify_full_returns_ssl_context() -> None:
    ctx = build_ssl_connect_args(ssl_mode="verify-full")["ssl"]
    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.check_hostname is True
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.minimum_version == ssl.TLSVersion.TLSv1_3


def test_verify_ca_returns_ssl_context() -> None:
    ctx = build_ssl_connect_args(ssl_mode="verify-ca")["ssl"]
    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.check_hostname is False
    assert ctx.verify_mode == ssl.CERT_REQUIRED


def test_require_with_client_certs_returns_context(tmp_path: Path) -> None:
    cert_path, key_path = _write_self_signed(tmp_path, "client")
    ctx = build_ssl_connect_args(ssl_mode="require", ssl_cert=str(cert_path), ssl_key=str(key_path))["ssl"]
    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.check_hostname is False
    assert ctx.verify_mode == ssl.CERT_NONE


def test_invalid_ca_cert_path_raises() -> None:
    with pytest.raises(FileNotFoundError):
        build_ssl_connect_args(ssl_mode="verify-full", ssl_root_cert="/nonexistent/ca.pem")


def test_unknown_mode_raises() -> None:
    with pytest.raises(ValueError, match="Unknown SSL mode"):
        build_ssl_connect_args(ssl_mode="bogus")


def test_strip_libpq_ssl_query_keeps_asyncpg_ssl() -> None:
    url = (
        "postgresql+asyncpg://user:p%40ss@pg:5432/db"
        "?ssl=require&sslrootcert=%2Fca.crt&sslcert=%2Fclient.crt&sslkey=%2Fclient.key&sslmode=verify-full"
    )
    stripped = strip_unsupported_asyncpg_ssl_query(url)
    assert "sslrootcert" not in stripped
    assert "sslcert" not in stripped
    assert "sslkey" not in stripped
    assert "sslmode" not in stripped
    assert "ssl=require" in stripped


def test_to_asyncpg_url_strips_libpq_ssl_query() -> None:
    url = "postgresql+asyncpg://user:password@localhost/test?sslrootcert=/ca.crt&ssl=require"
    assert to_asyncpg_url(url) == "postgresql://user:password@localhost/test?ssl=require"


def test_ssl_connect_args_from_env_defaults_to_prefer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EP_DB_SSL_MODE", raising=False)
    monkeypatch.delenv("EP_DB_SSL_ROOT_CERT", raising=False)
    assert ssl_connect_args_from_env() == {}


def test_ssl_connect_args_from_env_require(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EP_DB_SSL_MODE", "require")
    assert ssl_connect_args_from_env() == {"ssl": "require"}


def test_settings_default_ssl_mode_is_prefer() -> None:
    settings = Settings()
    assert settings.db_ssl_mode == "prefer"
    assert settings.db_ssl_root_cert is None


def test_settings_verify_full_requires_ca(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EP_DB_SSL_MODE", "verify-full")
    with pytest.raises(ValueError, match="EP_DB_SSL_ROOT_CERT"):
        Settings()
