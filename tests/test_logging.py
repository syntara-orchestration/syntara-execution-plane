"""Tests for structured application logging and OTEL emission."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import pytest
from pydantic import SecretStr

from execution_plane.config.base import EPSettings, get_ep_settings

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def isolated_root_logger(monkeypatch: pytest.MonkeyPatch) -> Iterator[logging.Logger]:
    """Return the root logger with its handlers restored after each test."""
    original_handlers = logging.getLogger().handlers[:]
    original_level = logging.getLogger().level
    yield logging.getLogger()
    logging.getLogger().handlers[:] = original_handlers
    logging.getLogger().setLevel(original_level)


def settings(**overrides: object) -> EPSettings:
    """Build EP settings with defaults suitable for logging tests."""
    return get_ep_settings().model_copy(
        update={
            "otel_endpoint": "https://collector.example.test/v1/logs",
            "otel_service_name": "execution-plane-test",
            **overrides,
        }
    )


def test_otel_settings_default_to_disabled() -> None:
    model = EPSettings.model_validate(
        {
            "EP_DATABASE_URL": "postgresql+asyncpg://localhost/execution_plane",
            "EP_CREDENTIAL_ENCRYPTION_KEY": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        }
    )

    assert model.otel_enabled is False
    assert model.otel_endpoint == "http://localhost:4318/v1/logs"
    assert model.otel_service_name == "execution-plane"
    assert model.otel_service_instance_id is not None
    assert model.otel_auth_header_name == "Authorization"


def test_otel_settings_require_complete_mtls_material() -> None:
    values = {
        "EP_DATABASE_URL": "postgresql+asyncpg://localhost/execution_plane",
        "EP_CREDENTIAL_ENCRYPTION_KEY": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        "EP_OTEL_ENABLED": "true",
        "EP_OTEL_ENDPOINT": "https://collector.example.test/v1/logs",
        "EP_OTEL_CLIENT_CERT_FILE": "/certs/client.crt",
    }
    with pytest.raises(ValueError, match="EP_OTEL_CLIENT_CERT_FILE requires EP_OTEL_CLIENT_KEY_FILE"):
        EPSettings.model_validate(
            values,
        )


def test_local_logging_is_structured_and_has_no_otel_handler_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
    isolated_root_logger: logging.Logger,
) -> None:
    from execution_plane.logging.logging import configure_app_logging

    monkeypatch.setattr("execution_plane.logging.logging.get_ep_settings", lambda: settings())
    configure_app_logging()

    assert [type(handler) for handler in isolated_root_logger.handlers] == [logging.StreamHandler]
    formatter = isolated_root_logger.handlers[0].formatter
    assert formatter is not None
    rendered = formatter.format(logging.LogRecord("test", logging.INFO, "path", 1, "hello", (), None))
    assert json.loads(rendered)["event"] == "hello"


def test_enabled_logging_attaches_one_otel_handler_to_root(
    monkeypatch: pytest.MonkeyPatch,
    isolated_root_logger: logging.Logger,
) -> None:
    from execution_plane.logging.logging import configure_app_logging

    otel_handler = Mock()
    otel_handler.level = logging.NOTSET
    monkeypatch.setattr(
        "execution_plane.logging.logging.create_otel_handler",
        lambda: otel_handler,
    )
    monkeypatch.setattr("execution_plane.logging.logging.get_ep_settings", lambda: settings(otel_enabled=True))
    configure_app_logging()

    assert len(isolated_root_logger.handlers) == 2
    assert isolated_root_logger.handlers[-1] is otel_handler


def test_otel_exporter_uses_bearer_and_certificate_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from execution_plane.logging.otel_handlers import create_otlp_exporter

    model = settings(
        otel_enabled=True,
        otel_api_key=SecretStr("secret"),
        otel_ca_cert_file="/certs/ca.crt",
        otel_client_cert_file="/certs/client.crt",
        otel_client_key_file="/certs/client.key",
    )
    monkeypatch.setattr("execution_plane.logging.otel_handlers.get_ep_settings", lambda: model)
    with patch("execution_plane.logging.otel_handlers.OTLPLogExporter") as exporter_class:
        create_otlp_exporter()

    exporter_class.assert_called_once_with(
        endpoint=model.otel_endpoint,
        headers={"Authorization": "Bearer secret"},
        certificate_file="/certs/ca.crt",
        client_certificate_file="/certs/client.crt",
        client_key_file="/certs/client.key",
    )


def test_otel_resource_identifies_service_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    from execution_plane.logging.otel_handlers import create_otel_resource

    model = settings(otel_service_instance_id="ep-pod-123")
    monkeypatch.setattr("execution_plane.logging.otel_handlers.get_ep_settings", lambda: model)
    resource = create_otel_resource()

    assert resource.attributes["service.name"] == "execution-plane-test"
    assert resource.attributes["service.instance.id"] == "ep-pod-123"


def test_lifecycle_configures_flushes_and_removes_logging(
    monkeypatch: pytest.MonkeyPatch,
    isolated_root_logger: logging.Logger,
) -> None:
    from execution_plane.logging import lifecycle

    otel_handler = Mock()
    otel_handler.level = logging.NOTSET
    otel_handler.logger_provider = Mock()
    monkeypatch.setattr(
        "execution_plane.logging.logging.create_otel_handler",
        lambda: otel_handler,
    )
    monkeypatch.setattr("execution_plane.logging.logging.get_ep_settings", lambda: settings(otel_enabled=True))

    lifecycle.start_loggers()
    assert otel_handler in isolated_root_logger.handlers

    lifecycle.stop_loggers()
    otel_handler.flush.assert_called_once()
    assert isolated_root_logger.handlers == []
    assert lifecycle._logging_state == lifecycle.OtelLoggingState.UNCONFIGURED


def test_flush_continues_after_handler_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    from execution_plane.logging.otel_handlers import flush_otel_handler

    target_logger = logging.getLogger("ep-test-otel-flush")
    failing = Mock()
    failing.logger_provider.force_flush.side_effect = RuntimeError("failed")
    succeeding = Mock()
    target_logger.handlers = [failing, succeeding]

    with patch("execution_plane.logging.otel_handlers.logger"):
        flush_otel_handler(target_logger)

    failing.logger_provider.force_flush.assert_called_once()
    succeeding.flush.assert_called_once()
    succeeding.logger_provider.force_flush.assert_called_once()


def test_create_otel_handler_is_disabled_when_otel_is_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from execution_plane.logging.otel_handlers import create_otel_handler

    monkeypatch.setattr("execution_plane.logging.otel_handlers.get_ep_settings", lambda: settings())
    assert create_otel_handler() is None
