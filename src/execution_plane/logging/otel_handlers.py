"""OTLP HTTP logging handler construction and flushing."""

from __future__ import annotations

import logging

import structlog
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

from execution_plane.config.base import get_ep_settings

logger = structlog.stdlib.get_logger(__name__)


def create_otel_handler() -> logging.Handler | None:
    """Create a root OTLP logging handler when OTEL is enabled."""
    if not get_ep_settings().otel_enabled:
        return None

    exporter = create_otlp_exporter()
    return LoggingHandler(
        level=logging.NOTSET,
        logger_provider=_create_logger_provider(exporter),
    )


def create_otlp_exporter() -> OTLPLogExporter:
    """Create an authenticated OTLP HTTP log exporter."""
    settings = get_ep_settings()
    headers = None
    if settings.otel_api_key is not None:
        headers = {
            settings.otel_auth_header_name: f"Bearer {settings.otel_api_key.get_secret_value()}",
        }

    has_mtls = settings.otel_client_cert_file is not None and settings.otel_client_key_file is not None
    if settings.otel_api_key is None and not has_mtls:
        logger.warning("OTLP endpoint configured without authentication", endpoint=settings.otel_endpoint)

    return OTLPLogExporter(
        endpoint=settings.otel_endpoint,
        headers=headers,
        certificate_file=settings.otel_ca_cert_file,
        client_certificate_file=settings.otel_client_cert_file,
        client_key_file=settings.otel_client_key_file,
    )


def create_otel_resource() -> Resource:
    """Create OTEL resource attributes identifying this Execution Plane instance."""
    settings = get_ep_settings()
    return Resource.create(
        {
            "service.name": settings.otel_service_name,
            "service.instance.id": settings.otel_service_instance_id,
        }
    )


def _create_logger_provider(exporter: OTLPLogExporter) -> LoggerProvider:
    provider = LoggerProvider(resource=create_otel_resource())
    provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
    return provider


def flush_otel_handler(target_logger: logging.Logger) -> None:
    """Best-effort flush of OTLP handlers on a logger."""
    for handler in target_logger.handlers:
        provider = getattr(handler, "logger_provider", None)
        if provider is None:
            continue
        try:
            handler.flush()
            provider.force_flush()
        except Exception:  # noqa: BLE001 - Shutdown must attempt every handler.
            logger.warning(
                "OTLP handler flush failed",
                logger_name=target_logger.name,
                exc_info=True,
            )
