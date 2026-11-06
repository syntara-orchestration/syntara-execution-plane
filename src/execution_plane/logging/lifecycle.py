"""Thread-safe application logging lifecycle."""

from __future__ import annotations

import logging
import threading
from enum import StrEnum

import structlog

from execution_plane.logging.logging import configure_app_logging
from execution_plane.logging.otel_handlers import flush_otel_handler

logger = structlog.stdlib.get_logger(__name__)


class OtelLoggingState(StrEnum):
    """Logging lifecycle states."""

    UNCONFIGURED = "unconfigured"
    CONFIGURED = "configured"


_logging_state_lock = threading.Lock()
_logging_state = OtelLoggingState.UNCONFIGURED


def start_loggers() -> None:
    """Configure root logging once, idempotently."""
    global _logging_state  # noqa: PLW0603

    with _logging_state_lock:
        if _logging_state == OtelLoggingState.CONFIGURED:
            logger.debug("Logging already configured", state=_logging_state)
            return
        configure_app_logging()
        _logging_state = OtelLoggingState.CONFIGURED
        logger.info("Logging configured")


def stop_loggers() -> None:
    """Flush OTLP records and remove root logging handlers."""
    global _logging_state  # noqa: PLW0603

    with _logging_state_lock:
        if _logging_state == OtelLoggingState.UNCONFIGURED:
            logger.debug("Logging flush skipped because logging was not configured", state=_logging_state)
            return

        root_logger = logging.getLogger()
        flush_otel_handler(root_logger)
        logger.info("Logging flushed and stopped")
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        _logging_state = OtelLoggingState.UNCONFIGURED
