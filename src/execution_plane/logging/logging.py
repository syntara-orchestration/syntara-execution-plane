"""Structured stdlib logging configuration."""

from __future__ import annotations

import json
import logging
from logging import Formatter
from typing import TYPE_CHECKING, Any

import structlog
from structlog.processors import JSONRenderer

from execution_plane.config.base import get_ep_settings
from execution_plane.logging.otel_handlers import create_otel_handler

if TYPE_CHECKING:
    from structlog.typing import EventDict, WrappedLogger

_UVICORN_LOGGER_NAMES = ("uvicorn", "uvicorn.error", "uvicorn.access")


class ExecutionPlaneLogRecordRenderer(JSONRenderer):
    """Render structured events as JSON while preserving scalar values."""

    def __call__(self, _: WrappedLogger, __: str, event_dict: EventDict) -> str | bytes:
        """Render an event dictionary as a JSON string."""
        return self._render_json(event_dict)

    def _make_serializable(self, value: object) -> object:
        """Recursively represent values that JSON cannot encode."""
        if isinstance(value, str | int | float | bool | type(None)):
            return value
        if isinstance(value, dict):
            return {key: self._make_serializable(item) for key, item in value.items()}
        if isinstance(value, list | tuple):
            return [self._make_serializable(item) for item in value]
        try:
            json.dumps(value)
        except (TypeError, ValueError):
            return repr(value)
        return value

    def _render_json(self, event_dict: EventDict) -> str:
        serializable = self._make_serializable(event_dict)
        return str(self._dumps(serializable, **self._dumps_kw))


def build_shared_formatters() -> list[Any]:
    """Build processors shared by native structlog and stdlib records."""
    return [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.ExtraAdder(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.format_exc_info,
    ]


def build_text_formatter() -> Formatter:
    """Build plain local text output."""
    return structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.dev.ConsoleRenderer(colors=False),
        ],
        foreign_pre_chain=build_shared_formatters(),
    )


def build_json_formatter() -> Formatter:
    """Build structured local JSON output."""
    return structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            ExecutionPlaneLogRecordRenderer(),
        ],
        foreign_pre_chain=build_shared_formatters(),
    )


def build_execution_plane_formatter() -> Formatter:
    """Build the configured local log formatter."""
    settings = get_ep_settings()
    if settings.log_output_format == "text":
        return build_text_formatter()
    return build_json_formatter()


def build_uvicorn_logging_config(log_level: str) -> dict[str, Any]:
    """Build a uvicorn logging configuration with EP formatters."""
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "execution-plane": {
                "()": "execution_plane.logging.logging.build_execution_plane_formatter",
            },
        },
        "handlers": {
            "execution-plane": {
                "formatter": "execution-plane",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            name: {"handlers": ["execution-plane"], "level": log_level, "propagate": False}
            for name in _UVICORN_LOGGER_NAMES
        },
        "root": {"handlers": ["execution-plane"], "level": log_level},
    }


def configure_app_logging() -> None:
    """Configure root stdout logging and optional OTLP emission."""
    settings = get_ep_settings()
    handler = logging.StreamHandler()
    handler.setFormatter(build_execution_plane_formatter())

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(settings.log_level)

    otel_handler = create_otel_handler()
    if otel_handler is not None:
        root_logger.addHandler(otel_handler)

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            *build_shared_formatters(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=False,
    )
