"""Forward SDK operational log batches to EP's logging pipeline.

The SDK helper collects structured operational log records during step
execution and delivers them as a bounded, invocation-scoped ``LogBatch`` on
the Execute stream (AAP-102015). This module forwards those records through
EP's stdlib logging pipeline: when OTEL is enabled the root OTLP handler
exports them to the configured collector, tagged with the correlation
attributes below, so the records are queryable against the workflow run and
the work item without any AO-side change.

Records are forwarded as received — EP does not inspect, redact, or scrub
them. Forwarding is best effort and never fails a work item.

Delivery model (per the ANSTRAT-2422 proposal): terminal-adjacent only. A
workload that is crashed, force-killed, or hung cannot send its terminal
outcome, and its batch never arrives; there is no partial-recovery
mechanism.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Mapping

from execution_plane.config.settings import get_settings

#: Defense in depth: the protocol already clamps batches; the forwarder does
#: not trust its peer and clamps again.
MAX_RECORDS_PER_BATCH = 128

_LEVELS: Mapping[str, int] = {
    "trace": logging.DEBUG // 2,
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
    "fatal": logging.CRITICAL,
}

_step_log_logger = logging.getLogger("execution_plane.step_logs")

_disabled_warning_lock = threading.Lock()
_disabled_warning_logged = False


class OperationalLogMetrics:
    """Thread-safe counters for the operational log conduit.

    Lightweight by design: EP exposes no scrape endpoint today, so these are
    introspection counters (tests, diagnostics) rather than a metrics
    framework dependency.
    """

    def __init__(self) -> None:
        """Zero all counters."""
        self._lock = threading.Lock()
        self.batches_received = 0
        self.records_forwarded = 0
        self.batches_dropped_disabled = 0
        self.batches_dropped_malformed = 0
        self.forwarding_failures = 0

    def bump(self, counter: str, amount: int = 1) -> None:
        """Increment one counter; never raises."""
        with self._lock:
            value = getattr(self, counter) + amount
            setattr(self, counter, value)


metrics = OperationalLogMetrics()


def _warn_disabled_once() -> None:
    global _disabled_warning_logged  # noqa: PLW0603
    with _disabled_warning_lock:
        if _disabled_warning_logged:
            return
        _disabled_warning_logged = True
        logging.getLogger(__name__).warning(
            "Operational log batches are being dropped: OTEL is not enabled "
            "(set EP_OTEL_ENABLED=true and configure EP_OTEL_ENDPOINT to forward them)"
        )


def _forwarding_enabled() -> bool:
    try:
        return bool(get_settings().otel_enabled)
    except Exception:  # noqa: BLE001 - Configuration failures must not fail a work item.
        return False


class OperationalLogForwarder:
    """Emit one invocation's SDK operational records with correlation metadata.

    The correlation mapping is attached to every forwarded record so the
    collector can join step logs with the work item, workflow run, and
    execution target without any AO-side change.
    """

    def __init__(self, correlation: Mapping[str, str]) -> None:
        """Capture the correlation attributes attached to every record."""
        self._correlation = dict(correlation)

    def __call__(self, batch: object) -> None:
        """Forward one batch; never raises and never fails the work item."""
        metrics.bump("batches_received")
        if not _forwarding_enabled():
            metrics.bump("batches_dropped_disabled")
            _warn_disabled_once()
            return
        records = getattr(batch, "records", None)
        if not isinstance(records, (list, tuple)):
            # Malformed batch (not a recognized record sequence): drop it.
            metrics.bump("batches_dropped_malformed")
            return
        for record in tuple(records)[:MAX_RECORDS_PER_BATCH]:
            self._forward_record(record)

    def _forward_record(self, record: object) -> None:
        level_name = str(getattr(record, "level", "") or "info").lower()
        message = getattr(record, "message", None)
        if not isinstance(message, str):
            metrics.bump("batches_dropped_malformed")
            return
        attributes = getattr(record, "attributes", None)
        if not isinstance(attributes, Mapping):
            attributes = {}
        extras = {**self._correlation, **dict(attributes)}
        try:
            # Unknown level names are forwarded verbatim at INFO severity:
            # the level string itself travels in the attributes.
            _step_log_logger.log(
                _LEVELS.get(level_name, logging.INFO),
                message,
                extra={"step_log_level": level_name, **extras},
            )
            metrics.bump("records_forwarded")
        except Exception:  # noqa: BLE001 - Forwarding failures must not fail a work item.
            metrics.bump("forwarding_failures")
