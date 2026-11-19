"""Forward SDK operational log batches to EP's logging pipeline.

The SDK helper collects structured operational records during step execution
(``context.toolbox.log(...)``) and delivers them as a bounded,
invocation-scoped ``ContainerLogBatch`` on the Execute stream immediately
before the terminal outcome (AAP-102015, ANSTRAT-2422). This module
forwards those records through EP's stdlib logging pipeline: when OTEL is
enabled the root OTLP handler exports them, tagged with the correlation
attributes below, so the records are queryable against the workflow run and
the work item without any AO-side change.

Records are forwarded as received — EP does not inspect, redact, or scrub
them. Forwarding is best effort and never fails a work item.

Delivery model (per the ANSTRAT-2422 proposal): terminal-adjacent only. A
workload that is crashed, force-killed, or hung cannot send its terminal
outcome, and its batch never arrives; there is no partial-recovery
mechanism. The SDK saturates ``dropped_count`` when the workload overflowed
its budget — EP surfaces that count instead of failing.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from execution_plane.config.settings import get_settings

if TYPE_CHECKING:
    from uuid import UUID

#: Defense in depth: the SDK clamps batches at 128 events / 256 KiB; the
#: forwarder does not trust its peer and clamps again.
MAX_RECORDS_PER_BATCH = 128

_LEVELS: Mapping[str, int] = {
    "TRACE": logging.DEBUG // 2,
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "FATAL": logging.CRITICAL,
    "CRITICAL": logging.CRITICAL,
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
        self.dropped_by_workload = 0
        self.forwarding_failures = 0

    def bump(self, counter: str, amount: int = 1) -> None:
        """Increment one counter; never raises."""
        with self._lock:
            setattr(self, counter, getattr(self, counter) + amount)


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


@dataclass(frozen=True)
class OperationalLogCorrelation:
    """Correlation metadata attached to every forwarded operational record.

    The dispatch-owned identities (attempt, work item, execution target) are
    always present; the caller-supplied workflow context keys are included
    only when the non-secret runtime context carries them (Syntara supplies
    ``workflow_id``/``execution_id``/``project_id``).
    """

    invocation_id: str
    work_item_id: str
    execution_target_id: UUID
    workflow_id: str | None = None
    execution_id: str | None = None
    project_id: str | None = None

    @classmethod
    def from_runtime_context(
        cls,
        *,
        invocation_id: str,
        work_item_id: str,
        execution_target_id: UUID,
        runtime_context: Mapping[str, object],
    ) -> OperationalLogCorrelation:
        """Build the correlation, harvesting the optional workflow-context keys."""
        context = runtime_context if isinstance(runtime_context, Mapping) else {}

        def _optional(key: str) -> str | None:
            value = context.get(key)
            return value if isinstance(value, str) and value else None

        return cls(
            invocation_id=invocation_id,
            work_item_id=work_item_id,
            execution_target_id=execution_target_id,
            workflow_id=_optional("workflow_id"),
            execution_id=_optional("execution_id"),
            project_id=_optional("project_id"),
        )

    def attributes(self) -> dict[str, str]:
        """Flatten to the attribute set attached to each forwarded record.

        The stringification happens here, at the very end, so the field keeps
        its domain type (UUID) everywhere else — the same name never holds
        two different types along the way.
        """
        flattened: dict[str, str] = {
            "invocation_id": self.invocation_id,
            "work_item_id": self.work_item_id,
            "execution_target_id": str(self.execution_target_id),
        }
        optional = {
            "workflow_id": self.workflow_id,
            "execution_id": self.execution_id,
            "project_id": self.project_id,
        }
        return flattened | {key: value for key, value in optional.items() if value is not None}


class OperationalLogForwarder:
    """Emit one invocation's SDK operational records with correlation metadata.

    The correlation attributes are attached to every forwarded record so the
    collector can join step logs with the work item, the caller's workflow
    context, and the execution target without any AO-side change.
    """

    def __init__(self, correlation: OperationalLogCorrelation) -> None:
        """Capture the correlation metadata attached to every record."""
        self._correlation = correlation.attributes()

    def __call__(self, batch: object) -> None:
        """Forward one batch; never raises and never fails the work item."""
        metrics.bump("batches_received")
        dropped_count = getattr(batch, "dropped_count", 0)
        if isinstance(dropped_count, int) and dropped_count > 0:
            # The workload overflowed the SDK's budget; surface the saturation
            # instead of silently losing the fact that records were dropped.
            metrics.bump("dropped_by_workload", dropped_count)
            _step_log_logger.warning("Workload dropped %d operational log record(s) before delivery", dropped_count)
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
        message = getattr(record, "message", None)
        if not isinstance(message, str):
            metrics.bump("batches_dropped_malformed")
            return
        severity = str(getattr(record, "severity", "") or "INFO").upper()
        attributes = getattr(record, "attributes", None)
        if not isinstance(attributes, Mapping):
            attributes = {}
        extras: dict[str, Any] = dict(self._correlation)
        extras["step_log_severity"] = severity
        timestamp = getattr(record, "timestamp", None)
        if isinstance(timestamp, str) and timestamp:
            # RFC 3339 with millisecond precision, produced by the SDK.
            extras["step_log_timestamp"] = timestamp
        event_name = getattr(record, "event_name", None)
        if isinstance(event_name, str) and event_name:
            extras["step_log_event_name"] = event_name
        extras.update(attributes)
        try:
            # Unknown severity names are forwarded verbatim at INFO: the
            # severity string itself travels in the attributes.
            _step_log_logger.log(_LEVELS.get(severity, logging.INFO), message, extra=extras)
            metrics.bump("records_forwarded")
        except Exception:  # noqa: BLE001 - Forwarding failures must not fail a work item.
            metrics.bump("forwarding_failures")
