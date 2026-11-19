"""Forward SDK operational log batches to EP's logging pipeline.

The SDK helper collects structured operational records during step execution
(``context.toolbox.log(...)``) and delivers them as a bounded,
invocation-scoped ``ContainerLogBatch`` on the Execute stream immediately
before the terminal outcome (AAP-102015, ANSTRAT-2422). This module
forwards those records through EP's structured logging pipeline: when OTEL is
enabled the root OTLP handler exports them, tagged with the correlation
fields below, so the records are queryable against the workflow run and the
work item without any AO-side change.

The SDK validates every batch before handing it over (at most 128 events and
256 KiB, a known severity, a bounded message, a JSON-object of attributes);
invalid batches never reach EP, so this module consumes the typed records
as-is. Records are forwarded as received — EP does not inspect, redact, or
scrub them. The workload's attributes are nested under ``step_log_attributes``
so they can never collide with, or overwrite, the EP-owned correlation fields.

Delivery model (per the ANSTRAT-2422 proposal): terminal-adjacent only. A
workload that is crashed, force-killed, or hung cannot send its terminal
outcome, and its batch never arrives; there is no partial-recovery
mechanism. The SDK saturates ``dropped_count`` when the workload overflowed
its budget — EP surfaces that count instead of failing.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog

from execution_plane.config.settings import get_settings

if TYPE_CHECKING:
    from collections.abc import Mapping
    from uuid import UUID

    from syntara_plugin.runtime import ContainerLogBatch
    from syntara_plugin.runtime.container import ContainerLogRecord

logger = structlog.stdlib.get_logger(__name__)

#: SDK severity → stdlib level. TRACE has no stdlib level and maps to DEBUG;
#: the original severity always travels in ``step_log_severity``.
_LEVELS: Mapping[str, int] = {
    "TRACE": logging.DEBUG,
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "FATAL": logging.CRITICAL,
}

_disabled_warning_lock = threading.Lock()
_disabled_warning_logged = False


def _warn_disabled_once() -> None:
    global _disabled_warning_logged  # noqa: PLW0603
    with _disabled_warning_lock:
        if _disabled_warning_logged:
            return
        _disabled_warning_logged = True
        logger.warning(
            "Operational log batches are being dropped: OTEL is not enabled "
            "(set EP_OTEL_ENABLED=true and configure EP_OTEL_ENDPOINT to forward them)"
        )


def _forwarding_enabled() -> bool:
    return get_settings().otel_enabled


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

        def _optional(key: str) -> str | None:
            value = runtime_context.get(key)
            return value if isinstance(value, str) and value else None

        return cls(
            invocation_id=invocation_id,
            work_item_id=work_item_id,
            execution_target_id=execution_target_id,
            workflow_id=_optional("workflow_id"),
            execution_id=_optional("execution_id"),
            project_id=_optional("project_id"),
        )

    def fields(self) -> dict[str, str]:
        """Flatten to the structured fields bound on each forwarded record.

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

    The correlation fields are bound once and attached to every forwarded
    record — including the workload-overflow warning — so the collector can
    join step logs with the work item, the caller's workflow context, and the
    execution target. Delivery failures are contained by the caller
    (``sdk_adaptor``), which never lets telemetry fail a work item.
    """

    def __init__(self, correlation: OperationalLogCorrelation) -> None:
        """Bind the correlation metadata attached to every record."""
        self._logger = logger.bind(**correlation.fields())

    def __call__(self, batch: ContainerLogBatch) -> None:
        """Forward one SDK-validated batch."""
        if batch.dropped_count > 0:
            # The workload overflowed the SDK's budget; surface the saturation
            # instead of silently losing the fact that records were dropped.
            self._logger.warning(
                "Workload dropped operational log records before delivery",
                step_log_dropped_count=batch.dropped_count,
            )
        if not _forwarding_enabled():
            _warn_disabled_once()
            return
        for record in batch.records:
            self._forward_record(record)

    def _forward_record(self, record: ContainerLogRecord) -> None:
        fields: dict[str, object] = {
            "step_log_severity": record.severity,
            "step_log_timestamp": record.timestamp,
        }
        if record.event_name is not None:
            fields["step_log_event_name"] = record.event_name
        if record.attributes:
            # Nested, never merged: workload keys cannot shadow EP-owned fields
            # or structlog's reserved keys (event, level, timestamp, exc_info).
            fields["step_log_attributes"] = dict(record.attributes)
        # A severity added by a future SDK is forwarded at INFO; the original
        # string still travels in step_log_severity.
        self._logger.log(_LEVELS.get(record.severity, logging.INFO), record.message, **fields)
