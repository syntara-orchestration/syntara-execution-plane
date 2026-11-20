"""Operational log batch forwarding (AAP-102015)."""

from __future__ import annotations

from uuid import UUID

import pytest
from structlog.testing import capture_logs
from syntara_plugin.runtime import ContainerLogBatch, ContainerLogRecord

from execution_plane import operational_logs
from execution_plane.operational_logs import OperationalLogCorrelation, OperationalLogForwarder

_CORRELATION = {
    "invocation_id": "attempt-1",
    "work_item_id": "wi-1",
    "execution_target_id": "00000000-0000-0000-0000-0000000000aa",
    "workflow_id": "wf-1",
}


def _record(
    severity: str = "INFO",
    message: str = "hello from the toolbox",
    attributes: dict | None = None,
    event_name: str | None = None,
) -> ContainerLogRecord:
    return ContainerLogRecord(
        timestamp="2026-11-19T17:00:00.123Z",
        severity=severity,
        message=message,
        attributes=attributes or {},
        event_name=event_name,
    )


def _batch(records: tuple[ContainerLogRecord, ...], dropped_count: int = 0) -> ContainerLogBatch:
    return ContainerLogBatch(invocation_id="attempt-1", version=1, records=records, dropped_count=dropped_count)


def _forwarder() -> OperationalLogForwarder:
    return OperationalLogForwarder(
        OperationalLogCorrelation(
            invocation_id="attempt-1",
            work_item_id="wi-1",
            execution_target_id=UUID("00000000-0000-0000-0000-0000000000aa"),
            workflow_id="wf-1",
        )
    )


@pytest.fixture
def enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(operational_logs, "_forwarding_enabled", lambda: True)


@pytest.fixture(autouse=True)
def reset_disabled_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(operational_logs, "_disabled_warning_logged", False)


def test_records_are_forwarded_with_correlation_fields(enabled: None) -> None:
    with capture_logs() as logs:
        _forwarder()(
            _batch(
                (
                    _record(severity="TRACE", attributes={"attempt": 2}, event_name="fixture.toolbox_log"),
                    _record(severity="WARN", message="second record"),
                )
            )
        )

    assert logs == [
        {
            **_CORRELATION,
            "event": "hello from the toolbox",
            "log_level": "debug",
            "step_log_severity": "TRACE",
            "step_log_timestamp": "2026-11-19T17:00:00.123Z",
            "step_log_event_name": "fixture.toolbox_log",
            "step_log_attributes": {"attempt": 2},
        },
        {
            **_CORRELATION,
            "event": "second record",
            "log_level": "warning",
            "step_log_severity": "WARN",
            "step_log_timestamp": "2026-11-19T17:00:00.123Z",
        },
    ]


def test_workload_attributes_cannot_shadow_ep_or_reserved_fields(enabled: None) -> None:
    hostile = {"work_item_id": "SPOOFED", "workflow_id": "SPOOFED", "event": "x", "level": "x", "name": "x"}
    with capture_logs() as logs:
        _forwarder()(_batch((_record(attributes=hostile),)))

    (entry,) = logs
    assert entry["event"] == "hello from the toolbox"
    assert entry["work_item_id"] == "wi-1"
    assert entry["workflow_id"] == "wf-1"
    assert entry["step_log_attributes"] == hostile


def test_disabled_otel_drops_batches_with_one_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(operational_logs, "_forwarding_enabled", lambda: False)
    forwarder = _forwarder()
    with capture_logs() as logs:
        forwarder(_batch((_record(message="one"),)))
        forwarder(_batch((_record(message="two"),)))

    assert [(entry["log_level"], "OTEL is not enabled" in entry["event"]) for entry in logs] == [("warning", True)]


def test_workload_dropped_count_is_surfaced_with_correlation(enabled: None) -> None:
    with capture_logs() as logs:
        _forwarder()(_batch((_record(message="kept"),), dropped_count=7))

    warning, kept = logs
    assert warning == {
        **_CORRELATION,
        "event": "Workload dropped operational log records before delivery",
        "log_level": "warning",
        "step_log_dropped_count": 7,
    }
    assert kept["event"] == "kept"


def test_correlation_from_runtime_context_harvests_optional_keys() -> None:
    correlation = OperationalLogCorrelation.from_runtime_context(
        invocation_id="attempt-1",
        work_item_id="wi-1",
        execution_target_id=UUID("00000000-0000-0000-0000-0000000000bb"),
        runtime_context={
            "workflow_id": "wf-1",
            "execution_id": "exec-1",
            "project_id": 42,  # non-string: ignored
            "driver": "test",  # unrelated: ignored
        },
    )

    assert correlation.project_id is None
    # fields() flattens to exactly the present keys, stringifying the UUID last
    assert correlation.fields() == {
        "invocation_id": "attempt-1",
        "work_item_id": "wi-1",
        "execution_target_id": "00000000-0000-0000-0000-0000000000bb",
        "workflow_id": "wf-1",
        "execution_id": "exec-1",
    }
