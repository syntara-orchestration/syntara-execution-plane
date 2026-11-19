"""Operational log batch forwarding (AAP-102015)."""

from __future__ import annotations

import logging
from typing import ClassVar

import pytest
from syntara_plugin.runtime import ContainerLogBatch, ContainerLogRecord

from execution_plane import operational_logs
from execution_plane.operational_logs import (
    MAX_RECORDS_PER_BATCH,
    OperationalLogCorrelation,
    OperationalLogForwarder,
    metrics,
)


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


def _batch(
    records: tuple[ContainerLogRecord, ...],
    dropped_count: int = 0,
    invocation_id: str = "attempt-1",
) -> ContainerLogBatch:
    return ContainerLogBatch(invocation_id=invocation_id, version=1, records=records, dropped_count=dropped_count)


@pytest.fixture
def captured(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    caplog.set_level(logging.DEBUG, logger="execution_plane.step_logs")
    return caplog


@pytest.fixture
def enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(operational_logs, "_forwarding_enabled", lambda: True)


@pytest.fixture(autouse=True)
def reset_metrics() -> None:
    metrics.batches_received = 0
    metrics.records_forwarded = 0
    metrics.batches_dropped_disabled = 0
    metrics.batches_dropped_malformed = 0
    metrics.dropped_by_workload = 0
    metrics.forwarding_failures = 0
    operational_logs._disabled_warning_logged = False


def _forwarder() -> OperationalLogForwarder:
    return OperationalLogForwarder(
        OperationalLogCorrelation(
            invocation_id="attempt-1",
            work_item_id="wi-1",
            execution_target_id="target-1",
            workflow_id="wf-1",
        )
    )


def test_records_are_forwarded_with_correlation_attributes(captured: pytest.LogCaptureFixture, enabled: None) -> None:
    _forwarder()(
        _batch(
            (
                _record(attributes={"attempt": 2}, event_name="fixture.toolbox_log"),
                _record(severity="WARN", message="second record"),
            )
        )
    )
    records = [r for r in captured.records if r.name == "execution_plane.step_logs"]
    assert [r.message for r in records] == ["hello from the toolbox", "second record"]
    assert [r.levelname for r in records] == ["INFO", "WARNING"]
    first = records[0]
    assert first.invocation_id == "attempt-1"
    assert first.work_item_id == "wi-1"
    assert first.execution_target_id == "target-1"
    assert first.workflow_id == "wf-1"
    assert first.attempt == 2
    assert first.step_log_severity == "INFO"
    assert first.step_log_timestamp == "2026-11-19T17:00:00.123Z"
    assert first.step_log_event_name == "fixture.toolbox_log"
    assert metrics.batches_received == 1
    assert metrics.records_forwarded == 2


def test_unknown_severity_is_forwarded_verbatim_at_info(captured: pytest.LogCaptureFixture, enabled: None) -> None:
    _forwarder()(_batch((_record(severity="VERBOSE", message="x"),)))
    (record,) = [r for r in captured.records if r.name == "execution_plane.step_logs"]
    assert record.levelname == "INFO"
    assert record.step_log_severity == "VERBOSE"


def test_disabled_otel_drops_batches_with_one_warning(
    monkeypatch: pytest.MonkeyPatch, captured: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(operational_logs, "_forwarding_enabled", lambda: False)
    forwarder = _forwarder()
    forwarder(_batch((_record(message="one"),)))
    forwarder(_batch((_record(message="two"),)))
    step_records = [r for r in captured.records if r.name == "execution_plane.step_logs"]
    warnings = [r for r in captured.records if r.levelname == "WARNING" and "OTEL" in r.message]
    assert step_records == []
    assert len(warnings) == 1
    assert metrics.batches_dropped_disabled == 2
    assert metrics.batches_received == 2


def test_workload_dropped_count_is_surfaced(captured: pytest.LogCaptureFixture, enabled: None) -> None:
    _forwarder()(_batch((_record(message="kept"),), dropped_count=7))
    records = [r for r in captured.records if r.name == "execution_plane.step_logs"]
    assert [r.message for r in records if r.levelname == "WARNING"] == [
        "Workload dropped 7 operational log record(s) before delivery"
    ]
    assert [r.message for r in records if r.levelname == "INFO"] == ["kept"]
    assert metrics.dropped_by_workload == 7


def test_malformed_batch_is_dropped_not_raised(enabled: None) -> None:
    _forwarder()(object())  # no .records attribute
    _forwarder()(_batch(()))  # empty batch is fine
    assert metrics.batches_dropped_malformed == 1
    assert metrics.batches_received == 2


def test_record_with_non_string_message_is_dropped(captured: pytest.LogCaptureFixture, enabled: None) -> None:
    # The SDK validates messages as strings; the forwarder still must not
    # trust its peer. Build a malformed record manually.
    class _Bad:
        timestamp: ClassVar[str] = "2026-11-19T17:00:00.123Z"
        severity: ClassVar[str] = "INFO"
        message: ClassVar[object] = 42
        attributes: ClassVar[dict] = {}
        event_name: ClassVar[str | None] = None

    _forwarder()(_batch((_record(message="kept"), _Bad())))  # type: ignore[arg-type]
    records = [r for r in captured.records if r.name == "execution_plane.step_logs"]
    assert [r.message for r in records] == ["kept"]
    assert metrics.batches_dropped_malformed == 1


def test_oversized_batch_is_clamped(captured: pytest.LogCaptureFixture, enabled: None) -> None:
    _forwarder()(_batch(tuple(_record(message=f"m{i}") for i in range(500))))
    records = [r for r in captured.records if r.name == "execution_plane.step_logs"]
    assert len(records) == MAX_RECORDS_PER_BATCH


def test_sink_failures_never_propagate(enabled: None, monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(message: str, extra: dict | None = None) -> None:
        msg = "exporter exploded"
        raise RuntimeError(msg)

    monkeypatch.setattr(operational_logs._step_log_logger, "log", boom)
    _forwarder()(_batch((_record(message="x"),)))
    assert metrics.forwarding_failures == 1


def test_correlation_from_runtime_context_harvests_optional_keys() -> None:
    correlation = OperationalLogCorrelation.from_runtime_context(
        invocation_id="attempt-1",
        work_item_id="wi-1",
        execution_target_id="target-1",
        runtime_context={
            "workflow_id": "wf-1",
            "execution_id": "exec-1",
            "project_id": 42,  # non-string: ignored
            "driver": "test",  # unrelated: ignored
        },
    )
    assert correlation.invocation_id == "attempt-1"
    assert correlation.workflow_id == "wf-1"
    assert correlation.execution_id == "exec-1"
    assert correlation.project_id is None
    # attributes() flattens to exactly the present keys
    assert correlation.attributes() == {
        "invocation_id": "attempt-1",
        "work_item_id": "wi-1",
        "execution_target_id": "target-1",
        "workflow_id": "wf-1",
        "execution_id": "exec-1",
    }


def test_correlation_from_non_mapping_runtime_context() -> None:
    correlation = OperationalLogCorrelation.from_runtime_context(
        invocation_id="a",
        work_item_id="b",
        execution_target_id="c",
        runtime_context=None,  # type: ignore[arg-type]
    )
    assert correlation.attributes() == {
        "invocation_id": "a",
        "work_item_id": "b",
        "execution_target_id": "c",
    }
