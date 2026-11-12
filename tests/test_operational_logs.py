"""Operational log batch forwarding (AAP-102015)."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import pytest

from execution_plane import operational_logs
from execution_plane.operational_logs import (
    MAX_RECORDS_PER_BATCH,
    OperationalLogForwarder,
    metrics,
)


@dataclass(frozen=True)
class _Record:
    level: str
    message: str
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class _Batch:
    invocation_id: str
    records: tuple[_Record, ...]


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
    metrics.forwarding_failures = 0
    operational_logs._disabled_warning_logged = False


def _forwarder() -> OperationalLogForwarder:
    return OperationalLogForwarder(
        {
            "invocation_id": "attempt-1",
            "workflow_run_id": "run-1",
            "work_item_id": "wi-1",
            "execution_target_id": "target-1",
        }
    )


def test_records_are_forwarded_with_correlation_attributes(captured: pytest.LogCaptureFixture, enabled: None) -> None:
    _forwarder()(
        _Batch(
            invocation_id="attempt-1",
            records=(
                _Record(level="info", message="hello from the helper", attributes={"attempt": 2}),
                _Record(level="warning", message="no attributes"),
            ),
        )
    )
    records = [r for r in captured.records if r.name == "execution_plane.step_logs"]
    assert [r.message for r in records] == ["hello from the helper", "no attributes"]
    assert records[0].levelname == "INFO"
    assert records[1].levelname == "WARNING"
    assert records[0].invocation_id == "attempt-1"
    assert records[0].workflow_run_id == "run-1"
    assert records[0].work_item_id == "wi-1"
    assert records[0].execution_target_id == "target-1"
    assert records[0].attempt == 2
    assert records[0].step_log_level == "info"
    assert metrics.batches_received == 1
    assert metrics.records_forwarded == 2


def test_unknown_level_is_forwarded_verbatim_at_info(captured: pytest.LogCaptureFixture, enabled: None) -> None:
    _forwarder()(_Batch("attempt-1", (_Record(level="verbose", message="x"),)))
    (record,) = [r for r in captured.records if r.name == "execution_plane.step_logs"]
    assert record.levelname == "INFO"
    assert record.step_log_level == "verbose"


def test_disabled_otel_drops_batches_with_one_warning(
    monkeypatch: pytest.MonkeyPatch, captured: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(operational_logs, "_forwarding_enabled", lambda: False)
    forwarder = _forwarder()
    forwarder(_Batch("attempt-1", (_Record(level="info", message="one"),)))
    forwarder(_Batch("attempt-1", (_Record(level="info", message="two"),)))
    step_records = [r for r in captured.records if r.name == "execution_plane.step_logs"]
    warnings = [r for r in captured.records if r.levelname == "WARNING" and "OTEL" in r.message]
    assert step_records == []
    assert len(warnings) == 1
    assert metrics.batches_dropped_disabled == 2
    assert metrics.batches_received == 2


def test_malformed_batch_is_dropped_not_raised(enabled: None) -> None:
    _forwarder()(object())  # no .records attribute
    _forwarder()(_Batch("attempt-1", ()))  # empty batch is fine
    assert metrics.batches_dropped_malformed == 1
    assert metrics.batches_received == 2


def test_record_with_non_string_message_is_dropped(captured: pytest.LogCaptureFixture, enabled: None) -> None:
    _forwarder()(_Batch("attempt-1", (_Record(level="info", message="kept"), object())))
    records = [r for r in captured.records if r.name == "execution_plane.step_logs"]
    assert [r.message for r in records] == ["kept"]
    assert metrics.batches_dropped_malformed == 1


def test_oversized_batch_is_clamped(captured: pytest.LogCaptureFixture, enabled: None) -> None:
    _forwarder()(_Batch("attempt-1", tuple(_Record(level="info", message=f"m{i}") for i in range(500))))
    records = [r for r in captured.records if r.name == "execution_plane.step_logs"]
    assert len(records) == MAX_RECORDS_PER_BATCH


def test_sink_failures_never_propagate(enabled: None, monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(message: str, extra: dict[str, Any] | None = None) -> None:
        msg = "exporter exploded"
        raise RuntimeError(msg)

    monkeypatch.setattr(operational_logs._step_log_logger, "log", boom)
    _forwarder()(_Batch("attempt-1", (_Record(level="info", message="x"),)))
    assert metrics.forwarding_failures == 1
