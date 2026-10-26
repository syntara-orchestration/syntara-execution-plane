"""Pure lifecycle policy for WorkItem transitions."""

from __future__ import annotations

from execution_plane.models.work_item import WorkItemStatus

TERMINAL_RESULT_STATUSES = frozenset(
    {
        WorkItemStatus.COMPLETED,
        WorkItemStatus.FAILED,
        WorkItemStatus.CANCELLED,
    }
)
REFRESHABLE_CLAIM_STATUSES = frozenset(
    {
        WorkItemStatus.CLAIMED,
        WorkItemStatus.DISPATCHED,
        WorkItemStatus.CANCEL_REQUESTED,
        WorkItemStatus.RECONCILIATION_REQUIRED,
    }
)
DRAIN_BLOCKING_STATUSES = frozenset(REFRESHABLE_CLAIM_STATUSES)
MAX_RECONCILIATION_REASON_LENGTH = 1000


def require_terminal_result_status(status: WorkItemStatus) -> None:
    """Reject execution results that cannot produce a terminal completion."""
    if status not in TERMINAL_RESULT_STATUSES:
        message = f"Work result requires a terminal status, received {status.value}"
        raise ValueError(message)


def bounded_reconciliation_reason(reason: str) -> str:
    """Bound uncertain-outcome explanations before persistence."""
    return reason[:MAX_RECONCILIATION_REASON_LENGTH]


def validate_completion_retry_delay(retry_after_seconds: int) -> None:
    """Reject retry schedules that would run in the past."""
    if retry_after_seconds < 0:
        raise ValueError("Completion retry delay must be non-negative")  # noqa: TRY003, EM101
