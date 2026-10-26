"""Backend-neutral contracts for one fenced execution attempt."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from uuid import UUID

    from execution_plane.models.work_item import WorkItemStatus


class RetryableDispatchError(Exception):
    """A known pre-submission failure that can be retried after releasing the worker."""


class WorkItemPayloadError(Exception):
    """The invocation cannot be executed by this backend."""


class BackendError(Exception):
    """A sanitized backend failure; execution may be uncertain after submission."""


class WorkloadOutcomeUnknownError(Exception):
    """External ownership or execution is uncertain and must not be retried."""


@dataclass(frozen=True)
class WorkAttempt:
    """Snapshot of the claim and request identity, independent of an ORM session."""

    work_item_id: UUID
    claim_owner_id: UUID
    claim_generation: int
    execution_target_id: UUID

    @property
    def identity(self) -> str:
        """Return the logical WorkItem identity."""
        return str(self.work_item_id)

    @property
    def attempt_id(self) -> str:
        """Return the identity of this particular dispatch attempt."""
        return f"{self.work_item_id}-{self.claim_generation}"


@dataclass(frozen=True)
class ExecutionResult:
    """A terminal result observed from the backend, including partial output."""

    status: WorkItemStatus
    result: dict[str, Any]


@dataclass(frozen=True)
class BackendResource:
    """Durable allocation identity, available before external creation."""

    name: str
    uid: str | None = None


@dataclass(frozen=True)
class ReleaseResult:
    """Keep infrastructure cleanup separate from confirmed execution termination."""

    status: Literal["not_required", "complete", "failed", "deferred"]
    execution_stopped: bool
    error: str | None = None


class Invocation(ABC):
    """An issued invocation whose transport remains open until the worker is released."""

    @abstractmethod
    async def result(self) -> ExecutionResult:
        """Observe exactly one terminal result; a missing result is uncertain."""

    @abstractmethod
    async def cancel(self) -> None:
        """Request termination without discarding a racing terminal result."""


class WorkerAllocation(ABC):
    """Own backend resources across allocation, readiness, submission, and release.

    Construction has no external effects. Cancellation of a stage must finish
    any outstanding resource mutation before returning control to the caller.
    Release also covers partial allocation and must never delete unowned resources.
    """

    @property
    @abstractmethod
    def resource(self) -> BackendResource | None:
        """Expose allocation intent first, then its confirmed external identity."""

    @abstractmethod
    async def allocate(self) -> None:
        """Obtain or reserve an exclusively owned execution slot."""

    @abstractmethod
    async def wait_ready(self) -> None:
        """Wait until the worker can accept this invocation, without submitting it."""

    @abstractmethod
    async def submit(self) -> Invocation:
        """Issue the invocation once and return its live handle.

        Errors from this operation are conservatively treated as possibly
        submitted. Validate requests during preparation/readiness instead.
        """

    @abstractmethod
    async def release(self) -> ReleaseResult:
        """Release or quarantine the worker and report whether execution has stopped."""
