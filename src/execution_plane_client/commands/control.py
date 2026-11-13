"""Cancellation requests and work completion commands."""

from enum import StrEnum
from typing import ClassVar
from uuid import UUID

from pydantic import Field, field_validator

from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.models.work_item import WorkItemStatus
from execution_plane_client.models.work_metadata import WorkMetadata
from execution_plane_client.protocol import validate_json


class WorkCancel(CommandPayload):
    """Request cancellation of an existing work item."""

    command_type: ClassVar[str] = "work.cancel"

    work_item_id: UUID = Field(description="Identifier of the existing work item to cancel.")


class CompletionStatus(StrEnum):
    """Work-item states that can be reported by a completion command."""

    COMPLETED = WorkItemStatus.COMPLETED.value
    FAILED = WorkItemStatus.FAILED.value
    CANCELLED = WorkItemStatus.CANCELLED.value
    RECONCILIATION_REQUIRED = WorkItemStatus.RECONCILIATION_REQUIRED.value


class Completion(CommandPayload):
    """Terminal or uncertain work outcome delivered to the initiating client."""

    command_type: ClassVar[str] = "work.execute.completed"
    command_revision: ClassVar[int] = 2

    work_item_id: UUID = Field(description="Identifier of the work item whose outcome is reported.")
    metadata: WorkMetadata = Field(default_factory=dict, repr=False)

    status: CompletionStatus
    result: dict[str, object] = Field(default_factory=dict, repr=False)
    error_code: str | None = None

    @field_validator("result", mode="before")
    @classmethod
    def check_result(cls, value: object) -> object:
        """Require finite JSON; work output limits are set by the invocation."""
        return validate_json(value, max_bytes=None)
