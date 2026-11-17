"""Transactional intent requests for a client-owned set of existing work items."""

from typing import ClassVar, Self
from uuid import UUID

from pydantic import Field, model_validator

from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.protocol import WireModel


class WorkItemSelector(WireModel):
    """Select by exactly one ID list or exact top-level runtime-context UUID string.

    Selection is always restricted to the authenticated client. Missing IDs,
    foreign IDs, and zero filter matches are successful no-ops. A filter selects
    existing rows at execution time, not work submitted after the command.
    """

    work_item_ids: list[UUID] | None = Field(default=None, min_length=1, max_length=100)
    project_id: UUID | None = Field(default=None, description="Exact runtime_context.project_id UUID string.")
    workflow_id: UUID | None = Field(default=None, description="Exact runtime_context.workflow_id UUID string.")
    execution_id: UUID | None = Field(default=None, description="Exact runtime_context.execution_id UUID string.")

    @model_validator(mode="after")
    def require_one_selector(self) -> Self:
        """Reject unscoped requests and ambiguous combinations of selectors."""
        if (
            sum(
                value is not None
                for value in (self.work_item_ids, self.project_id, self.workflow_id, self.execution_id)
            )
            != 1
        ):
            message = "Exactly one of work_item_ids, project_id, workflow_id, or execution_id is required"
            raise ValueError(message)
        return self


class WorkCancelBulk(CommandPayload):
    """Commit cancellation intent for all matches in one transaction.

    Success acknowledges durable intent, not stopped execution. Lifecycle and
    results are unchanged, including for terminal work. Repeating is safe.
    """

    command_type: ClassVar[str] = "work.cancel_bulk"
    selector: WorkItemSelector = Field(description="Exactly one selector; filter matches have no row-count limit.")


class WorkDeleteBulk(CommandPayload):
    """Commit deletion and cancellation intent together for all matches.

    Success follows the transaction commit, not resource cleanup or physical
    removal. EP workers own completion. Repeating after a lost receipt is safe.
    """

    command_type: ClassVar[str] = "work.delete_bulk"
    selector: WorkItemSelector = Field(description="Exactly one selector; filter matches have no row-count limit.")
