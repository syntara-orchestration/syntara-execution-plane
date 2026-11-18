"""Transactional intent requests for a client-owned set of existing work items."""

from typing import ClassVar, Self
from uuid import UUID

from pydantic import Field, JsonValue, model_validator

from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.protocol import WireModel


class WorkItemSelector(WireModel):
    """Select by exactly one ID list or a non-empty runtime-context containment filter.

    Selection is always restricted to the authenticated client. Missing IDs,
    foreign IDs, and zero filter matches are successful no-ops. A filter selects
    existing rows at execution time, not work submitted after the command.
    """

    work_item_ids: list[UUID] | None = Field(default=None, min_length=1, max_length=100)
    runtime_context: dict[str, JsonValue] | None = Field(
        default=None,
        min_length=1,
        description=(
            "Non-empty JSON object matched using PostgreSQL JSONB containment (@>). "
            "All supplied key/value pairs must be contained in the stored runtime_context. "
            "Nested objects may contain additional keys; arrays use JSONB containment semantics. "
            "Keys and values are caller-defined; EP assigns them no domain meaning."
        ),
    )

    @model_validator(mode="after")
    def require_one_selector(self) -> Self:
        """Reject unscoped requests and ambiguous combinations of selectors."""
        if (self.work_item_ids is not None) == (self.runtime_context is not None):
            message = "Exactly one of work_item_ids or runtime_context is required"
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
