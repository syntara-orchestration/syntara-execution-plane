"""Host operations requested by an isolated agent workload."""

from __future__ import annotations

from typing import Annotated, ClassVar, Literal, cast
from uuid import UUID  # noqa: TC003 -- Pydantic resolves this annotation when constructing the wire model.

from pydantic import Field, StrictStr, field_validator

from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.commands.work import (
    StepIdentity,  # noqa: TC001 -- Pydantic needs this Annotated alias at runtime.
)
from execution_plane_client.protocol import WireModel, validate_json

AgentOperationName = Literal["agent.model.turn", "agent.mcp.call"]


class AgentOperation(CommandPayload):
    """One workload host callback, correlated to its active EP attempt."""

    command_type: ClassVar[str] = "agent.operation"

    work_item_id: UUID
    agent_invocation_id: UUID
    attempt_id: Annotated[StrictStr, Field(min_length=1, max_length=300)]
    step_revision_id: StepIdentity
    operation: AgentOperationName
    request_id: Annotated[StrictStr, Field(min_length=1, max_length=256)]
    payload: dict[str, object]

    @field_validator("payload")
    @classmethod
    def bound_payload(cls, value: dict[str, object]) -> dict[str, object]:
        """Keep callback payloads within the ordinary command envelope limit."""
        return cast("dict[str, object]", validate_json(value))


class AgentOperationCommandResult(WireModel):
    """The typed response to one agent host operation."""

    command_type: Literal["agent.operation"] = "agent.operation"
    command_revision: Literal[1] = 1
    request_id: Annotated[StrictStr, Field(min_length=1, max_length=256)]
    result: dict[str, object]

    @field_validator("result")
    @classmethod
    def bound_result(cls, value: dict[str, object]) -> dict[str, object]:
        """Keep callback results within the ordinary command envelope limit."""
        return cast("dict[str, object]", validate_json(value))
