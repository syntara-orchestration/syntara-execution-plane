"""Scheduling commands for the SDK-owned container gRPC contract."""

from typing import Annotated, ClassVar, Literal

from pydantic import Field, StrictStr, field_validator

from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.protocol import PositiveInt, WireModel, validate_json

StepIdentity = Annotated[StrictStr, Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9._:-]+$")]


class WorkloadInvocation(WireModel):
    """Public invocation data; EP supplies the attempt ID and credential mounts.

    Script environment variables remain ordinary step inputs. Credential-file
    delivery is not implemented yet, so callers cannot supply secret values or
    claim that arbitrary file paths have been mounted by EP.
    """

    workflow_run_id: StepIdentity
    step_revision_id: StepIdentity
    inputs: dict[str, object]
    runtime_context: dict[str, object] = Field(default_factory=dict)
    timeout_seconds: Annotated[PositiveInt, Field(le=86_400)]

    @field_validator("inputs", "runtime_context", mode="before")
    @classmethod
    def check_json(cls, value: object) -> object:
        """Keep nested invocation data finite, portable, and bounded."""
        return validate_json(value)


class WorkExecute(CommandPayload):
    """Schedule one workload implementing the SDK container service."""

    command_type: ClassVar[str] = "work.execute"

    image: Annotated[StrictStr, Field(min_length=1, max_length=2048)]
    selectors: dict[StrictStr, StrictStr] = Field(default_factory=dict)
    invocation: WorkloadInvocation = Field(repr=False)
    output_config: dict[str, str] | None = None
    workload_type: Literal["container"] = "container"
