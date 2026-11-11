"""Scheduling commands for the SDK-owned container gRPC contract."""

from typing import Annotated, ClassVar, Literal
from uuid import UUID

from pydantic import Field, StrictStr, field_validator

from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.protocol import PositiveInt, WireModel, validate_json

StepIdentity = Annotated[StrictStr, Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9._:-]+$")]
CREDENTIAL_RECIPE_ID_PATTERN = r"^[a-z][a-z0-9-]{0,62}(?:\.[a-z][a-z0-9-]{0,62})+$"
CredentialRecipeId = Annotated[
    StrictStr,
    Field(min_length=1, max_length=256, pattern=CREDENTIAL_RECIPE_ID_PATTERN),
]


class WorkCredentialBinding(WireModel):
    """One Plugin SDK recipe-to-AO credential mapping for a work item."""

    credential_recipe_id: CredentialRecipeId
    credential_ref: UUID = Field(repr=False)


class WorkloadInvocation(WireModel):
    """Public invocation data; EP supplies the attempt ID and credential resolver.

    Script environment variables remain ordinary step inputs. Credential-file
    delivery is not implemented yet, so callers cannot supply secret values or
    claim that arbitrary file paths have been mounted by EP.
    """

    workflow_run_id: StepIdentity
    step_revision_id: StepIdentity
    inputs: dict[str, object]
    runtime_context: dict[str, object] = Field(default_factory=dict)
    credential_bindings: list[WorkCredentialBinding] = Field(default_factory=list, max_length=16, repr=False)
    timeout_seconds: Annotated[PositiveInt, Field(le=86_400)]

    @field_validator("inputs", "runtime_context", mode="before")
    @classmethod
    def check_json(cls, value: object) -> object:
        """Keep nested invocation data finite, portable, and bounded."""
        return validate_json(value)

    @field_validator("credential_bindings")
    @classmethod
    def check_unique_recipe_ids(cls, value: list[WorkCredentialBinding]) -> list[WorkCredentialBinding]:
        """Reject duplicate recipe IDs before host-side credential resolution."""
        recipe_ids = [binding.credential_recipe_id for binding in value]
        if len(set(recipe_ids)) != len(recipe_ids):
            message = "Credential bindings must have unique credential recipe IDs"
            raise ValueError(message)
        return value


class WorkExecute(CommandPayload):
    """Schedule one workload implementing the SDK container service."""

    command_type: ClassVar[str] = "work.execute"

    image: Annotated[StrictStr, Field(min_length=1, max_length=2048)]
    selectors: dict[StrictStr, StrictStr] = Field(default_factory=dict)
    invocation: WorkloadInvocation = Field(repr=False)
    output_config: dict[str, str] | None = None
    workload_type: Literal["container"] = "container"
