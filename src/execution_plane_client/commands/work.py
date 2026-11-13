"""Scheduling commands for the SDK-owned container gRPC contract."""

from typing import Annotated, ClassVar, Literal
from uuid import UUID

from pydantic import Field, StrictStr, field_validator

from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.models.work_metadata import WorkMetadata
from execution_plane_client.protocol import PositiveInt, WireModel, validate_json

StepIdentity = Annotated[StrictStr, Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9._:-]+$")]
CREDENTIAL_RECIPE_ID_PATTERN = r"^[a-z][a-z0-9-]{0,62}(?:\.[a-z][a-z0-9-]{0,62})+$"
CredentialRecipeId = Annotated[
    StrictStr,
    Field(min_length=1, max_length=256, pattern=CREDENTIAL_RECIPE_ID_PATTERN),
]


class WorkCredentialBinding(WireModel):
    """One Plugin SDK recipe-to-AO credential mapping for a work item."""

    credential_recipe_id: CredentialRecipeId = Field(
        description="Qualified Plugin SDK CredentialRecipe identifier that the workload may request at runtime."
    )
    credential_ref: UUID = Field(
        repr=False,
        description="Opaque AO credential reference; EP resolves it only after the workload requests this recipe.",
    )


class WorkloadInvocation(WireModel):
    """Public invocation data; EP supplies the attempt ID and credential resolver.

    Script environment variables remain ordinary step inputs. Credential-file
    delivery is not implemented yet, so callers cannot supply secret values or
    claim that arbitrary file paths have been mounted by EP.
    """

    workflow_run_id: StepIdentity = Field(description="Caller-owned identity of the workflow run containing this step.")
    step_revision_id: StepIdentity = Field(
        description="Workload-advertised revision of the step implementation to execute."
    )
    inputs: dict[str, object] = Field(
        description="Step inputs defined by the selected workload; never use this field for credential bindings."
    )
    runtime_context: dict[str, object] = Field(
        default_factory=dict,
        description="Non-secret execution context supplied to the workload; not for credential bindings.",
    )
    credential_bindings: list[WorkCredentialBinding] = Field(
        default_factory=list,
        max_length=16,
        repr=False,
        description="Host-side recipe-to-reference bindings; never plugin inputs or runtime context.",
    )
    timeout_seconds: Annotated[
        PositiveInt,
        Field(
            le=86_400,
            description="Maximum workload execution time in seconds, including SDK streaming but excluding cleanup.",
        ),
    ]

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
    command_revision: ClassVar[int] = 2

    work_item_id: UUID = Field(
        description=(
            "Caller-assigned identifier of the Execution Plane work item. Reuse this identifier "
            "when retrying submission and when addressing the same work."
        )
    )
    metadata: WorkMetadata = Field(default_factory=dict, repr=False)

    image: Annotated[
        StrictStr,
        Field(
            min_length=1,
            max_length=2048,
            description="OCI image reference for the isolated cold-start workload container.",
        ),
    ]
    selectors: dict[StrictStr, StrictStr] = Field(
        default_factory=dict,
        description="Exact label requirements used to select an execution target; all keys and values must match.",
    )
    invocation: WorkloadInvocation = Field(
        repr=False,
        description="Validated workload invocation, including host-side credential bindings.",
    )
    output_config: dict[str, str] | None = Field(
        default=None,
        description="Optional result-field allowlist mapping output names to workload result keys.",
    )
    workload_type: Literal["container"] = Field(
        default="container",
        description="Workload execution family; container is the only supported value.",
    )
