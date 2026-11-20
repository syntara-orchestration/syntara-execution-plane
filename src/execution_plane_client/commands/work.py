"""Scheduling commands for the SDK-owned container gRPC contract."""

from enum import StrEnum
from typing import Annotated, ClassVar, Literal, Self
from uuid import UUID

from pydantic import Field, StrictStr, field_validator, model_validator

from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.models.runtime_context import RuntimeContext
from execution_plane_client.protocol import PositiveInt, WireModel, validate_json

StepIdentity = Annotated[StrictStr, Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9._:-]+$")]
SchemaDigest = Annotated[StrictStr, Field(pattern=r"^sha256:[a-f0-9]{64}$")]
SchemaDigestName = Literal["input", "output", "error"]


class ResolvedExecutionBinding(WireModel):
    """One compiled custom-runner binding selected for a workload invocation."""

    step_revision_id: Annotated[StrictStr, Field(pattern=r"^sha256:[a-f0-9]{64}$")]
    step_identity: StepIdentity
    runtime_kind: Literal["custom-runner"]
    entry_point: Annotated[StrictStr, Field(min_length=1, max_length=512)]
    abi: Literal["syntara.container/v1alpha1"]
    schema_digests: dict[SchemaDigestName, SchemaDigest] = Field(min_length=2, max_length=3)

    @model_validator(mode="after")
    def require_input_and_output_digests(self) -> Self:
        """Require the two schemas used for every executable custom runner."""
        if not {"input", "output"}.issubset(self.schema_digests):
            message = "resolved execution requires input and output schema digests"
            raise ValueError(message)
        return self


class ResolvedExecution(WireModel):
    """Resolved binding metadata that EP mounts into a selected workload."""

    bindings: list[ResolvedExecutionBinding] = Field(min_length=1, max_length=1)


class WorkloadType(StrEnum):
    """Workload execution families the Execution Plane can dispatch."""

    CONTAINER = "container"


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

    step_revision_id: StepIdentity = Field(
        description=(
            "Handler identifier advertised by the selected workload image. The plugin runtime uses it to select "
            "the step implementation; it does not identify this work item or an execution."
        )
    )
    inputs: dict[str, object] = Field(
        description="Step inputs defined by the selected workload; never use this field for credential bindings."
    )
    credential_bindings: list[WorkCredentialBinding] = Field(
        default_factory=list,
        max_length=16,
        repr=False,
        description="Host-side recipe-to-reference bindings; never plugin inputs or runtime context.",
    )
    host_operations: list[Literal["agent.model.turn", "agent.mcp.call"]] = Field(
        default_factory=list,
        max_length=2,
        description="Host callbacks the trusted dispatcher may route for this Action.",
    )

    @field_validator("host_operations")
    @classmethod
    def unique_host_operations(cls, value: list[str]) -> list[str]:
        """Reject repeated capabilities in a workload grant."""
        if len(value) != len(set(value)):
            message = "host_operations must be unique"
            raise ValueError(message)
        return value

    resolved_execution: ResolvedExecution | None = Field(
        default=None,
        repr=False,
        description=(
            "Trusted compiled custom-runner binding metadata. EP writes this document as a read-only runtime file; "
            "it is not a step input or credential."
        ),
    )

    @model_validator(mode="after")
    def resolved_revision_matches_invocation(self) -> Self:
        """Ensure the mounted binding names the exact handler this work will execute."""
        if self.resolved_execution is not None:
            binding = self.resolved_execution.bindings[0]
            if binding.step_revision_id != self.step_revision_id:
                message = "resolved execution revision must match step_revision_id"
                raise ValueError(message)
        return self

    timeout_seconds: Annotated[
        PositiveInt,
        Field(
            le=86_400,
            description="Maximum workload execution time in seconds, including SDK streaming but excluding cleanup.",
        ),
    ]

    @field_validator("inputs", mode="before")
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
    command_revision: ClassVar[int] = 1

    work_item_id: UUID = Field(
        description=(
            "Caller-assigned UUID used as the primary key (WorkItem.id) of the new EP work item. "
            "It must be unique across EP work items, including other clients' work. An existing UUID returns "
            "HTTP 409 and never overwrites or reruns that item. On an uncertain submission retry, retain the UUID "
            "and read the existing item after a conflict. Use it for reads, cancellation, completion, and credential "
            "callbacks. Syntara supplies ActivityExecution.id, not the containing Execution.id."
        )
    )
    runtime_context: RuntimeContext = Field(default_factory=dict, repr=False)

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
    workload_type: WorkloadType = Field(
        default=WorkloadType.CONTAINER,
        description="Workload execution family; container is the only supported value.",
    )


class WorkDelete(CommandPayload):
    """Request asynchronous cancellation, resource cleanup, and removal of owned work."""

    command_type: ClassVar[str] = "work.delete"

    work_item_id: UUID
