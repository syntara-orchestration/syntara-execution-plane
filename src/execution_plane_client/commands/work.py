"""Scheduling commands for the SDK-owned container gRPC contract."""

import re
from enum import StrEnum
from typing import Annotated, ClassVar, Self
from uuid import UUID

from pydantic import ConfigDict, Field, StrictStr, field_validator, model_validator

from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.models.runtime_context import RuntimeContext
from execution_plane_client.protocol import PositiveInt, WireModel, validate_json

StepIdentity = Annotated[StrictStr, Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9._:-]+$")]
_IMAGE_REFERENCE_PATTERN = (
    r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)*"
    r"(?::[1-9][0-9]{0,4})?/[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*"
    r"(?:/[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*)*@sha256:[a-f0-9]{64}$"
)
_IMAGE_REFERENCE = re.compile(_IMAGE_REFERENCE_PATTERN)
SCRIPT_STEP_IDENTITIES: frozenset[str] = frozenset(
    {"syntara.script-python.script-python", "syntara.script-bash.script-bash"}
)


def is_digest_image_reference(value: object) -> bool:
    """Return whether an OCI image reference is fully qualified and sha256-pinned."""
    return isinstance(value, str) and _IMAGE_REFERENCE.fullmatch(value) is not None


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
    model_config = ConfigDict(
        json_schema_extra={
            "allOf": [
                {
                    "if": {
                        "properties": {
                            "invocation": {
                                "properties": {"step_revision_id": {"enum": sorted(SCRIPT_STEP_IDENTITIES)}},
                                "required": ["step_revision_id"],
                            }
                        },
                        "required": ["invocation"],
                    },
                    "then": {"properties": {"image": {"pattern": _IMAGE_REFERENCE_PATTERN}}},
                }
            ]
        }
    )

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
            description=(
                "OCI image reference for the isolated workload. Built-in Python and Bash script Actions require a "
                "fully qualified repository pinned to a SHA-256 manifest digest."
            ),
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

    @model_validator(mode="after")
    def require_digest_for_builtin_scripts(self) -> Self:
        """Require immutable image references for the credential-bearing script Actions."""
        if self.invocation.step_revision_id in SCRIPT_STEP_IDENTITIES and not is_digest_image_reference(self.image):
            message = "Built-in Python and Bash script Actions require a fully qualified sha256 image digest"
            raise ValueError(message)
        return self
