"""Credential-reference resolution commands."""

from __future__ import annotations

from typing import ClassVar
from uuid import UUID  # noqa: TC003 - Pydantic evaluates fields.

from pydantic import Field

from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.commands.work import CredentialRecipeId, StepIdentity  # noqa: TC001
from execution_plane_client.protocol import Identifier  # noqa: TC001


class CredentialResolve(CommandPayload):
    """Resolve an opaque credential reference into a short-lived value."""

    command_type: ClassVar[str] = "credential.resolve"
    credential_ref: UUID = Field(
        repr=False,
        description="Opaque AO credential reference.",
    )


class CredentialResolveWorkload(CommandPayload):
    """Resolve one credential reference for the identified work item."""

    command_type: ClassVar[str] = "credential.resolve.workload"
    command_revision: ClassVar[int] = 2
    work_item_id: UUID = Field(
        description=(
            "Primary key (WorkItem.id) of the EP work item requesting credentials. Syntara uses it to look up "
            "ActivityExecution.id and verify the credential binding for that work."
        )
    )
    attempt_id: Identifier = Field(
        description="EP dispatch-attempt identifier, sent to the plugin as invocation_id; distinct from WorkItem.id."
    )
    credential_ref: UUID = Field(
        repr=False,
        description="Opaque AO credential reference bound to the requested recipe.",
    )
    credential_recipe_id: CredentialRecipeId = Field(description="Logical Plugin SDK recipe requested by the workload.")
    step_revision_id: StepIdentity = Field(
        description=(
            "Handler identifier from this work item's invocation; identifies the implementation requesting credentials."
        )
    )
