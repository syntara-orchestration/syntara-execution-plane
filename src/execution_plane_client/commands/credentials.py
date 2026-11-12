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
    """Resolve one credential reference for the work item named by its parent command."""

    command_type: ClassVar[str] = "credential.resolve.workload"
    requires_parent: ClassVar[bool] = True
    attempt_id: Identifier = Field(description="Execution attempt that owns the workload invocation.")
    credential_ref: UUID = Field(
        repr=False,
        description="Opaque AO credential reference bound to the requested recipe.",
    )
    credential_recipe_id: CredentialRecipeId = Field(description="Logical Plugin SDK recipe requested by the workload.")
    workflow_run_id: StepIdentity = Field(description="Caller-owned workflow run containing the active step.")
    step_revision_id: StepIdentity = Field(description="Workload revision that requested the credential.")
