"""Typed command results and transport receipts."""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID  # noqa: TC003 - Pydantic resolves this model field at runtime.

from pydantic import Field, StrictBool, StrictStr

from execution_plane_client.protocol import ResourceReference, WireModel


class CredentialResolveCommandResult(WireModel):
    """Typed result returned for a credential-resolution command."""

    command_type: Literal["credential.resolve"] = "credential.resolve"
    command_revision: Literal[1] = 1
    credential: Annotated[StrictStr, Field(min_length=1, max_length=65536, repr=False)]


class TargetHealthCommandResult(WireModel):
    """Typed result returned for an execution-target health command."""

    command_type: Literal["execution_target.health"] = "execution_target.health"
    command_revision: Literal[1] = 1
    healthy: StrictBool
    message: StrictStr | None = None
    skipped: StrictBool = False


CommandResult = Annotated[
    CredentialResolveCommandResult | TargetHealthCommandResult,
    Field(discriminator="command_type"),
]


class TransportReceipt(WireModel):
    """Receipt of a handled request, without a persisted command record."""

    received: Literal[True] = True
    message_id: UUID
    resource: ResourceReference | None = None
    result: CommandResult | None = None
