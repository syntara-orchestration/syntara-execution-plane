"""Typed command results and transport receipts."""

from __future__ import annotations

import json
from typing import Annotated, Literal
from uuid import UUID  # noqa: TC003 - Pydantic resolves this model field at runtime.

from pydantic import Field, StrictBool, StrictStr, field_validator

from execution_plane_client.protocol import ResourceReference, WireModel

MAX_WORKLOAD_CREDENTIAL_FIELDS = 32
MAX_WORKLOAD_CREDENTIAL_BYTES = 32 * 1024


class CredentialResolveCommandResult(WireModel):
    """Typed result returned for a credential-resolution command."""

    command_type: Literal["credential.resolve"] = "credential.resolve"
    command_revision: Literal[1] = 1
    credential: Annotated[StrictStr, Field(min_length=1, max_length=65536, repr=False)]


class CredentialResolveWorkloadCommandResult(WireModel):
    """Typed result returned for a workload credential-resolution command."""

    command_type: Literal["credential.resolve.workload"] = "credential.resolve.workload"
    command_revision: Literal[2] = 2
    credential: dict[
        Annotated[StrictStr, Field(min_length=1, max_length=128)],
        Annotated[StrictStr, Field(min_length=1, max_length=8192)],
    ] = Field(repr=False)

    @field_validator("credential")
    @classmethod
    def check_document(cls, value: dict[str, str]) -> dict[str, str]:
        """Bound credential field count and encoded response size."""
        if not value:
            message = "Credential document must not be empty"
            raise ValueError(message)
        if len(value) > MAX_WORKLOAD_CREDENTIAL_FIELDS:
            message = "Credential document must contain at most 32 fields"
            raise ValueError(message)
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(encoded) > MAX_WORKLOAD_CREDENTIAL_BYTES:
            message = "Credential document exceeds 32 KiB"
            raise ValueError(message)
        return value


class TargetHealthCommandResult(WireModel):
    """Typed result returned for an execution-target health command."""

    command_type: Literal["execution_target.health"] = "execution_target.health"
    command_revision: Literal[1] = 1
    healthy: StrictBool
    message: StrictStr | None = None
    skipped: StrictBool = False


CommandResult = Annotated[
    CredentialResolveCommandResult | CredentialResolveWorkloadCommandResult | TargetHealthCommandResult,
    Field(discriminator="command_type"),
]


class TransportReceipt(WireModel):
    """Receipt of a handled request, without a persisted command record."""

    received: Literal[True] = True
    message_id: UUID
    resource: ResourceReference | None = None
    result: CommandResult | None = None
