"""Typed command results and transport receipts."""

from __future__ import annotations

import json
from typing import Annotated, Literal
from uuid import UUID  # noqa: TC003 - Pydantic resolves this model field at runtime.

from pydantic import Field, StrictBool, StrictStr, field_validator

from execution_plane_client.commands.agent import AgentOperationCommandResult
from execution_plane_client.protocol import ResourceReference, WireModel

MAX_CREDENTIAL_FIELDS = 32
MAX_CREDENTIAL_BYTES = 32 * 1024


def validate_credential_document(value: dict[str, str]) -> dict[str, str]:
    """Bound credential field count and encoded response size."""
    if not value:
        message = "Credential document must not be empty"
        raise ValueError(message)
    if len(value) > MAX_CREDENTIAL_FIELDS:
        message = "Credential document must contain at most 32 fields"
        raise ValueError(message)
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) > MAX_CREDENTIAL_BYTES:
        message = "Credential document exceeds 32 KiB"
        raise ValueError(message)
    return value


class CredentialResolveClusterCommandResult(WireModel):
    """Typed result returned for a cluster credential-resolution command."""

    command_type: Literal["credential.resolve.cluster"] = "credential.resolve.cluster"
    command_revision: Literal[1] = 1
    credential: dict[
        Annotated[StrictStr, Field(min_length=1, max_length=128)],
        Annotated[StrictStr, Field(min_length=1, max_length=8192)],
    ] = Field(repr=False)

    check_document = field_validator("credential")(validate_credential_document)


class CredentialResolveWorkloadCommandResult(WireModel):
    """Typed result returned for a workload credential-resolution command."""

    command_type: Literal["credential.resolve.workload"] = "credential.resolve.workload"
    command_revision: Literal[1] = Field(
        default=1, description="Payload contract version for credential.resolve.workload; matches the request revision."
    )
    credential: dict[
        Annotated[StrictStr, Field(min_length=1, max_length=128)],
        Annotated[StrictStr, Field(min_length=1, max_length=8192)],
    ] = Field(repr=False)

    check_document = field_validator("credential")(validate_credential_document)


class TargetHealthCommandResult(WireModel):
    """Typed result returned for an execution-target health command."""

    command_type: Literal["execution_target.health"] = "execution_target.health"
    command_revision: Literal[1] = 1
    healthy: StrictBool = Field(
        description=(
            "Whether the namespace probe succeeded. For Kubernetes this checks namespace "
            "readability, not workload execution. Check skipped first: a skipped probe also "
            "returns true."
        )
    )
    message: StrictStr | None = None
    skipped: StrictBool = Field(
        default=False,
        description=(
            "True when no health check ran because the backend, endpoint, or cluster/target "
            "lifecycle is outside probe scope; healthy then does not establish readiness."
        ),
    )


CommandResult = Annotated[
    CredentialResolveClusterCommandResult
    | CredentialResolveWorkloadCommandResult
    | TargetHealthCommandResult
    | AgentOperationCommandResult,
    Field(discriminator="command_type"),
]


class TransportReceipt(WireModel):
    """Receipt of a handled request, without a persisted command record."""

    received: Literal[True] = Field(
        default=True,
        description=(
            "Confirms the command handler returned successfully. Asynchronous work, cancellation, "
            "and deletion may still be in progress."
        ),
    )
    message_id: UUID = Field(
        description="The received delivery message's UUID, echoed so the sender can match this receipt."
    )
    resource: ResourceReference | None = None
    result: CommandResult | None = None
