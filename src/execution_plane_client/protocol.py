"""Strict, versioned command envelopes and receipt contracts."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID  # noqa: TC003 - Pydantic resolves these model fields at runtime.

from pydantic import BaseModel, Field, StrictInt, StrictStr, ValidationInfo, field_validator

MAX_PAYLOAD_BYTES = 512 * 1024
Identifier = Annotated[StrictStr, Field(min_length=1, max_length=255, pattern=r"^[A-Za-z0-9_.:@/-]+$")]
PositiveInt = Annotated[StrictInt, Field(gt=0)]
Sequence = Annotated[StrictInt, Field(ge=0)]


class WireModel(BaseModel):
    """Forbid silent field loss and freeze top-level wire values."""

    model_config = {"extra": "forbid", "frozen": True, "validate_default": True}


def validate_json(value: object, *, max_bytes: int | None = MAX_PAYLOAD_BYTES) -> object:
    """Reject coercions and non-finite JSON, optionally bounding its encoded size."""
    try:
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        message = "Payload is not finite UTF-8 JSON"
        raise ValueError(message) from exc
    if max_bytes is not None and len(encoded) > max_bytes:
        message = f"Payload exceeds {max_bytes // 1024} KiB"
        raise ValueError(message)
    pending = [value]
    while pending:
        item = pending.pop()
        if type(item) is dict:
            if any(not isinstance(key, str) for key in item):
                message = "JSON object keys must be strings"
                raise ValueError(message)
            pending.extend(item.values())
        elif type(item) is list:
            pending.extend(item)
        elif item is not None and type(item) not in (str, bool, int, float):
            message = "Payload must contain JSON values only"
            raise ValueError(message)
    return value


class CommandScope(WireModel):
    """Initiating service client; project authorization belongs to the caller."""

    client_id: Identifier


class Command(WireModel):
    """One versioned operation; participants persist only its domain effects."""

    command_type: Annotated[
        StrictStr,
        Field(
            pattern=r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$",
            description="Operation to perform; selects the payload contract.",
        ),
    ]
    command_revision: PositiveInt = Field(
        description="Version of this operation's payload contract, not a resource revision."
    )
    payload: dict[str, object] = Field(repr=False)
    occurred_at: datetime

    @field_validator("payload", mode="before")
    @classmethod
    def check_payload(cls, value: object, info: ValidationInfo) -> object:
        """Preserve the payload's JSON types and enforce its size limit."""
        limit = None if info.data.get("command_type") == "work.execute.completed" else MAX_PAYLOAD_BYTES
        return validate_json(value, max_bytes=limit)

    @field_validator("occurred_at")
    @classmethod
    def check_time(cls, value: datetime) -> datetime:
        """Require an explicit timezone and serialize instants in UTC."""
        if value.tzinfo is None or value.utcoffset() is None:
            message = "occurred_at requires a timezone"
            raise ValueError(message)
        return value.astimezone(UTC)


class MessageEnvelope(WireModel):
    """Participant identity around one logical command."""

    message_id: UUID = Field(
        description=(
            "UUID assigned by the sender to this delivery message and echoed in its transport receipt. "
            "It does not identify a work item or deduplicate submissions; resource IDs in the payload do that."
        )
    )
    message_type: Literal["command.submit"] = "command.submit"
    sequence: Sequence = 0
    source_id: Identifier
    destination_id: Identifier
    scope: CommandScope
    command: Command


class ResourceReference(WireModel):
    """Safe reference from a command to a participant-owned resource."""

    kind: Identifier
    resource_id: UUID
    resource_key: UUID | None = None
    revision: PositiveInt | None = None


class CapabilitiesRead(WireModel):
    """Command revisions installed by this participant."""

    protocol_revision: Literal[2] = Field(
        default=2, description="Envelope contract version. Version 2 removes command_id and parent_command_id."
    )
    commands: dict[str, list[int]]
