"""Participant-owned command handling, without database or HTTP dependencies."""

from dataclasses import dataclass
from typing import Protocol

from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.commands.results import CommandResult
from execution_plane_client.protocol import MessageEnvelope, ResourceReference


@dataclass(frozen=True)
class HandlingResult:
    """Optional resource created or affected by a command."""

    resource: ResourceReference | None = None
    result: CommandResult | None = None


class CommandHandler[ContextT](Protocol):
    """A participant supplies its own authenticated execution context."""

    async def handle(self, message: MessageEnvelope, payload: CommandPayload, context: ContextT) -> HandlingResult:
        """Apply domain effects using the catalog's validated payload and original envelope."""
        ...
