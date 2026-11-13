"""Supported command revisions and their wire payload contracts."""

from dataclasses import dataclass

from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.commands.clusters import ClusterDelete, ClusterRegister, ClusterUpdate
from execution_plane_client.commands.control import Completion, WorkCancel
from execution_plane_client.commands.credentials import CredentialResolve, CredentialResolveWorkload
from execution_plane_client.commands.targets import TargetDelete, TargetHealth, TargetRegister, TargetUpdate
from execution_plane_client.commands.work import WorkExecute
from execution_plane_client.errors import UnsupportedCommand
from execution_plane_client.protocol import MessageEnvelope


@dataclass(frozen=True)
class CommandDefinition:
    """One command type, revision and payload schema."""

    command_type: str
    revision: int
    payload_schema: type[CommandPayload]


class CommandCatalog:
    """Validate known commands independently of installed handlers."""

    def __init__(self) -> None:
        """Install supported work, resource and completion contracts."""
        payloads = (
            WorkExecute,
            Completion,
            CredentialResolve,
            CredentialResolveWorkload,
            ClusterRegister,
            ClusterUpdate,
            ClusterDelete,
            TargetRegister,
            TargetUpdate,
            TargetDelete,
            TargetHealth,
            WorkCancel,
        )
        self._definitions = {
            (schema.command_type, schema.command_revision): CommandDefinition(
                schema.command_type, schema.command_revision, schema
            )
            for schema in payloads
        }

    def definition(self, command_type: str, revision: int) -> CommandDefinition:
        """Return a supported revision or an explicit rejection."""
        try:
            return self._definitions[command_type, revision]
        except KeyError:
            raise UnsupportedCommand from None

    def validate(self, message: MessageEnvelope) -> CommandPayload:
        """Validate the typed command payload, returning the typed payload."""
        definition = self.definition(message.command.command_type, message.command.command_revision)
        return definition.payload_schema.model_validate(message.command.payload)
