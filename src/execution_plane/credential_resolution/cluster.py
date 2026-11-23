"""Cluster credential resolution through AO."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

import structlog

from execution_plane.credential_resolution.errors import (
    CREDENTIAL_RESOLUTION_MISMATCH,
    CredentialResolutionInvalidError,
    CredentialResolutionMismatchError,
)
from execution_plane_client.commands.credentials import CredentialResolveCluster
from execution_plane_client.commands.results import CredentialResolveClusterCommandResult

if TYPE_CHECKING:
    from execution_plane.config.settings import Settings
    from execution_plane.credential_resolution.client import CredentialResolutionClient


logger = structlog.stdlib.get_logger(__name__)


class ClusterCredentialResolver:
    """Construct and dispatch a typed `credential.resolve.cluster` command."""

    def __init__(self, settings: Settings, client: CredentialResolutionClient) -> None:
        """Bind one resolver to the shared credential-resolution client."""
        self._settings = settings
        self._client = client

    async def resolve(self, credential_ref: UUID, *, client_id: str) -> str:
        """Resolve one reference for the client that owns the Cluster."""
        if credential_ref == UUID(int=0):
            message = "Cluster has no credential_ref"
            logger.warning("Cluster credential resolution rejected an invalid reference", client_id=client_id)
            raise CredentialResolutionInvalidError(message)
        envelope = CredentialResolveCluster(credential_ref=credential_ref).to_message(
            source_id=self._settings.service_jwt_audience,
            destination_id=client_id,
            client_id=client_id,
        )
        receipt = await self._client.post(envelope)
        result = receipt.result
        if receipt.message_id != envelope.message_id or not isinstance(result, CredentialResolveClusterCommandResult):
            logger.warning(
                "Cluster credential resolution receipt did not match its command",
                command_name=envelope.command.command_type,
                message_id=str(envelope.message_id),
            )
            raise CredentialResolutionMismatchError(CREDENTIAL_RESOLUTION_MISMATCH)
        resolved = result.credential
        credential = str(resolved.get("bearer_token") or resolved.get("token") or resolved.get("api_key") or "")
        if not credential:
            message = "Credential did not resolve to an API token"
            logger.warning(
                "Cluster credential resolution returned no usable token",
                command_name=envelope.command.command_type,
                message_id=str(envelope.message_id),
            )
            raise CredentialResolutionInvalidError(message)
        logger.info(
            "Cluster credential resolved",
            command_name=envelope.command.command_type,
            message_id=str(envelope.message_id),
            client_id=client_id,
        )
        return credential
