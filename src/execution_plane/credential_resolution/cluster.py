"""Cluster credential resolution through AO."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from execution_plane.credential_resolution.errors import (
    CREDENTIAL_RESOLUTION_MISMATCH,
    CredentialResolutionInvalidError,
    CredentialResolutionMismatchError,
)
from execution_plane_client.commands.credentials import CredentialResolve
from execution_plane_client.commands.results import CredentialResolveCommandResult

if TYPE_CHECKING:
    from execution_plane.config.settings import Settings
    from execution_plane.credential_resolution.client import CredentialResolutionClient


class ClusterCredentialResolver:
    """Construct and dispatch a typed `credential.resolve` command."""

    def __init__(self, settings: Settings, client: CredentialResolutionClient) -> None:
        """Bind one resolver to the shared credential-resolution client."""
        self._settings = settings
        self._client = client

    async def resolve(self, credential_ref: UUID, *, client_id: str) -> str:
        """Resolve one reference for the client that owns the Cluster."""
        if credential_ref == UUID(int=0):
            message = "Cluster has no credential_ref"
            raise CredentialResolutionInvalidError(message)
        envelope = CredentialResolve(credential_ref=credential_ref).to_message(
            source_id=self._settings.service_jwt_audience,
            destination_id=client_id,
            client_id=client_id,
        )
        receipt = await self._client.post(envelope)
        result = receipt.result
        if receipt.message_id != envelope.message_id or not isinstance(result, CredentialResolveCommandResult):
            raise CredentialResolutionMismatchError(CREDENTIAL_RESOLUTION_MISMATCH)
        return result.credential
