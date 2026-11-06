"""Client that resolves credential references through AO."""

from __future__ import annotations

from typing import TYPE_CHECKING, Self
from uuid import UUID

import httpx
from pydantic import ValidationError

from execution_plane.callback_client import CallbackHTTPClient
from execution_plane_client.commands.credentials import CredentialResolve
from execution_plane_client.commands.results import CredentialResolveCommandResult, TransportReceipt

if TYPE_CHECKING:
    from execution_plane.config.base import EPSettings


CREDENTIAL_RESOLUTION_FAILED = "Credential resolution failed"
CREDENTIAL_RESOLUTION_MISMATCH = "Credential resolution does not match its command"


class CredentialResolutionError(RuntimeError):
    """A credential could not be resolved before workload execution."""


class CredentialResolutionUnavailableError(CredentialResolutionError):
    """The credential-resolution callback is not configured or reachable."""


class CredentialResolutionMismatchError(CredentialResolutionError):
    """AO returned a credential for a different command."""


class CredentialResolutionInvalidError(CredentialResolutionError):
    """AO returned an invalid credential-resolution response."""


class CredentialResolver:
    """Construct and dispatch a typed `credential.resolve` command."""

    def __init__(self, settings: EPSettings, client: CallbackHTTPClient | None = None) -> None:
        """Use EP's callback identity and a reusable mTLS HTTP client."""
        self._settings = settings
        self._client = client or CallbackHTTPClient(
            url=settings.credential_resolution_callback_url,
            ca_cert_path=settings.credential_resolution_callback_ca_cert_path,
            cert_path=settings.credential_resolution_callback_cert_path,
            key_path=settings.credential_resolution_callback_key_path,
            timeout_seconds=settings.credential_resolution_callback_timeout_seconds,
        )

    async def resolve(self, credential_ref: UUID, *, client_id: str) -> str:
        """Resolve one reference for the client that owns the Cluster."""
        if credential_ref == UUID(int=0):
            error_message = "Cluster has no credential_ref"
            raise CredentialResolutionInvalidError(error_message)
        if not self._client.configured:
            error_message = "Credential-resolution callback is not configured"
            raise CredentialResolutionUnavailableError(error_message)
        message = CredentialResolve(credential_ref=credential_ref).to_message(
            source_id=self._settings.service_jwt_audience,
            destination_id=client_id,
            client_id=client_id,
        )
        try:
            response = await self._client.post_json(message.model_dump(mode="json"))
            response.raise_for_status()
            receipt = TransportReceipt.model_validate(response.json())
        except (httpx.HTTPError, ValidationError, ValueError) as exc:
            raise CredentialResolutionUnavailableError(CREDENTIAL_RESOLUTION_FAILED) from exc

        result = receipt.result
        if receipt.message_id != message.message_id or not isinstance(result, CredentialResolveCommandResult):
            raise CredentialResolutionMismatchError(CREDENTIAL_RESOLUTION_MISMATCH)
        return result.credential

    async def close(self) -> None:
        """Close the underlying callback client."""
        await self._client.close()

    async def __aenter__(self) -> Self:
        """Return the resolver for an explicit ownership scope."""
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        """Close the callback client when its ownership scope exits."""
        await self.close()
