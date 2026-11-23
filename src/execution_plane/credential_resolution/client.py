"""Shared HTTP transport for credential-resolution commands."""

from __future__ import annotations

from typing import TYPE_CHECKING, Self

import httpx
import structlog
from pydantic import ValidationError

from execution_plane.callback_client import CallbackHTTPClient
from execution_plane.credential_resolution.errors import (
    CREDENTIAL_RESOLUTION_FAILED,
    CredentialResolutionUnavailableError,
)
from execution_plane.logging.command_payload import redacted_command_payload
from execution_plane_client.commands.results import TransportReceipt

logger = structlog.stdlib.get_logger(__name__)


if TYPE_CHECKING:
    from execution_plane.config.settings import Settings
    from execution_plane_client.protocol import MessageEnvelope


class CredentialResolutionClient:
    """Own the callback HTTP client for one credential-resolution scope."""

    def __init__(self, settings: Settings, client: CallbackHTTPClient | None = None) -> None:
        """Use EP's callback identity and a reusable mTLS HTTP client."""
        self._client = client or CallbackHTTPClient(
            url=settings.credential_resolution_callback_url,
            ca_cert_path=settings.credential_resolution_callback_ca_cert_path,
            cert_path=settings.credential_resolution_callback_cert_path,
            key_path=settings.credential_resolution_callback_key_path,
            timeout_seconds=settings.credential_resolution_callback_timeout_seconds,
        )

    @property
    def configured(self) -> bool:
        """Whether callback delivery is configured."""
        return self._client.configured

    async def post(self, envelope: MessageEnvelope) -> TransportReceipt:
        """Post one command and validate its transport receipt."""
        if not self.configured:
            message = "Credential-resolution callback is not configured"
            raise CredentialResolutionUnavailableError(message)
        try:
            logger.info(
                "Credential resolution command sent",
                command_name=envelope.command.command_type,
                command_revision=envelope.command.command_revision,
                message_id=str(envelope.message_id),
                destination_id=envelope.destination_id,
                payload=redacted_command_payload(envelope.command.payload),
            )
            response = await self._client.post_json(envelope.model_dump(mode="json"))
            response.raise_for_status()
            receipt = TransportReceipt.model_validate(response.json())
            logger.info(
                "Credential resolution command receipt received",
                command_name=envelope.command.command_type,
                message_id=str(envelope.message_id),
            )
            return receipt
        except (httpx.HTTPError, ValidationError, ValueError) as exc:
            logger.warning(
                "Credential resolution command delivery failed",
                command_name=envelope.command.command_type,
                message_id=str(envelope.message_id),
                error_type=type(exc).__name__,
            )
            raise CredentialResolutionUnavailableError(CREDENTIAL_RESOLUTION_FAILED) from exc

    async def close(self) -> None:
        """Close the underlying callback client."""
        await self._client.close()

    async def __aenter__(self) -> Self:
        """Return the client for an explicit ownership scope."""
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        """Close the callback client when its ownership scope exits."""
        await self.close()
