"""Workload credential resolution through AO."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

import structlog

from execution_plane.credential_resolution.errors import (
    CREDENTIAL_RESOLUTION_MISMATCH,
    CredentialResolutionInvalidError,
    CredentialResolutionMismatchError,
)
from execution_plane_client.commands.credentials import CredentialResolveWorkload
from execution_plane_client.commands.results import CredentialResolveWorkloadCommandResult

if TYPE_CHECKING:
    from collections.abc import Mapping

    from execution_plane.config.settings import Settings
    from execution_plane.credential_resolution.client import CredentialResolutionClient


logger = structlog.stdlib.get_logger(__name__)


class WorkloadCredentialResolver:
    """Construct and dispatch a typed `credential.resolve.workload` command."""

    def __init__(self, settings: Settings, client: CredentialResolutionClient) -> None:
        """Bind one resolver to the shared credential-resolution client."""
        self._settings = settings
        self._client = client

    async def resolve_workload(
        self,
        credential_ref: UUID,
        credential_recipe_id: str,
        *,
        work_item_id: UUID,
        attempt_id: str,
        step_revision_id: str,
        runtime_context: Mapping[str, object],
    ) -> Mapping[str, str]:
        """Resolve one approved recipe for the work item that owns the attempt."""
        if credential_ref == UUID(int=0) or not work_item_id:
            message = "Invalid workload credential request"
            logger.warning(
                "Workload credential resolution rejected an invalid request",
                work_item_id=str(work_item_id),
                credential_recipe_id=credential_recipe_id,
            )
            raise CredentialResolutionInvalidError(message)
        command = CredentialResolveWorkload(
            work_item_id=work_item_id,
            attempt_id=attempt_id,
            runtime_context=dict(runtime_context),
            credential_ref=credential_ref,
            credential_recipe_id=credential_recipe_id,
            step_revision_id=step_revision_id,
        )
        envelope = command.to_message(
            source_id=self._settings.service_jwt_audience,
            destination_id=self._settings.ao_client_id,
            client_id=self._settings.ao_client_id,
        )
        receipt = await self._client.post(envelope)
        result = receipt.result
        if (
            receipt.message_id != envelope.message_id
            or not isinstance(result, CredentialResolveWorkloadCommandResult)
            or receipt.resource is None
            or receipt.resource.resource_id != work_item_id
        ):
            logger.warning(
                "Workload credential resolution receipt did not match its command",
                command_name=envelope.command.command_type,
                message_id=str(envelope.message_id),
                work_item_id=str(work_item_id),
            )
            raise CredentialResolutionMismatchError(CREDENTIAL_RESOLUTION_MISMATCH)
        logger.info(
            "Workload credential resolved",
            command_name=envelope.command.command_type,
            message_id=str(envelope.message_id),
            work_item_id=str(work_item_id),
            credential_recipe_id=credential_recipe_id,
        )
        return result.credential
