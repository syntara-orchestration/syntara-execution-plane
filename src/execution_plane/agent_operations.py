"""Route SDK agent operations to the authenticated AO callback service."""

# Callback failures are normalized to safe workload errors at this boundary.
# ruff: noqa: TRY003, EM101

from __future__ import annotations

import asyncio
from concurrent.futures import TimeoutError as FutureTimeoutError
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, cast

import httpx
from syntara_plugin.runtime import HostOperationError

from execution_plane.callback_client import CallbackHTTPClient
from execution_plane_client.commands.agent import AgentOperation, AgentOperationCommandResult, AgentOperationName
from execution_plane_client.commands.results import TransportReceipt

if TYPE_CHECKING:
    from uuid import UUID

    from syntara_plugin.runtime.container import HostOperationRequestContext

    from execution_plane.config.settings import Settings
    from execution_plane_client.protocol import MessageEnvelope


class AgentOperationResolver:
    """Resolve only the host operations granted on one work invocation."""

    def __init__(
        self,
        settings: Settings,
        *,
        loop: asyncio.AbstractEventLoop,
        work_item_id: UUID,
        attempt_id: str,
        step_revision_id: str,
        invocation_id: UUID | None,
        allowed_operations: frozenset[str],
    ) -> None:
        """Bind callback identity and operation grants to one leased attempt."""
        self._settings = settings
        self._loop = loop
        self._work_item_id = work_item_id
        self._attempt_id = attempt_id
        self._step_revision_id = step_revision_id
        self._invocation_id = invocation_id
        self._allowed_operations = allowed_operations

    def __call__(self, context: HostOperationRequestContext) -> dict[str, Any]:
        """Post a workload request to AO and validate its correlated receipt."""
        if context.operation not in self._allowed_operations:
            raise HostOperationError("permission_denied", "Host operation was not granted.")
        if self._invocation_id is None:
            raise HostOperationError("invalid_request", "Agent invocation identity is missing.")
        if context.cancelled:
            raise HostOperationError("deadline_exceeded", "Host operation deadline expired.")

        request_id = context.request_id
        operation = cast("AgentOperationName", context.operation)
        envelope = AgentOperation(
            work_item_id=self._work_item_id,
            agent_invocation_id=self._invocation_id,
            attempt_id=self._attempt_id,
            step_revision_id=self._step_revision_id,
            operation=operation,
            request_id=request_id,
            payload=dict(context.payload),
        ).to_message(
            source_id=self._settings.service_jwt_audience,
            destination_id=self._settings.ao_client_id,
            client_id=self._settings.ao_client_id,
        )
        future = asyncio.run_coroutine_threadsafe(
            self._post(envelope, context.remaining_seconds, request_id), self._loop
        )
        try:
            while True:
                if context.cancelled:
                    future.cancel()
                    raise HostOperationError(  # noqa: TRY301 -- caller cancellation leaves the remote result unknown.
                        "outcome_unknown",
                        "The workload ended before AO confirmed the host operation.",
                        outcome_unknown=True,
                    )
                try:
                    return future.result(timeout=min(0.1, context.remaining_seconds))
                except FutureTimeoutError:
                    continue
        except HostOperationError:
            raise
        except Exception:  # noqa: BLE001 -- translate every callback transport failure to an unknown outcome.
            raise HostOperationError(
                "outcome_unknown",
                "AO host operation outcome is unknown.",
                outcome_unknown=True,
            ) from None

    async def _post(self, envelope: MessageEnvelope, timeout_seconds: float, request_id: str) -> dict[str, Any]:
        settings = self._settings
        client = CallbackHTTPClient(
            url=settings.credential_resolution_callback_url,
            ca_cert_path=settings.credential_resolution_callback_ca_cert_path,
            cert_path=settings.credential_resolution_callback_cert_path,
            key_path=settings.credential_resolution_callback_key_path,
            timeout_seconds=max(0.1, timeout_seconds),
        )
        if not client.configured:
            raise HostOperationError("upstream_failed", "AO callback is not configured.")
        try:
            response = await client.post_json(
                envelope.model_dump(mode="json"),
                timeout_seconds=max(0.1, timeout_seconds),
            )
            response.raise_for_status()
            receipt = TransportReceipt.model_validate(response.json())
            result = receipt.result
            if (
                receipt.message_id != envelope.message_id
                or not isinstance(result, AgentOperationCommandResult)
                or result.request_id != request_id
            ):
                raise ValueError("AO host operation receipt did not match the request")
            return result.result
        except httpx.HTTPStatusError as error:
            status_code = error.response.status_code
            if HTTPStatus.BAD_REQUEST <= status_code < HTTPStatus.INTERNAL_SERVER_ERROR:
                raise HostOperationError(
                    "permission_denied" if status_code in {401, 403, 404, 409} else "invalid_request",
                    "AO rejected the host operation.",
                ) from None
            raise HostOperationError(
                "outcome_unknown",
                "AO could not confirm the host operation outcome.",
                outcome_unknown=True,
            ) from None
        finally:
            await client.close()
