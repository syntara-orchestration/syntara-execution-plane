"""Durable outbound delivery loop for EP work-item completions."""

from __future__ import annotations

import asyncio
import secrets
from typing import TYPE_CHECKING

import httpx
import structlog

from execution_plane.callback_client import CallbackHTTPClient
from execution_plane.models.work_item import WorkItemStatus
from execution_plane_client.commands.control import Completion
from execution_plane_client.protocol import ResourceReference, TransportReceipt

if TYPE_CHECKING:
    from execution_plane.config import EPSettings
    from execution_plane.models.work_item import WorkItem
    from execution_plane.work_item.work_item_store import WorkStore

logger = structlog.stdlib.get_logger(__name__)

DELIVERY_POLL_SECONDS = 2
MAX_RETRY_SECONDS = 300


class CompletionNotifier:
    """Send committed work-item completions to a fixed, operator-configured AO URL."""

    def __init__(self, settings: EPSettings) -> None:
        """Prepare the callback client from EP-owned identity and endpoint settings."""
        self._local_id = settings.service_jwt_audience
        self._client = CallbackHTTPClient(
            url=settings.completion_callback_url,
            ca_cert_path=settings.completion_callback_ca_cert_path,
            cert_path=settings.completion_callback_cert_path,
            key_path=settings.completion_callback_key_path,
            timeout_seconds=settings.completion_callback_timeout_seconds,
        )

    async def close(self) -> None:
        """Close the reusable HTTP connection pool."""
        await self._client.close()

    async def run(self, work_store: WorkStore) -> None:
        """Continuously retry due work-item completion notifications; callback failure never loses results."""
        while True:
            items = await work_store.claim_work_items_that_need_completion_notification()
            if not items:
                await asyncio.sleep(DELIVERY_POLL_SECONDS)
                continue
            for item in items:
                await self._deliver_one(work_store, item)

    async def _deliver_one(self, work_store: WorkStore, item: WorkItem) -> None:
        if not self._client.configured:
            logger.warning(
                "Completion callback is not configured; retaining completion notification",
                work_item_id=str(item.id),
            )
            await work_store.record_completion_notification_attempt(
                item.id,
                claim_owner_id=item.completion_notification_claim_owner_id,
                error="completion callback is not configured",
                retry_after_seconds=MAX_RETRY_SECONDS,
            )
            return

        statuses = {
            WorkItemStatus.COMPLETED: "succeeded",
            WorkItemStatus.FAILED: "failed",
            WorkItemStatus.CANCELLED: "cancelled",
            WorkItemStatus.RECONCILIATION_REQUIRED: "outcome_unknown",
        }
        completion = Completion.model_validate(
            {
                "status": statuses[WorkItemStatus(item.status)],
                "result": item.result,
                "resource": ResourceReference(kind="work_item", resource_id=item.id).model_dump(mode="json"),
            }
        )
        body = completion.to_message(
            source_id=self._local_id,
            destination_id=item.client_id,
            client_id=item.client_id,
            command_id=item.completion_command_id,
            parent_command_id=item.id,
            occurred_at=item.completed_at or item.created_at,
        )
        try:
            response = await self._client.post_json(body.model_dump(mode="json"))
            response.raise_for_status()
            receipt = TransportReceipt.model_validate(response.json())
            if receipt.message_id != body.message_id:
                error = "Completion receipt does not match its message"
                raise ValueError(error)  # noqa: TRY301 - A mismatched receipt follows the same retry path.
        except (httpx.HTTPError, RuntimeError, ValueError) as exc:
            retry_after = min(MAX_RETRY_SECONDS, 2 ** min(item.completion_notification_attempts + 1, 8))
            retry_after = int(retry_after * secrets.SystemRandom().uniform(0.8, 1.2))
            await work_store.record_completion_notification_attempt(
                item.id,
                claim_owner_id=item.completion_notification_claim_owner_id,
                error=type(exc).__name__,
                retry_after_seconds=retry_after,
            )
            logger.warning(
                "Completion notification delivery failed; scheduled retry",
                work_item_id=str(item.id),
                attempt=item.completion_notification_attempts + 1,
                retry_after_seconds=retry_after,
                error=type(exc).__name__,
            )
        else:
            await work_store.record_completion_notification_delivery(
                item.id,
                claim_owner_id=item.completion_notification_claim_owner_id,
            )
            logger.info("Completion notification acknowledged by client", work_item_id=str(item.id))
