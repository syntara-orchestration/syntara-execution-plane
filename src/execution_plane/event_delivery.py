"""Durable outbound delivery loop for EP completion events."""

from __future__ import annotations

import asyncio
import secrets
from typing import TYPE_CHECKING

import httpx
import structlog

from execution_plane.api.schemas import CompletionEventRequest
from execution_plane.models.work_item import WorkItemStatus

if TYPE_CHECKING:
    from execution_plane.config import EPSettings
    from execution_plane.models.completion_event import CompletionEvent
    from execution_plane.work_store import WorkStore

logger = structlog.stdlib.get_logger(__name__)

DELIVERY_POLL_SECONDS = 2
MAX_RETRY_SECONDS = 300


class CompletionEventDelivery:
    """Send committed completion events to a fixed, operator-configured AO URL."""

    def __init__(self, settings: EPSettings) -> None:
        """Prepare the callback client from EP-owned identity and endpoint settings."""
        self._url = settings.completion_callback_url
        self._client: httpx.AsyncClient | None = None
        if self._url:
            cert = None
            if settings.completion_callback_cert_path and settings.completion_callback_key_path:
                cert = (settings.completion_callback_cert_path, settings.completion_callback_key_path)
            verify: bool | str = settings.completion_callback_ca_cert_path or True
            self._client = httpx.AsyncClient(
                timeout=settings.completion_callback_timeout_seconds,
                verify=verify,
                cert=cert,
            )

    async def close(self) -> None:
        """Close the reusable HTTP connection pool."""
        if self._client is not None:
            await self._client.aclose()

    async def run(self, store: WorkStore) -> None:
        """Continuously retry due outbox records; callback failure never loses results."""
        while True:
            events = await store.get_pending_completion_events()
            if not events:
                await asyncio.sleep(DELIVERY_POLL_SECONDS)
                continue
            for event in events:
                await self._deliver_one(store, event)

    async def _deliver_one(self, store: WorkStore, event: CompletionEvent) -> None:
        if self._client is None or self._url is None:
            logger.warning("Completion callback is not configured; retaining event", event_id=str(event.id))
            await store.record_completion_event_attempt(
                event.id,
                delivered=False,
                error="completion callback is not configured",
                retry_after_seconds=MAX_RETRY_SECONDS,
            )
            return

        body = CompletionEventRequest(
            event_id=event.id,
            client_id=event.client_id,
            project_id=event.project_id,
            work_id=event.work_item_id,
            request_id=event.request_id,
            state_revision=event.state_revision,
            status=WorkItemStatus(event.status),
            result=event.result,
            completed_at=event.created_at,
        )
        try:
            response = await self._client.post(self._url, json=body.model_dump(mode="json"))
            response.raise_for_status()
        except httpx.HTTPError as exc:
            retry_after = min(MAX_RETRY_SECONDS, 2 ** min(event.attempts + 1, 8))
            retry_after = int(retry_after * secrets.SystemRandom().uniform(0.8, 1.2))
            await store.record_completion_event_attempt(
                event.id,
                delivered=False,
                error=type(exc).__name__,
                retry_after_seconds=retry_after,
            )
            logger.warning(
                "Completion event delivery failed; scheduled retry",
                event_id=str(event.id),
                attempt=event.attempts + 1,
                retry_after_seconds=retry_after,
                error=type(exc).__name__,
            )
        else:
            await store.record_completion_event_attempt(event.id, delivered=True)
            logger.info("Completion event acknowledged by client", event_id=str(event.id))
