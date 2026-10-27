"""Tests for durable completion notification delivery configuration."""

from __future__ import annotations

import asyncio
import ssl
from unittest.mock import AsyncMock, MagicMock

import pytest

from execution_plane import callback_client as callback_client_module
from execution_plane.callback_client import CallbackHTTPClient
from execution_plane.completion_notifier import notifier as event_delivery
from execution_plane.config import EPSettings


def test_completion_notification_client_loads_identity_into_ca_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """The completion notification TLS context must include the trusted CA and EP identity."""
    context = MagicMock(spec=ssl.SSLContext)
    context_factory = MagicMock(return_value=context)
    client_factory = MagicMock()
    monkeypatch.setattr(callback_client_module.ssl, "create_default_context", context_factory)
    monkeypatch.setattr(callback_client_module.httpx, "AsyncClient", client_factory)
    settings = EPSettings(
        EP_DATABASE_URL="postgresql+asyncpg://user:password@localhost/test",
        EP_COMPLETION_CALLBACK_URL="https://ao.example.test/api/execution_plane/v1/commands",
        EP_CALLBACK_CA_CERT_PATH="/secrets/ca.pem",
        EP_CALLBACK_CERT_PATH="/secrets/ep.crt",
        EP_CALLBACK_KEY_PATH="/secrets/ep.key",
    )

    CallbackHTTPClient(
        url=settings.completion_callback_url,
        ca_cert_path=settings.completion_callback_ca_cert_path,
        cert_path=settings.completion_callback_cert_path,
        key_path=settings.completion_callback_key_path,
    )

    context_factory.assert_called_once_with(cafile="/secrets/ca.pem")
    context.load_cert_chain.assert_called_once_with("/secrets/ep.crt", "/secrets/ep.key")
    assert client_factory.call_args.kwargs["verify"] is context
    assert "cert" not in client_factory.call_args.kwargs


async def test_completion_is_a_stable_command_rebuilt_from_work_item():
    """Retries reuse the command identity and correlate directly with the work UUID."""
    from datetime import UTC, datetime
    from uuid import uuid4

    import httpx

    from execution_plane.models.work_item import CompletionNotificationState, WorkItem, WorkItemStatus
    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.protocol import MessageEnvelope

    bodies = []

    def receive(request) -> httpx.Response:
        envelope = MessageEnvelope.model_validate_json(request.content)
        CommandCatalog().validate(envelope)
        bodies.append(envelope)
        return httpx.Response(202, json={"received": True, "message_id": str(envelope.message_id)})

    settings = EPSettings(EP_COMPLETION_CALLBACK_URL="https://ao.test/v1/commands")
    delivery = event_delivery.CompletionNotifier(settings)
    await delivery.close()
    callback_client = CallbackHTTPClient(url=settings.completion_callback_url)
    await callback_client.close()
    callback_client._client = httpx.AsyncClient(transport=httpx.MockTransport(receive))
    delivery._client = callback_client
    claim_owner_id = uuid4()
    item = WorkItem(
        id=uuid4(),
        client_id="ao",
        status=WorkItemStatus.COMPLETED,
        result={"stdout": "done"},
        created_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        completion_command_id=uuid4(),
        completion_notification_state=CompletionNotificationState.DELIVERING,
        completion_notification_next_attempt_at=datetime.now(UTC),
        completion_notification_claim_owner_id=claim_owner_id,
    )
    service = AsyncMock()
    try:
        await delivery._deliver_one(service, item)
        await delivery._deliver_one(service, item)
    finally:
        await delivery.close()
    assert len(bodies) == 2
    assert bodies[0] == bodies[1]
    assert bodies[0].command.command_type == "work.execute.completed"
    assert bodies[0].command.parent_command_id == item.id
    assert bodies[0].command.command_id == item.completion_command_id
    assert bodies[0].command.payload["status"] == "succeeded"
    assert bodies[0].command.payload["result"] == {"stdout": "done"}
    assert service.record_completion_notification_delivery.await_args_list[0].kwargs["claim_owner_id"] == claim_owner_id


async def test_wrong_completion_receipt_is_retried():
    from datetime import UTC, datetime
    from uuid import uuid4

    import httpx

    from execution_plane.models.work_item import CompletionNotificationState, WorkItem, WorkItemStatus

    delivery = event_delivery.CompletionNotifier(EPSettings(EP_COMPLETION_CALLBACK_URL="https://ao.test/v1/commands"))
    await delivery.close()
    callback_client = CallbackHTTPClient(url="https://ao.test/v1/commands")
    await callback_client.close()
    callback_client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(202, json={"received": True, "message_id": str(uuid4())})
        )
    )
    delivery._client = callback_client
    item = WorkItem(
        id=uuid4(),
        client_id="ao",
        status=WorkItemStatus.FAILED,
        result={},
        created_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        completion_command_id=uuid4(),
        completion_notification_state=CompletionNotificationState.DELIVERING,
        completion_notification_next_attempt_at=datetime.now(UTC),
        completion_notification_claim_owner_id=uuid4(),
    )
    service = AsyncMock()
    try:
        await delivery._deliver_one(service, item)
    finally:
        await delivery.close()
    assert service.record_completion_notification_attempt.call_args.kwargs["error"] == "ValueError"


async def test_unconfigured_completion_notification_is_retained():
    """A missing notification endpoint must retain the completion instead of dropping it."""
    from datetime import UTC, datetime
    from uuid import uuid4

    from execution_plane.models.work_item import CompletionNotificationState, WorkItem, WorkItemStatus

    delivery = event_delivery.CompletionNotifier(
        EPSettings(EP_DATABASE_URL="postgresql+asyncpg://user:password@localhost/test")
    )
    item = WorkItem(
        id=uuid4(),
        client_id="ao",
        status=WorkItemStatus.CANCELLED,
        result={},
        created_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        completion_command_id=uuid4(),
        completion_notification_state=CompletionNotificationState.DELIVERING,
        completion_notification_next_attempt_at=datetime.now(UTC),
        completion_notification_claim_owner_id=uuid4(),
    )
    service = AsyncMock()
    try:
        await delivery._deliver_one(service, item)
    finally:
        await delivery.close()

    service.record_completion_notification_attempt.assert_awaited_once_with(
        item.id,
        claim_owner_id=item.completion_notification_claim_owner_id,
        error="completion notification endpoint is not configured",
        retry_after_seconds=event_delivery.MAX_RETRY_SECONDS,
    )


async def test_run_processes_claimed_items_and_sleeps_when_idle(monkeypatch: pytest.MonkeyPatch):
    """The run loop should deliver claimed work and wait when the backlog is empty."""
    from datetime import UTC, datetime
    from uuid import uuid4

    from execution_plane.models.work_item import CompletionNotificationState, WorkItem, WorkItemStatus

    monkeypatch.setattr(event_delivery, "DELIVERY_POLL_SECONDS", 0)
    delivery = event_delivery.CompletionNotifier(
        EPSettings(
            EP_DATABASE_URL="postgresql+asyncpg://user:password@localhost/test",
            EP_COMPLETION_CALLBACK_URL="https://ao.test/v1/commands",
        )
    )
    deliver_one = AsyncMock()
    monkeypatch.setattr(delivery, "_deliver_one", deliver_one)
    item = WorkItem(
        id=uuid4(),
        client_id="ao",
        status=WorkItemStatus.RECONCILIATION_REQUIRED,
        result={},
        created_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        completion_command_id=uuid4(),
        completion_notification_state=CompletionNotificationState.DELIVERING,
        completion_notification_next_attempt_at=datetime.now(UTC),
        completion_notification_claim_owner_id=uuid4(),
    )
    service = AsyncMock()
    service.claim_work_items_that_need_completion_notification.side_effect = [[item], [], asyncio.CancelledError()]
    try:
        with pytest.raises(asyncio.CancelledError):
            await delivery.run(service)
    finally:
        await delivery.close()

    deliver_one.assert_awaited_once_with(service, item)
    assert service.claim_work_items_that_need_completion_notification.await_count == 3
