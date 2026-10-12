"""Tests for durable completion callback delivery configuration."""

from __future__ import annotations

import ssl
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from execution_plane import event_delivery
from execution_plane.config import EPSettings

if TYPE_CHECKING:
    import pytest


def test_callback_client_loads_client_identity_into_ca_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """The callback TLS context must include both the trusted CA and EP client identity."""
    context = MagicMock(spec=ssl.SSLContext)
    context_factory = MagicMock(return_value=context)
    client_factory = MagicMock()
    monkeypatch.setattr(event_delivery.ssl, "create_default_context", context_factory)
    monkeypatch.setattr(event_delivery.httpx, "AsyncClient", client_factory)
    settings = EPSettings(
        EP_DATABASE_URL="postgresql+asyncpg://user:password@localhost/test",
        EP_COMPLETION_CALLBACK_URL="https://ao.example.test/events",
        EP_CALLBACK_CA_CERT_PATH="/secrets/ca.pem",
        EP_CALLBACK_CERT_PATH="/secrets/ep.crt",
        EP_CALLBACK_KEY_PATH="/secrets/ep.key",
    )

    event_delivery.CompletionEventDelivery(settings)

    context_factory.assert_called_once_with(cafile="/secrets/ca.pem")
    context.load_cert_chain.assert_called_once_with("/secrets/ep.crt", "/secrets/ep.key")
    assert client_factory.call_args.kwargs["verify"] is context
    assert "cert" not in client_factory.call_args.kwargs
