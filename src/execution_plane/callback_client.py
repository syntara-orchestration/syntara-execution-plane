"""Reusable mTLS HTTP client for AO callbacks."""

from __future__ import annotations

import ssl
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from collections.abc import Mapping


class CallbackHTTPClient:
    """Post JSON commands to a fixed operator-configured callback URL."""

    def __init__(
        self,
        *,
        url: str | None,
        ca_cert_path: str | None = None,
        cert_path: str | None = None,
        key_path: str | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        """Build an mTLS client only when a callback URL is configured."""
        self._url = url
        self._client: httpx.AsyncClient | None = None
        if url:
            ssl_context = ssl.create_default_context(cafile=ca_cert_path)
            if cert_path and key_path:
                ssl_context.load_cert_chain(cert_path, key_path)
            self._client = httpx.AsyncClient(timeout=timeout_seconds, verify=ssl_context)

    @property
    def configured(self) -> bool:
        """Return whether a callback URL and client are available."""
        return self._client is not None and self._url is not None

    async def post_json(self, body: Mapping[str, object], *, timeout_seconds: float | None = None) -> httpx.Response:
        """Post one JSON body to the configured callback URL."""
        if self._client is None or self._url is None:
            message = "Callback client is not configured"
            raise RuntimeError(message)
        if timeout_seconds is None:
            return await self._client.post(self._url, json=dict(body))
        return await self._client.post(self._url, json=dict(body), timeout=timeout_seconds)

    async def close(self) -> None:
        """Close the reusable HTTP connection pool."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
