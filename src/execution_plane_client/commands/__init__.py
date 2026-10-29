"""Shared resource identities and HTTPS configuration validation."""

from urllib.parse import urlsplit
from uuid import UUID

from execution_plane_client.protocol import WireModel


class ResourceIdentity(WireModel):
    """Caller-supplied physical resource identity."""

    resource_id: UUID


def validate_endpoint(value: str) -> str:
    """Require HTTPS without embedded credentials, queries or fragments."""
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or any(char.isspace() for char in value)
    ):
        message = "Endpoint must be an HTTPS URL without userinfo, query or fragment"
        raise ValueError(message)
    _ = parsed.port
    return value
