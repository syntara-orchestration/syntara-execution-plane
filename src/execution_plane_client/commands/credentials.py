"""Credential-reference resolution commands."""

from __future__ import annotations

from typing import ClassVar
from uuid import UUID  # noqa: TC003 - Pydantic evaluates fields.

from execution_plane_client.commands.base import CommandPayload


class CredentialResolve(CommandPayload):
    """Resolve an opaque credential reference into a short-lived value."""

    command_type: ClassVar[str] = "credential.resolve"
    credential_ref: UUID
