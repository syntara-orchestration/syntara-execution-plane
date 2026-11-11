"""Credential resolution for SDK container invocations."""

from collections.abc import Mapping
from typing import Any, Literal

from syntara_plugin.runtime import CredentialAccessError, CredentialRequestContext


class CredentialResolver:
    """Reject credential requests until host-side resolution is implemented."""

    def __call__(self, _context: CredentialRequestContext) -> Mapping[str, Any]:
        """Reject one invocation-scoped credential request."""
        reason: Literal["unavailable"] = "unavailable"
        raise CredentialAccessError(reason)
