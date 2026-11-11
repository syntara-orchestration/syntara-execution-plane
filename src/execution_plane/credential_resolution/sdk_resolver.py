"""Credential resolution for SDK container invocations."""

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal

from syntara_plugin.runtime import CredentialAccessError, CredentialRequestContext

from execution_plane_client.commands.work import WorkCredentialBinding

if TYPE_CHECKING:
    from uuid import UUID


class CredentialResolver:
    """Reject credential requests until host-side resolution is implemented."""

    def __init__(self, credential_bindings: Sequence[WorkCredentialBinding] = ()) -> None:
        """Index approved recipe IDs without resolving their credential references."""
        self._credential_bindings: Mapping[str, UUID] = {
            binding.credential_recipe_id: binding.credential_ref for binding in credential_bindings
        }

    def __call__(self, _context: CredentialRequestContext) -> Mapping[str, Any]:
        """Reject one invocation-scoped credential request."""
        reason: Literal["missing", "unavailable"] = (
            "missing" if _context.name not in self._credential_bindings else "unavailable"
        )
        raise CredentialAccessError(reason)
