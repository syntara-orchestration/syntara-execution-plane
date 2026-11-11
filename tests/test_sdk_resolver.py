"""Tests for the SDK credential resolver."""

import time
from threading import Event
from uuid import uuid4

import pytest
from syntara_plugin.runtime import CredentialAccessError, CredentialRequestContext

from execution_plane.credential_resolution.sdk_resolver import CredentialResolver
from execution_plane_client.commands.work import WorkCredentialBinding


def test_credential_resolver_rejects_unbound_recipe_ids_as_missing() -> None:
    resolver = CredentialResolver(
        [
            WorkCredentialBinding(
                credential_recipe_id="acme.echo.api-token",
                credential_ref=uuid4(),
            )
        ]
    )
    context = CredentialRequestContext(
        invocation_id="work-1-attempt-2",
        name="acme.echo.workspace-token",
        cancellation=Event(),
        deadline=time.monotonic() + 1,
    )

    with pytest.raises(CredentialAccessError) as error:
        resolver(context)

    assert error.value.reason == "missing"


def test_credential_resolver_rejects_bound_recipes_as_unavailable_until_resolution_is_implemented() -> None:
    resolver = CredentialResolver(
        [
            WorkCredentialBinding(
                credential_recipe_id="acme.echo.api-token",
                credential_ref=uuid4(),
            )
        ]
    )
    context = CredentialRequestContext(
        invocation_id="work-1-attempt-2",
        name="acme.echo.api-token",
        cancellation=Event(),
        deadline=time.monotonic() + 1,
    )

    with pytest.raises(CredentialAccessError) as error:
        resolver(context)

    assert error.value.reason == "unavailable"
