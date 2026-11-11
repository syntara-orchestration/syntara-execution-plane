"""Tests for the SDK credential resolver."""

import time
from threading import Event

import pytest
from syntara_plugin.runtime import CredentialAccessError, CredentialRequestContext

from execution_plane.credential_resolution.sdk_resolver import CredentialResolver


def test_credential_resolver_safely_rejects_requests() -> None:
    resolver = CredentialResolver()
    context = CredentialRequestContext(
        invocation_id="work-1-attempt-2",
        name="database",
        cancellation=Event(),
        deadline=time.monotonic() + 1,
    )

    with pytest.raises(CredentialAccessError) as error:
        resolver(context)

    assert error.value.reason == "unavailable"
