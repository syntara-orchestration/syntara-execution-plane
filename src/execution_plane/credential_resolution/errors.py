"""Credential-resolution failures shared by command resolvers."""

from __future__ import annotations

CREDENTIAL_RESOLUTION_FAILED = "Credential resolution failed"
CREDENTIAL_RESOLUTION_MISMATCH = "Credential resolution does not match its command"


class CredentialResolutionError(RuntimeError):
    """A credential could not be resolved before workload execution."""


class CredentialResolutionUnavailableError(CredentialResolutionError):
    """The credential-resolution callback is not configured or reachable."""


class CredentialResolutionMismatchError(CredentialResolutionError):
    """AO returned a credential for a different command."""


class CredentialResolutionInvalidError(CredentialResolutionError):
    """AO returned an invalid credential-resolution response."""
