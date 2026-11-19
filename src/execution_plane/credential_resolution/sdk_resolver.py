"""Credential resolution for SDK container invocations."""

import asyncio
import concurrent.futures
import threading
from collections.abc import Mapping, Sequence
from enum import StrEnum
from typing import Any
from uuid import UUID

from syntara_plugin.runtime import CredentialAccessError, CredentialRequestContext

from execution_plane.credential_resolution.errors import CredentialResolutionError
from execution_plane.credential_resolution.workload import WorkloadCredentialResolver
from execution_plane_client.commands.work import WorkCredentialBinding

_POLL_INTERVAL_SECONDS = 0.05


class CredentialFailureReason(StrEnum):
    """Why an SDK credential request could not be satisfied."""

    MISSING = "missing"
    EXPIRED = "expired"
    UNAVAILABLE = "unavailable"


def _credential_error(reason: CredentialFailureReason) -> CredentialAccessError:
    """Raise-typed bridge from the enum onto the SDK's Literal reason set."""
    return CredentialAccessError(reason.value)


class SDKCredentialResolver:
    """Bridge synchronous SDK credential requests to AO's async callback path."""

    def __init__(
        self,
        credential_bindings: Sequence[WorkCredentialBinding],
        credential_resolver: WorkloadCredentialResolver,
        *,
        loop: asyncio.AbstractEventLoop,
        work_item_id: UUID,
        attempt_id: str,
        invocation_id: str,
        step_revision_id: str,
        max_concurrent_reads: int = 4,
    ) -> None:
        """Bind resolution to one workload attempt and bound concurrent reads."""
        if max_concurrent_reads < 1:
            message = "max_concurrent_reads must be positive"
            raise ValueError(message)
        self._credential_bindings: Mapping[str, UUID] = {
            binding.credential_recipe_id: binding.credential_ref for binding in credential_bindings
        }
        self._credential_resolver = credential_resolver
        self._loop = loop
        self._work_item_id = work_item_id
        self._attempt_id = attempt_id
        self._invocation_id = invocation_id
        self._step_revision_id = step_revision_id
        self._read_slots = threading.BoundedSemaphore(max_concurrent_reads)

    def __call__(self, context: CredentialRequestContext) -> Mapping[str, Any]:
        """Resolve one invocation-scoped credential request without leaking details."""
        credential_ref = self._credential_bindings.get(context.name)
        if credential_ref is None:
            raise _credential_error(CredentialFailureReason.MISSING)
        if context.invocation_id != self._invocation_id or context.cancelled:
            raise _credential_error(CredentialFailureReason.EXPIRED)

        if not self._acquire_read_slot(context):
            raise _credential_error(CredentialFailureReason.EXPIRED)
        try:
            if context.cancelled:
                raise _credential_error(CredentialFailureReason.EXPIRED)
            future = self._dispatch(credential_ref, context.name)
            credential = self._wait_for_result(future, context)
        finally:
            self._read_slots.release()
        if credential is None:
            raise _credential_error(CredentialFailureReason.EXPIRED)
        return credential

    def _acquire_read_slot(self, context: CredentialRequestContext) -> bool:
        while True:
            timeout = self._poll_timeout(context)
            if timeout <= 0 or context.cancelled:
                return False
            if self._read_slots.acquire(timeout=timeout):
                return True

    @staticmethod
    def _poll_timeout(context: CredentialRequestContext) -> float:
        return min(_POLL_INTERVAL_SECONDS, context.remaining_seconds)

    def _dispatch(self, credential_ref: UUID, recipe_id: str) -> concurrent.futures.Future[Mapping[str, Any]]:
        return asyncio.run_coroutine_threadsafe(
            self._credential_resolver.resolve_workload(
                credential_ref,
                recipe_id,
                work_item_id=self._work_item_id,
                attempt_id=self._attempt_id,
                step_revision_id=self._step_revision_id,
            ),
            self._loop,
        )

    def _wait_for_result(
        self,
        future: concurrent.futures.Future[Mapping[str, Any]],
        context: CredentialRequestContext,
    ) -> Mapping[str, Any] | None:
        while not future.done():
            if context.cancelled:
                future.cancel()
                return None
            timeout = self._poll_timeout(context)
            if timeout <= 0:
                future.cancel()
                return None
            concurrent.futures.wait({future}, timeout=timeout)
        if context.cancelled:
            return None
        try:
            credential = future.result()
        except (asyncio.CancelledError, CredentialResolutionError):
            raise _credential_error(CredentialFailureReason.UNAVAILABLE) from None
        if not credential:
            raise _credential_error(CredentialFailureReason.UNAVAILABLE)
        return credential
