"""Cold-start Kubernetes WorkerManager: one isolated attempt resource over gRPC.

The manager is agnostic to the node type. The AO activity selects the container
image and builds the full invocation envelope; this backend allocates an
exclusive cold-start Job/Pod, streams the result over gRPC, and maps it to the
EP result contract. The WorkItem lifecycle does not depend on a Pod lifecycle.
No dependency on the main syntara package.
"""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, NoReturn

import structlog
from pydantic import ValidationError

from execution_plane.credential_resolution.resolver import CredentialResolutionError, CredentialResolver
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.models.work_item import WorkItemStatus
from execution_plane.worker_manager.base import (
    ExecutionStartPersistenceError,
    ExecutionStartRejectedError,
    WorkerManagerBase,
)
from execution_plane.worker_manager.vanilla_k8s.transport import TransportError, run_job
from execution_plane_client.commands.work import WorkExecute

if TYPE_CHECKING:
    from collections.abc import Callable

    from execution_plane.cluster.cluster_store import ClusterStore
    from execution_plane.config import EPSettings
    from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
    from execution_plane.models.cluster import Cluster
    from execution_plane.models.execution_target import ExecutionTarget
    from execution_plane.models.work_item import WorkItem
    from execution_plane.work_item.work_item_store import WorkStore

logger = structlog.stdlib.get_logger(__name__)

MAX_STATUS_CODE = 255
CLUSTER_CREDENTIAL_RESOLUTION_FAILED = "Cluster credential could not be resolved"


class RetryableDispatchError(Exception):
    """Transient transport/capacity failure — requeue the work item, do not fail it."""


class NodeExecutionError(Exception):
    """The node ran and returned a non-zero result — terminal for the activity."""

    def __init__(self, message: str, *, error_type: str, output: dict[str, Any] | None = None) -> None:
        """Carry the safe error classification and any partial output."""
        super().__init__(message)
        self.error_type = error_type
        self.output = output
        self.persisted = False


class WorkItemPayloadError(Exception):
    """The work item payload is missing required dispatch fields — terminal."""


class WorkloadOutcomeUnknownError(Exception):
    """The node may have run, but EP did not receive a durable terminal result."""


class WorkloadCancelledError(Exception):
    """Cancellation was confirmed and its relationship to Execute is retained."""

    def __init__(self, *, execution_started: bool) -> None:
        """Describe a confirmed cancellation and whether Execute was submitted."""
        self.execution_started = execution_started
        message = (
            "Node workload termination was confirmed after execution started"
            if execution_started
            else "Node workload termination was confirmed before Execute"
        )
        super().__init__(message)


@dataclass(frozen=True)
class _DispatchContext:
    """State shared by the callbacks used for one dispatch attempt."""

    work_item: WorkItem
    target_config: dict[str, Any]
    image: str
    invocation: dict[str, Any]
    output_config: dict[str, str] | None
    identity: str
    attempt_id: str
    cancelled: threading.Event
    loop: asyncio.AbstractEventLoop
    execution_start: Callable[[], None]


class VanillaK8sWorkerManager(WorkerManagerBase):
    """Dispatch a work item to a fresh Kubernetes pod and return its terminal result."""

    def __init__(
        self,
        target_credentials: ExecutionTargetStore,
        cluster_credentials: ClusterStore,
        work_progress: WorkStore,
        settings: EPSettings,
        credential_resolver: CredentialResolver | None = None,
    ) -> None:
        """Bind this backend to EP-owned stores and cold-start settings."""
        super().__init__(work_progress)
        self._target_credentials = target_credentials
        self._cluster_credentials = cluster_credentials
        self._settings = settings
        self._credential_resolver = credential_resolver or CredentialResolver(settings)

    async def _dispatch(self, work_item: WorkItem) -> dict[str, Any]:
        """Allocate the worker, stream its result, and persist the WorkItem outcome.

        Returns ``{"output": ...}`` on success. Raises:
        - ``RetryableDispatchError`` when the worker could not run (requeue with backoff),
        - ``NodeExecutionError`` when the node ran but failed (fail the activity),
        - ``WorkItemPayloadError`` when the payload is malformed (fail the activity).
        """
        context = await self._prepare_dispatch(work_item)
        frame = await self._run_job(context)
        return self._terminal_result(frame, context)

    async def _prepare_dispatch(self, work_item: WorkItem) -> _DispatchContext:
        """Validate and resolve all state needed by one dispatch attempt."""
        if work_item.execution_target_id is None:
            message = "Work item has no execution target assigned"
            raise RetryableDispatchError(message)
        target = await self._target_credentials.get_with_credentials(work_item.execution_target_id)
        if target is None or not target.enabled:
            message = "Assigned execution target is unavailable"
            raise RetryableDispatchError(message)

        try:
            command = WorkExecute.model_validate(work_item.payload or {})
        except ValidationError as exc:
            message = "Work item payload is not a valid work.execute command"
            raise WorkItemPayloadError(message) from exc
        image = command.image
        invocation = command.invocation.model_dump(mode="json")
        output_config = command.output_config
        cluster = await self._cluster_credentials.get_with_credentials(target.cluster_id)
        if cluster is None or not cluster.enabled:
            message = "Assigned cluster is unavailable"
            raise RetryableDispatchError(message)
        credential = await self._resolve_cluster_credential(cluster)

        identity = str(work_item.id)
        attempt_id = f"{identity}-{work_item.claim_generation}"
        target_config = self._k8s_target(target, cluster, credential)
        return _DispatchContext(
            work_item=work_item,
            target_config=target_config,
            image=image,
            invocation=invocation,
            output_config=output_config,
            identity=identity,
            attempt_id=attempt_id,
            cancelled=threading.Event(),
            loop=asyncio.get_running_loop(),
            execution_start=self.execution_start_callback(work_item),
        )

    async def _run_job(self, context: _DispatchContext) -> dict[str, Any]:
        """Run the transport while keeping its lease claim refreshed."""
        try:
            transport_task = asyncio.create_task(
                asyncio.to_thread(
                    run_job,
                    target=context.target_config,
                    image=context.image,
                    invocation=context.invocation,
                    identity=context.identity,
                    attempt_id=context.attempt_id,
                    startup=self._settings.node_startup_seconds,
                    grace=self._settings.node_grace_seconds,
                    cancelled=context.cancelled,
                    progress=partial(self._on_progress, context),
                    on_result=partial(self._persist_result_before_cleanup, context),
                    on_resource=partial(self._record_resource_state, context),
                    on_execute_submitted=partial(self._mark_execute_submitted, context),
                    node_selectors=context.target_config["node_selectors"],
                    tolerations=context.target_config["tolerations"],
                    allowed_egress_cidrs=self._settings.workload_allowed_egress_cidrs,
                    forbidden_egress_cidrs=self._settings.workload_forbidden_egress_cidrs,
                )
            )
            while not transport_task.done():
                done, _pending = await asyncio.wait({transport_task}, timeout=5)
                if not done:
                    await self._refresh_cancellation(context)
            return await transport_task
        except TransportError as exc:
            self._raise_transport_error(exc)

    async def _refresh_cancellation(self, context: _DispatchContext) -> None:
        """Request cancellation when the WorkItem claim reports cancellation or loss."""
        cancellation_requested = await self._work_progress.refresh_claim(
            context.work_item.id,
            claim_owner_id=context.work_item.claim_owner_id,
            claim_generation=context.work_item.claim_generation,
        )
        if cancellation_requested is not False:
            context.cancelled.set()

    @staticmethod
    def _raise_transport_error(exc: TransportError) -> NoReturn:
        """Translate a transport failure into its WorkItem activity error."""
        if exc.retryable:
            raise RetryableDispatchError(str(exc)) from None
        if exc.cancelled_confirmed:
            raise WorkloadCancelledError(execution_started=exc.submitted) from None
        if exc.submitted:
            raise WorkloadOutcomeUnknownError(str(exc)) from None
        raise NodeExecutionError(str(exc), error_type="NodeTransportError") from None

    def _terminal_result(self, frame: dict[str, Any], context: _DispatchContext) -> dict[str, Any]:
        """Map a successful transport frame and retain its persisted error state."""
        try:
            return _map_result(frame, context.output_config)
        except NodeExecutionError as exc:
            exc.persisted = True
            raise

    async def _persist_result(self, context: _DispatchContext, frame: dict[str, Any]) -> None:
        """Persist a terminal node result before worker cleanup."""
        try:
            result = _map_result(frame, context.output_config)
            status = WorkItemStatus.COMPLETED
        except NodeExecutionError as exc:
            result, status = self._terminal_execution_result(context, exc)
        await self._work_progress.set_result(
            context.work_item.id,
            result,
            status,
            claim_owner_id=context.work_item.claim_owner_id,
            claim_generation=context.work_item.claim_generation,
        )

    @staticmethod
    def _terminal_execution_result(
        context: _DispatchContext,
        exc: NodeExecutionError,
    ) -> tuple[dict[str, Any], WorkItemStatus]:
        """Build the result and status for a failed or cancelled node execution."""
        if context.cancelled.is_set() and exc.error_type == "CancelledError":
            result: dict[str, Any] = {"cancelled": True, "execution_started": True, "reason": str(exc)}
            status = WorkItemStatus.CANCELLED
        else:
            result = {"error": str(exc), "error_type": exc.error_type}
            status = WorkItemStatus.FAILED
        if exc.output is not None:
            result["output"] = exc.output
        return result, status

    def _persist_result_before_cleanup(self, context: _DispatchContext, frame: dict[str, Any]) -> None:
        """Wait for durable result persistence from the transport thread."""
        future = asyncio.run_coroutine_threadsafe(self._persist_result(context, frame), context.loop)
        try:
            future.result(timeout=30)
        except Exception:  # noqa: BLE001 - never lose a terminal result to a logging or callback error
            future.cancel()
            message = "Could not durably store node result"
            raise TransportError(message, submitted=True) from None

    def _record_resource_state(
        self,
        context: _DispatchContext,
        name: str,
        uid: str | None,
        status: str,
        error: str | None,
    ) -> None:
        """Persist cold-start resource allocation and cleanup state."""
        future = asyncio.run_coroutine_threadsafe(
            self._work_progress.update_backend_resource(
                context.work_item.id,
                resource_name=name,
                resource_uid=uid,
                cleanup_status=status,
                cleanup_error=error,
                claim_owner_id=context.work_item.claim_owner_id,
                claim_generation=context.work_item.claim_generation,
            ),
            context.loop,
        )
        try:
            future.result(timeout=30)
        except Exception:
            future.cancel()
            if status == "pending":
                message = "Could not persist cold-start worker allocation state"
                raise TransportError(message, submitted=False) from None
            logger.exception(
                "Could not persist cold-start resource cleanup state",
                work_item_id=context.identity,
                cleanup_status=status,
            )

    def _on_progress(self, context: _DispatchContext, frame: dict[str, Any]) -> None:
        """Log transport progress."""
        logger.debug("node progress", work_item_id=context.identity, node_event=frame.get("event"))

    def _mark_execute_submitted(self, context: _DispatchContext) -> None:
        """Persist execution start at the exact Execute submission boundary."""
        try:
            context.execution_start()
        except ExecutionStartRejectedError:
            context.cancelled.set()
            message = "Node execution cancelled before submission"
            raise TransportError(message, cancellation_requested=True) from None
        except ExecutionStartPersistenceError as exc:
            message = "Could not persist node execution start"
            raise TransportError(message, submitted=True) from exc

    async def _resolve_cluster_credential(self, cluster: Cluster) -> str:
        """Resolve the Cluster's credential reference for a dispatch attempt."""
        if cluster.source_client_id is None:
            message = "Assigned cluster has no credential-resolution client"
            raise RetryableDispatchError(message)
        try:
            return await self._credential_resolver.resolve(
                cluster.credential_ref,
                client_id=cluster.source_client_id,
            )
        except CredentialResolutionError as exc:
            raise RetryableDispatchError(CLUSTER_CREDENTIAL_RESOLUTION_FAILED) from exc

    def _k8s_target(
        self,
        target: ExecutionTarget,
        cluster: Cluster,
        credential: str,
    ) -> dict[str, Any]:
        """Map an ExecutionTarget onto the connection dict run_job expects.

        This is the single place that reads target topology fields. Namespace (and
        node selectors/tolerations) live in the backend-specific ``placement`` block
        under a K8s/RHEL discriminator (AAP-95135); this manager only handles the
        Kubernetes variant.
        """
        placement = target.placement
        if not isinstance(placement, KubernetesPlacement):
            message = f"vanilla-k8s manager requires a Kubernetes placement, got {placement.type!r}"
            raise WorkItemPayloadError(message)
        return {
            "base_url": cluster.endpoint,
            "namespace": placement.namespace,
            "token": credential,
            "ca_certificate": cluster.ca_certificate,
            "verify_ssl": True,
            "node_selectors": placement.node_selectors,
            "tolerations": placement.tolerations,
        }


def _map_result(frame: dict[str, Any], output_config: dict[str, str] | None) -> dict[str, Any]:
    """Translate the SDK node result frame into a Temporal activity result dict.

    Output mapping is *simple field selection* only. Template-expression output
    mapping (e.g. ``"${result.stdout}"``) requires NamespaceResolver from the
    syntara package, which the execution plane must not import — deferred to
    AAP-93073, matching the existing script-node limitation.
    """
    result = frame.get("result") if isinstance(frame, dict) else None
    if not isinstance(result, dict) or type(result.get("StatusCode")) is not int:
        message = "Node returned an invalid result"
        raise NodeExecutionError(message, error_type="NodeProtocolError")
    status_code = result["StatusCode"]
    if not 0 <= status_code <= MAX_STATUS_CODE:
        message = "Node returned an out-of-range status code"
        raise NodeExecutionError(message, error_type="NodeProtocolError")

    raw = result.get("Result")
    if raw is not None and not isinstance(raw, dict):
        message = "Node returned invalid output"
        raise NodeExecutionError(message, error_type="NodeProtocolError")
    output = _select_output(raw or {}, output_config)

    if status_code != 0:
        failure = frame.get("error") or {}
        message = result.get("ErrorMessage") or result.get("StatusMessage") or "Node execution failed"
        raise NodeExecutionError(message, error_type=failure.get("type") or "NodeExecutionError", output=output)
    return {"output": output}


def _select_output(raw: dict[str, Any], output_config: dict[str, str] | None) -> dict[str, Any]:
    """Select output fields by name, mirroring the current script-node behaviour."""
    if output_config is None:
        return raw
    return {key: raw[key] for key in output_config if key in raw}
