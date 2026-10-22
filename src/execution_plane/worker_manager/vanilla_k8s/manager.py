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
from typing import TYPE_CHECKING, Any

import structlog
from pydantic import ValidationError
from syntara_plugin.runtime import ContainerResult

from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.models.work_item import WorkItemStatus
from execution_plane.worker_manager.vanilla_k8s.transport import TransportError, run_job
from execution_plane_client.commands.work import WorkExecute

if TYPE_CHECKING:
    from execution_plane.cluster.cluster_store import ClusterStore
    from execution_plane.config import EPSettings
    from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
    from execution_plane.models.cluster import Cluster
    from execution_plane.models.execution_target import ExecutionTarget
    from execution_plane.models.work_item import WorkItem
    from execution_plane.work_item.work_item_store import WorkStore

logger = structlog.stdlib.get_logger(__name__)


class RetryableDispatchError(Exception):
    """Transient transport/capacity failure — requeue the work item, do not fail it."""


class NodeExecutionError(Exception):
    """The node ran and returned a non-zero result — terminal for the activity."""

    def __init__(
        self,
        message: str,
        *,
        error_type: str,
        output: dict[str, Any] | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Carry the safe error classification and any partial output."""
        super().__init__(message)
        self.error_type = error_type
        self.output = output
        self.details = details or {}
        self.persisted = False


class WorkItemPayloadError(Exception):
    """The work item payload is missing required dispatch fields — terminal."""


class WorkloadOutcomeUnknownError(Exception):
    """The node may have run, but EP did not receive a durable terminal result."""


class WorkloadCancelledError(Exception):
    """Cancellation was observed before Execute could be submitted."""

    def __init__(self) -> None:
        """Describe a confirmed cold-start cancellation."""
        super().__init__("Node workload termination was confirmed after cancellation")


class VanillaK8sWorkerManager:
    """Dispatch a work item to a fresh Kubernetes pod and return its terminal result."""

    def __init__(
        self,
        target_credentials: ExecutionTargetStore,
        cluster_credentials: ClusterStore,
        work_progress: WorkStore,
        settings: EPSettings,
    ) -> None:
        """Bind this backend to EP-owned stores and cold-start settings."""
        self._target_credentials = target_credentials
        self._cluster_credentials = cluster_credentials
        self._work_progress = work_progress
        self._settings = settings

    async def dispatch(self, work_item: WorkItem) -> dict[str, Any]:  # noqa: C901, PLR0915
        """Allocate the worker, stream its result, and persist the WorkItem outcome.

        Returns ``{"output": ...}`` on success. Raises:
        - ``RetryableDispatchError`` when the worker could not run (requeue with backoff),
        - ``NodeExecutionError`` when the node ran but failed (fail the activity),
        - ``WorkItemPayloadError`` when the payload is malformed (fail the activity).
        """
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

        identity = str(work_item.id)
        attempt_id = f"{identity}-{work_item.claim_generation}"
        target_config = self._k8s_target(target, cluster)
        cancelled = threading.Event()
        loop = asyncio.get_running_loop()

        async def persist_result(frame: dict[str, Any]) -> None:
            try:
                result = _map_result(frame, output_config)
                status = WorkItemStatus.COMPLETED
            except NodeExecutionError as exc:
                if cancelled.is_set() and exc.error_type == "CancelledError":
                    result = {"cancelled": True, "execution_started": True, "reason": str(exc)}
                    if exc.output is not None:
                        result["output"] = exc.output
                    status = WorkItemStatus.CANCELLED
                else:
                    result = {"error": str(exc), "error_type": exc.error_type}
                    if exc.output is not None:
                        result["output"] = exc.output
                    status = WorkItemStatus.FAILED
                result.update(exc.details)
            await self._work_progress.set_result(
                work_item.id,
                result,
                status,
                claim_owner_id=work_item.claim_owner_id,
                claim_generation=work_item.claim_generation,
            )

        def persist_result_before_cleanup(frame: dict[str, Any]) -> None:
            future = asyncio.run_coroutine_threadsafe(persist_result(frame), loop)
            try:
                future.result(timeout=30)
            except Exception:  # noqa: BLE001 - never lose a terminal result to a logging or callback error
                future.cancel()
                message = "Could not durably store node result"
                raise TransportError(message, submitted=True) from None

        def record_resource_state(name: str, uid: str | None, status: str, error: str | None) -> None:
            future = asyncio.run_coroutine_threadsafe(
                self._work_progress.update_backend_resource(
                    work_item.id,
                    resource_name=name,
                    resource_uid=uid,
                    cleanup_status=status,
                    cleanup_error=error,
                    claim_owner_id=work_item.claim_owner_id,
                    claim_generation=work_item.claim_generation,
                ),
                loop,
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
                    work_item_id=identity,
                    cleanup_status=status,
                )

        def on_progress(frame: dict[str, Any]) -> None:
            logger.debug("node progress", work_item_id=identity, node_event=frame.get("event"))

        try:
            transport_task = asyncio.create_task(
                asyncio.to_thread(
                    run_job,
                    target=target_config,
                    image=image,
                    invocation=invocation,
                    identity=identity,
                    attempt_id=attempt_id,
                    startup=self._settings.node_startup_seconds,
                    grace=self._settings.node_grace_seconds,
                    cancelled=cancelled,
                    progress=on_progress,
                    on_result=persist_result_before_cleanup,
                    on_resource=record_resource_state,
                    node_selectors=target_config["node_selectors"],
                    tolerations=target_config["tolerations"],
                    allowed_egress_cidrs=self._settings.workload_allowed_egress_cidrs,
                    forbidden_egress_cidrs=self._settings.workload_forbidden_egress_cidrs,
                )
            )
            while not transport_task.done():
                done, _pending = await asyncio.wait({transport_task}, timeout=5)
                if not done:
                    cancellation_requested = await self._work_progress.refresh_claim(
                        work_item.id,
                        claim_owner_id=work_item.claim_owner_id,
                        claim_generation=work_item.claim_generation,
                    )
                    if cancellation_requested is not False:
                        cancelled.set()
            frame = await transport_task
        except TransportError as exc:
            if exc.retryable:
                raise RetryableDispatchError(str(exc)) from None
            if exc.cancelled_confirmed:
                raise WorkloadCancelledError from None
            if exc.submitted:
                raise WorkloadOutcomeUnknownError(str(exc)) from None
            raise NodeExecutionError(str(exc), error_type="NodeTransportError") from None

        try:
            return _map_result(frame, output_config)
        except NodeExecutionError as exc:
            exc.persisted = True
            raise

    def _k8s_target(self, target: ExecutionTarget, cluster: Cluster) -> dict[str, Any]:
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
            "token": cluster.api_key,
            "ca_certificate": cluster.ca_certificate,
            "verify_ssl": True,
            "node_selectors": placement.node_selectors,
            "tolerations": placement.tolerations,
        }


def _map_result(frame: dict[str, Any], output_config: dict[str, str] | None) -> dict[str, Any]:
    """Preserve the SDK outcome and apply optional simple output field selection."""
    if frame.get("kind") == "failure":
        code = frame["code"]
        message = "The workload did not complete"
        raise NodeExecutionError(
            message,
            error_type="CancelledError" if code == "CANCELLED" else code,
            details={"failure_code": code, "retryable": frame["retryable"], "outcome": frame["outcome"]},
        )
    result = frame.get("result")
    if not isinstance(result, ContainerResult):
        message = "Workload returned an invalid result"
        raise NodeExecutionError(message, error_type="WorkloadProtocolError")
    details: dict[str, Any] = {
        "output_schema_digest": result.output_schema_digest,
        "status_code": result.status_code,
        "status_message": result.status_message,
        "retryable": result.retryable,
    }
    if result.object_reference is not None:
        details["object_reference"] = result.object_reference
    output = _select_output(dict(result.output), output_config) if result.output is not None else None
    if result.status_code != 0:
        details["error_code"] = result.error_code
        details["error_message"] = result.error_message
        raise NodeExecutionError(
            result.error_message,
            error_type="CancelledError" if result.error_code == "CANCELLED" else result.error_code,
            output=output,
            details=details,
        )
    if output is not None:
        details["output"] = output
    return details


def _select_output(raw: dict[str, Any], output_config: dict[str, str] | None) -> dict[str, Any]:
    """Select output fields by name, mirroring the current script-node behaviour."""
    if output_config is None:
        return raw
    return {key: raw[key] for key in output_config if key in raw}
