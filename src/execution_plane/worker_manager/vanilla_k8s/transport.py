"""An owned Kubernetes allocation with explicit readiness, submission, and release stages."""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import tempfile
import time
from contextlib import ExitStack
from dataclasses import dataclass, field
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import structlog
from grpc import aio
from kubernetes import client
from kubernetes.client.exceptions import ApiException
from kubernetes.stream import portforward

from execution_plane.node_protocol.codec import CHANNEL_OPTIONS, MAX_MESSAGE_BYTES, PORT, encode_request
from execution_plane.worker_manager.blocking import run_blocking
from execution_plane.worker_manager.lifecycle import (
    BackendError,
    BackendResource,
    Invocation,
    ReleaseResult,
    RetryableDispatchError,
    WorkAttempt,
    WorkerAllocation,
    WorkItemPayloadError,
    WorkloadOutcomeUnknownError,
)
from execution_plane.worker_manager.node_client import NodeClient
from execution_plane.worker_manager.vanilla_k8s.forward import forward_socket

if TYPE_CHECKING:
    from collections.abc import Callable

logger = structlog.stdlib.get_logger(__name__)

MAX_FRAME_BYTES = MAX_MESSAGE_BYTES


def _network_contains(
    parent: ipaddress.IPv4Network | ipaddress.IPv6Network,
    child: ipaddress.IPv4Network | ipaddress.IPv6Network,
) -> bool:
    """Return whether a same-family parent CIDR contains a child CIDR."""
    if isinstance(parent, ipaddress.IPv4Network):
        return isinstance(child, ipaddress.IPv4Network) and child.subnet_of(parent)
    return isinstance(child, ipaddress.IPv6Network) and child.subnet_of(parent)


# Cap how much of a Kubernetes error body we write to the admin log. The API
# server's failure body is a small Status object, but a misbehaving proxy can
# return an arbitrarily large page — truncate so one bad response can't flood
# the log.
_MAX_LOGGED_BODY_CHARS = 2048


def _truncate_for_log(value: object) -> str | None:
    """Stringify and bound a value for a log field; ``None`` stays ``None``."""
    if value is None:
        return None
    text = value if isinstance(value, str) else str(value)
    if len(text) > _MAX_LOGGED_BODY_CHARS:
        return text[:_MAX_LOGGED_BODY_CHARS] + "…(truncated)"
    return text


def pod_body(
    name: str,
    identity: str,
    attempt_id: str,
    image: str,
    invocation: dict[str, Any],
    *,
    startup: int,
    grace: int,
    node_selectors: list[str] | None = None,
    tolerations: list[str] | None = None,
) -> dict[str, Any]:
    """Never put invocation data or cluster credentials into the pod specification."""
    volumes: list[dict[str, Any]] = [{"name": "tmp", "emptyDir": {"medium": "Memory", "sizeLimit": "64Mi"}}]
    mounts: list[dict[str, Any]] = [{"name": "tmp", "mountPath": "/tmp"}]  # noqa: S108 - isolated memory-backed pod volume
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "labels": {
                "app.kubernetes.io/name": "syntara-node",
                "app.kubernetes.io/managed-by": "execution-plane",
                "execution-plane.syntara.io/work-item": identity,
                "execution-plane.syntara.io/attempt": attempt_id,
            },
        },
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "activeDeadlineSeconds": startup + invocation["timeout_seconds"] + grace,
            "terminationGracePeriodSeconds": grace,
            "nodeSelector": _node_selector(node_selectors or []),
            "tolerations": _tolerations(tolerations or []),
            "securityContext": {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
            "volumes": volumes,
            "containers": [
                {
                    "name": "node",
                    "image": image,
                    "ports": [{"name": "grpc", "containerPort": PORT}],
                    "volumeMounts": mounts,
                    "resources": {
                        "requests": {"cpu": "100m", "memory": "128Mi"},
                        "limits": {"cpu": "1", "memory": "512Mi"},
                    },
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "readOnlyRootFilesystem": True,
                        "capabilities": {"drop": ["ALL"]},
                    },
                }
            ],
        },
    }


def _node_selector(values: list[str]) -> dict[str, str]:
    """Convert validated key=value placement entries to a Kubernetes selector map."""
    result: dict[str, str] = {}
    for value in values:
        key, separator, selected_value = value.partition("=")
        if not separator or not key:
            message = "Invalid node selector"
            raise ValueError(message)
        result[key] = selected_value
    return result


def _tolerations(values: list[str]) -> list[dict[str, str]]:
    """Convert the typed placement model's compact toleration strings."""
    result: list[dict[str, str]] = []
    for value in values:
        expression, separator, effect = value.rpartition(":")
        if not separator:
            expression, effect = value, ""
        key, equals, selected_value = expression.partition("=")
        toleration: dict[str, str] = {"operator": "Equal" if equals else "Exists"}
        if key:
            toleration["key"] = key
        if equals:
            toleration["value"] = selected_value
        if effect:
            toleration["effect"] = effect
        result.append(toleration)
    return result


def network_policy_body(
    identity: str,
    attempt_id: str,
    resource_name: str,
    allowed_egress_cidrs: list[str],
    forbidden_egress_cidrs: list[str],
) -> dict[str, Any]:
    """Deny workload ingress and restrict egress to DNS and configured networks."""
    labels = {
        "app.kubernetes.io/managed-by": "execution-plane",
        "execution-plane.syntara.io/work-item": identity,
        "execution-plane.syntara.io/attempt": attempt_id,
    }
    denied = [ipaddress.ip_network(value, strict=False) for value in forbidden_egress_cidrs]
    egress: list[dict[str, Any]] = [
        {
            "to": [
                {
                    "namespaceSelector": {
                        "matchExpressions": [
                            {
                                "key": "kubernetes.io/metadata.name",
                                "operator": "In",
                                "values": ["kube-system", "openshift-dns"],
                            }
                        ]
                    }
                }
            ],
            "ports": [{"protocol": "UDP", "port": 53}, {"protocol": "TCP", "port": 53}],
        }
    ]
    for value in allowed_egress_cidrs:
        network = ipaddress.ip_network(value, strict=False)
        exclusions: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
        fully_denied = False
        for candidate in denied:
            if not (_network_contains(network, candidate) or _network_contains(candidate, network)):
                continue
            if _network_contains(candidate, network):
                fully_denied = True
                break
            if _network_contains(network, candidate):
                exclusions.append(candidate)
        if fully_denied:
            continue

        # NetworkPolicy IPBlock exclusions must be contained by the allowed
        # CIDR. Keep only the broadest exclusions when configured ranges nest.
        exclusions = [
            candidate
            for candidate in exclusions
            if not any(candidate != other and _network_contains(other, candidate) for other in exclusions)
        ]
        ip_block: dict[str, Any] = {"cidr": str(network)}
        if exclusions:
            ip_block["except"] = [str(candidate) for candidate in exclusions]
        egress.append({"to": [{"ipBlock": ip_block}]})
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": f"{resource_name}-network", "labels": labels},
        "spec": {
            "podSelector": {
                "matchLabels": {
                    "execution-plane.syntara.io/work-item": identity,
                    "execution-plane.syntara.io/attempt": attempt_id,
                }
            },
            "policyTypes": ["Ingress", "Egress"],
            "ingress": [],
            "egress": egress,
        },
    }


def job_body(
    name: str,
    identity: str,
    attempt_id: str,
    image: str,
    invocation: dict[str, Any],
    *,
    startup: int,
    grace: int,
    node_selectors: list[str] | None = None,
    tolerations: list[str] | None = None,
) -> dict[str, Any]:
    """Wrap one hardened worker Pod in a single-attempt cold-start Job."""
    pod = pod_body(
        name,
        identity,
        attempt_id,
        image,
        invocation,
        startup=startup,
        grace=grace,
        node_selectors=node_selectors,
        tolerations=tolerations,
    )
    pod["metadata"].pop("name", None)
    pod["metadata"].pop("namespace", None)
    pod.pop("apiVersion", None)
    pod.pop("kind", None)
    labels = pod["metadata"]["labels"]
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "labels": labels,
        },
        "spec": {
            "completions": 1,
            "parallelism": 1,
            "backoffLimit": 0,
            "activeDeadlineSeconds": startup + invocation["timeout_seconds"] + grace,
            "ttlSecondsAfterFinished": 3600,
            "template": pod,
        },
    }


@dataclass(frozen=True)
class KubernetesTarget:
    """Verified cluster connection and placement, resolved before allocation."""

    endpoint: str
    namespace: str
    token: str = field(repr=False)
    ca_certificate: str | None
    node_selectors: list[str]
    tolerations: list[str]
    verify_ssl: bool = True


class KubernetesWorker(WorkerAllocation):
    """Allocate, connect, invoke, and release one owned cold-start Kubernetes Job."""

    def __init__(
        self,
        *,
        attempt: WorkAttempt,
        target: KubernetesTarget,
        image: str,
        invocation: dict[str, Any],
        output_config: dict[str, str] | None,
        startup: int,
        grace: int,
        allowed_egress_cidrs: list[str],
        forbidden_egress_cidrs: list[str],
    ) -> None:
        """Describe the allocation and validate its wire request before external creation."""
        self._attempt = attempt
        self._target = target
        self._image = image
        self._invocation = invocation
        self._output_config = output_config
        self._startup = startup
        self._grace = grace
        self._allowed_egress_cidrs = allowed_egress_cidrs
        self._forbidden_egress_cidrs = forbidden_egress_cidrs
        self._name = "syntara-node-" + hashlib.sha256(attempt.attempt_id.encode()).hexdigest()[:32]
        self._namespace = target.namespace
        self._job_uid: str | None = None
        self._job_owned = False
        self._job_may_exist = False
        self._policy_owned = False
        self._policy_may_exist = False
        self._policy_uid: str | None = None
        self._policy_name = f"{self._name}-network"
        self._pod_name: str | None = None
        self._resources = ExitStack()
        self._forward = ExitStack()
        self._channel: aio.Channel | None = None
        self._node: NodeClient | None = None
        try:
            self._request = encode_request(invocation, attempt.attempt_id)
            if self._request.ByteSize() > MAX_FRAME_BYTES:
                message = "Node invocation exceeds transport limit"
                raise WorkItemPayloadError(message)
        except (ValueError, TypeError, OverflowError):
            message = "Node invocation cannot be encoded for the transport"
            raise WorkItemPayloadError(message) from None

    @property
    def resource(self) -> BackendResource:
        """Expose deterministic intent before creation and the acknowledged UID afterwards."""
        return BackendResource(self._name, self._job_uid)

    async def allocate(self) -> None:
        """Create the isolated worker and retain ownership before returning."""
        await self._call(self._allocate)

    def _open_api(self) -> None:
        directory = self._resources.enter_context(tempfile.TemporaryDirectory(prefix="syntara-node-"))
        config = client.Configuration()
        config.host = self._target.endpoint
        # Kubernetes v36's compatibility shim reads the old token key but the
        # prefix from BearerToken. Set both for supported client versions.
        for key in ("authorization", "BearerToken"):
            config.api_key[key] = self._target.token
            config.api_key_prefix[key] = "Bearer"
        config.verify_ssl = self._target.verify_ssl
        if self._target.ca_certificate:
            ca = Path(directory) / "ca.pem"
            ca.write_text(self._target.ca_certificate)
            config.ssl_ca_cert = str(ca)
        api_client = self._resources.enter_context(client.ApiClient(config))
        self._api = client.CoreV1Api(api_client)
        self._batch_api = client.BatchV1Api(api_client)
        self._network_api = client.NetworkingV1Api(api_client)

    def _allocate(self) -> None:
        self._open_api()
        self._create_policy()
        self._job_may_exist = True
        try:
            job = self._batch_api.create_namespaced_job(
                self._namespace,
                job_body(
                    self._name,
                    self._attempt.identity,
                    self._attempt.attempt_id,
                    self._image,
                    self._invocation,
                    startup=self._startup,
                    grace=self._grace,
                    node_selectors=self._target.node_selectors,
                    tolerations=self._target.tolerations,
                ),
                _request_timeout=15,
            )
        except ApiException as exc:
            if exc.status == HTTPStatus.CONFLICT:
                message = "A Kubernetes Job already exists for this execution attempt"
                raise WorkloadOutcomeUnknownError(message) from None
            try:
                self._batch_api.read_namespaced_job(self._name, self._namespace, _request_timeout=10)
            except ApiException as read_error:
                if read_error.status == HTTPStatus.NOT_FOUND:
                    self._job_may_exist = False
                    raise exc from None
                raise
            message = "Kubernetes Job creation outcome is uncertain"
            raise WorkloadOutcomeUnknownError(message) from None
        self._job_uid = str(job.metadata.uid) if job.metadata and job.metadata.uid else None
        self._job_owned = self._job_uid is not None
        if not self._job_owned:
            message = "Kubernetes Job creation did not acknowledge its resource identity"
            raise WorkloadOutcomeUnknownError(message)
        self._network_api.patch_namespaced_network_policy(
            self._policy_name,
            self._namespace,
            {"metadata": {"annotations": {"execution-plane.syntara.io/job-uid": self._job_uid}}},
            _request_timeout=15,
        )

    def _create_policy(self) -> None:
        self._policy_may_exist = True
        try:
            policy = self._network_api.create_namespaced_network_policy(
                self._namespace,
                network_policy_body(
                    self._attempt.identity,
                    self._attempt.attempt_id,
                    self._name,
                    self._allowed_egress_cidrs,
                    self._forbidden_egress_cidrs,
                ),
                _request_timeout=15,
            )
        except ApiException as exc:
            if exc.status != HTTPStatus.CONFLICT:
                raise
            policy = self._network_api.read_namespaced_network_policy(
                self._policy_name, self._namespace, _request_timeout=15
            )
            actual = policy.spec.pod_selector.match_labels or {}
            labels = policy.metadata.labels or {}
            if (
                actual.get("execution-plane.syntara.io/work-item") != self._attempt.identity
                or actual.get("execution-plane.syntara.io/attempt") != self._attempt.attempt_id
                or labels.get("app.kubernetes.io/managed-by") != "execution-plane"
            ):
                message = "Existing workload NetworkPolicy does not match this work item"
                raise WorkloadOutcomeUnknownError(message) from None
        self._policy_uid = str(policy.metadata.uid) if policy.metadata and policy.metadata.uid else None
        self._policy_owned = self._policy_uid is not None
        if not self._policy_owned:
            message = "NetworkPolicy creation did not acknowledge its resource identity"
            raise BackendError(message)

    async def wait_ready(self) -> None:
        """Verify Pod ownership, establish its tunnel, and health-check the node service."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._startup
        while self._pod_name is None:
            self._pod_name = await self._call(self._ready_pod)
            if self._pod_name is not None:
                break
            if loop.time() >= deadline:
                message = "Node worker Job did not become ready"
                raise RetryableDispatchError(message)
            await asyncio.sleep(0.25)
        grpc_deadline = loop.time() + min(30, self._startup)
        while True:
            try:
                address = await self._call(self._open_forward)
                self._channel = aio.insecure_channel(address, options=CHANNEL_OPTIONS)
                self._node = NodeClient(self._channel, self._request, self._output_config)
                await self._node.wait_ready(min(2, max(0.1, grpc_deadline - loop.time())))
                return
            except RetryableDispatchError:
                await self._close_forward()
                if loop.time() >= grpc_deadline:
                    raise
                await asyncio.sleep(0.1)

    def _ready_pod(self) -> str | None:
        pods = self._api.list_namespaced_pod(
            self._namespace,
            label_selector=f"batch.kubernetes.io/job-name={self._name}",
            _request_timeout=15,
        ).items
        if len(pods) > 1:
            message = "Kubernetes Job has multiple worker Pods"
            raise WorkloadOutcomeUnknownError(message)
        if pods:
            pod = pods[0]
            owners = pod.metadata.owner_references or [] if pod.metadata else []
            if not any(owner.uid == self._job_uid and owner.kind == "Job" and owner.controller for owner in owners):
                message = "Kubernetes worker Pod ownership does not match its Job"
                raise WorkloadOutcomeUnknownError(message)
            phase = pod.status.phase if pod.status else None
            if phase == "Running" and pod.metadata and pod.metadata.name:
                return str(pod.metadata.name)
            if phase in ("Failed", "Succeeded"):
                message = "Node worker Pod stopped before becoming ready"
                raise RetryableDispatchError(message)
        job = self._batch_api.read_namespaced_job(self._name, self._namespace, _request_timeout=15)
        if not job.metadata or job.metadata.uid != self._job_uid:
            message = "Kubernetes Job identity changed while waiting for readiness"
            raise WorkloadOutcomeUnknownError(message)
        if job.status and job.status.failed:
            message = "Kubernetes Job failed before node execution started"
            raise RetryableDispatchError(message)
        return None

    def _open_forward(self) -> str:
        connection = portforward(
            self._api.connect_get_namespaced_pod_portforward,
            self._pod_name,
            self._namespace,
            ports=str(PORT),
            _request_timeout=15,
        )
        self._forward.callback(connection.close)
        return self._forward.enter_context(forward_socket(connection.socket(PORT)))

    async def submit(self) -> Invocation:
        """Issue Execute locally; this does not acknowledge receipt or user-code execution."""
        if self._node is None:
            message = "Worker has not established its node connection"
            raise BackendError(message)
        # Issue Execute and return its handle in the same event-loop turn.
        return self._node.submit(self._grace)

    async def _close_forward(self) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel = None
        self._node = None
        await run_blocking(self._forward.close)

    async def release(self) -> ReleaseResult:
        """Close the protocol and confirm exclusive Job/Pod termination before deleting policy."""
        close_error = None
        try:
            await self._close_forward()
        except Exception:
            logger.exception("Could not close worker transport", work_item_id=self._attempt.identity)
            close_error = "Worker transport cleanup failed"
        try:
            released = await run_blocking(self._release_resources)
        finally:
            try:
                await run_blocking(self._resources.close)
            except Exception:
                logger.exception("Could not close Kubernetes client", work_item_id=self._attempt.identity)
                close_error = "Kubernetes client cleanup failed"
        if close_error is not None:
            return ReleaseResult("failed", execution_stopped=released.execution_stopped, error=close_error)
        return released

    def _release_resources(self) -> ReleaseResult:  # noqa: C901 - ownership and termination are independent
        stopped = not self._job_may_exist
        status: Literal["not_required", "complete", "failed", "deferred"] = "not_required"
        error = None
        if self._job_owned:
            try:
                stopped = self._delete_job()
            except Exception:  # Preserve terminal execution results when cleanup fails.
                logger.exception("Kubernetes Job cleanup failed", work_item_id=self._attempt.identity)
            status = "complete" if stopped else "failed"
            if not stopped:
                error = "Kubernetes Job and worker Pod cleanup was not confirmed"
        elif self._job_may_exist:
            status, error = "deferred", "Kubernetes Job ownership is uncertain; cleanup was deferred"
        if self._policy_owned and stopped:
            try:
                self._network_api.delete_namespaced_network_policy(
                    self._policy_name,
                    self._namespace,
                    body=client.V1DeleteOptions(preconditions=client.V1Preconditions(uid=self._policy_uid)),
                    _request_timeout=15,
                )
                if status == "not_required":
                    status = "complete"
            except ApiException as exc:
                if exc.status != HTTPStatus.NOT_FOUND:
                    status, error = "failed", "Workload NetworkPolicy cleanup failed"
            except Exception:  # noqa: BLE001 - cleanup diagnostics must not expose credentials
                status, error = "failed", "Workload NetworkPolicy cleanup failed"
        elif self._policy_may_exist and status != "failed":
            status, error = "deferred", "Workload NetworkPolicy remains while ownership or termination is uncertain"
        return ReleaseResult(status, execution_stopped=stopped, error=error)

    def _delete_job(self) -> bool:
        try:
            self._batch_api.delete_namespaced_job(
                self._name,
                self._namespace,
                body=client.V1DeleteOptions(preconditions=client.V1Preconditions(uid=self._job_uid)),
                propagation_policy="Foreground",
                grace_period_seconds=self._grace,
                _request_timeout=15,
            )
        except ApiException as exc:
            if exc.status != HTTPStatus.NOT_FOUND:
                raise
        deadline = time.monotonic() + max(self._grace, 5) + 10
        while time.monotonic() < deadline:
            try:
                self._batch_api.read_namespaced_job(self._name, self._namespace, _request_timeout=15)
            except ApiException as exc:
                if exc.status != HTTPStatus.NOT_FOUND:
                    raise
                pods = self._api.list_namespaced_pod(
                    self._namespace,
                    label_selector=f"batch.kubernetes.io/job-name={self._name}",
                    _request_timeout=15,
                ).items
                if not pods:
                    return True
            time.sleep(0.25)
        return False

    async def _call[T](self, operation: Callable[[], T]) -> T:
        """Keep Kubernetes diagnostics server-side and classify only known pre-Execute failures."""
        try:
            return await run_blocking(operation)
        except (BackendError, RetryableDispatchError, WorkloadOutcomeUnknownError):
            raise
        except ApiException as exc:
            logger.warning(
                "Kubernetes API request failed",
                work_item_id=self._attempt.identity,
                namespace=self._namespace,
                http_status=exc.status,
                reason=exc.reason,
                response_body=_truncate_for_log(exc.body),
            )
            if self._job_may_exist and not self._job_owned:
                message = "Kubernetes allocation ownership is uncertain"
                raise WorkloadOutcomeUnknownError(message) from None
            message = f"Kubernetes API request failed (HTTP {exc.status})"
            if exc.status in {429, 502, 503, 504}:
                raise RetryableDispatchError(message) from None
            raise BackendError(message) from None
        except Exception as exc:  # noqa: BLE001 - never expose raw API credentials or responses
            logger.warning(
                "Kubernetes transport failed", work_item_id=self._attempt.identity, error_type=type(exc).__name__
            )
            if self._job_may_exist and not self._job_owned:
                message = "Kubernetes allocation ownership is uncertain"
                raise WorkloadOutcomeUnknownError(message) from None
            message = "Kubernetes transport failed"
            raise BackendError(message) from None
