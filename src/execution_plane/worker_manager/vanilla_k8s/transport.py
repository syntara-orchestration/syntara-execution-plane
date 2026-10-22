# ruff: noqa: TRY301
"""One pod and one gRPC invocation over an authenticated Kubernetes port-forward."""

from __future__ import annotations

import hashlib
import ipaddress
import tempfile
import time
from contextlib import suppress
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Any

import grpc
import structlog
from kubernetes import client
from kubernetes.client.exceptions import ApiException
from kubernetes.stream import portforward

from execution_plane.worker_manager.vanilla_k8s.forward import forward_socket
from execution_plane.workload import CHANNEL_OPTIONS, PORT, WorkloadRpcError, invoke
from execution_plane_client.commands.work import WorkloadInvocation

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable

logger = structlog.stdlib.get_logger(__name__)


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


class TransportError(Exception):
    """Failure with an explicit safe-to-retry classification."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool = False,
        submitted: bool = False,
        cancellation_requested: bool = False,
    ) -> None:
        """Initialize the node contract and execution state."""
        super().__init__(message)
        self.retryable = retryable
        self.submitted = submitted
        self.cancellation_requested = cancellation_requested
        self.cancelled_confirmed = False


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


def run_job(  # noqa: C901, PLR0912, PLR0915 - one owned cold-start allocation
    *,
    target: dict[str, Any],
    image: str,
    invocation: dict[str, Any],
    identity: str,
    attempt_id: str,
    startup: int,
    grace: int,
    cancelled: threading.Event,
    progress: Callable[[dict[str, Any]], None],
    on_result: Callable[[dict[str, Any]], None],
    on_resource: Callable[[str, str | None, str, str | None], None],
    node_selectors: list[str] | None = None,
    tolerations: list[str] | None = None,
    allowed_egress_cidrs: list[str] | None = None,
    forbidden_egress_cidrs: list[str] | None = None,
) -> dict[str, Any]:
    """Execute once in a cold-start Job; application data travels only over gRPC."""
    name = "syntara-node-" + hashlib.sha256(attempt_id.encode()).hexdigest()[:32]
    namespace = target["namespace"]
    try:
        WorkloadInvocation.model_validate(invocation)
    except ValueError:
        message = "Workload invocation is invalid"
        raise TransportError(message) from None
    with tempfile.TemporaryDirectory(prefix="syntara-node-") as directory:
        config = client.Configuration()
        config.host = target["base_url"]
        # The kubernetes Python client library (PyPI `kubernetes`) renamed the bearer
        # auth scheme from 'authorization' to 'BearerToken' in v36. Its backward-compat
        # shim reads the token from the legacy 'authorization' key but looks the prefix
        # up ONLY under 'BearerToken' (kubernetes-client/python#2595), so setting the
        # prefix under 'authorization' alone silently drops the "Bearer " scheme and the
        # API server rejects the header as anonymous. Set both keys so every client
        # version sends "Bearer <token>".
        config.api_key["authorization"] = target["token"]
        config.api_key["BearerToken"] = target["token"]
        config.api_key_prefix["authorization"] = "Bearer"
        config.api_key_prefix["BearerToken"] = "Bearer"
        # Local kind/minikube API servers present a self-signed cert with no CA on
        # the ExecutionTarget; a target may opt out of verification for local dev.
        config.verify_ssl = target.get("verify_ssl", True)
        if target.get("ca_certificate"):
            ca = Path(directory) / "ca.pem"
            ca.write_text(target["ca_certificate"])
            config.ssl_ca_cert = str(ca)
        with client.ApiClient(config) as api_client:
            api = client.CoreV1Api(api_client)
            batch_api = client.BatchV1Api(api_client)
            network_api = client.NetworkingV1Api(api_client)
            job_owned = False
            job_may_exist = False
            job_uid: str | None = None
            submitted = False
            connection = None
            pod_name: str | None = None
            policy_name = f"{name}-network"
            policy_owned = False
            pending_transport_error: TransportError | None = None
            deleted = False
            cleanup_status = "not_required"
            cleanup_error: str | None = None
            try:
                on_resource(name, None, "pending", None)
                policy_body_value = network_policy_body(
                    identity,
                    attempt_id,
                    name,
                    allowed_egress_cidrs or [],
                    forbidden_egress_cidrs or [],
                )
                try:
                    network_api.create_namespaced_network_policy(
                        namespace,
                        policy_body_value,
                        _request_timeout=15,
                    )
                    policy_owned = True
                except ApiException as exc:
                    if exc.status != HTTPStatus.CONFLICT:
                        raise
                    existing_policy = network_api.read_namespaced_network_policy(
                        policy_name,
                        namespace,
                        _request_timeout=15,
                    )
                    actual = existing_policy.spec.pod_selector.match_labels or {}
                    existing_labels = existing_policy.metadata.labels or {}
                    if (
                        actual.get("execution-plane.syntara.io/work-item") != identity
                        or actual.get("execution-plane.syntara.io/attempt") != attempt_id
                        or existing_labels.get("app.kubernetes.io/managed-by") != "execution-plane"
                    ):
                        message = "Existing workload NetworkPolicy does not match this work item"
                        raise TransportError(message) from None
                    policy_owned = True
                job_may_exist = True
                try:
                    job = batch_api.create_namespaced_job(
                        namespace,
                        job_body(
                            name,
                            identity,
                            attempt_id,
                            image,
                            invocation,
                            startup=startup,
                            grace=grace,
                            node_selectors=node_selectors,
                            tolerations=tolerations,
                        ),
                        _request_timeout=15,
                    )
                    job_owned = True
                    job_uid = str(job.metadata.uid) if job.metadata and job.metadata.uid else None
                    on_resource(name, job_uid, "pending", None)
                except ApiException as exc:
                    if exc.status == HTTPStatus.CONFLICT:
                        # A deterministic Job may be left by a controller that lost
                        # its claim. Never attach and Execute again without persisted
                        # attempt state proving that Execute was not previously sent.
                        message = "A Kubernetes Job already exists for this execution attempt"
                        raise TransportError(message, submitted=True) from None
                    try:
                        existing_job = batch_api.read_namespaced_job(name, namespace, _request_timeout=10)
                    except ApiException as read_error:
                        if read_error.status == HTTPStatus.NOT_FOUND:
                            job_may_exist = False
                            raise exc from read_error
                        raise exc from read_error
                    labels = existing_job.metadata.labels or {} if existing_job.metadata else {}
                    if (
                        labels.get("execution-plane.syntara.io/work-item") != identity
                        or labels.get("execution-plane.syntara.io/attempt") != attempt_id
                    ):
                        message = "A Kubernetes Job with this deterministic name has different ownership"
                        raise TransportError(message) from None
                    message = "Kubernetes Job creation outcome is uncertain"
                    raise TransportError(message, submitted=True) from None

                if job_uid:
                    network_api.patch_namespaced_network_policy(
                        policy_name,
                        namespace,
                        {"metadata": {"annotations": {"execution-plane.syntara.io/job-uid": job_uid}}},
                        _request_timeout=15,
                    )
                deadline = time.monotonic() + startup
                while True:
                    if cancelled.is_set():
                        message = "Node execution cancelled"
                        raise TransportError(message)
                    pods = api.list_namespaced_pod(
                        namespace,
                        label_selector=f"batch.kubernetes.io/job-name={name}",
                        _request_timeout=15,
                    ).items
                    if len(pods) > 1:
                        message = "Kubernetes Job has multiple worker Pods; execution was not started"
                        raise TransportError(message, submitted=True)
                    if pods:
                        pod = pods[0]
                        owner_references = pod.metadata.owner_references or [] if pod.metadata else []
                        if job_uid is None or not any(
                            owner.uid == job_uid and owner.kind == "Job" and owner.controller
                            for owner in owner_references
                        ):
                            message = "Kubernetes worker Pod ownership does not match its Job"
                            raise TransportError(message, submitted=True)
                        pod_name = pod.metadata.name if pod.metadata else None
                        phase = pod.status.phase if pod.status else None
                        if phase == "Running" and pod_name:
                            break
                        if phase in ("Failed", "Succeeded"):
                            message = "Node worker Pod stopped before becoming ready"
                            raise TransportError(message, retryable=True)
                    job = batch_api.read_namespaced_job(name, namespace, _request_timeout=15)
                    if job.status and job.status.failed:
                        message = "Kubernetes Job failed before node execution started"
                        raise TransportError(message, retryable=True)
                    if time.monotonic() >= deadline:
                        message = "Node worker Job did not become ready"
                        raise TransportError(message, retryable=True)
                    time.sleep(0.25)
                grpc_deadline = time.monotonic() + min(30, max(1, startup))
                while True:
                    if pod_name is None:
                        message = "Kubernetes Job has no owned worker Pod"
                        raise TransportError(message, retryable=True)
                    connection = portforward(
                        api.connect_get_namespaced_pod_portforward,
                        pod_name,
                        namespace,
                        ports=str(PORT),
                        _request_timeout=15,
                    )
                    try:
                        with (
                            forward_socket(connection.socket(PORT)) as address,
                            grpc.insecure_channel(address, options=CHANNEL_OPTIONS) as channel,
                        ):

                            def mark_submitted() -> None:
                                nonlocal submitted
                                submitted = True

                            frame = invoke(
                                channel,
                                invocation,
                                identity=attempt_id,
                                progress=progress,
                                cancelled=cancelled,
                                startup=min(2, max(0.1, grpc_deadline - time.monotonic())),
                                grace=grace,
                                on_execute_submitted=mark_submitted,
                            )
                            on_result(frame)
                            return frame
                    except WorkloadRpcError as exc:
                        # Running can precede the listener. A refused kubelet
                        # connection needs a new port-forward, before Execute only.
                        if not exc.retryable or time.monotonic() >= grpc_deadline:
                            raise
                        submitted = False
                        cancelled.wait(0.1)
                    finally:
                        with suppress(Exception):
                            connection.close()
                        connection = None
            except WorkloadRpcError as exc:
                pending_transport_error = TransportError(
                    str(exc),
                    retryable=exc.retryable,
                    submitted=submitted,
                    cancellation_requested=exc.cancelled,
                )
                raise pending_transport_error from None
            except TransportError as exc:
                pending_transport_error = exc
                raise
            except ApiException as exc:
                # Admins need the full failure detail to diagnose auth/RBAC/quota
                # problems, but that detail must not leak to whoever reads the
                # work-item result. Log the rich context server-side — keyed by work
                # item and pod so it is traceable — then raise a sanitized error.
                logger.warning(
                    "Kubernetes API request failed",
                    work_item_id=identity,
                    pod=pod_name or name,
                    namespace=namespace,
                    http_status=exc.status,
                    reason=exc.reason,
                    response_body=_truncate_for_log(exc.body),
                )
                # Surface only the HTTP status code — never exc.reason/exc.body, which
                # can echo request/response detail or credentials. The status alone
                # distinguishes an auth failure (401/403) from a server error (5xx),
                # which is otherwise invisible to whoever reads the work-item result.
                message = f"Kubernetes API request failed (HTTP {exc.status})"
                raise TransportError(
                    message,
                    retryable=not submitted and exc.status in {429, 502, 503, 504},
                    submitted=submitted or (job_may_exist and not job_owned),
                ) from None
            except Exception as exc:  # noqa: BLE001 - never expose raw API credentials or responses
                # Catch-all for non-API failures (DNS, TLS, socket). Log both the
                # exception type and message for admin triage. str(exc) is safe here
                # because this branch only fires for non-ApiException errors (those are
                # caught above) whose messages do not carry raw Kubernetes responses.
                logger.warning(
                    "Node transport failed",
                    work_item_id=identity,
                    pod=pod_name or name,
                    namespace=namespace,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                message = "Node transport failed"
                raise TransportError(
                    message,
                    retryable=False,
                    submitted=submitted or (job_may_exist and not job_owned),
                ) from None
            finally:
                if connection is not None:
                    with suppress(Exception):
                        connection.close()
                if job_owned:
                    try:
                        batch_api.delete_namespaced_job(
                            name,
                            namespace,
                            propagation_policy="Foreground",
                            grace_period_seconds=grace,
                            _request_timeout=15,
                        )
                        delete_deadline = time.monotonic() + max(grace, 5) + 10
                        while time.monotonic() < delete_deadline:
                            try:
                                batch_api.read_namespaced_job(name, namespace, _request_timeout=15)
                            except ApiException as exc:
                                if exc.status == HTTPStatus.NOT_FOUND:
                                    remaining_pods = api.list_namespaced_pod(
                                        namespace,
                                        label_selector=f"batch.kubernetes.io/job-name={name}",
                                        _request_timeout=15,
                                    ).items
                                    if not remaining_pods:
                                        deleted = True
                                        break
                                    time.sleep(0.25)
                                    continue
                                raise
                            time.sleep(0.25)
                        if not deleted:
                            cleanup_status = "failed"
                            cleanup_error = "Kubernetes Job and worker Pod cleanup was not confirmed"
                            progress(
                                {"version": 1, "kind": "progress", "event": "cleanup_failed", "data": {"job": name}}
                            )
                    except ApiException as exc:
                        if exc.status == HTTPStatus.NOT_FOUND:
                            delete_deadline = time.monotonic() + max(grace, 5) + 10
                            while time.monotonic() < delete_deadline:
                                remaining_pods = api.list_namespaced_pod(
                                    namespace,
                                    label_selector=f"batch.kubernetes.io/job-name={name}",
                                    _request_timeout=15,
                                ).items
                                if not remaining_pods:
                                    deleted = True
                                    break
                                time.sleep(0.25)
                        else:
                            cleanup_status = "failed"
                            cleanup_error = "Kubernetes Job deletion failed"
                            progress(
                                {"version": 1, "kind": "progress", "event": "cleanup_failed", "data": {"job": name}}
                            )
                    except Exception:  # noqa: BLE001 - cleanup must not hide the execution result
                        cleanup_status = "failed"
                        cleanup_error = "Kubernetes Job cleanup failed"
                        progress({"version": 1, "kind": "progress", "event": "cleanup_failed", "data": {"job": name}})
                elif job_may_exist:
                    cleanup_status = "deferred"
                    cleanup_error = "Kubernetes Job ownership is uncertain; cleanup was deferred"
                    progress({"version": 1, "kind": "progress", "event": "cleanup_deferred", "data": {"job": name}})
                if job_owned and deleted:
                    cleanup_status = "complete"
                if pending_transport_error is not None and (
                    pending_transport_error.cancellation_requested or (cancelled.is_set() and not submitted)
                ):
                    pending_transport_error.cancelled_confirmed = deleted or not job_may_exist
                if policy_owned and (deleted or not job_may_exist):
                    try:
                        network_api.delete_namespaced_network_policy(policy_name, namespace, _request_timeout=15)
                    except ApiException as exc:
                        if exc.status != HTTPStatus.NOT_FOUND:
                            cleanup_status = "failed"
                            cleanup_error = "Workload NetworkPolicy cleanup failed"
                            progress(
                                {
                                    "version": 1,
                                    "kind": "progress",
                                    "event": "cleanup_failed",
                                    "data": {"network_policy": policy_name},
                                }
                            )
                    except Exception:  # noqa: BLE001 - policy cleanup must not hide the result
                        cleanup_status = "failed"
                        cleanup_error = "Workload NetworkPolicy cleanup failed"
                        progress(
                            {
                                "version": 1,
                                "kind": "progress",
                                "event": "cleanup_failed",
                                "data": {"network_policy": policy_name},
                            }
                        )
                elif policy_owned:
                    if cleanup_status != "failed":
                        cleanup_status = "deferred"
                        cleanup_error = "Workload NetworkPolicy remains while its Job may exist"
                    progress(
                        {
                            "version": 1,
                            "kind": "progress",
                            "event": "cleanup_deferred",
                            "data": {"network_policy": policy_name},
                        }
                    )
                if cleanup_status == "not_required" and policy_owned:
                    cleanup_status = "complete"
                try:
                    on_resource(name, job_uid, cleanup_status, cleanup_error)
                except Exception:
                    logger.exception("Could not persist backend resource cleanup state", work_item_id=identity)
