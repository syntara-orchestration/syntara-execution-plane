"""Dispatch scripts as isolated Kubernetes Jobs on an ExecutionTarget."""

from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import json
import ssl
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import quote, urlsplit

import httpx
import structlog

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from uuid import UUID

    from execution_plane.config import EPSettings

logger = structlog.stdlib.get_logger(__name__)
POLL_INTERVAL_SECONDS = 2
MAX_JOB_LOG_BYTES = 8 * 1024 * 1024
JOB_RETENTION_SECONDS = 86_400


class WorkloadExecutionError(RuntimeError):
    """A Kubernetes Job reached a terminal state without a successful result."""

    def __init__(self, message: str, result: dict[str, Any]) -> None:
        """Keep a safe structured result to persist as the terminal work outcome."""
        super().__init__(message)
        self.result = result


class WorkloadOutcomeUnknownError(RuntimeError):
    """Kubernetes did not provide enough evidence to safely resolve execution."""


class WorkloadCancelledError(RuntimeError):
    """A cancellation request was confirmed after the Kubernetes workload stopped."""

    def __init__(self, message: str, *, execution_started: bool) -> None:
        """Record whether a workload pod could have begun execution."""
        super().__init__(message)
        self.execution_started = execution_started


class KubernetesJobManager:
    """Create, observe, and recover deterministic Jobs in the selected target namespace."""

    def __init__(self, settings: EPSettings) -> None:
        """Bind workload image, resource limits, and cluster trust configuration."""
        self._settings = settings
        self._image = settings.workload_runner_image
        self._ca_bundle = settings.workload_cluster_ca_bundle_path

    async def execute(
        self,
        *,
        work_item_id: UUID,
        endpoint: str,
        api_token: str,
        ca_certificate: str | None,
        namespace: str,
        payload: dict[str, Any],
        create_if_missing: bool,
        heartbeat: Callable[[], Awaitable[bool]],
    ) -> dict[str, Any]:
        """Run a workload once, or recover its existing Job after controller restart."""
        if not self._image:
            raise WorkloadExecutionError(
                "Execution Plane workload runner image is not configured",
                {
                    "error": "Execution Plane workload runner image is not configured",
                    "error_type": "ConfigurationError",
                },
            )
        parsed = urlsplit(endpoint)
        if parsed.scheme != "https" or not parsed.netloc:
            raise WorkloadExecutionError(
                "Execution target must use an HTTPS Kubernetes API endpoint",
                {
                    "error": "Execution target must use an HTTPS Kubernetes API endpoint",
                    "error_type": "ConfigurationError",
                },
            )

        job_name = f"ep-work-{work_item_id.hex}"
        secret_name = f"{job_name}-input"
        verify = ssl.create_default_context(cafile=self._ca_bundle)
        if ca_certificate:
            verify.load_verify_locations(cadata=ca_certificate)
        timeout = httpx.Timeout(connect=10, read=30, write=30, pool=10)
        async with httpx.AsyncClient(verify=verify, timeout=timeout) as client:
            client.headers["Authorization"] = f"Bearer {api_token}"
            base_url = endpoint.rstrip("/")
            job_url = f"{base_url}/apis/batch/v1/namespaces/{quote(namespace, safe='')}/jobs/{job_name}"

            if create_if_missing:
                if await heartbeat():
                    raise WorkloadCancelledError(
                        "Workload was cancelled before Kubernetes dispatch",
                        execution_started=False,
                    )
                try:
                    await self._create_network_policy(client, base_url, namespace, job_name, work_item_id)
                    await self._create_secret(client, base_url, namespace, secret_name, payload)
                    await self._create_job(client, base_url, namespace, job_name, secret_name, work_item_id, payload)
                except WorkloadExecutionError:
                    await self._cleanup_job_resources(client, base_url, namespace, job_name, secret_name)
                    raise
            else:
                try:
                    response = await client.get(job_url)
                except httpx.RequestError as exc:
                    raise WorkloadOutcomeUnknownError("Could not reconcile the previously dispatched workload") from exc
                if response.status_code == HTTPStatus.NOT_FOUND:
                    await self._cleanup_job_resources(client, base_url, namespace, job_name, secret_name)
                    raise WorkloadOutcomeUnknownError(
                        "Previously dispatched workload Job is missing; execution outcome is unknown"
                    )
                if response.is_error:
                    raise WorkloadOutcomeUnknownError("Could not observe the previously dispatched workload Job")

            return await self._wait_for_result(
                client=client,
                base_url=base_url,
                namespace=namespace,
                job_name=job_name,
                secret_name=secret_name,
                work_item_id=work_item_id,
                job_url=job_url,
                timeout_seconds=self._workload_timeout(payload),
                heartbeat=heartbeat,
            )

    async def _create_network_policy(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        namespace: str,
        job_name: str,
        work_item_id: UUID,
    ) -> None:
        """Deny workload ingress and limit egress to DNS plus operator-approved CIDRs."""
        labels = {"execution-plane.syntara.io/work-item": str(work_item_id)}
        allowed_networks = [
            ipaddress.ip_network(value, strict=False) for value in self._settings.workload_allowed_egress_cidrs
        ]
        forbidden_networks = [
            ipaddress.ip_network(value, strict=False) for value in self._settings.workload_forbidden_egress_cidrs
        ]
        egress_rules: list[dict[str, Any]] = [
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
                "ports": [
                    {"protocol": "UDP", "port": 53},
                    {"protocol": "TCP", "port": 53},
                ],
            }
        ]
        for network in allowed_networks:
            exclusions = []
            for denied in forbidden_networks:
                if isinstance(network, ipaddress.IPv4Network):
                    if isinstance(denied, ipaddress.IPv4Network) and denied.subnet_of(network):
                        exclusions.append(str(denied))
                elif isinstance(denied, ipaddress.IPv6Network) and denied.subnet_of(network):
                    exclusions.append(str(denied))
            ip_block: dict[str, Any] = {"cidr": str(network)}
            if exclusions:
                ip_block["except"] = exclusions
            egress_rules.append({"to": [{"ipBlock": ip_block}]})

        body = {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {
                "name": f"{job_name}-network",
                "labels": {"app.kubernetes.io/managed-by": "execution-plane"},
            },
            "spec": {
                "podSelector": {"matchLabels": labels},
                "policyTypes": ["Ingress", "Egress"],
                "ingress": [],
                "egress": egress_rules,
            },
        }
        url = f"{base_url}/apis/networking.k8s.io/v1/namespaces/{quote(namespace, safe='')}/networkpolicies"
        try:
            response = await client.post(url, json=body)
        except httpx.RequestError as exc:
            raise WorkloadOutcomeUnknownError("Could not confirm workload NetworkPolicy creation") from exc
        if response.status_code != HTTPStatus.CONFLICT:
            self._raise_for_known_api_error(response)

    async def _create_secret(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        namespace: str,
        secret_name: str,
        payload: dict[str, Any],
    ) -> None:
        """Create the immutable, job-scoped input Secret idempotently."""
        url = f"{base_url}/api/v1/namespaces/{quote(namespace, safe='')}/secrets"
        body = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": secret_name, "labels": {"app.kubernetes.io/managed-by": "execution-plane"}},
            "type": "Opaque",
            "stringData": {"workload.json": json.dumps(payload, separators=(",", ":"))},
        }
        try:
            response = await client.post(url, json=body)
        except httpx.RequestError as exc:
            raise WorkloadOutcomeUnknownError("Could not confirm workload input Secret creation") from exc
        if response.status_code == HTTPStatus.CONFLICT:
            return
        self._raise_for_known_api_error(response)

    async def _create_job(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        namespace: str,
        job_name: str,
        secret_name: str,
        work_item_id: UUID,
        payload: dict[str, Any],
    ) -> None:
        """Create a constrained workload pod with no service-account token."""
        timeout = self._workload_timeout(payload)
        labels = {
            "app.kubernetes.io/name": "execution-plane-workload",
            "app.kubernetes.io/managed-by": "execution-plane",
            "execution-plane.syntara.io/work-item": str(work_item_id),
        }
        body = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": job_name, "labels": labels},
            "spec": {
                "backoffLimit": 0,
                "activeDeadlineSeconds": timeout + 30,
                "ttlSecondsAfterFinished": JOB_RETENTION_SECONDS,
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "restartPolicy": "Never",
                        "securityContext": {
                            "runAsNonRoot": True,
                            "seccompProfile": {"type": "RuntimeDefault"},
                        },
                        "containers": [
                            {
                                "name": "runner",
                                "image": self._image,
                                "imagePullPolicy": "IfNotPresent",
                                "command": [
                                    "python3",
                                    "-m",
                                    "execution_plane.workload_runner",
                                    "/run/ep/workload.json",
                                ],
                                "resources": {
                                    "requests": {
                                        "cpu": self._settings.workload_runner_cpu_request,
                                        "memory": self._settings.workload_runner_memory_request,
                                    },
                                    "limits": {
                                        "cpu": self._settings.workload_runner_cpu_limit,
                                        "memory": self._settings.workload_runner_memory_limit,
                                    },
                                },
                                "securityContext": {
                                    "allowPrivilegeEscalation": False,
                                    "readOnlyRootFilesystem": True,
                                    "capabilities": {"drop": ["ALL"]},
                                },
                                "env": [
                                    {"name": "HOME", "value": "/tmp"},  # noqa: S108
                                    {"name": "TMPDIR", "value": "/tmp"},  # noqa: S108
                                ],
                                "volumeMounts": [
                                    {"name": "workload-input", "mountPath": "/run/ep", "readOnly": True},
                                    {"name": "tmp", "mountPath": "/tmp"},  # noqa: S108
                                ],
                            }
                        ],
                        "volumes": [
                            {
                                "name": "workload-input",
                                "secret": {"secretName": secret_name, "defaultMode": 0o444},
                            },
                            {"name": "tmp", "emptyDir": {"sizeLimit": "64Mi"}},
                        ],
                    },
                },
            },
        }
        url = f"{base_url}/apis/batch/v1/namespaces/{quote(namespace, safe='')}/jobs"
        try:
            response = await client.post(url, json=body)
        except httpx.RequestError as exc:
            raise WorkloadOutcomeUnknownError("Could not confirm workload Job creation") from exc
        if response.status_code == HTTPStatus.CONFLICT:
            return
        self._raise_for_known_api_error(response)

    async def _wait_for_result(
        self,
        *,
        client: httpx.AsyncClient,
        base_url: str,
        namespace: str,
        job_name: str,
        secret_name: str,
        work_item_id: UUID,
        job_url: str,
        timeout_seconds: int,
        heartbeat: Callable[[], Awaitable[bool]],
    ) -> dict[str, Any]:
        deadline = asyncio.get_running_loop().time() + timeout_seconds + 60
        while True:
            cancellation_requested = await heartbeat()
            try:
                response = await client.get(job_url)
            except httpx.RequestError as exc:
                raise WorkloadOutcomeUnknownError("Could not observe workload Job state") from exc
            if response.status_code == HTTPStatus.NOT_FOUND or response.is_error:
                raise WorkloadOutcomeUnknownError("Could not observe workload Job state")
            status = response.json().get("status", {})
            if status.get("succeeded", 0) > 0:
                logs = await self._read_job_logs(client, base_url, namespace, work_item_id)
                await self._cleanup_job_resources(client, base_url, namespace, job_name, secret_name)
                return self._decode_result(logs)
            if status.get("failed", 0) > 0:
                logs = ""
                with contextlib.suppress(WorkloadOutcomeUnknownError):
                    logs = await self._read_job_logs(client, base_url, namespace, work_item_id)
                await self._cleanup_job_resources(client, base_url, namespace, job_name, secret_name)
                raise WorkloadExecutionError(
                    "Isolated workload pod failed before returning a result",
                    {
                        "error": (
                            "Isolated workload pod failed before returning a result; side-effect outcome may be unknown"
                        ),
                        "error_type": "WorkloadOutcomeUnknownError",
                        "stderr": logs[:4096],
                    },
                )
            if cancellation_requested:
                terminal_result = await self._stop_job(
                    client=client,
                    base_url=base_url,
                    namespace=namespace,
                    job_name=job_name,
                    work_item_id=work_item_id,
                )
                await self._cleanup_job_resources(client, base_url, namespace, job_name, secret_name)
                if terminal_result is not None:
                    return terminal_result
                raise WorkloadCancelledError(
                    "Workload cancellation was confirmed by Kubernetes",
                    execution_started=True,
                )
            if asyncio.get_running_loop().time() >= deadline:
                terminal_result = await self._stop_job(
                    client=client,
                    base_url=base_url,
                    namespace=namespace,
                    job_name=job_name,
                    work_item_id=work_item_id,
                )
                await self._cleanup_job_resources(client, base_url, namespace, job_name, secret_name)
                if terminal_result is not None:
                    return terminal_result
                raise WorkloadExecutionError(
                    "Isolated workload exceeded its execution deadline",
                    {"error": "Isolated workload exceeded its execution deadline", "error_type": "TimeoutError"},
                )
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    async def _stop_job(  # noqa: C901, PLR0912
        self,
        *,
        client: httpx.AsyncClient,
        base_url: str,
        namespace: str,
        job_name: str,
        work_item_id: UUID,
    ) -> dict[str, Any] | None:
        """Delete a running Job and wait for its Job and owned pods to disappear.

        Return a result if completion won the race; return None only after the
        controller has observed foreground deletion of the Job and its pods.
        """
        namespace_path = f"/namespaces/{quote(namespace, safe='')}"
        job_url = f"{base_url}/apis/batch/v1{namespace_path}/jobs/{quote(job_name, safe='')}"
        delete_url = f"{job_url}?propagationPolicy=Foreground&gracePeriodSeconds=5"
        try:
            response = await client.delete(delete_url)
        except httpx.RequestError as exc:
            raise WorkloadOutcomeUnknownError("Could not confirm workload termination") from exc
        if response.status_code == HTTPStatus.NOT_FOUND:
            raise WorkloadOutcomeUnknownError("Workload Job disappeared before termination was confirmed")
        if response.is_error:
            raise WorkloadOutcomeUnknownError("Kubernetes rejected workload termination")

        if response.content:
            with contextlib.suppress(ValueError):
                body = response.json()
                job_status = body.get("status", {}) if isinstance(body, dict) else {}
                if not isinstance(job_status, dict):
                    job_status = {}
                terminal_result = await self._result_if_terminal(client, base_url, namespace, job_status, work_item_id)
                if terminal_result is not None:
                    return terminal_result

        deadline = asyncio.get_running_loop().time() + 60
        pod_list_url = f"{base_url}/api/v1{namespace_path}/pods?labelSelector=" + quote(
            f"execution-plane.syntara.io/work-item={work_item_id}", safe="="
        )
        while asyncio.get_running_loop().time() < deadline:
            try:
                job_response = await client.get(job_url)
                pods_response = await client.get(pod_list_url)
            except httpx.RequestError as exc:
                raise WorkloadOutcomeUnknownError("Could not verify workload termination") from exc
            if job_response.status_code == HTTPStatus.NOT_FOUND:
                if pods_response.is_error:
                    raise WorkloadOutcomeUnknownError("Could not verify workload pod termination")
                if not pods_response.json().get("items", []):
                    return None
            else:
                if job_response.is_error or pods_response.is_error:
                    raise WorkloadOutcomeUnknownError("Could not verify workload termination")
                terminal_result = await self._result_if_terminal(
                    client,
                    base_url,
                    namespace,
                    job_response.json().get("status", {}),
                    work_item_id,
                )
                if terminal_result is not None:
                    return terminal_result
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
        raise WorkloadOutcomeUnknownError("Kubernetes did not confirm workload termination before the deadline")

    async def _result_if_terminal(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        namespace: str,
        status: dict[str, Any],
        work_item_id: UUID,
    ) -> dict[str, Any] | None:
        """Recover a completed result if workload completion wins a cancel race."""
        if status.get("succeeded", 0) > 0:
            logs = await self._read_job_logs(client, base_url, namespace, work_item_id)
            return self._decode_result(logs)
        if status.get("failed", 0) > 0:
            logs = ""
            with contextlib.suppress(WorkloadOutcomeUnknownError):
                logs = await self._read_job_logs(client, base_url, namespace, work_item_id)
            raise WorkloadExecutionError(
                "Isolated workload pod failed before returning a result",
                {
                    "error": (
                        "Isolated workload pod failed before returning a result; side-effect outcome may be unknown"
                    ),
                    "error_type": "WorkloadOutcomeUnknownError",
                    "stderr": logs[:4096],
                },
            )
        return None

    async def _cleanup_job_resources(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        namespace: str,
        job_name: str,
        secret_name: str,
    ) -> None:
        """Remove the job-scoped credential Secret and policy after execution ends."""
        namespace_path = f"/namespaces/{quote(namespace, safe='')}"
        policy_name = quote(f"{job_name}-network", safe="")
        urls = (
            f"{base_url}/api/v1{namespace_path}/secrets/{quote(secret_name, safe='')}",
            f"{base_url}/apis/networking.k8s.io/v1{namespace_path}/networkpolicies/{policy_name}",
        )
        for url in urls:
            with contextlib.suppress(httpx.RequestError):
                response = await client.delete(url)
                if response.status_code not in {HTTPStatus.OK, HTTPStatus.ACCEPTED, HTTPStatus.NOT_FOUND}:
                    logger.warning("Could not clean up workload resource", status_code=response.status_code)

    async def _read_job_logs(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        namespace: str,
        work_item_id: UUID,
    ) -> str:
        """Read bounded stdout from the pod selected by its immutable work-item label."""
        query = quote(f"execution-plane.syntara.io/work-item={work_item_id}", safe="=")
        pod_list_url = f"{base_url}/api/v1/namespaces/{quote(namespace, safe='')}/pods?labelSelector={query}"
        try:
            pods_response = await client.get(pod_list_url)
        except httpx.RequestError as exc:
            raise WorkloadOutcomeUnknownError("Could not locate the workload result pod") from exc
        if pods_response.is_error:
            raise WorkloadOutcomeUnknownError("Could not list workload pods for result retrieval")
        pods = pods_response.json().get("items", [])
        if not pods:
            raise WorkloadOutcomeUnknownError("Workload Job is complete but its result pod is not available")
        pod_name = pods[0].get("metadata", {}).get("name")
        if not pod_name:
            raise WorkloadOutcomeUnknownError("Workload result pod has no Kubernetes name")
        logs_url = (
            f"{base_url}/api/v1/namespaces/{quote(namespace, safe='')}/pods/"
            f"{quote(pod_name, safe='')}/log?limitBytes={MAX_JOB_LOG_BYTES}"
        )
        try:
            response = await client.get(logs_url)
        except httpx.RequestError as exc:
            raise WorkloadOutcomeUnknownError("Could not read the workload result") from exc
        if response.is_error:
            raise WorkloadOutcomeUnknownError("Could not read the workload result")
        return response.text

    @staticmethod
    def _decode_result(logs: str) -> dict[str, Any]:
        lines = [line for line in logs.splitlines() if line.strip()]
        if not lines:
            raise WorkloadOutcomeUnknownError("Workload completed without a result envelope")
        try:
            result = json.loads(lines[-1])
        except json.JSONDecodeError as exc:
            raise WorkloadOutcomeUnknownError("Workload result envelope is invalid") from exc
        if not isinstance(result, dict) or not isinstance(result.get("result"), dict):
            raise WorkloadOutcomeUnknownError("Workload result envelope has an invalid shape")
        if result.get("status") == "completed":
            return cast("dict[str, Any]", result["result"])
        if result.get("status") == "failed":
            failure = result["result"]
            raise WorkloadExecutionError(str(failure.get("error", "Script execution failed")), failure)
        raise WorkloadOutcomeUnknownError("Workload result has an unsupported status")

    @staticmethod
    def _workload_timeout(payload: dict[str, Any]) -> int:
        input_config = payload.get("input_config")
        if not isinstance(input_config, dict):
            return 300
        try:
            return max(1, int(input_config.get("_engine_timeout_seconds", 300)))
        except (TypeError, ValueError):
            return 300

    @staticmethod
    def _raise_for_known_api_error(response: httpx.Response) -> None:
        if response.status_code >= HTTPStatus.BAD_REQUEST and response.status_code != HTTPStatus.CONFLICT:
            # A response proves the API rejected the operation; do not include its body,
            # which can contain cluster details or echoed request data.
            if response.status_code in {
                HTTPStatus.BAD_REQUEST,
                HTTPStatus.UNAUTHORIZED,
                HTTPStatus.FORBIDDEN,
                HTTPStatus.NOT_FOUND,
                HTTPStatus.UNPROCESSABLE_ENTITY,
            }:
                raise WorkloadExecutionError(
                    f"Kubernetes API rejected workload management ({response.status_code})",
                    {
                        "error": f"Kubernetes API rejected workload management ({response.status_code})",
                        "error_type": "WorkloadManagementError",
                    },
                )
            msg = f"Kubernetes API returned {response.status_code} during workload management"
            raise WorkloadOutcomeUnknownError(msg)
