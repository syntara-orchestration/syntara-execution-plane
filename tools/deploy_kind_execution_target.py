#!/usr/bin/env python3
"""Create a local kind cluster and register it as an Execution Plane target.

AO registers compute using a cluster command; the EP worker reconciles its binding into a
Cluster plus a default vanilla-Kubernetes ExecutionTarget.

This script:

1. Creates a kind cluster (or reuses one with the same name).
2. Applies deploy/kubernetes/execution-target/rbac.yaml (syntara-dispatcher).
3. Attaches the running EP worker container to the kind network so the
   in-cluster API IP is reachable from podman-compose.
4. Upserts the cluster binding against the local EP API.

Usage:
    make setup && uvx podman-compose up --build -d
    AO_ADMIN_PASSWORD=<password> uv run python tools/deploy_kind_execution_target.py \
      --create-ao-credential --ao-insecure

    # Then list targets:
    TOKEN=$(uv run python tools/generate_jwt_for_ep.py)
    curl -k -H "Authorization: Bearer $TOKEN" https://127.0.0.1:8001/v1/execution-targets
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import ssl
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import UTC, datetime
from http import HTTPStatus
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from execution_plane.work_item_client import ep_service_token
from execution_plane_client.protocol import Command, CommandScope, MessageEnvelope

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CLUSTER_NAME = "ep-kind"
DEFAULT_NAMESPACE = "execution-plane"
DEFAULT_BINDING_NAME = "kind-local"
DEFAULT_API_URL = "https://127.0.0.1:8001"
DEFAULT_AO_API_URL = "https://127.0.0.1:8443"
DEFAULT_WAIT_SECONDS = 90
DEFAULT_SERVICE_ACCOUNT = "syntara-dispatcher"
KIND_NETWORK = "kind"
CONTROL_PLANE_PORT = 6443
EP_CA_PATH = PROJECT_ROOT / ".secrets" / "certs" / "ca.pem"
WORKLOAD_RBAC_PATH = PROJECT_ROOT / "deploy" / "kubernetes" / "execution-target" / "rbac.yaml"
CREATE_AO_CREDENTIAL_PATH = PROJECT_ROOT / "tools" / "create_ao_credential.py"


def _run(
    args: list[str],
    *,
    input_text: str | None = None,
    check: bool = True,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a host command and return stdout/stderr as text."""
    return subprocess.run(args, check=check, capture_output=True, text=True, input=input_text, env=env)


def _require_binaries() -> None:
    """Fail fast when kind or kubectl is missing."""
    missing = [name for name in ("kind", "kubectl") if shutil.which(name) is None]
    if missing:
        msg = f"Missing required command(s): {', '.join(missing)}"
        raise FileNotFoundError(msg)


def _configure_kind_provider() -> None:
    """Use podman when Docker is absent, matching this machine's local-dev stack."""
    if os.environ.get("KIND_EXPERIMENTAL_PROVIDER"):
        return
    if shutil.which("docker") is None and shutil.which("podman") is not None:
        os.environ["KIND_EXPERIMENTAL_PROVIDER"] = "podman"


def _container_engine() -> str:
    """Return the engine kind is using."""
    if os.environ.get("KIND_EXPERIMENTAL_PROVIDER") == "podman" or shutil.which("docker") is None:
        return "podman"
    return "docker"


def _kind_clusters() -> set[str]:
    result = _run(["kind", "get", "clusters"], check=False)
    if result.returncode != 0:
        return set()
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def ensure_kind_cluster(name: str, *, recreate: bool) -> None:
    """Create the kind cluster, or replace it when --recreate is set."""
    exists = name in _kind_clusters()
    if exists and recreate:
        print(f"[INFO] Deleting kind cluster {name}")
        _run(["kind", "delete", "cluster", "--name", name])
        exists = False
    if exists:
        print(f"[INFO] Reusing kind cluster {name}")
        return
    print(f"[INFO] Creating kind cluster {name}")
    _run(["kind", "create", "cluster", "--name", name, "--wait", "120s"])


def export_kubeconfig(cluster_name: str, kubeconfig: Path) -> None:
    """Write a cluster-scoped kubeconfig without replacing the default file."""
    _run(["kind", "export", "kubeconfig", "--name", cluster_name, "--kubeconfig", str(kubeconfig)])


def kubectl(
    kubeconfig: Path,
    *args: str,
    input_text: str | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run kubectl against the kind kubeconfig."""
    return _run(["kubectl", "--kubeconfig", str(kubeconfig), *args], input_text=input_text, check=check)


def apply_workload_rbac(kubeconfig: Path, namespace: str) -> None:
    """Install the namespace and the dispatcher Role from the shared target manifest."""
    if not WORKLOAD_RBAC_PATH.is_file():
        msg = f"Workload RBAC manifest not found: {WORKLOAD_RBAC_PATH}"
        raise FileNotFoundError(msg)
    print(f"[INFO] Applying {WORKLOAD_RBAC_PATH} in namespace {namespace}")
    manifest = WORKLOAD_RBAC_PATH.read_text()
    if namespace != DEFAULT_NAMESPACE:
        manifest = manifest.replace(DEFAULT_NAMESPACE, namespace)
    kubectl(kubeconfig, "apply", "-f", "-", input_text=manifest)


def create_ao_credential(
    *,
    kubeconfig: Path,
    cluster_name: str,
    namespace: str,
    api_url: str,
    admin_password: str,
    insecure: bool,
) -> uuid.UUID:
    """Create or refresh the AO credential for the Kind dispatcher token."""
    token_result = kubectl(
        kubeconfig,
        "create",
        "token",
        DEFAULT_SERVICE_ACCOUNT,
        "--namespace",
        namespace,
        "--duration",
        "12h",
    )
    environment = dict(os.environ)
    environment["AO_ADMIN_PASSWORD"] = admin_password
    environment["AO_CREDENTIAL_TOKEN"] = token_result.stdout.strip()
    arguments = [
        sys.executable,
        str(CREATE_AO_CREDENTIAL_PATH),
        "--api-url",
        api_url,
        "--cluster-name",
        cluster_name,
    ]
    if insecure:
        arguments.append("--insecure")
    result = _run(arguments, env=environment)
    return uuid.UUID(result.stdout.strip())


def cluster_ca_pem(kubeconfig: Path) -> str:
    """Return the Kubernetes API CA as PEM text for EP cluster TLS verification."""
    result = kubectl(
        kubeconfig,
        "config",
        "view",
        "--raw",
        "-o",
        "jsonpath={.clusters[0].cluster.certificate-authority-data}",
    )
    encoded = result.stdout.strip()
    if not encoded:
        msg = "kind kubeconfig has no certificate-authority-data"
        raise RuntimeError(msg)
    return base64.b64decode(encoded).decode("ascii")


def node_internal_ip(kubeconfig: Path) -> str:
    """Return the kind control-plane InternalIP (present on the API serving cert)."""
    result = kubectl(
        kubeconfig,
        "get",
        "nodes",
        "-o",
        "jsonpath={.items[0].status.addresses[?(@.type=='InternalIP')].address}",
    )
    ip = result.stdout.strip()
    if not ip:
        msg = "Could not read the kind node InternalIP"
        raise RuntimeError(msg)
    return ip


def connect_worker_to_kind_network() -> str | None:
    """Attach the compose worker to the kind network so it can reach the API IP."""
    engine = _container_engine()
    if shutil.which(engine) is None:
        print(f"[WARN] {engine} not found; skip connecting the EP worker to the kind network")
        return None
    listing = _run([engine, "ps", "--format", "{{.Names}}"], check=False)
    workers = [name for name in listing.stdout.splitlines() if "worker" in name and "execution-plane" in name]
    if not workers:
        print("[WARN] No running EP worker container found; start compose before submitting work")
        return None
    worker = workers[0]
    connected = _run([engine, "network", "connect", KIND_NETWORK, worker], check=False)
    if connected.returncode == 0:
        print(f"[INFO] Connected {worker} to the {KIND_NETWORK} network")
    elif (
        "already exists" in (connected.stderr + connected.stdout).lower()
        or "already connected" in (connected.stderr + connected.stdout).lower()
    ):
        print(f"[INFO] {worker} is already on the {KIND_NETWORK} network")
    else:
        print(f"[WARN] Could not connect {worker} to {KIND_NETWORK}: {connected.stderr.strip()}")
    return worker


def _ssl_context() -> ssl.SSLContext:
    ca_path = Path(os.environ.get("EP_API_CA_FILE", str(EP_CA_PATH)))
    if not ca_path.is_file():
        msg = f"EP API CA not found at {ca_path}; set EP_API_CA_FILE or generate certificates"
        raise FileNotFoundError(msg)
    return ssl.create_default_context(cafile=str(ca_path))


def ep_request(method: str, url: str, token: str, body: dict[str, Any] | None = None) -> tuple[int, Any]:
    """Call the local EP API and return (status, JSON-or-text)."""
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = Request(  # noqa: S310
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )
    try:
        with urlopen(request, context=_ssl_context(), timeout=15) as response:  # noqa: S310
            payload = response.read().decode("utf-8")
            parsed: Any = json.loads(payload) if payload else None
            return response.status, parsed
    except HTTPError as exc:
        payload = exc.read().decode("utf-8")
        try:
            parsed = json.loads(payload) if payload else {"detail": str(exc)}
        except json.JSONDecodeError:
            parsed = {"detail": payload or str(exc)}
        return exc.code, parsed


def wait_for_ep_api(api_url: str) -> None:
    """Block until the local API answers readiness."""
    url = f"{api_url.rstrip('/')}/healthz/ready"
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            request = Request(url, method="GET")  # noqa: S310
            with urlopen(request, context=_ssl_context(), timeout=5) as response:  # noqa: S310
                if response.status == HTTPStatus.OK:
                    return
        except (HTTPError, URLError, TimeoutError, ssl.SSLError):
            time.sleep(1)
    msg = f"Execution Plane API is not ready at {url}"
    raise TimeoutError(msg)


def next_revision(api_url: str, token: str, source_integration_id: uuid.UUID) -> int:
    """Return the next cluster-binding revision for this local kind identity."""
    status, body = ep_request("GET", f"{api_url}/v1/clusters/{source_integration_id}", token)
    if status == HTTPStatus.NOT_FOUND:
        return 1
    if status >= HTTPStatus.BAD_REQUEST:
        msg = f"Could not read cluster binding: {body}"
        raise RuntimeError(msg)
    return int(body["desired_revision"]) + 1


def upsert_binding(
    api_url: str,
    token: str,
    source_integration_id: uuid.UUID,
    *,
    name: str,
    endpoint: str,
    namespace: str,
    credential_ref: uuid.UUID,
    ca_certificate: str,
) -> dict[str, Any]:
    """Submit versioned desired state for the kind cluster."""
    revision = next_revision(api_url, token, source_integration_id)
    body = {
        "resource_key": str(source_integration_id),
        "desired_revision": revision,
        "name": name,
        "endpoint": endpoint,
        "namespace": namespace,
        "credential_ref": str(credential_ref),
        "ca_certificate": ca_certificate,
        "enabled": True,
        "is_default": True,
        "labels": {"execution-plane.syntara.io/local": "kind"},
    }
    print(f"[INFO] Upserting cluster binding {source_integration_id} revision {revision}")
    command_id = uuid.uuid4()
    client_id = os.environ.get("EP_AO_CLIENT_ID", "syntara-orchestration")
    envelope = MessageEnvelope(
        message_id=uuid.uuid4(),
        command_id=command_id,
        source_id=client_id,
        destination_id=os.environ.get("EP_SERVICE_JWT_AUDIENCE", "execution-plane"),
        scope=CommandScope(client_id=client_id),
        command=Command(
            command_id=command_id,
            command_type="cluster.provision",
            command_revision=1,
            occurred_at=datetime.now(UTC),
            payload=body,
        ),
    )
    status, payload = ep_request("POST", f"{api_url}/v1/commands", token, envelope.model_dump(mode="json"))
    if status >= HTTPStatus.BAD_REQUEST:
        msg = f"Cluster binding upsert failed ({status}): {payload}"
        raise RuntimeError(msg)
    if not isinstance(payload, dict):
        msg = f"Unexpected cluster command response: {payload}"
        raise TypeError(msg)
    return payload


def wait_for_binding(api_url: str, token: str, source_integration_id: uuid.UUID, timeout: int) -> dict[str, Any]:
    """Poll until the worker has reconciled the binding to ready."""
    deadline = time.time() + timeout
    url = f"{api_url}/v1/clusters/{source_integration_id}"
    while time.time() < deadline:
        status, payload = ep_request("GET", url, token)
        if status == HTTPStatus.OK:
            if not isinstance(payload, dict):
                msg = f"Unexpected cluster response: {payload}"
                raise TypeError(msg)
            binding_status = payload.get("status")
            print(f"[INFO] Cluster binding status: {binding_status}")
            if binding_status == "ready":
                return payload
            if binding_status == "error":
                msg = payload.get("status_message") or "Cluster binding reconciliation failed"
                raise RuntimeError(msg)
        time.sleep(3)
    msg = f"Timed out waiting for cluster binding {source_integration_id} to become ready"
    raise TimeoutError(msg)


def list_execution_targets(api_url: str, token: str) -> list[dict[str, Any]]:
    """Return registered execution targets visible to the service token."""
    status, payload = ep_request("GET", f"{api_url}/v1/execution-targets", token)
    if status >= HTTPStatus.BAD_REQUEST:
        msg = f"Listing execution targets failed ({status}): {payload}"
        raise RuntimeError(msg)
    resources = payload.get("resources") if isinstance(payload, dict) else None
    if not isinstance(resources, list):
        msg = f"Unexpected execution-target response: {payload}"
        raise TypeError(msg)
    return resources


def source_integration_id(cluster_name: str) -> uuid.UUID:
    """Stable identity so re-runs update the same binding instead of creating another."""
    return uuid.uuid5(uuid.NAMESPACE_URL, f"https://execution-plane.local/kind/{cluster_name}")


def register_kind_binding(
    *,
    api_url: str,
    cluster_name: str,
    binding_name: str,
    endpoint: str,
    namespace: str,
    credential_ref: uuid.UUID,
    ca_certificate: str,
    wait_seconds: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Upsert the cluster binding and wait until EP reports a ready target."""
    wait_for_ep_api(api_url)
    token = ep_service_token()
    integration_id = source_integration_id(cluster_name)
    upsert_binding(
        api_url,
        token,
        integration_id,
        name=binding_name,
        endpoint=endpoint,
        namespace=namespace,
        credential_ref=credential_ref,
        ca_certificate=ca_certificate,
    )
    binding = wait_for_binding(api_url, token, integration_id, wait_seconds)
    return binding, list_execution_targets(api_url, token)


def _parse_args() -> argparse.Namespace:
    """Parse command-line options for the local Kind target workflow."""
    parser = argparse.ArgumentParser(
        description="Deploy a kind cluster and register it as an Execution Plane target",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--cluster-name", default=DEFAULT_CLUSTER_NAME)
    parser.add_argument(
        "--namespace",
        default=DEFAULT_NAMESPACE,
        help="Workload namespace (rewritten into the shared RBAC manifest when not the default)",
    )
    parser.add_argument("--binding-name", default=DEFAULT_BINDING_NAME)
    parser.add_argument("--api-url", default=os.environ.get("EP_API_URL", DEFAULT_API_URL))
    parser.add_argument(
        "--credential-ref",
        type=uuid.UUID,
        default=uuid.UUID(os.environ["EP_KIND_CREDENTIAL_REF"]) if os.environ.get("EP_KIND_CREDENTIAL_REF") else None,
        help="AO Credential UUID that grants access to this kind cluster",
    )
    parser.add_argument("--create-ao-credential", action="store_true")
    parser.add_argument("--ao-api-url", default=os.environ.get("AO_API_BASE", DEFAULT_AO_API_URL))
    parser.add_argument("--ao-admin-password-file", type=Path)
    parser.add_argument("--ao-insecure", action="store_true")
    parser.add_argument("--wait-seconds", type=int, default=DEFAULT_WAIT_SECONDS)
    parser.add_argument("--recreate", action="store_true", help="Delete and recreate the kind cluster")
    parser.add_argument("--host-worker", action="store_true", help="Use the host endpoint for AO's make dev")
    return parser.parse_args()


def _ao_admin_password(args: argparse.Namespace) -> str:
    """Read the AO admin password without placing it on the command line."""
    if args.ao_admin_password_file is not None:
        password = args.ao_admin_password_file.read_text(encoding="utf-8").strip()
    else:
        password = os.environ.get("AO_ADMIN_PASSWORD", "")
    if not password:
        msg = "--ao-admin-password-file or AO_ADMIN_PASSWORD is required with --create-ao-credential"
        raise RuntimeError(msg)
    return password


def main() -> int:
    """Create kind, register the cluster binding, and wait until the target is ready."""
    args = _parse_args()
    if args.credential_ref is None and not args.create_ao_credential:
        msg = "--credential-ref, EP_KIND_CREDENTIAL_REF, or --create-ao-credential is required"
        raise RuntimeError(msg)
    admin_password = _ao_admin_password(args) if args.create_ao_credential and args.credential_ref is None else ""

    kubeconfig: Path | None = None
    try:
        _require_binaries()
        _configure_kind_provider()
        ensure_kind_cluster(args.cluster_name, recreate=args.recreate)
        with tempfile.NamedTemporaryFile(prefix=f"kubeconfig-{args.cluster_name}-", delete=False) as handle:
            kubeconfig = Path(handle.name)
        export_kubeconfig(args.cluster_name, kubeconfig)
        apply_workload_rbac(kubeconfig, args.namespace)
        if args.credential_ref is None:
            args.credential_ref = create_ao_credential(
                kubeconfig=kubeconfig,
                cluster_name=args.cluster_name,
                namespace=args.namespace,
                api_url=args.ao_api_url,
                admin_password=admin_password,
                insecure=args.ao_insecure,
            )
        ca_certificate = cluster_ca_pem(kubeconfig)
        endpoint = f"https://{node_internal_ip(kubeconfig)}:{CONTROL_PLANE_PORT}"
        if args.host_worker:
            endpoint = kubectl(
                kubeconfig, "config", "view", "-o", "jsonpath={.clusters[0].cluster.server}"
            ).stdout.strip()
        else:
            connect_worker_to_kind_network()
        print(f"[INFO] Kind API endpoint for the EP worker: {endpoint}")
        binding, targets = register_kind_binding(
            api_url=args.api_url,
            cluster_name=args.cluster_name,
            binding_name=args.binding_name,
            endpoint=endpoint,
            namespace=args.namespace,
            credential_ref=args.credential_ref,
            ca_certificate=ca_certificate,
            wait_seconds=args.wait_seconds,
        )
    except (FileNotFoundError, RuntimeError, TimeoutError, TypeError, subprocess.CalledProcessError) as exc:
        if isinstance(exc, subprocess.CalledProcessError):
            detail = (exc.stderr or exc.stdout or "").strip() or str(exc)
            print(f"Error: command failed: {' '.join(exc.cmd)}\n{detail}", file=sys.stderr)
        else:
            print(f"Error: {exc}", file=sys.stderr)
        return 1
    finally:
        if kubeconfig is not None:
            kubeconfig.unlink(missing_ok=True)

    print()
    print(f"[INFO] Cluster binding {binding['id']} is {binding['status']}")
    print(f"[INFO] Cluster id: {binding.get('cluster_id')}")
    print("[INFO] Execution targets:")
    print(json.dumps(targets, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
