"""HTTP client for submitting and polling Execution Plane work items.

Used by local-dev tools and ``tests/integration``. This is not part of the
service runtime; it talks to a running API over HTTPS.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import jwt

from execution_plane_client.commands.work import WorkExecute

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_NODE_IMAGE = "localhost/syntara-sdk-script:dev"
_REQUEST_TIMEOUT_SECONDS = 15.0
_POLL_PAUSE_SECONDS = 2.0
_PRIVATE_KEY_CANDIDATES = (
    _REPO_ROOT / ".secrets" / "jwt-primary.pem",
    _REPO_ROOT.parent / "syntara" / "backend" / ".secrets" / "jwt-primary.pem",
)

DEFAULT_JWT_ISSUER = "http://localhost:8000"
DEFAULT_JWT_AUDIENCE = "execution-plane"
DEFAULT_JWT_CLIENT_ID = "syntara-orchestration"
DEFAULT_TOKEN_TTL = timedelta(hours=8)
DEFAULT_TOKEN_SCOPES = (
    "work-items:read",
    "work.execute",
    "command.cancel",
    "execution-targets:read",
    "clusters:read",
    "cluster.register",
    "cluster.provision",
    "cluster.update",
    "cluster.delete",
    "execution_target.register",
    "execution_target.update",
    "execution_target.delete",
    "capabilities:read",
)

DEFAULT_API_URL = "https://127.0.0.1:8001"
DEFAULT_PROJECT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
DEFAULT_NODE_IMAGE = _DEFAULT_NODE_IMAGE
TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled", "reconciliation_required"})


def default_node_image() -> str:
    """Return ``EP_NODE_IMAGE`` or the local-dev script node image."""
    return os.environ.get("EP_NODE_IMAGE", _DEFAULT_NODE_IMAGE)


def default_api_url() -> str:
    """Return ``EP_API_URL`` or the compose publish address."""
    return os.environ.get("EP_API_URL", DEFAULT_API_URL)


def _ca_path() -> Path:
    env_path = os.environ.get("EP_API_CA_FILE")
    if env_path:
        return Path(env_path)
    return _REPO_ROOT / ".secrets" / "certs" / "ca.pem"


def resolve_private_key_path(explicit: Path | None = None) -> Path:
    """Prefer an explicit path, then ``EP_AO_JWT_PRIVATE_KEY_FILE``, then local keys."""
    if explicit is not None:
        return explicit
    env_path = os.environ.get("EP_AO_JWT_PRIVATE_KEY_FILE")
    if env_path:
        return Path(env_path)
    for candidate in _PRIVATE_KEY_CANDIDATES:
        if candidate.is_file():
            return candidate
    searched = ", ".join(str(path) for path in _PRIVATE_KEY_CANDIDATES)
    msg = (
        "AO JWT private key not found. Pass private_key, set "
        f"EP_AO_JWT_PRIVATE_KEY_FILE, or place jwt-primary.pem at one of: {searched}"
    )
    raise FileNotFoundError(msg)


def service_token_claims(
    project_id: uuid.UUID | None = None,
    *,
    all_projects: bool | None = None,
    issuer: str | None = None,
    audience: str | None = None,
    client_id: str | None = None,
    scopes: Sequence[str] | None = None,
    now: datetime | None = None,
    ttl: timedelta | None = None,
) -> dict[str, object]:
    """Return JWT claims accepted by ``execution_plane.api.auth``."""
    grant_all_projects = project_id is None if all_projects is None else all_projects
    if project_id is None and not grant_all_projects:
        msg = "Token needs a project_id or all_projects"
        raise ValueError(msg)
    issued_at = now or datetime.now(UTC)
    claims: dict[str, object] = {
        "iss": issuer or os.environ.get("EP_AO_JWT_ISSUER", DEFAULT_JWT_ISSUER),
        "aud": audience or os.environ.get("EP_SERVICE_JWT_AUDIENCE", DEFAULT_JWT_AUDIENCE),
        "client_id": client_id or os.environ.get("EP_AO_CLIENT_ID", DEFAULT_JWT_CLIENT_ID),
        "scope": " ".join(scopes if scopes is not None else DEFAULT_TOKEN_SCOPES),
        "iat": issued_at,
        "exp": issued_at + (ttl or DEFAULT_TOKEN_TTL),
    }
    if project_id is not None:
        claims["project_id"] = str(project_id)
    if grant_all_projects:
        claims["all_projects"] = True
    return claims


def ep_service_token(
    project_id: uuid.UUID | None = None,
    *,
    all_projects: bool | None = None,
    private_key: Path | None = None,
    issuer: str | None = None,
    audience: str | None = None,
    client_id: str | None = None,
    scopes: Sequence[str] | None = None,
    now: datetime | None = None,
    ttl: timedelta | None = None,
) -> str:
    """Mint an AO service JWT for local Execution Plane API calls.

    Omit ``project_id`` to issue an ``all_projects`` token. A project UUID is
    optional caller metadata; EP authorizes commands and reads by client ID.
    """
    key_path = resolve_private_key_path(private_key)
    claims = service_token_claims(
        project_id,
        all_projects=all_projects,
        issuer=issuer,
        audience=audience,
        client_id=client_id,
        scopes=scopes,
        now=now,
        ttl=ttl,
    )
    token = jwt.encode(claims, key_path.read_text(encoding="utf-8"), algorithm="ES256")
    if not token:
        msg = "Failed to mint an AO service token"
        raise RuntimeError(msg)
    return token


def ep_request(method: str, url: str, token: str, body: dict[str, Any] | None = None) -> tuple[int, Any]:
    """Call the local EP API and return ``(status, JSON-or-None)``."""
    ca_path = _ca_path()
    if not ca_path.is_file():
        msg = f"EP API CA not found at {ca_path}; run ./tools/generate_certs.py first"
        raise FileNotFoundError(msg)
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    with httpx.Client(verify=str(ca_path), timeout=_REQUEST_TIMEOUT_SECONDS) as client:
        response = client.request(method, url, headers=headers, json=body)
    if not response.content:
        return response.status_code, None
    try:
        parsed: Any = response.json()
    except json.JSONDecodeError:
        parsed = {"detail": response.text}
    return response.status_code, parsed


def build_payload(
    *,
    language: str,
    code: str,
    timeout_seconds: int,
    environment: dict[str, str],
    image: str | None = None,
    selectors: dict[str, str] | None = None,
    workflow_run_id: str | None = None,
    step_revision_id: str = "script.v1",
) -> dict[str, Any]:
    """Build SDK invocation data for a script runner advertising the selected step."""
    inputs: dict[str, Any] = {"language": language, "code": code}
    if environment:
        inputs["environment"] = environment
    return {
        "invocation": {
            "workflow_run_id": workflow_run_id or str(uuid.uuid4()),
            "step_revision_id": step_revision_id,
            "inputs": inputs,
            "timeout_seconds": timeout_seconds,
        },
        "image": image or default_node_image(),
        "selectors": selectors or {},
        "output_config": {"stdout": "stdout", "stderr": "stderr", "exit_code": "exit_code"},
    }


def submit_work_item(
    api_url: str,
    token: str,
    *,
    work_id: uuid.UUID,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """POST a work.execute command and return its acknowledgement."""
    client_id = os.environ.get("EP_AO_CLIENT_ID", DEFAULT_JWT_CLIENT_ID)
    body = (
        WorkExecute.model_validate(payload)
        .to_message(
            source_id=client_id,
            destination_id=os.environ.get("EP_SERVICE_JWT_AUDIENCE", DEFAULT_JWT_AUDIENCE),
            client_id=client_id,
            command_id=work_id,
        )
        .model_dump(mode="json")
    )
    status, response = ep_request("POST", f"{api_url.rstrip('/')}/v1/commands", token, body)
    if status not in {HTTPStatus.ACCEPTED, HTTPStatus.OK}:
        msg = f"Work-item submit failed ({status}): {response}"
        raise RuntimeError(msg)
    if not isinstance(response, dict):
        msg = f"Unexpected submit response: {response}"
        raise TypeError(msg)
    return response


def get_work_item(api_url: str, token: str, work_id: uuid.UUID) -> dict[str, Any] | None:
    """Read a work item by id, or None if it is not visible yet."""
    status, response = ep_request("GET", f"{api_url.rstrip('/')}/v1/work-items/{work_id}", token)
    if status == HTTPStatus.NOT_FOUND:
        return None
    if status >= HTTPStatus.BAD_REQUEST:
        msg = f"Work-item lookup failed ({status}): {response}"
        raise RuntimeError(msg)
    if not isinstance(response, dict):
        msg = f"Unexpected work-item response: {response}"
        raise TypeError(msg)
    return response


def list_execution_targets(api_url: str, token: str) -> list[dict[str, Any]]:
    """Return execution targets visible to the service token."""
    status, response = ep_request("GET", f"{api_url.rstrip('/')}/v1/execution-targets", token)
    if status >= HTTPStatus.BAD_REQUEST:
        msg = f"Listing execution targets failed ({status}): {response}"
        raise RuntimeError(msg)
    if not isinstance(response, list):
        msg = f"Unexpected execution-target response: {response}"
        raise TypeError(msg)
    return response


def wait_for_work_item(
    api_url: str,
    token: str,
    work_id: uuid.UUID,
    timeout_seconds: int,
    *,
    on_status: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Poll until the work item reaches a terminal status."""
    deadline = time.time() + timeout_seconds
    latest: dict[str, Any] | None = None
    while time.time() < deadline:
        latest = get_work_item(api_url, token, work_id)
        if latest is not None:
            status = str(latest.get("status"))
            if on_status is not None:
                on_status(status)
            if status in TERMINAL_STATUSES:
                return latest
        time.sleep(_POLL_PAUSE_SECONDS)
    msg = f"Timed out waiting for work item {work_id} to finish"
    if latest is not None:
        msg = f"{msg}: {json.dumps(latest, default=str)}"
    raise TimeoutError(msg)
