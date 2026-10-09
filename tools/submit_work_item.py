#!/usr/bin/env python3
"""Submit a script work item to the local Execution Plane API.

Submission requires an AO service token with `work-items:submit` and a
`project_id` claim. `all_projects` is not enough.

Usage:
    uv run python tools/submit_work_item.py
    uv run python tools/submit_work_item.py --language bash --code 'echo hello'
    uv run python tools/submit_work_item.py --code-file ./job.py --wait
"""

from __future__ import annotations

import argparse
import json
import os
import ssl
import subprocess
import sys
import time
import uuid
from http import HTTPStatus
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_API_URL = "https://127.0.0.1:8001"
DEFAULT_PROJECT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
DEFAULT_LANGUAGE = "python"
DEFAULT_PYTHON = "print('hello from execution-plane')"
DEFAULT_BASH = "echo hello from execution-plane"
DEFAULT_TIMEOUT_SECONDS = 60
EP_CA_PATH = PROJECT_ROOT / ".secrets" / "certs" / "ca.pem"
TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled"})


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    """Run a host command and return stdout/stderr as text."""
    return subprocess.run(args, check=True, capture_output=True, text=True)


def _ssl_context() -> ssl.SSLContext:
    if not EP_CA_PATH.is_file():
        msg = f"EP API CA not found at {EP_CA_PATH}; run ./tools/generate_certs.py first"
        raise FileNotFoundError(msg)
    return ssl.create_default_context(cafile=str(EP_CA_PATH))


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


def ep_service_token(project_id: uuid.UUID) -> str:
    """Mint a project-scoped token that can submit and read work items."""
    result = _run(
        [
            sys.executable,
            str(PROJECT_ROOT / "tools" / "generate_jwt_for_ep.py"),
            "--project-id",
            str(project_id),
        ]
    )
    token = result.stdout.strip()
    if not token:
        msg = "generate_jwt_for_ep.py produced an empty token"
        raise RuntimeError(msg)
    return token


def parse_env_pairs(values: list[str] | None) -> dict[str, str]:
    """Parse KEY=VALUE pairs for the script environment."""
    environment: dict[str, str] = {}
    for raw in values or []:
        key, separator, value = raw.partition("=")
        if not separator or not key:
            msg = f"Environment entry must be KEY=VALUE: {raw}"
            raise ValueError(msg)
        environment[key] = value
    return environment


def build_payload(*, language: str, code: str, timeout_seconds: int, environment: dict[str, str]) -> dict[str, Any]:
    """Build the script workload payload accepted by the Kubernetes runner."""
    input_config: dict[str, Any] = {
        "language": language,
        "code": code,
        "_engine_timeout_seconds": timeout_seconds,
    }
    if environment:
        input_config["environment"] = environment
    return {
        "input_config": input_config,
        "invocation": {
            "version": 1,
            "operation": "execute",
            "credentials": {},
            "workflow_context": {},
            "settings": {},
            "max_output_bytes": 10**4,
            "timeout_seconds": 15,
            "inputs": {},
        },
        "image": "aa",
    }


def submit_work_item(
    api_url: str,
    token: str,
    *,
    request_id: str,
    work_correlation_id: uuid.UUID,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """POST one script work item and return the accepted record."""
    body = {
        "request_id": request_id,
        "work_correlation_id": str(work_correlation_id),
        "workload_type": "script",
        "payload": payload,
    }
    status, response = ep_request("POST", f"{api_url.rstrip('/')}/v1/work-items", token, body)
    if status not in {HTTPStatus.ACCEPTED, HTTPStatus.OK}:
        msg = f"Work-item submit failed ({status}): {response}"
        raise RuntimeError(msg)
    if not isinstance(response, dict):
        msg = f"Unexpected submit response: {response}"
        raise TypeError(msg)
    return response


def get_work_item(api_url: str, token: str, request_id: str) -> dict[str, Any] | None:
    """Read a work item by stable request ID, or None if it is not visible yet."""
    status, response = ep_request("GET", f"{api_url.rstrip('/')}/v1/work-items/by-request/{request_id}", token)
    if status == HTTPStatus.NOT_FOUND:
        return None
    if status >= HTTPStatus.BAD_REQUEST:
        msg = f"Work-item lookup failed ({status}): {response}"
        raise RuntimeError(msg)
    if not isinstance(response, dict):
        msg = f"Unexpected work-item response: {response}"
        raise TypeError(msg)
    return response


def wait_for_work_item(api_url: str, token: str, request_id: str, timeout_seconds: int) -> dict[str, Any]:
    """Poll until the work item reaches a terminal status."""
    deadline = time.time() + timeout_seconds
    latest: dict[str, Any] | None = None
    while time.time() < deadline:
        latest = get_work_item(api_url, token, request_id)
        if latest is not None:
            status = str(latest.get("status"))
            print(f"[INFO] Work item {request_id} status: {status}")
            if status in TERMINAL_STATUSES:
                return latest
        time.sleep(2)
    msg = f"Timed out waiting for work item {request_id} to finish"
    if latest is not None:
        msg = f"{msg}: {json.dumps(latest, default=str)}"
    raise TimeoutError(msg)


def resolve_code(language: str, code: str | None, code_file: Path | None) -> str:
    """Return inline code, file contents, or the language default snippet."""
    if code is not None and code_file is not None:
        msg = "Use either --code or --code-file, not both"
        raise ValueError(msg)
    if code_file is not None:
        return code_file.read_text(encoding="utf-8")
    if code is not None:
        return code
    if language == "bash":
        return DEFAULT_BASH
    return DEFAULT_PYTHON


def load_payload_file(path: Path) -> dict[str, Any]:
    """Load a caller-supplied JSON object used as the work-item payload."""
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        msg = "--payload-file must contain a JSON object"
        raise TypeError(msg)
    return loaded


def main() -> int:
    """Submit a local-dev script work item and optionally wait for completion."""
    parser = argparse.ArgumentParser(
        description="Submit a script work item to the local Execution Plane API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--api-url", default=os.environ.get("EP_API_URL", DEFAULT_API_URL))
    parser.add_argument("--project-id", type=uuid.UUID, default=DEFAULT_PROJECT_ID)
    parser.add_argument("--request-id", help="Idempotency key; defaults to a new UUID")
    parser.add_argument("--work-correlation-id", type=uuid.UUID, help="Caller correlation UUID")
    parser.add_argument("--language", choices=("python", "bash"), default=DEFAULT_LANGUAGE)
    parser.add_argument("--code", help="Inline script source")
    parser.add_argument("--code-file", type=Path, help="Read script source from a file")
    parser.add_argument("--env", action="append", dest="env_pairs", help="Repeatable KEY=VALUE for the script env")
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--payload-file", type=Path, help="Replace the generated payload with a JSON object")
    parser.add_argument("--wait", action="store_true", help="Poll until the work item is terminal")
    parser.add_argument(
        "--wait-seconds",
        type=int,
        default=None,
        help="How long --wait should poll (default: timeout-seconds + 60)",
    )
    args = parser.parse_args()

    try:
        request_id = args.request_id or str(uuid.uuid4())
        work_correlation_id = args.work_correlation_id or uuid.uuid4()
        if args.payload_file is not None:
            payload = load_payload_file(args.payload_file)
        else:
            payload = build_payload(
                language=args.language,
                code=resolve_code(args.language, args.code, args.code_file),
                timeout_seconds=args.timeout_seconds,
                environment=parse_env_pairs(args.env_pairs),
            )
        print(f"[INFO] Submitting request_id={request_id} project_id={args.project_id}")
        token = ep_service_token(args.project_id)
        item = submit_work_item(
            args.api_url,
            token,
            request_id=request_id,
            work_correlation_id=work_correlation_id,
            payload=payload,
        )
        if args.wait:
            wait_budget = args.wait_seconds if args.wait_seconds is not None else args.timeout_seconds + 60
            item = wait_for_work_item(args.api_url, token, request_id, wait_budget)
    except (FileNotFoundError, RuntimeError, TimeoutError, TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "").strip() or str(exc)
        print(f"Error: command failed: {' '.join(exc.cmd)}\n{detail}", file=sys.stderr)
        return 1

    print(json.dumps(item, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
