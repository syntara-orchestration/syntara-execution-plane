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
import sys
import uuid
from pathlib import Path
from typing import Any

from execution_plane.work_item_client import (
    DEFAULT_API_URL,
    DEFAULT_NODE_IMAGE,
    DEFAULT_PROJECT_ID,
    build_payload,
    default_api_url,
    default_node_image,
    ep_service_token,
    submit_work_item,
    wait_for_work_item,
)

DEFAULT_LANGUAGE = "python"
DEFAULT_PYTHON = "print('hello from execution-plane')"
DEFAULT_BASH = "echo hello from execution-plane"
DEFAULT_TIMEOUT_SECONDS = 60


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
    parser.add_argument(
        "--image",
        default=None,
        help=f"Node image the execution target can pull (default: {DEFAULT_NODE_IMAGE} or EP_NODE_IMAGE)",
    )
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
                image=args.image or default_node_image(),
            )
        print(f"[INFO] Submitting request_id={request_id} project_id={args.project_id}")
        token = ep_service_token(args.project_id)
        item = submit_work_item(
            args.api_url or default_api_url(),
            token,
            request_id=request_id,
            work_correlation_id=work_correlation_id,
            payload=payload,
        )
        if args.wait:
            wait_budget = args.wait_seconds if args.wait_seconds is not None else args.timeout_seconds + 60
            item = wait_for_work_item(
                args.api_url,
                token,
                request_id,
                wait_budget,
                on_status=lambda status: print(f"[INFO] Work item {request_id} status: {status}"),
            )
    except (FileNotFoundError, RuntimeError, TimeoutError, TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(item, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
