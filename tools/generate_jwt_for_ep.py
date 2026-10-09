#!/usr/bin/env python3
"""Mint an AO service JWT for local Execution Plane API calls.

EP does not issue tokens. It verifies ES256 tokens signed with AO's private
key. This script signs a service token that matches the compose defaults.

Usage:
    uv run python tools/generate_jwt_for_ep.py
    TOKEN=$(uv run python tools/generate_jwt_for_ep.py)
    curl -k -H "Authorization: Bearer $TOKEN" https://127.0.0.1:8001/v1/work-items

    # Submit requires a project UUID, not only all_projects:
    uv run python tools/generate_jwt_for_ep.py --project-id 00000000-0000-0000-0000-000000000001
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

from execution_plane.work_item_client import (
    DEFAULT_JWT_AUDIENCE,
    DEFAULT_JWT_CLIENT_ID,
    DEFAULT_JWT_ISSUER,
    DEFAULT_TOKEN_SCOPES,
    DEFAULT_TOKEN_TTL,
    ep_service_token,
    resolve_private_key_path,
    service_token_claims,
)


def main() -> int:
    """Parse CLI arguments and print a service token."""
    parser = argparse.ArgumentParser(
        description="Generate an AO service JWT for the local Execution Plane API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--private-key",
        type=Path,
        help="AO ES256 private key PEM (default: local or sibling Syntara jwt-primary.pem)",
    )
    parser.add_argument("--issuer", default=os.environ.get("EP_AO_JWT_ISSUER", DEFAULT_JWT_ISSUER))
    parser.add_argument("--audience", default=os.environ.get("EP_SERVICE_JWT_AUDIENCE", DEFAULT_JWT_AUDIENCE))
    parser.add_argument("--client-id", default=os.environ.get("EP_AO_CLIENT_ID", DEFAULT_JWT_CLIENT_ID))
    parser.add_argument(
        "--scope",
        action="append",
        dest="scopes",
        help="Repeatable scope. Default: all local-dev EP scopes",
    )
    parser.add_argument("--project-id", type=UUID, help="Project UUID required to submit work")
    parser.add_argument(
        "--all-projects",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Grant every project (default: on when --project-id is omitted)",
    )
    parser.add_argument("--ttl-hours", type=float, default=DEFAULT_TOKEN_TTL.total_seconds() / 3600)
    parser.add_argument("--json", action="store_true", help="Print token and claims as JSON")
    args = parser.parse_args()

    all_projects = args.project_id is None if args.all_projects is None else args.all_projects
    scopes = args.scopes if args.scopes else list(DEFAULT_TOKEN_SCOPES)
    now = datetime.now(UTC)
    ttl = timedelta(hours=args.ttl_hours)

    try:
        key_path = resolve_private_key_path(args.private_key)
        token = ep_service_token(
            args.project_id,
            all_projects=all_projects,
            private_key=key_path,
            issuer=args.issuer,
            audience=args.audience,
            client_id=args.client_id,
            scopes=scopes,
            now=now,
            ttl=ttl,
        )
    except (FileNotFoundError, ValueError, OSError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        claims = service_token_claims(
            args.project_id,
            all_projects=all_projects,
            issuer=args.issuer,
            audience=args.audience,
            client_id=args.client_id,
            scopes=scopes,
            now=now,
            ttl=ttl,
        )
        serializable = {
            key: (value.isoformat() if isinstance(value, datetime) else value) for key, value in claims.items()
        }
        print(json.dumps({"token": token, "private_key": str(key_path), "claims": serializable}, indent=2))
    else:
        print(token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
