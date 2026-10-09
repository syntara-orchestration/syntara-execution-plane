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

import jwt

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ISSUER = "http://localhost:8000"
DEFAULT_AUDIENCE = "execution-plane"
DEFAULT_CLIENT_ID = "syntara-orchestration"
DEFAULT_TTL = timedelta(hours=8)
DEFAULT_SCOPES = (
    "work-items:read",
    "work-items:submit",
    "work-items:cancel",
    "execution-targets:read",
    "cluster-bindings:read",
    "cluster-bindings:write",
)
CANDIDATE_PRIVATE_KEYS = (
    PROJECT_ROOT / ".secrets" / "jwt-primary.pem",
    PROJECT_ROOT.parent / "syntara" / "backend" / ".secrets" / "jwt-primary.pem",
)


def _resolve_private_key_path(explicit: Path | None) -> Path:
    """Prefer an explicit path, then a local key, then a sibling Syntara key."""
    if explicit is not None:
        return explicit
    env_path = os.environ.get("EP_AO_JWT_PRIVATE_KEY_FILE")
    if env_path:
        return Path(env_path)
    for candidate in CANDIDATE_PRIVATE_KEYS:
        if candidate.is_file():
            return candidate
    searched = ", ".join(str(path) for path in CANDIDATE_PRIVATE_KEYS)
    msg = (
        "AO JWT private key not found. Pass --private-key, set "
        f"EP_AO_JWT_PRIVATE_KEY_FILE, or place jwt-primary.pem at one of: {searched}"
    )
    raise FileNotFoundError(msg)


def build_claims(
    *,
    issuer: str,
    audience: str,
    client_id: str,
    scopes: list[str],
    project_id: UUID | None,
    all_projects: bool,
    now: datetime,
    ttl: timedelta,
) -> dict[str, object]:
    """Return JWT claims accepted by execution_plane.api.auth."""
    if project_id is None and not all_projects:
        msg = "Token needs --project-id or --all-projects"
        raise ValueError(msg)
    claims: dict[str, object] = {
        "iss": issuer,
        "aud": audience,
        "client_id": client_id,
        "scope": " ".join(scopes),
        "iat": now,
        "exp": now + ttl,
    }
    if project_id is not None:
        claims["project_id"] = str(project_id)
    if all_projects:
        claims["all_projects"] = True
    return claims


def mint_token(private_key_pem: str, claims: dict[str, object]) -> str:
    """Sign claims with AO's ES256 private key."""
    return jwt.encode(claims, private_key_pem, algorithm="ES256")


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
    parser.add_argument("--issuer", default=os.environ.get("EP_AO_JWT_ISSUER", DEFAULT_ISSUER))
    parser.add_argument("--audience", default=os.environ.get("EP_SERVICE_JWT_AUDIENCE", DEFAULT_AUDIENCE))
    parser.add_argument("--client-id", default=os.environ.get("EP_AO_CLIENT_ID", DEFAULT_CLIENT_ID))
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
    parser.add_argument("--ttl-hours", type=float, default=DEFAULT_TTL.total_seconds() / 3600)
    parser.add_argument("--json", action="store_true", help="Print token and claims as JSON")
    args = parser.parse_args()

    all_projects = args.project_id is None if args.all_projects is None else args.all_projects
    scopes = args.scopes if args.scopes else list(DEFAULT_SCOPES)

    try:
        key_path = _resolve_private_key_path(args.private_key)
        claims = build_claims(
            issuer=args.issuer,
            audience=args.audience,
            client_id=args.client_id,
            scopes=scopes,
            project_id=args.project_id,
            all_projects=all_projects,
            now=datetime.now(UTC),
            ttl=timedelta(hours=args.ttl_hours),
        )
        token = mint_token(key_path.read_text(encoding="utf-8"), claims)
    except (FileNotFoundError, ValueError, OSError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        serializable = {
            key: (value.isoformat() if isinstance(value, datetime) else value) for key, value in claims.items()
        }
        print(json.dumps({"token": token, "private_key": str(key_path), "claims": serializable}, indent=2))
    else:
        print(token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
