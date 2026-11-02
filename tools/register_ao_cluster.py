#!/usr/bin/env python3
"""Register a cluster as an Execution Plane execution target through the AO API.

Replaces the direct EP ClusterProvision path (AAP-100760): instead of minting
an ES256 JWT from AO's signing key and posting a ClusterProvision envelope to
the EP API, callers log in to AO and POST /api/execution_plane/v1/clusters.
The registration is idempotent per cluster name (AO derives a stable resource
key from it), so retries and re-runs are safe.

Requires the AO cluster-registration endpoint (syntara#1127 / AAP-100760).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from uuid import UUID

from create_ao_credential import DEFAULT_API_URL, _login, _read_secret, _request_json


def register_cluster(
    *,
    api_url: str,
    cluster_name: str,
    endpoint: str,
    namespace: str,
    credential_ref: UUID,
    ca_certificate: str | None = None,
    labels: dict[str, str] | None = None,
    username: str,
    password: str,
    insecure: bool = False,
) -> dict[str, object]:
    """Register (or idempotently re-apply) the cluster through the AO API."""
    api_root = api_url.rstrip("/")
    access_token = _login(api_root, username, password, insecure=insecure)
    headers = {"Authorization": f"Bearer {access_token}"}
    payload: dict[str, object] = {
        "name": cluster_name,
        "endpoint": endpoint,
        "namespace": namespace,
        "credential_ref": str(credential_ref),
        "labels": labels or {"execution-plane.syntara.io/local": "kind"},
        "enabled": True,
    }
    if ca_certificate:
        payload["ca_certificate"] = ca_certificate
    cluster = _request_json(
        "POST",
        f"{api_root}/api/execution_plane/v1/clusters",
        headers=headers,
        payload=payload,
        insecure=insecure,
    )
    if not isinstance(cluster, dict) or not isinstance(cluster.get("id"), str):
        message = "AO cluster registration response has no id"
        raise TypeError(message)
    return cluster


def main() -> int:
    """Parse command-line arguments and print the registered cluster id."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", default=os.environ.get("AO_API_BASE", DEFAULT_API_URL))
    parser.add_argument("--cluster-name", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--credential-ref", required=True, type=UUID)
    parser.add_argument("--ca-file", type=Path)
    parser.add_argument("--admin-username", default=os.environ.get("ADMIN_USERNAME", "admin"))
    parser.add_argument("--admin-password-file", type=Path)
    parser.add_argument("--insecure", action="store_true")
    args = parser.parse_args()

    try:
        admin_password = _read_secret(
            os.environ.get("AO_ADMIN_PASSWORD"),
            args.admin_password_file,
            "AO_ADMIN_PASSWORD",
            "--admin-password-file",
        )
        ca_certificate = args.ca_file.read_text(encoding="utf-8") if args.ca_file else None
        cluster = register_cluster(
            api_url=args.api_url,
            cluster_name=args.cluster_name,
            endpoint=args.endpoint,
            namespace=args.namespace,
            credential_ref=args.credential_ref,
            ca_certificate=ca_certificate,
            username=args.admin_username,
            password=admin_password,
            insecure=args.insecure,
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)  # noqa: T201
        return 1
    print(cluster["id"])  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
