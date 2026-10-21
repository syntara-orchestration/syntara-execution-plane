"""Local-dev EP client helpers used by tools/ CLIs."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from execution_plane.work_item_client import (
    DEFAULT_JWT_AUDIENCE,
    DEFAULT_JWT_CLIENT_ID,
    DEFAULT_JWT_ISSUER,
    DEFAULT_NODE_IMAGE,
    build_payload,
    default_api_url,
    default_node_image,
    resolve_private_key_path,
    service_token_claims,
)
from execution_plane_client.commands.work import WorkExecute

_PROJECT_ID = UUID("00000000-0000-0000-0000-000000000001")


def test_build_payload_is_accepted_by_work_execute_schema() -> None:
    payload = build_payload(
        language="python",
        code="print('hello')",
        timeout_seconds=60,
        environment={},
    )
    WorkExecute.model_validate(payload)
    assert payload["image"] == DEFAULT_NODE_IMAGE
    assert payload["invocation"]["workflow_run_id"]
    assert payload["invocation"]["step_revision_id"] == "script.v1"
    assert "environment" not in payload["invocation"]["inputs"]


def test_build_payload_includes_environment_and_image_override() -> None:
    payload = build_payload(
        language="bash",
        code="echo hi",
        timeout_seconds=30,
        environment={"FOO": "bar"},
        image="quay.io/example/node:dev",
    )
    assert payload["image"] == "quay.io/example/node:dev"
    assert payload["invocation"]["inputs"]["environment"] == {"FOO": "bar"}
    WorkExecute.model_validate(payload)


def test_build_payload_rejects_invalid_timeout_through_schema() -> None:
    payload = build_payload(
        language="python",
        code="print(1)",
        timeout_seconds=0,
        environment={},
    )
    with pytest.raises(ValidationError, match="timeout_seconds"):
        WorkExecute.model_validate(payload)


def test_service_token_claims_include_project_id() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    claims = service_token_claims(_PROJECT_ID, now=now, ttl=timedelta(hours=1))
    assert claims["iss"] == DEFAULT_JWT_ISSUER
    assert claims["aud"] == DEFAULT_JWT_AUDIENCE
    assert claims["client_id"] == DEFAULT_JWT_CLIENT_ID
    assert claims["project_id"] == str(_PROJECT_ID)
    assert "all_projects" not in claims
    assert "work.execute" in str(claims["scope"])
    assert claims["exp"] == now + timedelta(hours=1)


def test_service_token_claims_grant_all_projects_when_project_id_omitted() -> None:
    claims = service_token_claims()
    assert claims["all_projects"] is True
    assert "project_id" not in claims


def test_service_token_claims_require_a_project_grant() -> None:
    with pytest.raises(ValueError, match="project_id or all_projects"):
        service_token_claims(all_projects=False)


def test_resolve_private_key_path_prefers_explicit_path(tmp_path: Path) -> None:
    key = tmp_path / "jwt-primary.pem"
    assert resolve_private_key_path(key) == key


def test_resolve_private_key_path_reads_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    key = tmp_path / "from-env.pem"
    monkeypatch.setenv("EP_AO_JWT_PRIVATE_KEY_FILE", str(key))
    assert resolve_private_key_path() == key


def test_resolve_private_key_path_errors_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EP_AO_JWT_PRIVATE_KEY_FILE", raising=False)
    monkeypatch.setattr(
        "execution_plane.work_item_client._PRIVATE_KEY_CANDIDATES",
        (Path("/nonexistent/jwt-primary.pem"),),
    )
    with pytest.raises(FileNotFoundError, match="AO JWT private key not found"):
        resolve_private_key_path()


def test_default_urls_and_images_honor_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EP_API_URL", "https://ep.example:8443")
    monkeypatch.setenv("EP_NODE_IMAGE", "quay.io/example/node:ci")
    assert default_api_url() == "https://ep.example:8443"
    assert default_node_image() == "quay.io/example/node:ci"
