"""Tests for fail-closed workload image policy verification."""

import json
from pathlib import Path
from subprocess import CompletedProcess, TimeoutExpired
from unittest.mock import Mock

import pytest

from execution_plane.image_policy import (
    WorkloadImagePolicyError,
    is_script_step_identity,
    verify_workload_image,
)

_STEP_IDENTITY = "syntara.script-python.script-python"
_REPOSITORY = "registry.redhat.io/ansible-automation-platform/ao-python-rhel9"
_REFERENCE = _REPOSITORY + "@sha256:" + "a" * 64


def _write_policy(directory: Path, *, identity: str = _STEP_IDENTITY, key_path: Path | None = None) -> Path:
    key = key_path or directory / "cosign-redhat.pub"
    key.write_text("test public key", encoding="utf-8")
    policy = {
        "version": 1,
        "actions": {identity: {"image_reference": _REFERENCE, "key_file": key.name}},
    }
    policy_path = directory / "policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")
    return policy_path


def test_workload_image_policy_runs_cosign_for_approved_repository(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    key_path = tmp_path / "cosign-redhat.pub"
    key_path.write_text("test public key", encoding="utf-8")
    policy_path = _write_policy(tmp_path, key_path=key_path)
    monkeypatch.setattr("execution_plane.image_policy.shutil.which", lambda _name: "/usr/bin/cosign")
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(arguments: list[str], **kwargs: object) -> CompletedProcess[str]:
        calls.append((arguments, kwargs))
        return CompletedProcess(arguments, 0, "verified", "")

    monkeypatch.setattr("execution_plane.image_policy.subprocess.run", run)

    verify_workload_image(_STEP_IDENTITY, _REFERENCE, str(policy_path))

    assert calls[0][0] == ["/usr/bin/cosign", "verify", f"--key={key_path}", _REFERENCE]
    assert calls[0][1]["shell"] is False


@pytest.mark.parametrize(
    ("step_identity", "reference", "policy_path"),
    [
        (_STEP_IDENTITY, "registry.redhat.io/ansible-automation-platform/ao-python-rhel9:latest", "policy.json"),
        ("syntara.script-bash.script-bash", _REFERENCE, "policy.json"),
        (_STEP_IDENTITY, _REFERENCE, None),
    ],
)
def test_workload_image_policy_rejects_invalid_inputs(
    tmp_path: Path, step_identity: str, reference: str, policy_path: str | None
) -> None:
    configured_path = _write_policy(tmp_path)
    actual_path = None if policy_path is None else str(configured_path)

    with pytest.raises(WorkloadImagePolicyError):
        verify_workload_image(step_identity, reference, actual_path)


def test_workload_image_policy_rejects_keys_outside_policy_directory(tmp_path: Path) -> None:
    external_key = tmp_path.parent / "outside-cosign.pub"
    external_key.write_text("test public key", encoding="utf-8")
    policy_path = _write_policy(tmp_path, key_path=external_key)

    with pytest.raises(WorkloadImagePolicyError, match="key is unavailable"):
        verify_workload_image(_STEP_IDENTITY, _REFERENCE, str(policy_path))


def test_workload_image_policy_rejects_missing_key_file(tmp_path: Path) -> None:
    policy_path = _write_policy(tmp_path)
    (tmp_path / "cosign-redhat.pub").unlink()

    with pytest.raises(WorkloadImagePolicyError, match="key is unavailable"):
        verify_workload_image(_STEP_IDENTITY, _REFERENCE, str(policy_path))


def test_workload_image_policy_rejects_key_path_traversal(tmp_path: Path) -> None:
    policy_path = _write_policy(tmp_path)
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    policy["actions"][_STEP_IDENTITY]["key_file"] = "../cosign-redhat.pub"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    with pytest.raises(WorkloadImagePolicyError, match="No valid Cosign verification key"):
        verify_workload_image(_STEP_IDENTITY, _REFERENCE, str(policy_path))


@pytest.mark.parametrize("contents", ["not-json", "[]", '{"version":2,"actions":{}}', '{"version":1,"actions":[]}'])
def test_workload_image_policy_rejects_unreadable_or_unsupported_policy(tmp_path: Path, contents: str) -> None:
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(contents, encoding="utf-8")

    with pytest.raises(WorkloadImagePolicyError):
        verify_workload_image(_STEP_IDENTITY, _REFERENCE, str(policy_path))


def test_workload_image_policy_rejects_missing_policy_file(tmp_path: Path) -> None:
    with pytest.raises(WorkloadImagePolicyError, match="could not be read"):
        verify_workload_image(_STEP_IDENTITY, _REFERENCE, str(tmp_path / "missing-policy.json"))


def test_workload_image_policy_rejects_digest_not_in_exact_action_approval(tmp_path: Path) -> None:
    policy_path = _write_policy(tmp_path)
    unapproved_digest = _REFERENCE[:-1] + "c"

    with pytest.raises(WorkloadImagePolicyError, match="not approved"):
        verify_workload_image(_STEP_IDENTITY, unapproved_digest, str(policy_path))


def test_workload_image_policy_hides_cosign_output(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    key_path = tmp_path / "cosign-redhat.pub"
    key_path.write_text("test public key", encoding="utf-8")
    policy_path = _write_policy(tmp_path, key_path=key_path)
    monkeypatch.setattr("execution_plane.image_policy.shutil.which", lambda _name: "/usr/bin/cosign")
    monkeypatch.setattr(
        "execution_plane.image_policy.subprocess.run",
        lambda arguments, **_kwargs: CompletedProcess(arguments, 1, "", "registry credentials"),
    )

    with pytest.raises(WorkloadImagePolicyError, match="signature verification failed") as error:
        verify_workload_image(_STEP_IDENTITY, _REFERENCE, str(policy_path))

    assert "registry credentials" not in str(error.value)


def test_workload_image_policy_requires_cosign_binary(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    policy_path = _write_policy(tmp_path)
    monkeypatch.setattr("execution_plane.image_policy.shutil.which", lambda _name: None)

    with pytest.raises(WorkloadImagePolicyError, match="Cosign is required"):
        verify_workload_image(_STEP_IDENTITY, _REFERENCE, str(policy_path))


@pytest.mark.parametrize("failure", [OSError("cosign unavailable"), TimeoutExpired("cosign", 120)])
def test_workload_image_policy_fails_closed_when_cosign_cannot_run(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: Exception
) -> None:
    policy_path = _write_policy(tmp_path)
    monkeypatch.setattr("execution_plane.image_policy.shutil.which", lambda _name: "/usr/bin/cosign")
    monkeypatch.setattr("execution_plane.image_policy.subprocess.run", Mock(side_effect=failure))

    with pytest.raises(WorkloadImagePolicyError, match="Cosign could not verify") as error:
        verify_workload_image(_STEP_IDENTITY, _REFERENCE, str(policy_path))
    assert error.value.__cause__ is failure


def test_workload_image_policy_does_not_restrict_generic_workloads() -> None:
    verify_workload_image("custom.script.v1", "registry.example/custom-node:dev", None)


def test_script_identity_check_only_matches_builtin_actions() -> None:
    assert is_script_step_identity(_STEP_IDENTITY)
    assert not is_script_step_identity("custom.script.v1")
