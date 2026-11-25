"""Tests for fail-closed workload image policy verification."""

import json
from pathlib import Path
from subprocess import CompletedProcess

import pytest

from execution_plane.image_policy import WorkloadImagePolicyError, verify_workload_image

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
