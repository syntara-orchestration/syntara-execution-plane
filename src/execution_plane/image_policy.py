"""Validate and verify the exact approved Python and Bash workload images."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import NoReturn

from execution_plane_client.commands.work import SCRIPT_STEP_IDENTITIES, is_digest_image_reference

_COSIGN_TIMEOUT_SECONDS = 120


class WorkloadImagePolicyError(ValueError):
    """A script workload image is not approved or does not have a valid signature."""


def _raise_policy_error(message: str) -> NoReturn:
    raise WorkloadImagePolicyError(message)


def validate_workload_image(step_identity: str, reference: str, policy_path: str | None) -> Path:
    """Require the exact image digest approved for a script Action and return its key path."""
    if not is_digest_image_reference(reference):
        _raise_policy_error("Workload image must use repository@sha256:<64 lowercase hex digits>")
    if not policy_path:
        _raise_policy_error("EP_WORKLOAD_IMAGE_POLICY_PATH must point to the installed image policy")

    policy_file = Path(policy_path)
    try:
        policy = json.loads(policy_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        message = "Installed workload image policy could not be read"
        raise WorkloadImagePolicyError(message) from error
    if not isinstance(policy, dict) or policy.get("version") != 1 or not isinstance(policy.get("actions"), dict):
        _raise_policy_error("Installed workload image policy has an unsupported format")

    action_policy = policy["actions"].get(step_identity)
    approved_reference = action_policy.get("image_reference") if isinstance(action_policy, dict) else None
    key_file = action_policy.get("key_file") if isinstance(action_policy, dict) else None
    if approved_reference != reference:
        _raise_policy_error("Workload image reference is not approved for this step identity")
    if not isinstance(key_file, str) or Path(key_file).name != key_file:
        _raise_policy_error("No valid Cosign verification key is configured for this workload image")

    policy_directory = policy_file.resolve().parent
    key_path = (policy_directory / key_file).resolve()
    if not key_path.is_relative_to(policy_directory) or not key_path.is_file():
        _raise_policy_error("Configured Cosign verification key is unavailable")
    return key_path


def verify_workload_image(step_identity: str, reference: str, policy_path: str | None) -> None:
    """Require the exact approved Action image to have a valid Cosign signature."""
    if step_identity not in SCRIPT_STEP_IDENTITIES:
        return
    key_path = validate_workload_image(step_identity, reference, policy_path)
    cosign = shutil.which("cosign")
    if cosign is None:
        _raise_policy_error("Cosign is required to verify workload images")

    try:
        result = subprocess.run(  # noqa: S603 - executable is the installed Cosign binary; arguments are validated
            [cosign, "verify", f"--key={key_path}", reference],
            capture_output=True,
            check=False,
            shell=False,
            timeout=_COSIGN_TIMEOUT_SECONDS,
            text=True,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        message = "Cosign could not verify the workload image"
        raise WorkloadImagePolicyError(message) from error
    if result.returncode != 0:
        _raise_policy_error("Workload image signature verification failed")


def is_script_step_identity(value: str) -> bool:
    """Return whether a step identity belongs to a built-in script Action."""
    return value in SCRIPT_STEP_IDENTITIES
