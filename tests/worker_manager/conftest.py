"""Shared runtime policy double for worker-manager unit tests."""

import pytest

from execution_plane.worker_manager.vanilla_k8s import manager


@pytest.fixture(autouse=True)
def mock_workload_image_signature_verification(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep worker-manager unit tests independent of an external registry and Cosign."""
    monkeypatch.setattr(manager, "verify_workload_image", lambda *_args: None)
