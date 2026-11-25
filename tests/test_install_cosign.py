"""Tests for installing the checksum-pinned Cosign release artifact."""

import hashlib
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock
from urllib.parse import urlparse

import pytest

from tools import install_cosign


def _redirect_install_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    target = tmp_path / "bin" / "cosign"
    target.parent.mkdir()
    original_path = install_cosign.Path

    def redirected_path(value: str | Path) -> Path:
        if str(value) == "/cachi2/output/deps/generic":
            return tmp_path / "generic"
        if str(value) == "/usr/local/bin/cosign":
            return target
        return original_path(value)

    monkeypatch.setattr(install_cosign, "Path", redirected_path)
    return target


def _set_artifact_digest(monkeypatch: pytest.MonkeyPatch, artifact: bytes) -> str:
    digest = hashlib.sha256(artifact).hexdigest()
    monkeypatch.setattr(install_cosign, "_ARTIFACTS", {"amd64": digest, "arm64": digest})
    return digest


def test_installs_hermeto_artifact_after_checksum_verification(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    artifact = b"verified test cosign binary"
    _set_artifact_digest(monkeypatch, artifact)
    target = _redirect_install_paths(monkeypatch, tmp_path)
    cache = tmp_path / "generic"
    cache.mkdir()
    (cache / "cosign-linux-amd64").write_bytes(artifact)
    monkeypatch.setattr(install_cosign.platform, "machine", lambda: "x86_64")
    monkeypatch.setenv("PIP_FIND_LINKS", "/cachi2/output/deps/wheels")

    install_cosign.main()

    assert target.read_bytes() == artifact
    assert target.stat().st_mode & 0o777 == 0o755


def test_rejects_a_cached_artifact_with_the_wrong_checksum(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    target = _redirect_install_paths(monkeypatch, tmp_path)
    cache = tmp_path / "generic"
    cache.mkdir()
    (cache / "cosign-linux-amd64").write_bytes(b"unexpected artifact")
    monkeypatch.setattr(install_cosign.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(install_cosign, "_ARTIFACTS", {"amd64": "0" * 64})
    monkeypatch.setenv("PIP_FIND_LINKS", "/cachi2/output/deps/wheels")

    with pytest.raises(SystemExit, match="checksum mismatch"):
        install_cosign.main()
    assert not target.exists()


def test_fails_when_hermeto_did_not_prefetch_the_artifact(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _redirect_install_paths(monkeypatch, tmp_path)
    (tmp_path / "generic").mkdir()
    monkeypatch.setattr(install_cosign.platform, "machine", lambda: "aarch64")
    monkeypatch.setenv("PIP_FIND_LINKS", "/cachi2/output/deps/wheels")

    with pytest.raises(SystemExit, match="artifact is missing"):
        install_cosign.main()


def test_rejects_an_unsupported_architecture(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(install_cosign.platform, "machine", lambda: "ppc64le")

    with pytest.raises(SystemExit, match="Unsupported Cosign architecture"):
        install_cosign.main()


def test_downloads_the_release_asset_and_checks_its_digest(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    artifact = b"downloaded test cosign binary"
    _set_artifact_digest(monkeypatch, artifact)
    target = _redirect_install_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(install_cosign.platform, "machine", lambda: "aarch64")
    monkeypatch.setattr(install_cosign.tempfile, "gettempdir", lambda: str(tmp_path))
    response = BytesIO(artifact)
    download = Mock(return_value=response)
    monkeypatch.setattr(install_cosign, "urlopen", download)
    monkeypatch.delenv("PIP_FIND_LINKS", raising=False)

    install_cosign.main()

    assert target.read_bytes() == artifact
    assert target.stat().st_mode & 0o777 == 0o755
    assert urlparse(download.call_args.args[0]).path.endswith("/v3.1.4/cosign-linux-arm64")
    assert download.call_args.kwargs == {"timeout": 120}
