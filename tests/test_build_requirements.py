"""Tests for the deterministic offline build-requirements input."""

from pathlib import Path

from tools.ci import prepare_build_requirements


def test_prepare_build_requirements_adds_the_pinned_vcs_backend(
    tmp_path: Path,
    monkeypatch,
) -> None:
    requirements = tmp_path / "requirements.txt"
    output = tmp_path / "requirements-build-input.txt"
    requirements.write_text(
        "httpx==0.28.1\nsyntara-plugin-contracts @ git+https://example.invalid/sdk.git@abc123#subdirectory=contracts\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(prepare_build_requirements, "_REQUIREMENTS", requirements)
    monkeypatch.setattr(prepare_build_requirements, "_OUTPUT", output)

    assert prepare_build_requirements.main() == 0
    assert output.read_text(encoding="utf-8") == "httpx==0.28.1\nhatchling==1.32.4\n"


def test_prepare_build_requirements_leaves_registry_only_inputs_unchanged(
    tmp_path: Path,
    monkeypatch,
) -> None:
    requirements = tmp_path / "requirements.txt"
    output = tmp_path / "requirements-build-input.txt"
    requirements.write_text("httpx==0.28.1\n", encoding="utf-8")
    monkeypatch.setattr(prepare_build_requirements, "_REQUIREMENTS", requirements)
    monkeypatch.setattr(prepare_build_requirements, "_OUTPUT", output)

    assert prepare_build_requirements.main() == 0
    assert output.read_text(encoding="utf-8") == "httpx==0.28.1\n"
