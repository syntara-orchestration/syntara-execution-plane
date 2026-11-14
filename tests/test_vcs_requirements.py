"""Git sources must not turn on hash mode or consume the next package block."""

from tools.ci.split_vcs_requirements import split_vcs_requirements


def test_export_separates_git_and_hash_checked_packages(tmp_path):
    requirements = tmp_path / "requirements.txt"
    package = "ordinary==1.0 \\\n    --hash=sha256:abc\n    # via example\n"
    git = "contracts @ git+https://example.test/sdk.git@abcdef#subdirectory=packages/contracts\n    # via example\n"
    requirements.write_text("# exported\n" + git + package)
    split_vcs_requirements(requirements)
    assert requirements.read_text() == "# exported\n" + package
    vcs = requirements.with_name("requirements-vcs.txt").read_text()
    assert vcs.endswith(git)
    assert "--hash" not in vcs


def test_offline_sdk_source_retains_canonical_contracts(tmp_path):
    import io
    import tarfile

    from tools.ci.prefetched_sdk_source import prefetched_sdk_source

    commit = "a" * 40
    (tmp_path / "pyproject.toml").write_text(
        '[tool.uv.sources]\nsyntara-plugin-contracts = { git = "https://example.test/sdk.git", '
        f'rev = "{commit}", subdirectory = "sdks/python/packages/contracts" }}\n'
    )
    (tmp_path / "uv.lock").write_text(
        '[[package]]\nname = "syntara-plugin-contracts"\n'
        f'source = {{git = "https://example.test/sdk.git?rev={commit}#{commit}"}}\n'
    )
    with tarfile.open(tmp_path / f"sdk-gitcommit-{commit}.tar.gz", "w:gz") as archive:
        for name in [
            "app/sdks/python/packages/contracts/pyproject.toml",
            "app/contracts/schemas/test.json",
            "app/unrelated.txt",
        ]:
            value = b"{}"
            entry = tarfile.TarInfo(name)
            entry.size = len(value)
            archive.addfile(entry, io.BytesIO(value))
    destination = tmp_path / "extracted"
    project = prefetched_sdk_source(tmp_path, tmp_path, destination)
    assert (project / "pyproject.toml").is_file()
    assert (project.parents[3] / "contracts/schemas/test.json").is_file()
    assert not (destination / "app/unrelated.txt").exists()
