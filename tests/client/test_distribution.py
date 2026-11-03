"""Install the actual client artifact without application dependencies."""

import json
import os
import subprocess
import tarfile
import zipfile
from pathlib import Path


def test_client_wheel_and_sdist_install_without_ep(tmp_path):
    root = Path(__file__).resolve().parents[2]
    artifacts = tmp_path / "artifacts"
    subprocess.run(
        ["uv", "build", str(root / "src/execution_plane_client"), "--out-dir", str(artifacts)],
        check=True,
        capture_output=True,
    )
    wheel = next(artifacts.glob("*.whl"))
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        assert "execution_plane_client/py.typed" in names
        assert {
            "execution_plane_client/models/shared.py",
            "execution_plane_client/models/work_item.py",
            "execution_plane_client/models/execution_target.py",
            "execution_plane_client/models/cluster.py",
        } <= set(names)
        assert not any(name.startswith("execution_plane/") for name in names)
    source = tmp_path / "source"
    with tarfile.open(next(artifacts.glob("*.tar.gz"))) as archive:
        archive.extractall(source, filter="data")
    subprocess.run(
        ["uv", "build", str(next(source.iterdir())), "--wheel", "--out-dir", str(tmp_path / "rebuilt")],
        check=True,
        capture_output=True,
    )
    environment = tmp_path / "isolated"
    subprocess.run(["uv", "venv", str(environment)], check=True, capture_output=True)
    python = environment / "bin/python"
    subprocess.run(["uv", "pip", "install", "--python", str(python), str(wheel)], check=True, capture_output=True)
    probe = subprocess.run(
        [
            str(python),
            "-I",
            "-c",
            "import json; from importlib.metadata import distributions; "
            "from execution_plane_client.catalog import CommandCatalog; "
            "from execution_plane_client.models.work_item import WorkItemRead; "
            "from execution_plane_client.registry import CommandRegistry; "
            "print(json.dumps(sorted(d.metadata['Name'].lower() for d in distributions())))",
        ],
        env={key: value for key, value in os.environ.items() if not key.startswith(("EP_", "PYTHONPATH"))},
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )
    packages = set(json.loads(probe.stdout))
    assert {"execution-plane-client", "pydantic"} <= packages
    assert not packages & {"execution-plane", "fastapi", "sqlalchemy", "sqlmodel", "httpx", "pyjwt", "cryptography"}


def test_service_sdist_includes_local_client_dependency(tmp_path):
    root = Path(__file__).resolve().parents[2]
    subprocess.run(["uv", "build", str(root), "--out-dir", str(tmp_path)], check=True, capture_output=True)
    with tarfile.open(next(tmp_path.glob("*.tar.gz"))) as archive:
        assert any(name.endswith("/src/execution_plane_client/pyproject.toml") for name in archive.getnames())
    with zipfile.ZipFile(next(tmp_path.glob("*.whl"))) as archive:
        assert not any(name.startswith("execution_plane_client/") for name in archive.namelist())
