"""Fixtures for execution-plane performance tests.

Performance tests require a dedicated environment. Every required input below
skips the test (or session) when absent so runners without a perf stack do
not fail with FileNotFoundError / connection errors.

Set ``EP_PERF_REQUIRED=1`` in dedicated perf CI so a missing prerequisite
fails the job instead of skipping every test green.

Required:

* ``EP_TEST_DATABASE_URL`` — PostgreSQL URL shared with the deployed EP stack
* ``EP_PERF_API_URL`` — base URL of the deployed EP API
* ``EP_PERF_NAMESPACE`` — Kubernetes namespace where EP deploys worker pods
* ``EP_AO_JWT_PRIVATE_KEY_FILE`` — ES256 private key for minting service JWTs
  (or a candidate path used by ``tools.work_item_client.resolve_private_key_path``)
* ``EP_API_CA_FILE`` — CA PEM used to verify HTTPS to the EP API when
  ``EP_PERF_API_URL`` is ``https://...`` (defaults to ``.secrets/certs/ca.pem``)
* ``EP_NODE_IMAGE`` — worker image the cluster can pull for echo payloads
* Kubernetes credentials — ``KUBECONFIG`` or an in-cluster service account

Optional:

* ``EP_PERF_CREDENTIAL_ENCRYPTION_KEY`` — deployment payload key; only needed if a
  test loads ORM ``WorkItem.payload`` (encrypted). The shared runner waits on
  status/timestamps/result columns and does not require it.
* ``EP_AO_CLIENT_ID`` — must match the deployment's AO client id (default
  ``syntara-orchestration``); used by the JWT and by DB polls
* ``EP_PERF_REQUIRED`` — when ``1``/``true``/``yes``, missing prerequisites
  raise ``pytest.fail`` instead of ``pytest.skip``

All of the above (except ``EP_PERF_REQUIRED``) are checked in fixtures that
call ``pytest.skip`` by default, so a normal ``pytest`` invocation without a
perf environment stays green.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tools.work_item_client import ep_service_token, resolve_private_key_path

if TYPE_CHECKING:
    from kubernetes.client import CoreV1Api

# >= 2 * largest concurrent batch (one submit + one pod poll per item) with headroom.
K8S_CONNECTION_POOL_MAXSIZE = 128


def perf_required() -> bool:
    """Return whether missing perf prerequisites should fail instead of skip."""
    return os.environ.get("EP_PERF_REQUIRED", "").strip().lower() in {"1", "true", "yes"}


def missing_prerequisite(message: str) -> None:
    """Skip normally; fail when ``EP_PERF_REQUIRED`` is set so misconfigured CI goes red."""
    if perf_required():
        pytest.fail(f"EP_PERF_REQUIRED=1 and prerequisite missing: {message}")
    pytest.skip(message)


def _api_ca_path() -> Path:
    """Return the CA path ``tools.work_item_client`` would use for HTTPS verify."""
    env_path = os.environ.get("EP_API_CA_FILE")
    if env_path:
        return Path(env_path)
    return Path(__file__).resolve().parents[2] / ".secrets" / "certs" / "ca.pem"


@pytest.fixture(autouse=True)
def perf_credential_encryption_key(ep_settings_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """Prefer the deployment payload key when set; do not require it for runner waits.

    ``tests/conftest.py`` installs a dummy ``EP_CREDENTIAL_ENCRYPTION_KEY``. When
    ``EP_PERF_CREDENTIAL_ENCRYPTION_KEY`` is present, override so any accidental
    ORM ``payload`` load can decrypt rows the deployed API wrote.
    """
    key = os.environ.get("EP_PERF_CREDENTIAL_ENCRYPTION_KEY")
    if key:
        monkeypatch.setenv("EP_CREDENTIAL_ENCRYPTION_KEY", key)


@pytest.fixture(autouse=True)
def perf_node_image() -> None:
    """Require ``EP_NODE_IMAGE`` so the cluster does not pull the local-dev default."""
    if not os.environ.get("EP_NODE_IMAGE"):
        missing_prerequisite("EP_NODE_IMAGE must be set to a cluster-pullable worker image for perf tests")


@pytest.fixture(scope="session")
def perf_database_url() -> str:
    """Return the perf PostgreSQL URL, or skip/fail the session if absent."""
    url = os.environ.get("EP_TEST_DATABASE_URL")
    if not url:
        missing_prerequisite("EP_TEST_DATABASE_URL must be set for perf tests")
    return url


@pytest.fixture(scope="session")
def perf_api_url() -> str:
    """Return the deployed EP API base URL, or skip/fail when URL/CA prerequisites are missing.

    HTTPS submissions verify TLS against ``EP_API_CA_FILE`` (or
    ``.secrets/certs/ca.pem``). Missing CA files raise inside
    ``submit_work_item``; skip/fail here instead so non-perf runners stay clean.
    """
    url = os.environ.get("EP_PERF_API_URL")
    if not url:
        missing_prerequisite("EP_PERF_API_URL must be set for perf tests")
    if url.startswith("https://"):
        ca_path = _api_ca_path()
        if not ca_path.is_file():
            missing_prerequisite(
                f"EP API CA not found at {ca_path}; set EP_API_CA_FILE to a PEM that verifies EP_PERF_API_URL"
            )
    return url


@pytest.fixture(scope="session")
def perf_token() -> str:
    """Mint an ES256-signed EP service token, or skip/fail when the private key is missing."""
    try:
        resolve_private_key_path()
    except FileNotFoundError as exc:
        missing_prerequisite(f"AO JWT private key not available for perf tests: {exc}")
    return ep_service_token(all_projects=True)


@pytest.fixture(scope="session")
def perf_ep_namespace() -> str:
    """Return the Kubernetes namespace where EP deploys worker pods, or skip/fail if absent."""
    ns = os.environ.get("EP_PERF_NAMESPACE")
    if not ns:
        missing_prerequisite("EP_PERF_NAMESPACE must be set for perf tests")
    return ns


@pytest.fixture(scope="session")
def perf_k8s_core_api() -> CoreV1Api:
    """Return a Kubernetes CoreV1Api client, or skip/fail if credentials are missing.

    Tries in-cluster config first, then local ``KUBECONFIG``.

    Returns:
        Configured ``CoreV1Api`` with a pool sized for concurrent perf polls.

    """
    from kubernetes import client as k8s_client
    from kubernetes import config as k8s_config
    from kubernetes.config import ConfigException

    try:
        k8s_config.load_incluster_config()
    except ConfigException:
        try:
            k8s_config.load_kube_config()
        except ConfigException:
            missing_prerequisite("Kubernetes cluster credentials not available for perf tests")

    # The default urllib3 pool holds 4 connections; concurrent perf batches poll the
    # API from ~100 threads, which would discard/re-open connections (extra TLS
    # handshakes) and add latency noise. Size the pool for the largest batch.
    configuration = k8s_client.Configuration.get_default_copy()
    configuration.connection_pool_maxsize = K8S_CONNECTION_POOL_MAXSIZE
    return k8s_client.CoreV1Api(k8s_client.ApiClient(configuration))
