"""Fixtures for execution-plane performance tests.

Performance tests require a dedicated environment. Every required input below
skips the test (or session) when absent so runners without a perf stack do
not fail with FileNotFoundError / connection errors.

Set ``EP_PERF_REQUIRED=1`` in dedicated perf CI so a missing prerequisite
fails the job instead of skipping every test green.

Required for pod-dispatch / API tests (PERF-A / PERF-B / PERF-S):

* ``EP_TEST_DATABASE_URL`` — PostgreSQL URL used by the deployed EP stack
* ``EP_PERF_API_URL`` — base URL of the deployed EP API
* ``EP_PERF_NAMESPACE`` — Kubernetes namespace where EP deploys worker pods
* Kubernetes credentials — ``KUBECONFIG`` or an in-cluster service account
* ``EP_NODE_IMAGE`` — cluster-pullable worker image

Required for DB-only tests (3.1 / 3.2):

* ``EP_PERF_DB_URL`` — exclusive PostgreSQL URL with **no live EP worker or
  CompletionNotifier connected**. Seeded PENDING rows and completion outbox
  events are globally claimable; a deployed worker on the same database will
  steal claims and invalidate correctness/latency/drain measurements.
  Use a disposable database, not the stack DB while workers are running.

Optional (never skip/fail when absent):

* ``EP_PERF_CREDENTIAL_ENCRYPTION_KEY`` — deployment payload key. Not required:
  DB-only tests seed/read with the dummy key from ``tests/conftest.py``; the
  shared runner waits on status/timestamps/result and never loads encrypted
  ``WorkItem.payload``. Set this only if a test must decrypt API-written payloads.
* ``EP_AO_CLIENT_ID`` — must match the deployment's AO client id (default
  ``syntara-orchestration``); used by the JWT and by DB polls
* ``EP_PERF_REQUIRED`` — when ``1``/``true``/``yes``, missing **required**
  prerequisites raise ``pytest.fail`` instead of ``pytest.skip``
* ``EP_PERF_OUTBOX_BATCH_SIZE`` — completion-notification backlog for test 3.2 (default 500)
* ``EP_PERF_OUTAGE_SECONDS`` — mock callback outage duration for test 3.2 (default 120)

Required inputs above are checked in fixtures that ``pytest.skip`` by default
(or ``pytest.fail`` when ``EP_PERF_REQUIRED=1``), so a normal ``pytest``
invocation without a perf environment stays green.
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

_TRUTHY = frozenset({"1", "true", "yes"})


def perf_required() -> bool:
    """Return whether missing perf prerequisites should fail instead of skip."""
    return os.environ.get("EP_PERF_REQUIRED", "").strip().lower() in _TRUTHY


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
    """Optionally override the dummy payload key; never skip/fail if unset.

    ``tests/conftest.py`` installs a dummy ``EP_CREDENTIAL_ENCRYPTION_KEY``.
    Current perf paths do not require the deployment key. When
    ``EP_PERF_CREDENTIAL_ENCRYPTION_KEY`` is set, override so ORM ``payload``
    loads can decrypt rows the deployed API wrote.
    """
    key = os.environ.get("EP_PERF_CREDENTIAL_ENCRYPTION_KEY")
    if key:
        monkeypatch.setenv("EP_CREDENTIAL_ENCRYPTION_KEY", key)


@pytest.fixture
def perf_node_image() -> None:
    """Require ``EP_NODE_IMAGE`` so the cluster does not pull the local-dev default.

    Requested by pod-dispatch / dispatch-overhead tests. DB-only tests (3.1 / 3.2)
    seed PostgreSQL directly and do not need a worker image.
    """
    if not os.environ.get("EP_NODE_IMAGE"):
        missing_prerequisite("EP_NODE_IMAGE must be set to a cluster-pullable worker image for perf tests")


@pytest.fixture(scope="session")
def perf_database_url() -> str:
    """Return the stack PostgreSQL URL for pod-dispatch / API tests."""
    url = os.environ.get("EP_TEST_DATABASE_URL", "").strip()
    if not url:
        missing_prerequisite("EP_TEST_DATABASE_URL must be set for perf tests")
    return url


@pytest.fixture(scope="session")
def perf_db_url() -> str:
    """Return exclusive PostgreSQL URL from ``EP_PERF_DB_URL`` (tests 3.1 / 3.2).

    Isolation contract: no EP worker (``reserve_one`` / poll loop) and no
    ``CompletionNotifier`` may be connected. Seeded work is globally visible; a
    live stack worker will race the harness.
    """
    url = os.environ.get("EP_PERF_DB_URL", "").strip()
    if not url:
        missing_prerequisite(
            "EP_PERF_DB_URL must be set for DB-only perf tests 3.1/3.2 "
            "(exclusive DB with no live EP worker or CompletionNotifier)"
        )
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
