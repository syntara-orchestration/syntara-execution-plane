"""Fixtures for execution-plane performance tests.

Performance tests require:

* A live PostgreSQL database — ``EP_TEST_DATABASE_URL``
* A deployed EP stack pointing at that database (API + worker)
* ``EP_PERF_API_URL`` — base URL of the deployed EP API
* ``EP_PERF_NAMESPACE`` — Kubernetes namespace where EP deploys worker pods
* Kubernetes credentials — ``KUBECONFIG`` or an in-cluster service account
* ``EP_API_TLS_INSECURE=1`` — set when the EP API cert is not verifiable from the
  test runner (typical for kind clusters with self-signed certificates)

All tests are skipped automatically when the above are absent so that
the standard unit/integration suite continues to work on runners without a
dedicated perf environment.
"""

from __future__ import annotations

import os

import pytest

from execution_plane.work_item_client import ep_service_token


@pytest.fixture(autouse=True)
def perf_credential_encryption_key(ep_settings_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """Decrypt persisted payloads with the deployment's key.

    ``tests/conftest.py`` installs a dummy ``EP_CREDENTIAL_ENCRYPTION_KEY`` for every
    test, which cannot decrypt rows the deployed API wrote. Depending on
    ``ep_settings_env`` makes this fixture run after it, so the real key wins.
    """
    key = os.environ.get("EP_PERF_CREDENTIAL_ENCRYPTION_KEY")
    if not key:
        pytest.skip("EP_PERF_CREDENTIAL_ENCRYPTION_KEY must be set to the deployment's key for perf tests")
    monkeypatch.setenv("EP_CREDENTIAL_ENCRYPTION_KEY", key)


@pytest.fixture(scope="session")
def perf_database_url() -> str:
    """Return the perf PostgreSQL URL, or skip the session if absent."""
    url = os.environ.get("EP_TEST_DATABASE_URL")
    if not url:
        pytest.skip("EP_TEST_DATABASE_URL must be set for perf tests")
    return url


@pytest.fixture(scope="session")
def perf_api_url() -> str:
    """Return the deployed EP API base URL, or skip the session if absent."""
    url = os.environ.get("EP_PERF_API_URL")
    if not url:
        pytest.skip("EP_PERF_API_URL must be set for perf tests")
    return url


@pytest.fixture(scope="session")
def perf_token() -> str:
    """Mint an ES256-signed EP service token valid for all projects."""
    return ep_service_token(all_projects=True)


@pytest.fixture(scope="session")
def perf_ep_namespace() -> str:
    """Return the Kubernetes namespace where EP deploys worker pods, or skip if absent."""
    ns = os.environ.get("EP_PERF_NAMESPACE")
    if not ns:
        pytest.skip("EP_PERF_NAMESPACE must be set for perf tests")
    return ns


@pytest.fixture(scope="session")
def perf_k8s_core_api():
    """Return a Kubernetes CoreV1Api client, or skip if no cluster credentials are available.

    Tries in-cluster config first (when the test runner is a pod), then falls
    back to the local ``KUBECONFIG`` for developer workstation runs.
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
            pytest.skip("Kubernetes cluster credentials not available for perf tests")

    return k8s_client.CoreV1Api()
