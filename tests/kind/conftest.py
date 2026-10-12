"""Kubernetes cluster fixture for the dispatch integration tests.

These tests need a live Kubernetes cluster in addition to PostgreSQL. The
cluster is provisioned *outside* the suite (a kind cluster in CI, any reachable
cluster locally) and its coordinates are passed in through the environment:

* ``EP_IT_K8S_ENDPOINT``  — API server URL (e.g. ``https://127.0.0.1:PORT``)
* ``EP_IT_K8S_TOKEN``     — bearer token for the ``syntara-dispatcher`` ServiceAccount
* ``EP_IT_K8S_NAMESPACE`` — target namespace (default: ``execution-plane``)
* ``EP_IT_K8S_CA_CERT``   — PEM CA certificate for the API server (optional; skips
                            TLS verification when absent and ``verify_ssl`` is False)

The ``test-integration-postgres-kind`` CI job provisions a kind cluster and
exports all of these. Local developers can point at any running cluster that
satisfies the RBAC in ``docs/feature-branch-assets/execution-plane-init.yaml``.

The shared PostgreSQL fixtures (``migrated_database`` etc.) come from the parent
``tests/integration/conftest.py``.
"""

from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING

import pytest

from execution_plane.cluster.cluster_registry import ClusterRegistry, NoopDiscoveryMechanism
from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.execution_target.execution_target_registry import ExecutionTargetRegistry
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.models.cluster import Cluster, ClusterType
from execution_plane.models.execution_target_placement import KubernetesPlacement

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

_EP_IT_ENDPOINT = "EP_IT_K8S_ENDPOINT"
_EP_IT_TOKEN = "EP_IT_K8S_TOKEN"  # noqa: S105 — env var name, not a secret value
_EP_IT_NAMESPACE = "EP_IT_K8S_NAMESPACE"
_EP_IT_CA_CERT = "EP_IT_K8S_CA_CERT"
_DEFAULT_NAMESPACE = "execution-plane"
_CLUSTER_NAME = "integration-test"
_CLI_ACTOR_ID = uuid.UUID(int=0)


@pytest.fixture(scope="session")
async def ep_cluster(migrated_database: str) -> AsyncGenerator[Cluster, None]:
    """Provision a kind cluster as an ACTIVE Cluster+ExecutionTarget in the test DB.

    Requires ``EP_IT_K8S_ENDPOINT`` and ``EP_IT_K8S_TOKEN`` in the environment.
    Yields the provisioned ``Cluster`` and tears it down at the end of the session.
    """
    endpoint = os.environ[_EP_IT_ENDPOINT]
    token = os.environ[_EP_IT_TOKEN]
    namespace = os.environ.get(_EP_IT_NAMESPACE, _DEFAULT_NAMESPACE)
    ca_cert = os.environ.get(_EP_IT_CA_CERT)

    cluster_store = ClusterStore.from_database_url(migrated_database)
    target_store = ExecutionTargetStore.from_database_url(migrated_database)

    cluster: Cluster | None = None
    try:
        target_registry = ExecutionTargetRegistry(target_store)
        cluster_registry = ClusterRegistry(cluster_store, target_registry, NoopDiscoveryMechanism())

        cluster = await cluster_registry.provision(
            _CLUSTER_NAME,
            endpoint,
            token,
            KubernetesPlacement(namespace=namespace),
            _CLI_ACTOR_ID,
            labels={"provider": "kind", "cluster": _CLUSTER_NAME},
            cluster_type=ClusterType.OPENSHIFT,
        )

        if ca_cert:
            cluster = await cluster_store.update(
                cluster.id,
                updated_by=_CLI_ACTOR_ID,
                ca_certificate=ca_cert,
            )

        yield cluster

    finally:
        if cluster is not None:
            await cluster_store.request_delete(cluster.id, _CLI_ACTOR_ID)
            for target in await target_store.list(cluster_id=cluster.id):
                await target_store.finalize_delete(target.id)
            await cluster_store.finalize_delete(cluster.id)
        await cluster_store.close()
        await target_store.close()
