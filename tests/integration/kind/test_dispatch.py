"""End-to-end dispatch integration test: submit → dispatch → COMPLETED.

Requires a real PostgreSQL database (``EP_TEST_DATABASE_URL``) and a live
Kubernetes cluster (``EP_IT_K8S_*`` env vars). Both are provisioned in CI by
the ``test-integration`` GitHub Actions job. Tests are skipped automatically
when either resource is absent.
"""

from __future__ import annotations

import os
import uuid

import pytest

from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.config import get_ep_settings
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.execution_target_reconciler.placement import WorkerManagerRegistry
from execution_plane.models.execution_target import BackendType
from execution_plane.models.work_item import WorkItemStatus
from execution_plane.work_store import WorkStore
from execution_plane.worker import _process_item
from execution_plane.worker_manager.vanilla_k8s.manager import VanillaK8sWorkerManager

_NODE_IMAGE = "quay.io/ahetheri/syntara-node-script:migration-test"

_SCRIPT_INVOCATION = {
    "version": 1,
    "operation": "execute",
    "inputs": {"language": "bash", "code": "echo ep-integration-ok"},
    "credentials": {"resolved": {}},
    "workflow_context": {},
    "settings": {},
    "timeout_seconds": 120,
}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_work_item_dispatched_to_kind_cluster_reaches_completed(ep_cluster) -> None:
    """Submit a script work item and assert it completes through a single dispatch cycle.

    The ep_cluster fixture provisions an ACTIVE Cluster+ExecutionTarget backed by
    the kind cluster provisioned in CI. claim_one() assigns that target to the work
    item; _process_item() drives the full cold-start pod lifecycle synchronously.
    """
    database_url = os.environ["EP_TEST_DATABASE_URL"]
    settings = get_ep_settings()
    client_id = "ep-it-dispatch"

    async with (
        WorkStore.from_database_url(database_url) as work_store,
        ClusterStore.from_database_url(database_url) as cluster_store,
        ExecutionTargetStore.from_database_url(database_url) as target_store,
    ):
        item = await work_store.dispatch(
            client_id=client_id,
            item_id=uuid.uuid4(),
            payload={
                "image": _NODE_IMAGE,
                "invocation": _SCRIPT_INVOCATION,
            },
        )

        claimed = await work_store.claim_one()
        assert claimed is not None, "claim_one() returned None — no eligible target is ACTIVE"
        assert claimed.id == item.id

        worker_managers = WorkerManagerRegistry()
        workload_manager = VanillaK8sWorkerManager(target_store, cluster_store, work_store, settings)
        worker_managers.register(BackendType.VANILLA_K8S, workload_manager)

        await _process_item(claimed, work_store, target_store, worker_managers, settings)

        final_item = await work_store.get(item.id, client_id=client_id)

    assert final_item is not None
    assert final_item.status is WorkItemStatus.COMPLETED, (
        f"Expected COMPLETED but got {final_item.status}; result={final_item.result}"
    )
