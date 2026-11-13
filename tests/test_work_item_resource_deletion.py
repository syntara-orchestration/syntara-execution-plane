"""Explicit WorkItem deletion preserves resource ownership and stop confirmation."""

from contextlib import nullcontext
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from kubernetes.client.exceptions import ApiException

from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.work_item.work_item_store import WorkStore
from execution_plane.worker_manager.vanilla_k8s import cleanup as resource_cleanup
from execution_plane_client.models.execution_target import BackendType, KubernetesPlacement


@pytest.fixture
def resources(monkeypatch):
    item = WorkItem(
        id=uuid4(),
        client_id="ao",
        status=WorkItemStatus.RECONCILIATION_REQUIRED,
        created_at=datetime.now(UTC),
        claim_generation=1,
        backend_resource_name="owned-job",
        backend_resource_uid="owned-uid",
        resource_cleanup_status="deferred",
        deletion_requested=True,
        cancellation_requested=True,
    )
    batch, pods, network = Mock(), Mock(), Mock()
    batch.read_namespaced_job.side_effect = ApiException(status=404)
    pods.list_namespaced_pod.return_value.items = []
    network.read_namespaced_network_policy.side_effect = ApiException(status=404)
    monkeypatch.setattr(resource_cleanup, "api_client_from_cluster", lambda *_: nullcontext(object()))
    monkeypatch.setattr(resource_cleanup.client, "BatchV1Api", lambda _: batch)
    monkeypatch.setattr(resource_cleanup.client, "CoreV1Api", lambda _: pods)
    monkeypatch.setattr(resource_cleanup.client, "NetworkingV1Api", lambda _: network)
    return item, batch, pods, network


def _metadata(item, *, uid="owned-uid") -> SimpleNamespace:
    return SimpleNamespace(
        uid=uid,
        labels={
            "app.kubernetes.io/managed-by": "execution-plane",
            "execution-plane.syntara.io/work-item": str(item.id),
            "execution-plane.syntara.io/attempt": f"{item.id}-{item.claim_generation}",
        },
    )


def test_missing_resources_confirm_cleanup(resources):
    item, _batch, _pods, _network = resources
    assert resource_cleanup.delete_work_item_resources(object(), "token", "namespace", item) is True


def test_job_deletion_does_not_imply_cleanup_complete(resources):
    item, batch, _pods, network = resources
    batch.read_namespaced_job.side_effect = None
    batch.read_namespaced_job.return_value.metadata = _metadata(item)
    assert resource_cleanup.delete_work_item_resources(object(), "token", "namespace", item) is False
    batch.delete_namespaced_job.assert_called_once()
    options = batch.delete_namespaced_job.call_args.kwargs["body"]
    assert options.preconditions.uid == "owned-uid"
    assert options.propagation_policy == "Foreground"
    network.delete_namespaced_network_policy.assert_not_called()


@pytest.mark.parametrize("change", ["uid", "attempt", "owner"])
def test_foreign_or_recreated_job_is_not_deleted(resources, change):
    item, batch, _pods, network = resources
    metadata = _metadata(item)
    if change == "uid":
        metadata.uid = "recreated"
    elif change == "attempt":
        metadata.labels["execution-plane.syntara.io/attempt"] = "another-attempt"
    else:
        metadata.labels["execution-plane.syntara.io/work-item"] = str(uuid4())
    batch.read_namespaced_job.side_effect = None
    batch.read_namespaced_job.return_value.metadata = metadata
    assert resource_cleanup.delete_work_item_resources(object(), "token", "namespace", item) is False
    batch.delete_namespaced_job.assert_not_called()
    network.delete_namespaced_network_policy.assert_not_called()


def test_remaining_pod_prevents_policy_deletion(resources):
    item, _batch, pods, network = resources
    pods.list_namespaced_pod.return_value.items = [object()]
    assert resource_cleanup.delete_work_item_resources(object(), "token", "namespace", item) is False
    network.read_namespaced_network_policy.assert_not_called()


def test_policy_deletion_uses_its_uid_and_waits_for_absence(resources):
    item, _batch, _pods, network = resources
    network.read_namespaced_network_policy.side_effect = None
    network.read_namespaced_network_policy.return_value.metadata = _metadata(item, uid="policy-uid")
    assert resource_cleanup.delete_work_item_resources(object(), "token", "namespace", item) is False
    assert network.delete_namespaced_network_policy.call_args.kwargs["body"].preconditions.uid == "policy-uid"


async def test_resource_cleanup_failure_retains_intent_and_continues(resources, monkeypatch):
    item, _batch, _pods, _network = resources
    item.execution_target_id = uuid4()
    store, targets, clusters, resolver = AsyncMock(spec=WorkStore), AsyncMock(), AsyncMock(), AsyncMock()
    store.work_awaiting_requested_cleanup.side_effect = [[item], []]
    targets.get_with_credentials.side_effect = RuntimeError("cluster unavailable")
    await resource_cleanup.cleanup_requested_work_item_resources(store, targets, clusters, resolver)
    store.update_backend_resource.assert_not_awaited()
    assert item.deletion_requested is True


@pytest.mark.parametrize("confirmed", [False, True])
async def test_cleanup_records_completion_only_after_confirmed_absence(resources, monkeypatch, confirmed):
    item, _batch, _pods, _network = resources
    item.execution_target_id = uuid4()
    store, targets, clusters, resolver = AsyncMock(spec=WorkStore), AsyncMock(), AsyncMock(), AsyncMock()
    store.work_awaiting_requested_cleanup.side_effect = [[item], []]
    targets.get_with_credentials.return_value = SimpleNamespace(
        backend_type=BackendType.KUBERNETES,
        placement=KubernetesPlacement(namespace="namespace"),
        cluster_id=uuid4(),
    )
    cluster = SimpleNamespace(source_client_id="ao", credential_ref=str(uuid4()))
    clusters.get_with_credentials.return_value = cluster
    resolver.resolve.return_value = "test-token"
    cleanup = Mock(return_value=confirmed)
    monkeypatch.setattr(resource_cleanup, "delete_work_item_resources", cleanup)

    await resource_cleanup.cleanup_requested_work_item_resources(store, targets, clusters, resolver)

    cleanup.assert_called_once_with(cluster, "test-token", "namespace", item)
    resolver.resolve.assert_awaited_once_with(cluster.credential_ref, client_id="ao")
    if confirmed:
        store.update_backend_resource.assert_awaited_once_with(
            item.id,
            resource_name=item.backend_resource_name,
            resource_uid=item.backend_resource_uid,
            cleanup_status="complete",
            claim_owner_id=None,
            claim_generation=item.claim_generation,
        )
    else:
        store.update_backend_resource.assert_not_awaited()
    assert store.work_awaiting_requested_cleanup.await_args.kwargs == {"after_id": item.id}
