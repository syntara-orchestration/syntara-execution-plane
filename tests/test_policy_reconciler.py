"""Credential handling for Kubernetes policy reconciliation."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane.worker_manager.vanilla_k8s.reconciler import _target_config
from execution_plane_client.models.execution_target import KubernetesPlacement


def test_policy_reconciler_uses_the_resolved_cluster_credential() -> None:
    now = datetime.now(UTC)
    cluster = Cluster(
        id=uuid4(),
        name="workers",
        endpoint="https://cluster.example",
        credential_ref=uuid4(),
        status=ClusterStatus.ACTIVE,
        created_by=uuid4(),
        created_at=now,
        updated_by=uuid4(),
        updated_at=now,
    )
    target = ExecutionTarget(
        cluster_id=cluster.id,
        name="default",
        backend_type=BackendType.KUBERNETES,
        endpoint=cluster.endpoint,
        placement=KubernetesPlacement(namespace="execution"),
        credential_ref=uuid4(),
        status=TargetStatus.ACTIVE,
        created_by=uuid4(),
        created_at=now,
        updated_by=uuid4(),
        updated_at=now,
    )

    connection = _target_config(target, cluster, "resolved-token")

    assert connection is not None
    assert connection[0]["token"] == "resolved-token"  # noqa: S105
