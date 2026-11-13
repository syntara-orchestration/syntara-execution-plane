"""Kind integration: Resource Monitor probes a live cluster and excludes degraded targets.

Requires PostgreSQL (``EP_TEST_DATABASE_URL``) and Kind coordinates (``EP_IT_K8S_*``),
same as the dispatch tests. Skips when either is absent.
"""

from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING

import pytest

from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.config.settings import get_settings
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.execution_target_reconciler.adapters import build_placement_resolver
from execution_plane.execution_target_reconciler.types import IneligibilityReason, ResolveOutcome, WorkRequirements
from execution_plane.resource_monitor.monitor import ResourceMonitor
from execution_plane_client.models.execution_target import BackendType, KubernetesPlacement, TargetStatus

if TYPE_CHECKING:
    from execution_plane.credential_resolution.cluster import ClusterCredentialResolver
    from execution_plane.models.cluster import Cluster

_ACTOR_ID = uuid.UUID(int=0)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_resource_monitor_excludes_a_missing_namespace_on_kind(
    ep_cluster: Cluster,
    integration_credential_resolver: ClusterCredentialResolver,
) -> None:
    """Probe the live Kind API: a missing namespace becomes DEGRADED and HEALTH-ineligible."""
    database_url = os.environ["EP_TEST_DATABASE_URL"]
    settings = get_settings().model_copy(
        update={
            "resource_monitor_interval_seconds": 5.0,
            "resource_monitor_probe_timeout_seconds": 3.0,
        }
    )
    marker = uuid.uuid4().hex[:8]
    missing_namespace = f"ep-missing-{marker}"
    extra_id: uuid.UUID | None = None

    async with (
        ClusterStore.from_database_url(database_url) as cluster_store,
        ExecutionTargetStore.from_database_url(database_url) as target_store,
    ):
        defaults = await target_store.list(cluster_id=ep_cluster.id)
        assert defaults, "Expected the kind fixture to provision an ExecutionTarget"
        default = defaults[0]
        extra = await target_store.create(
            ep_cluster.id,
            f"missing-{marker}",
            BackendType.KUBERNETES,
            ep_cluster.endpoint,
            uuid.uuid4(),
            is_default=False,
            created_by=_ACTOR_ID,
            placement=KubernetesPlacement(namespace=missing_namespace),
            labels={"probe": marker},
        )
        extra_id = extra.id
        try:
            await target_store.activate(extra.id, _ACTOR_ID)
            monitor = ResourceMonitor(
                target_store,
                cluster_store,
                settings,
                cluster_credential_resolver=integration_credential_resolver,
                interval=0.01,
            )

            await monitor.scan()
            after_one = await target_store.get(extra.id)
            assert after_one is not None
            assert after_one.status is TargetStatus.ACTIVE

            for _ in range(settings.resource_monitor_failure_threshold - 1):
                await monitor.scan()

            healthy = await target_store.get(default.id)
            degraded = await target_store.get(extra.id)
            assert healthy is not None
            assert healthy.status is TargetStatus.ACTIVE
            assert degraded is not None
            assert degraded.status is TargetStatus.DEGRADED
            assert degraded.status_message == f"Namespace {missing_namespace} does not exist"

            resolver = build_placement_resolver(cluster_store, target_store)
            default_result = await resolver.resolve(WorkRequirements())
            assert default_result.outcome is ResolveOutcome.MATCHED
            assert default.id in {target.id for target in default_result.available_targets}

            missing_result = await resolver.resolve(WorkRequirements(selectors={"probe": marker}))
            assert missing_result.outcome is ResolveOutcome.NO_MATCHING_TARGETS
            assert missing_result.available_targets == []
            assert any(
                item.target.id == extra.id and item.reason is IneligibilityReason.HEALTH
                for item in missing_result.ineligible_targets
            )
        finally:
            if extra_id is not None:
                await target_store.request_delete(extra_id, _ACTOR_ID)
                await target_store.finalize_delete(extra_id)
