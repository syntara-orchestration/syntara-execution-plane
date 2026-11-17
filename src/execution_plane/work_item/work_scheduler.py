"""Claim and place WorkItem use cases."""

from __future__ import annotations

from typing import TYPE_CHECKING

from execution_plane.execution_target_reconciler.adapters import (
    cluster_snapshot_from_model,
    execution_target_snapshot_from_model,
)
from execution_plane.execution_target_reconciler.types import WorkRequirements
from execution_plane_client.models.work_item import WorkItemStatus

if TYPE_CHECKING:
    from execution_plane.execution_target_reconciler.placement import PlacementResolver
    from execution_plane.models.cluster import Cluster
    from execution_plane.models.execution_target import ExecutionTarget
    from execution_plane.models.work_item import WorkItem
    from execution_plane.work_item.work_item_store import WorkStore


class WorkScheduler:
    """Claim queued Work and assign an eligible ExecutionTarget."""

    def __init__(self, store: WorkStore, placement_resolver: PlacementResolver) -> None:
        """Use transactional claim transitions and the placement policy."""
        self._store = store
        self._placement_resolver = placement_resolver

    async def claim_and_place_work(self) -> WorkItem | None:
        """Return one newly cancelled item, or claim and place runnable work."""
        item = await self._store.reserve_one()
        if item is None or item.status is WorkItemStatus.CANCELLED:
            return item
        return await self._place_claimed_item(item)

    async def _place_claimed_item(self, item: WorkItem) -> WorkItem | None:
        claim_owner_id = item.claim_owner_id
        if claim_owner_id is None:
            raise ValueError("Reserved work item has no claim owner")  # noqa: TRY003, EM101

        requirements = self._requirements_for(item)
        resolved = await self._placement_resolver.resolve(requirements)
        if not resolved.available_targets:
            await self._store.set_result(
                item.id,
                {"error": "No execution target matched placement requirements", "error_type": "TargetUnavailable"},
                WorkItemStatus.FAILED,
                claim_owner_id=claim_owner_id,
                claim_generation=item.claim_generation,
            )
            return None

        async def target_is_eligible(cluster: Cluster, target: ExecutionTarget) -> bool:
            cluster_snapshot = cluster_snapshot_from_model(cluster)
            return await self._placement_resolver.target_is_eligible(
                cluster_snapshot,
                execution_target_snapshot_from_model(target, cluster_snapshot),
                requirements,
            )

        for target in resolved.available_targets:
            assigned = await self._store.assign_target(
                item.id,
                target.id,
                claim_owner_id=claim_owner_id,
                claim_generation=item.claim_generation,
                eligibility=target_is_eligible,
            )
            if assigned is not None:
                return assigned

        await self._store.release_claim(
            item.id,
            claim_owner_id=claim_owner_id,
            claim_generation=item.claim_generation,
        )
        return None

    @staticmethod
    def _requirements_for(item: WorkItem) -> WorkRequirements:
        """Extract resolver requirements from the versioned work payload."""
        selectors: dict[str, str] = item.payload.get("selectors", {})
        if not isinstance(selectors, dict):
            selectors = {}
        workload_type = item.payload.get("workload_type")
        return WorkRequirements(
            selectors=selectors,
            workload_type=workload_type if isinstance(workload_type, str) else None,
        )
