"""Reconcile EP-owned cluster desired state independently from the API."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import structlog
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import col

from execution_plane.cluster.cluster_registry import ClusterRegistry, NoopDiscoveryMechanism
from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.execution_target.execution_target_registry import ExecutionTargetRegistry
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.cluster_binding import ClusterBinding
from execution_plane.models.execution_target_placement import KubernetesPlacement

logger = structlog.stdlib.get_logger(__name__)

POLL_SECONDS = 3
SYSTEM_ACTOR_ID = uuid.UUID(int=0)


class ClusterBindingReconciler:
    """Apply durable, versioned integration state to EP-owned cluster resources."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        cluster_store: ClusterStore,
        target_store: ExecutionTargetStore,
    ) -> None:
        """Use a short-lived session factory and the registry's resource stores."""
        self._session_factory = session_factory
        self._cluster_store = cluster_store
        self._registry = ClusterRegistry(
            cluster_store,
            ExecutionTargetRegistry(target_store),
            NoopDiscoveryMechanism(),
        )

    async def run(self) -> None:
        """Claim desired states and retry transient setup/drain errors forever."""
        while True:
            try:
                bindings = await self._claim()
                for binding in bindings:
                    await self._reconcile(binding)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Cluster-binding reconciliation iteration failed")
            await asyncio.sleep(POLL_SECONDS)

    async def _claim(self) -> list[ClusterBinding]:
        async with self._session_factory() as session:
            result = await session.execute(
                select(ClusterBinding)
                .where(
                    or_(
                        col(ClusterBinding.status).in_(["pending", "error", "deleting"]),
                        and_(
                            col(ClusterBinding.status) == "reconciling",
                            col(ClusterBinding.updated_at) < datetime.now(UTC) - timedelta(seconds=60),
                        ),
                    )
                )
                .order_by(col(ClusterBinding.updated_at))
                .limit(50)
                .with_for_update(skip_locked=True)
            )
            bindings = list(result.scalars().all())
            for binding in bindings:
                binding.status = "reconciling"
                binding.updated_at = datetime.now(UTC)
            await session.commit()
            return bindings

    async def _reconcile(self, binding: ClusterBinding) -> None:
        """Apply one desired state and persist only if the revision is still current."""
        try:
            cluster = await self._cluster_store.get_by_source(binding.client_id, binding.source_integration_id)
            if not binding.enabled:
                status = await self._reconcile_delete(cluster)
                cluster_id = None if cluster is None else cluster.id
            else:
                status, cluster_id = await self._reconcile_upsert(binding, cluster)
            message = None
        except Exception as exc:
            status = "error"
            cluster_id = binding.cluster_id
            message = "Cluster setup failed; EP will retry. Review EP worker logs for details."
            logger.exception(
                "Cluster binding could not be reconciled",
                source_integration_id=str(binding.source_integration_id),
                desired_revision=binding.revision,
                error_type=type(exc).__name__,
            )
        async with self._session_factory() as session:
            current = await session.get(
                ClusterBinding,
                (binding.client_id, binding.source_integration_id),
                with_for_update=True,
            )
            if current is None or current.revision != binding.revision:
                return
            current.cluster_id = cluster_id
            current.observed_revision = binding.revision
            current.status = status
            current.status_message = message
            current.updated_at = datetime.now(UTC)
            if status == "deleted":
                current.credential = ""
                current.ca_certificate = None
            await session.commit()

    async def _reconcile_upsert(
        self,
        binding: ClusterBinding,
        cluster: Cluster | None,
    ) -> tuple[str, uuid.UUID | None]:
        """Create or update a cluster and its default target using stable identity."""
        if cluster is None:
            created = await self._registry.provision(
                binding.name,
                binding.endpoint,
                binding.credential,
                KubernetesPlacement(namespace=binding.namespace),
                SYSTEM_ACTOR_ID,
                binding.labels,
                source_client_id=binding.client_id,
                source_integration_id=binding.source_integration_id,
                source_revision=binding.revision,
                project_ids=binding.project_ids,
            )
            created = await self._cluster_store.update_source_binding(
                created.id,
                client_id=binding.client_id,
                integration_id=binding.source_integration_id,
                revision=binding.revision,
                project_ids=binding.project_ids,
                ca_certificate=binding.ca_certificate,
            )
            return ("ready" if created.status is ClusterStatus.ACTIVE else "error", created.id)

        current = cluster
        if current.status is ClusterStatus.DRAINING or not current.enabled:
            current = await self._registry.provision(
                binding.name,
                binding.endpoint,
                binding.credential,
                KubernetesPlacement(namespace=binding.namespace),
                SYSTEM_ACTOR_ID,
                binding.labels,
                source_client_id=binding.client_id,
                source_integration_id=binding.source_integration_id,
                source_revision=binding.revision,
                project_ids=binding.project_ids,
            )
        await self._registry.sync_update(
            current.id,
            updated_by=SYSTEM_ACTOR_ID,
            name=binding.name,
            endpoint=binding.endpoint,
            api_key=binding.credential,
            placement=KubernetesPlacement(namespace=binding.namespace),
        )
        current = await self._cluster_store.update_source_binding(
            current.id,
            client_id=binding.client_id,
            integration_id=binding.source_integration_id,
            revision=binding.revision,
            project_ids=binding.project_ids,
            ca_certificate=binding.ca_certificate,
        )
        return ("ready" if current.status is ClusterStatus.ACTIVE and current.enabled else "error", current.id)

    async def _reconcile_delete(self, cluster: Cluster | None) -> str:
        """Disable placement first, then report deletion after EP drains its work."""
        if cluster is None:
            return "deleted"
        if cluster.status is not ClusterStatus.DRAINING:
            await self._registry.request_delete(cluster.id, SYSTEM_ACTOR_ID)
        return "deleting"


async def run_cluster_binding_reconciler(database_url: str) -> None:
    """Run the EP controller with its own database pool."""
    engine = create_async_engine(database_url)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    cluster_store = ClusterStore.from_engine(engine)
    target_store = ExecutionTargetStore.from_engine(engine)
    reconciler = ClusterBindingReconciler(session_factory, cluster_store, target_store)
    try:
        await reconciler.run()
    finally:
        await engine.dispose()
