"""Execution Plane worker — executes accepted work and records completion events."""

from __future__ import annotations

import asyncio
import contextlib
import logging

import asyncpg  # type: ignore[import-untyped]
import structlog

from execution_plane.cluster.binding_reconciler import run_cluster_binding_reconciler
from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.config import get_ep_settings, to_asyncpg_url
from execution_plane.drain_monitor import DrainMonitor
from execution_plane.event_delivery import CompletionEventDelivery
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.execution_target_reconciler.adapters import build_placement_resolver
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.work_store import WorkStore
from execution_plane.worker_manager.kubernetes_job import (
    KubernetesJobManager,
    WorkloadCancelledError,
    WorkloadExecutionError,
    WorkloadOutcomeUnknownError,
)

logger = structlog.stdlib.get_logger(__name__)

POLL_INTERVAL_SECONDS = 5
NOTIFY_CHANNEL = "execution_plane_work_items"


async def _process_item(
    item: WorkItem,
    store: WorkStore,
    target_store: ExecutionTargetStore,
    cluster_store: ClusterStore,
    workload_manager: KubernetesJobManager,
) -> None:
    """Dispatch in a workload pod and persist its result or a confirmed failure."""
    wi_id = str(item.id)
    if item.execution_target_id is None:
        await store.set_result(
            item.id,
            {"error": "No execution target was assigned", "error_type": "TargetUnavailable"},
            WorkItemStatus.FAILED,
        )
        return
    target = await target_store.get(item.execution_target_id, include_secret=True)
    if target is None:
        await store.set_result(
            item.id,
            {"error": "Assigned execution target no longer exists", "error_type": "TargetUnavailable"},
            WorkItemStatus.FAILED,
        )
        return
    if not isinstance(target.placement, KubernetesPlacement):
        await store.set_result(
            item.id,
            {
                "error": "Assigned execution target placement is not supported by this worker",
                "error_type": "TargetUnavailable",
            },
            WorkItemStatus.FAILED,
        )
        return
    cluster = await cluster_store.get(target.cluster_id, include_secret=True)
    if cluster is None:
        await store.set_result(
            item.id,
            {"error": "Assigned cluster no longer exists", "error_type": "TargetUnavailable"},
            WorkItemStatus.FAILED,
        )
        return

    create_if_missing = item.status is WorkItemStatus.CLAIMED
    # Record the external-dispatch boundary before a request whose response may
    # be lost. Recovery only attaches to the deterministic Job.
    if create_if_missing and not await store.mark_dispatched(item.id):
        return
    try:
        activity_result = await workload_manager.execute(
            work_item_id=item.id,
            endpoint=cluster.endpoint,
            api_token=cluster.api_key,
            ca_certificate=cluster.ca_certificate,
            namespace=target.placement.namespace,
            payload=item.payload,
            create_if_missing=create_if_missing,
            heartbeat=lambda: store.refresh_claim(item.id),
        )
        item = await store.set_result(item.id, activity_result, WorkItemStatus.COMPLETED)
        logger.info("Script executed successfully", work_item_id=wi_id)
    except WorkloadExecutionError as exc:
        await store.set_result(item.id, exc.result, WorkItemStatus.FAILED)
        logger.warning("Isolated workload failed", work_item_id=wi_id, error=str(exc))
    except WorkloadCancelledError as exc:
        await store.set_result(
            item.id,
            {"cancelled": True, "execution_started": exc.execution_started, "reason": str(exc)},
            WorkItemStatus.CANCELLED,
        )
        logger.info("Isolated workload cancellation confirmed", work_item_id=wi_id)
    except WorkloadOutcomeUnknownError as exc:
        # Keep the uncertain result visible and recover only by attaching to the
        # deterministic Job; never create a second Job for an ambiguous dispatch.
        await store.mark_reconciliation_required(item.id, str(exc))
        logger.exception("Isolated workload outcome needs reconciliation", work_item_id=wi_id, error=str(exc))
    except Exception as e:
        logger.exception("Could not reconcile isolated workload", work_item_id=wi_id, error_type=type(e).__name__)


async def _listen_loop(database_url: str, wakeup_event: asyncio.Event) -> None:
    """Hold a LISTEN connection and set the wakeup_event on every NOTIFY.

    Known gap: a zombie TCP connection (NAT expiry, silent load-balancer drop,
    VM migration) will not trigger the termination listener, so the worker
    silently falls back to POLL_INTERVAL_SECONDS cadence until the OS-level
    TCP keepalive eventually kills the connection. Fix: periodic self-NOTIFY or
    a LISTEN/UNLISTEN probe to detect stale connections. See AAP-92715.
    """
    while True:
        try:
            disconnected = asyncio.Event()
            conn: asyncpg.Connection = await asyncpg.connect(database_url)
            try:
                conn.add_termination_listener(lambda _, ev=disconnected: ev.set())
                await conn.add_listener(NOTIFY_CHANNEL, lambda *_: wakeup_event.set())
                # Recheck work queued before LISTEN became active (also on reconnect).
                wakeup_event.set()
                logger.info("Listening for notifications", channel=NOTIFY_CHANNEL)
                await disconnected.wait()
            finally:
                with contextlib.suppress(Exception):
                    await conn.close()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Notification listener failed, reconnecting in 5s")
            await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def _poll_loop(
    store: WorkStore,
    target_store: ExecutionTargetStore,
    cluster_store: ClusterStore,
    workload_manager: KubernetesJobManager,
    wakeup_event: asyncio.Event,
) -> None:
    """Claim one work item at a time; sleep between polls when queue is empty."""
    logger.info("Execution Plane worker started, polling for work items")
    wakeup_event.set()  # process any items already present at startup
    while True:
        item = None
        try:
            item = await store.claim_one()
            if item:
                logger.info("Claimed work item", work_item_id=str(item.id))
                await _process_item(item, store, target_store, cluster_store, workload_manager)
        except Exception:
            logger.exception("Error in polling loop, will retry")

        if not item:
            wakeup_event.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(wakeup_event.wait(), timeout=POLL_INTERVAL_SECONDS)


async def run_worker(database_url: str) -> None:
    """Run processing and callback delivery against the EP-owned database.

    Cancellation closes both the notification listener and the polling task,
    then disposes the WorkStore.
    """
    settings = get_ep_settings()
    async with (
        WorkStore.from_database_url(database_url) as work_store,
        ClusterStore.from_database_url(database_url) as cluster_store,
        ExecutionTargetStore.from_database_url(database_url) as target_store,
    ):
        event_delivery = CompletionEventDelivery(settings)
        workload_manager = KubernetesJobManager(settings)
        drain_monitor = DrainMonitor(target_store, cluster_store, work_store)
        placement_resolver = build_placement_resolver(cluster_store, target_store)
        logger.debug(
            "ExecutionTarget reconciler constructed",
            resolver=type(placement_resolver).__name__,
        )
        await drain_monitor.start()
        try:
            wakeup_event = asyncio.Event()
            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    _listen_loop(to_asyncpg_url(database_url), wakeup_event),
                    name="ep-listener",
                )
                tg.create_task(
                    _poll_loop(work_store, target_store, cluster_store, workload_manager, wakeup_event),
                    name="ep-poll",
                )
                tg.create_task(event_delivery.run(work_store), name="ep-completion-delivery")
                tg.create_task(run_cluster_binding_reconciler(database_url), name="ep-cluster-bindings")
        finally:
            await drain_monitor.stop()
            await event_delivery.close()


async def _run() -> None:
    settings = get_ep_settings()
    await run_worker(settings.database_url)


def main() -> None:
    """Entry point for the execution-plane-worker CLI command."""
    logging.basicConfig(level=logging.INFO)
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )
    asyncio.run(_run())


if __name__ == "__main__":
    main()
