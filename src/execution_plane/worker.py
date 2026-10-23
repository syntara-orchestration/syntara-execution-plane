"""Execution Plane worker — executes accepted work and records completion events."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Any

import asyncpg  # type: ignore[import-untyped]
import structlog

from execution_plane.cluster.binding_reconciler import run_cluster_binding_reconciler
from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.completion_notifier.notifier import CompletionNotifier
from execution_plane.config import get_ep_settings, to_asyncpg_url
from execution_plane.credential_resolution.resolver import CredentialResolver
from execution_plane.database.ssl import ssl_connect_args_from_env
from execution_plane.drain_monitor import DrainMonitor
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.execution_target_reconciler.adapters import (
    build_placement_resolver,
)
from execution_plane.execution_target_reconciler.exceptions import UnknownBackendTypeError
from execution_plane.execution_target_reconciler.placement import WorkerManagerRegistry
from execution_plane.models.execution_target import BackendType
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.resource_monitor.monitor import ResourceMonitor
from execution_plane.runtime import run_until_shutdown
from execution_plane.work_item.work_item_store import WorkStore
from execution_plane.work_item.work_scheduler import WorkScheduler
from execution_plane.worker_manager.vanilla_k8s.manager import (
    NodeExecutionError,
    RetryableDispatchError,
    VanillaK8sWorkerManager,
    WorkItemPayloadError,
    WorkloadCancelledError,
    WorkloadOutcomeUnknownError,
)
from execution_plane.worker_manager.vanilla_k8s.reconciler import run_orphan_policy_reconciler

if TYPE_CHECKING:
    from execution_plane.config import EPSettings

logger = structlog.stdlib.get_logger(__name__)

POLL_INTERVAL_SECONDS = 5
NOTIFY_CHANNEL = "execution_plane_work_items"


async def _process_item(  # noqa: C901, PLR0912 - dispatch outcomes have distinct persistence transitions
    item: WorkItem,
    work_progress: WorkStore,
    target_queries: ExecutionTargetStore,
    worker_managers: WorkerManagerRegistry,
    settings: EPSettings,
) -> None:
    """Dispatch through the selected WorkerManager and persist safe terminal states."""
    wi_id = str(item.id)
    claim_owner_id = item.claim_owner_id
    if claim_owner_id is None:
        logger.error("Claimed work item has no claim owner", work_item_id=wi_id)
        return
    claim_generation = item.claim_generation
    if item.execution_target_id is None:
        await work_progress.set_result(
            item.id,
            {"error": "No execution target was assigned", "error_type": "TargetUnavailable"},
            WorkItemStatus.FAILED,
            claim_owner_id=claim_owner_id,
            claim_generation=claim_generation,
        )
        return
    target = await target_queries.get(item.execution_target_id)
    if target is None:
        await work_progress.set_result(
            item.id,
            {"error": "Assigned execution target no longer exists", "error_type": "TargetUnavailable"},
            WorkItemStatus.FAILED,
            claim_owner_id=claim_owner_id,
            claim_generation=claim_generation,
        )
        return
    try:
        workload_manager = worker_managers.get(target.backend_type)
    except UnknownBackendTypeError:
        await work_progress.set_result(
            item.id,
            {
                "error": f"No WorkerManager is registered for backend '{target.backend_type.value}'",
                "error_type": "TargetUnavailable",
            },
            WorkItemStatus.FAILED,
            claim_owner_id=claim_owner_id,
            claim_generation=claim_generation,
        )
        return
    try:
        dispatch_result = await workload_manager.dispatch(item)
        if dispatch_result is None:
            return
        logger.info("Node execution result persisted", work_item_id=wi_id)
    except RetryableDispatchError as exc:
        await asyncio.sleep(settings.dispatch_retry_backoff_seconds)
        await work_progress.requeue_pre_execute_failure(
            item.id,
            claim_owner_id=claim_owner_id,
            claim_generation=claim_generation,
        )
        logger.warning("Worker dispatch will retry", work_item_id=wi_id, error=str(exc))
    except WorkItemPayloadError as exc:
        await work_progress.set_result(
            item.id,
            {"error": str(exc), "error_type": "WorkItemPayloadError"},
            WorkItemStatus.FAILED,
            claim_owner_id=claim_owner_id,
            claim_generation=claim_generation,
        )
        logger.warning("Work item payload was rejected", work_item_id=wi_id, error=str(exc))
    except NodeExecutionError as exc:
        if exc.persisted:
            logger.warning("Node returned a terminal failure", work_item_id=wi_id, error=str(exc))
            return
        result: dict[str, Any] = {"error": str(exc), "error_type": exc.error_type}
        if exc.output is not None:
            result["output"] = exc.output
        await work_progress.set_result(
            item.id,
            result,
            WorkItemStatus.FAILED,
            claim_owner_id=claim_owner_id,
            claim_generation=claim_generation,
        )
        logger.warning("Worker dispatch failed", work_item_id=wi_id, error=str(exc))
    except WorkloadCancelledError as exc:
        execution_started = exc.execution_started
        await work_progress.set_result(
            item.id,
            {"cancelled": True, "execution_started": execution_started, "reason": str(exc)},
            WorkItemStatus.CANCELLED,
            claim_owner_id=claim_owner_id,
            claim_generation=claim_generation,
        )
        logger.info("Node cancellation was confirmed", work_item_id=wi_id, execution_started=execution_started)
    except WorkloadOutcomeUnknownError as exc:
        await work_progress.mark_reconciliation_required(
            item.id,
            str(exc),
            claim_owner_id=claim_owner_id,
            claim_generation=claim_generation,
        )
        logger.exception("Work item requires outcome reconciliation", work_item_id=wi_id, error=str(exc))
    except Exception as e:
        await work_progress.mark_reconciliation_required(
            item.id,
            "Worker failed after dispatch began; the execution outcome is unknown",
            claim_owner_id=claim_owner_id,
            claim_generation=claim_generation,
        )
        logger.exception("Worker outcome needs reconciliation", work_item_id=wi_id, error_type=type(e).__name__)


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
            conn: asyncpg.Connection = await asyncpg.connect(database_url, **ssl_connect_args_from_env())
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
    work_scheduler: WorkScheduler,
    work_progress: WorkStore,
    target_queries: ExecutionTargetStore,
    worker_managers: WorkerManagerRegistry,
    settings: EPSettings,
    wakeup_event: asyncio.Event,
) -> None:
    """Claim one work item at a time; sleep between polls when queue is empty."""
    logger.info("Execution Plane worker started, polling for work items")
    wakeup_event.set()  # process any items already present at startup
    while True:
        item = None
        try:
            item = await work_scheduler.claim_and_place_work()
            if item:
                logger.info("Claimed work item", work_item_id=str(item.id))
                await _process_item(item, work_progress, target_queries, worker_managers, settings)
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
        WorkStore.from_database_url(database_url) as work_item_store,
        ClusterStore.from_database_url(database_url) as cluster_store,
        ExecutionTargetStore.from_database_url(database_url) as target_store,
    ):
        event_delivery = CompletionNotifier(settings)
        credential_resolver = CredentialResolver(settings)
        worker_managers = WorkerManagerRegistry()
        placement_resolver = build_placement_resolver(cluster_store, target_store, worker_managers)
        work_scheduler = WorkScheduler(work_item_store, placement_resolver)
        workload_manager = VanillaK8sWorkerManager(
            target_store,
            cluster_store,
            work_item_store,
            settings,
            credential_resolver=credential_resolver,
        )
        worker_managers.register(BackendType.VANILLA_K8S, workload_manager)
        drain_monitor = DrainMonitor(target_store, cluster_store, work_item_store)
        resource_monitor = ResourceMonitor(
            target_store,
            cluster_store,
            settings,
            credential_resolver=credential_resolver,
        )
        logger.debug(
            "ExecutionTarget reconciler constructed",
            resolver=type(placement_resolver).__name__,
        )
        await drain_monitor.start()
        await resource_monitor.start()
        try:
            wakeup_event = asyncio.Event()
            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    _listen_loop(to_asyncpg_url(database_url), wakeup_event),
                    name="ep-listener",
                )
                tg.create_task(
                    _poll_loop(
                        work_scheduler,
                        work_item_store,
                        target_store,
                        worker_managers,
                        settings,
                        wakeup_event,
                    ),
                    name="ep-poll",
                )
                tg.create_task(event_delivery.run(work_item_store), name="ep-completion-delivery")
                tg.create_task(run_cluster_binding_reconciler(database_url), name="ep-cluster-bindings")
                tg.create_task(
                    run_orphan_policy_reconciler(
                        target_store,
                        cluster_store,
                        work_item_store,
                        credential_resolver,
                    ),
                    name="ep-k8s-policy-reconciler",
                )
        finally:
            await resource_monitor.stop()
            await drain_monitor.stop()
            await event_delivery.close()
            await credential_resolver.close()


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
    asyncio.run(run_until_shutdown(_run()))


if __name__ == "__main__":
    main()
