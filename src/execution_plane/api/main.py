"""Standalone HTTP entry point for the Execution Plane service."""

from __future__ import annotations

import logging
import ssl
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Annotated
from uuid import UUID  # noqa: TC003 — FastAPI resolves this annotation at runtime

import structlog
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from execution_plane.api.auth import ServiceIdentity, require_scope
from execution_plane.api.schemas import (
    CapabilitiesResponse,
    ClusterBindingRead,
    ClusterBindingUpsert,
    ExecutionTargetRead,
    WorkItemCancelRequest,
    WorkItemRead,
    WorkItemSubmit,
)
from execution_plane.config import get_ep_settings
from execution_plane.execution_target.execution_target_registry import ExecutionTargetRegistry
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.models.cluster_binding import ClusterBinding
from execution_plane.work_store import (
    IdempotencyConflictError,
    WorkItemNotFoundError,
    WorkStore,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from execution_plane.models.work_item import WorkItem

logger = structlog.stdlib.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open an EP-owned database pool for the HTTP service lifetime."""
    settings = get_ep_settings()
    engine = create_async_engine(settings.database_url)
    app.state.ep_settings = settings
    app.state.ep_engine = engine
    app.state.ep_session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield
    finally:
        await engine.dispose()


def create_app() -> FastAPI:  # noqa: C901, PLR0915
    """Create the standalone, versioned Execution Plane API."""
    app = FastAPI(
        title="Execution Plane API",
        version="1.0.0",
        description="Accept, track, and retrieve execution work independently of Syntara workflows.",
        lifespan=lifespan,
    )

    @app.get("/healthz/live", include_in_schema=False)
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/healthz/ready", include_in_schema=False)
    async def ready(request: Request) -> dict[str, str]:
        engine: AsyncEngine = request.app.state.ep_engine
        try:
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
        except Exception as exc:
            logger.warning("EP readiness check failed", error=str(exc))
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database unavailable") from exc
        return {"status": "ready"}

    @app.get("/v1/capabilities", response_model=CapabilitiesResponse)
    async def capabilities() -> CapabilitiesResponse:
        return CapabilitiesResponse()

    @app.post("/v1/work-items", response_model=WorkItemRead, status_code=status.HTTP_202_ACCEPTED)
    async def submit_work_item(
        request: Request,
        submission: WorkItemSubmit,
        identity: Annotated[ServiceIdentity, Depends(require_scope("work-items:submit"))],
    ) -> WorkItemRead:
        store = WorkStore.from_engine(request.app.state.ep_engine)
        payload = {"workload_type": submission.workload_type, **submission.payload}
        if identity.project_id is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Project scope is required for submission",
            )
        try:
            item = await store.dispatch(
                client_id=identity.client_id,
                project_id=identity.project_id,
                request_id=submission.request_id,
                work_correlation_id=submission.work_correlation_id,
                payload=payload,
            )
        except IdempotencyConflictError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        return await _work_item_read(store, item)

    @app.get("/v1/work-items", response_model=list[WorkItemRead])
    async def list_work_items(
        request: Request,
        identity: Annotated[ServiceIdentity, Depends(require_scope("work-items:read"))],
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
    ) -> list[WorkItemRead]:
        store = WorkStore.from_engine(request.app.state.ep_engine)
        if identity.all_projects:
            items = await store.list_for_client(client_id=identity.client_id, limit=limit)
        elif identity.project_id is not None:
            items = await store.list_scoped(client_id=identity.client_id, project_id=identity.project_id, limit=limit)
        else:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project scope is required")
        return [WorkItemRead.model_validate(item) for item in items]

    @app.get(
        "/v1/execution-targets",
        response_model=list[ExecutionTargetRead],
        dependencies=[Depends(require_scope("execution-targets:read"))],
    )
    async def list_execution_targets(
        request: Request,
        identity: Annotated[ServiceIdentity, Depends(require_scope("execution-targets:read"))],
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
    ) -> list[ExecutionTargetRead]:
        registry = ExecutionTargetRegistry(ExecutionTargetStore.from_engine(request.app.state.ep_engine))
        targets = await registry.list(limit=limit, project_id=None if identity.all_projects else identity.project_id)
        return [ExecutionTargetRead.model_validate(target) for target in targets]

    @app.put(
        "/v1/cluster-bindings/{source_integration_id}",
        response_model=ClusterBindingRead,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def upsert_cluster_binding(
        request: Request,
        source_integration_id: UUID,
        desired: ClusterBindingUpsert,
        identity: Annotated[ServiceIdentity, Depends(require_scope("cluster-bindings:write"))],
    ) -> ClusterBindingRead:
        """Persist a versioned desired state for the independently managed cluster."""
        if not identity.all_projects:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Integration management scope is required",
            )
        if not desired.endpoint.startswith("https://"):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Cluster endpoints require HTTPS",
            )
        if desired.insecure_skip_tls_verify:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Cluster TLS verification cannot be disabled; provide the cluster CA certificate instead",
            )
        if desired.enabled and not desired.credential:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Enabled cluster bindings require a credential",
            )
        async with request.app.state.ep_session_factory() as session:
            key = (identity.client_id, source_integration_id)
            binding = await session.get(ClusterBinding, key, with_for_update=True)
            now = datetime.now(UTC)
            if binding is not None and desired.revision < binding.revision:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Stale integration revision")
            if binding is not None and desired.revision == binding.revision:
                same_state = (
                    binding.name == desired.name
                    and binding.endpoint == desired.endpoint
                    and binding.namespace == desired.namespace
                    and binding.credential == desired.credential
                    and binding.ca_certificate == desired.ca_certificate
                    and binding.project_ids == desired.project_ids
                    and binding.labels == desired.labels
                    and binding.enabled == desired.enabled
                )
                if not same_state:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="Revision conflicts with stored desired state",
                    )
            elif binding is None:
                binding = ClusterBinding(
                    client_id=identity.client_id,
                    source_integration_id=source_integration_id,
                    revision=desired.revision,
                    name=desired.name,
                    endpoint=desired.endpoint,
                    namespace=desired.namespace,
                    credential=desired.credential,
                    ca_certificate=desired.ca_certificate,
                    project_ids=desired.project_ids,
                    labels=desired.labels,
                    enabled=desired.enabled,
                    created_at=now,
                    updated_at=now,
                )
                session.add(binding)
            else:
                binding.revision = desired.revision
                binding.name = desired.name
                binding.endpoint = desired.endpoint
                binding.namespace = desired.namespace
                binding.credential = desired.credential
                binding.ca_certificate = desired.ca_certificate
                binding.project_ids = desired.project_ids
                binding.labels = desired.labels
                binding.enabled = desired.enabled
                binding.status = "pending"
                binding.status_message = None
                binding.updated_at = now
            await session.commit()
            return _cluster_binding_read(binding)

    @app.get("/v1/cluster-bindings/{source_integration_id}", response_model=ClusterBindingRead)
    async def get_cluster_binding(
        request: Request,
        source_integration_id: UUID,
        identity: Annotated[ServiceIdentity, Depends(require_scope("cluster-bindings:read"))],
    ) -> ClusterBindingRead:
        """Return safe desired and observed state for a source integration."""
        async with request.app.state.ep_session_factory() as session:
            binding = await session.get(ClusterBinding, (identity.client_id, source_integration_id))
        if binding is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cluster binding not found")
        if (
            not identity.all_projects
            and binding.project_ids is not None
            and (identity.project_id is None or identity.project_id not in binding.project_ids)
        ):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cluster binding not found")
        return _cluster_binding_read(binding)

    @app.get("/v1/work-items/by-request/{request_id}", response_model=WorkItemRead)
    async def get_work_item_by_request_id(
        request: Request,
        request_id: str,
        identity: Annotated[ServiceIdentity, Depends(require_scope("work-items:read"))],
    ) -> WorkItemRead:
        store = WorkStore.from_engine(request.app.state.ep_engine)
        if identity.project_id is None:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project scope is required")
        item = await store.get_by_request_id(request_id, client_id=identity.client_id, project_id=identity.project_id)
        if item is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Work item not found")
        return await _work_item_read(store, item)

    @app.post("/v1/work-items/by-request/{request_id}/cancel", response_model=WorkItemRead)
    async def cancel_work_item_by_request_id(
        request: Request,
        request_id: str,
        cancellation: WorkItemCancelRequest,
        identity: Annotated[ServiceIdentity, Depends(require_scope("work-items:cancel"))],
    ) -> WorkItemRead:
        """Reconcile cancellation when AO has not yet received the work ID."""
        store = WorkStore.from_engine(request.app.state.ep_engine)
        if identity.project_id is None:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project scope is required")
        try:
            item = await store.request_cancel_by_request_id(
                request_id,
                client_id=identity.client_id,
                project_id=identity.project_id,
                work_correlation_id=cancellation.work_correlation_id,
            )
        except WorkItemNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Work item not found") from exc
        return await _work_item_read(store, item)

    @app.get("/v1/work-items/{work_id}", response_model=WorkItemRead)
    async def get_work_item(
        request: Request,
        work_id: UUID,
        identity: Annotated[ServiceIdentity, Depends(require_scope("work-items:read"))],
    ) -> WorkItemRead:
        store = WorkStore.from_engine(request.app.state.ep_engine)
        if identity.project_id is None:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project scope is required")
        item = await store.get(work_id, client_id=identity.client_id, project_id=identity.project_id)
        if item is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Work item not found")
        return await _work_item_read(store, item)

    @app.post("/v1/work-items/{work_id}/cancel", response_model=WorkItemRead)
    async def cancel_work_item(
        request: Request,
        work_id: UUID,
        identity: Annotated[ServiceIdentity, Depends(require_scope("work-items:cancel"))],
    ) -> WorkItemRead:
        store = WorkStore.from_engine(request.app.state.ep_engine)
        if identity.project_id is None:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project scope is required")
        try:
            item = await store.request_cancel(work_id, client_id=identity.client_id, project_id=identity.project_id)
        except WorkItemNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Work item not found") from exc
        return await _work_item_read(store, item)

    @app.get("/v1/", include_in_schema=False)
    async def root() -> Response:
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    return app


app = create_app()


def _cluster_binding_read(binding: ClusterBinding) -> ClusterBindingRead:
    """Convert an ORM binding into a credential-free API response."""
    return ClusterBindingRead(
        client_id=binding.client_id,
        source_integration_id=binding.source_integration_id,
        desired_revision=binding.revision,
        observed_revision=binding.observed_revision,
        cluster_id=binding.cluster_id,
        status=binding.status,
        status_message=binding.status_message,
        enabled=binding.enabled,
        updated_at=binding.updated_at,
    )


async def _work_item_read(store: WorkStore, item: WorkItem) -> WorkItemRead:
    """Add stable outbox identity needed to reconcile a missed callback."""
    response = WorkItemRead.model_validate(item)
    event = await store.get_completion_event(response.id)
    if event is not None:
        response.completion_event_id = event.id
        response.state_revision = event.state_revision
    return response


def main() -> None:
    """Run the standalone HTTP server with optional service-owned TLS."""
    logging.basicConfig(level=logging.INFO)
    settings = get_ep_settings()
    uvicorn.run(
        "execution_plane.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        ssl_certfile=settings.api_tls_cert_path,
        ssl_keyfile=settings.api_tls_key_path,
        ssl_ca_certs=settings.api_tls_client_ca_path,
        ssl_cert_reqs=ssl.CERT_OPTIONAL if settings.api_tls_client_ca_path else ssl.CERT_NONE,
    )
