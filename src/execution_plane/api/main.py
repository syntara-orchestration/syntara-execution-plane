"""EP-owned command ingress and authenticated domain resource queries."""

from __future__ import annotations

import json
import logging
import ssl
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Annotated
from uuid import UUID  # noqa: TC003

import structlog
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from execution_plane.api.auth import ServiceIdentity, get_service_identity, require_scope
from execution_plane.api.list_queries import list_filter_openapi, list_query_filters
from execution_plane.api.openapi import configure_command_openapi
from execution_plane.api.pagination import (
    ClusterListParams,
    ExecutionTargetListParams,
    ResourcesResponse,
    WorkItemListParams,
    generate_response,
)
from execution_plane.api.schemas import ClusterRead, ExecutionTargetRead, WorkItemRead  # noqa: TC001
from execution_plane.cluster.cluster_store import DefaultClusterError
from execution_plane.commands.clusters import ResourceNotFoundError
from execution_plane.commands.context import EPCommandContext
from execution_plane.commands.queries import EPResourceQueries
from execution_plane.commands.registry import build_ep_registry
from execution_plane.config import get_ep_settings
from execution_plane.database.engine import create_ep_engine
from execution_plane.execution_target.execution_target_store import (
    ClusterNotAvailableError,
    DefaultExecutionTargetError,
    TargetNotEligibleForDefaultError,
)
from execution_plane.models.cluster import Cluster
from execution_plane.models.execution_target import ExecutionTarget
from execution_plane.models.work_item import WorkItem
from execution_plane.work_item.errors import DuplicateWorkItemError, WorkItemNotFoundError
from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.errors import ProtocolConflict, UnsupportedCommand
from execution_plane_client.protocol import CapabilitiesRead, MessageEnvelope, TransportReceipt

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

logger = structlog.stdlib.get_logger(__name__)
_BODY_LIMIT = 1024 * 1024


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open an EP-owned database pool for the HTTP service lifetime."""
    settings = get_ep_settings()
    engine = create_ep_engine(settings.database_url)
    app.state.ep_settings = settings
    app.state.ep_engine = engine
    app.state.ep_session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield
    finally:
        await engine.dispose()


def strict_json(data: bytes) -> object:
    """Reject duplicate keys and non-finite constants without echoing input."""

    def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in items:
            if key in result:
                error = "Duplicate JSON object key"
                raise ValueError(error)
            result[key] = value
        return result

    def constant(_value: str) -> object:
        error = "Non-finite JSON value"
        raise ValueError(error)

    try:
        return json.loads(data, object_pairs_hook=pairs, parse_constant=constant)
    except RecursionError:
        error = "JSON nesting is too deep"
        raise ValueError(error) from None


def create_app() -> FastAPI:  # noqa: C901, PLR0915
    """Replace legacy mutations with commands; expose only resource state."""
    app = FastAPI(
        title="Execution Plane API",
        version="1.0.0",
        lifespan=lifespan,
        description="Command submission and client-scoped execution resources. No command history is retained.",
    )
    registry = build_ep_registry()
    catalog = CommandCatalog()

    @app.exception_handler(ResourceNotFoundError)
    async def missing(_request: Request, _error: ResourceNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": "Resource not found"})

    @app.get("/healthz/live", include_in_schema=False)
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/healthz/ready", include_in_schema=False)
    async def ready(request: Request) -> dict[str, str]:
        engine: AsyncEngine = request.app.state.ep_engine
        try:
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
        except Exception:  # noqa: BLE001 - Readiness must not expose database exception details.
            raise HTTPException(status_code=503, detail="Database unavailable") from None
        return {"status": "ready"}

    @app.get("/v1/capabilities")
    async def capabilities(
        _identity: Annotated[ServiceIdentity, Depends(require_scope("capabilities:read"))],
    ) -> CapabilitiesRead:
        return registry.capabilities()

    @app.post(
        "/v1/commands",
        status_code=202,
        responses={422: {"description": "Invalid or unsupported command"}},
    )
    async def receive_command(
        request: Request, identity: Annotated[ServiceIdentity, Depends(get_service_identity)]
    ) -> TransportReceipt:
        data = bytearray()
        async for chunk in request.stream():
            if len(data) + len(chunk) > _BODY_LIMIT:
                raise HTTPException(status_code=413, detail="Command too large")
            data.extend(chunk)
        try:
            message = MessageEnvelope.model_validate(strict_json(bytes(data)))
            payload = catalog.validate(message)
            handler = registry.get(message.command.command_type, message.command.command_revision)
            if (
                message.scope.client_id != identity.client_id
                or message.source_id != identity.client_id
                or message.destination_id != get_ep_settings().service_jwt_audience
                or message.command.command_type not in identity.scopes
            ):
                raise HTTPException(status_code=403, detail="Command is not authorized")
            result = await handler.handle(
                message, payload, EPCommandContext(identity.client_id, request.app.state.ep_engine)
            )
        except (
            DuplicateWorkItemError,
            DefaultExecutionTargetError,
            DefaultClusterError,
            ClusterNotAvailableError,
            TargetNotEligibleForDefaultError,
        ):
            raise HTTPException(status_code=409, detail="Resource state conflicts with the request") from None
        except (ValidationError, ValueError, UnsupportedCommand):
            raise HTTPException(status_code=422, detail="Invalid or unsupported command") from None
        except (ProtocolConflict, IntegrityError):
            raise HTTPException(status_code=409, detail="Resource state conflicts with the request") from None
        except WorkItemNotFoundError:
            raise HTTPException(status_code=404, detail="Resource not found") from None
        return TransportReceipt(message_id=message.message_id, resource=result.resource)

    @app.get(
        "/v1/work-items",
        responses={422: {"description": "Invalid query parameters or cursor"}},
        openapi_extra=list_filter_openapi(WorkItem),
    )
    async def work_items(
        request: Request,
        identity: Annotated[ServiceIdentity, Depends(require_scope("work-items:read"))],
        params: Annotated[WorkItemListParams, Query()],
    ) -> ResourcesResponse[WorkItemRead]:
        sort_field, sort_direction = params.sort_order
        filters = list_query_filters(request)
        queries = EPResourceQueries(request.app.state.ep_engine, identity.client_id)
        try:
            raw = await queries.work_items(
                params.limit,
                cursor=params.cursor,
                sort_field=sort_field,
                sort_direction=sort_direction,
                filters=filters,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        trimmed, next_cur, prev_cur = generate_response(
            raw, params.limit, params.cursor, sort_field=sort_field, sort_direction=sort_direction
        )
        total = await queries.count_work_items(filters) if params.include_total else None
        return ResourcesResponse(resources=trimmed, next=next_cur, prev=prev_cur, total=total)

    @app.get("/v1/work-items/{resource_id}")
    async def work_item(
        request: Request,
        resource_id: UUID,
        identity: Annotated[ServiceIdentity, Depends(require_scope("work-items:read"))],
    ) -> WorkItemRead:
        return await EPResourceQueries(request.app.state.ep_engine, identity.client_id).work_item(resource_id)

    @app.get(
        "/v1/clusters",
        responses={422: {"description": "Invalid query parameters or cursor"}},
        openapi_extra=list_filter_openapi(Cluster),
    )
    async def clusters(
        request: Request,
        identity: Annotated[ServiceIdentity, Depends(require_scope("clusters:read"))],
        params: Annotated[ClusterListParams, Query()],
    ) -> ResourcesResponse[ClusterRead]:
        sort_field, sort_direction = params.sort_order
        filters = list_query_filters(request)
        queries = EPResourceQueries(request.app.state.ep_engine, identity.client_id)
        try:
            raw = await queries.clusters(
                params.limit,
                cursor=params.cursor,
                sort_direction=sort_direction,
                sort_field=sort_field,
                filters=filters,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        trimmed, next_cur, prev_cur = generate_response(
            raw, params.limit, params.cursor, sort_field=sort_field, sort_direction=sort_direction
        )
        total = await queries.count_clusters(filters) if params.include_total else None
        return ResourcesResponse(resources=trimmed, next=next_cur, prev=prev_cur, total=total)

    @app.get("/v1/clusters/{resource_id}")
    async def cluster(
        request: Request,
        resource_id: UUID,
        identity: Annotated[ServiceIdentity, Depends(require_scope("clusters:read"))],
    ) -> ClusterRead:
        return await EPResourceQueries(request.app.state.ep_engine, identity.client_id).cluster(resource_id)

    @app.get(
        "/v1/execution-targets",
        responses={422: {"description": "Invalid query parameters or cursor"}},
        openapi_extra=list_filter_openapi(ExecutionTarget),
    )
    async def targets(
        request: Request,
        identity: Annotated[ServiceIdentity, Depends(require_scope("execution-targets:read"))],
        params: Annotated[ExecutionTargetListParams, Query()],
    ) -> ResourcesResponse[ExecutionTargetRead]:
        sort_field, sort_direction = params.sort_order
        filters = list_query_filters(request)
        queries = EPResourceQueries(request.app.state.ep_engine, identity.client_id)
        try:
            raw = await queries.targets(
                params.limit,
                cursor=params.cursor,
                sort_direction=sort_direction,
                sort_field=sort_field,
                filters=filters,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        trimmed, next_cur, prev_cur = generate_response(
            raw, params.limit, params.cursor, sort_field=sort_field, sort_direction=sort_direction
        )
        total = await queries.count_targets(filters) if params.include_total else None
        return ResourcesResponse(resources=trimmed, next=next_cur, prev=prev_cur, total=total)

    @app.get("/v1/execution-targets/{resource_id}")
    async def target(
        request: Request,
        resource_id: UUID,
        identity: Annotated[ServiceIdentity, Depends(require_scope("execution-targets:read"))],
    ) -> ExecutionTargetRead:
        return await EPResourceQueries(request.app.state.ep_engine, identity.client_id).target(resource_id)

    configure_command_openapi(app, catalog, registry.capabilities())
    return app


app = create_app()


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
        timeout_graceful_shutdown=settings.api_graceful_shutdown_seconds,
    )
