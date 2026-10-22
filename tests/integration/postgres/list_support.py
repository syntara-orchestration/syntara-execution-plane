"""Shared PostgreSQL list datasets and authenticated HTTP client construction."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import httpx
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from execution_plane.api.auth import ServiceIdentity, get_service_identity
from execution_plane.api.main import create_app
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.models.work_item import WorkItem, WorkItemStatus

PATHS = ["/v1/work-items", "/v1/execution-targets", "/v1/clusters"]
BASE_TIME = datetime(2025, 6, 1, 12, tzinfo=UTC)
LIST_SIZE = 6
NAMES = ["alpha", "bravo", "charlie", "delta", "literal%_", "false"]
ALL_SCOPES = frozenset(
    {
        "work-items:read",
        "clusters:read",
        "execution-targets:read",
        "work.execute",
        "cluster.register",
        "execution_target.register",
    }
)


async def seed_list_dataset(session, path: str, db_url: str):
    """Insert six owned rows and six foreign rows for a list endpoint, with tied sort values."""
    client_id = f"list-resources-{uuid.uuid4()}"
    records = []
    foreign_ids = []
    for owner in (client_id, f"{client_id}-foreign"):
        if path == "/v1/execution-targets":
            cluster = Cluster(
                id=uuid.uuid4(),
                source_client_id=owner,
                source_integration_id=uuid.uuid4(),
                name=owner,
                endpoint="https://cluster.test",
                credential_ref=uuid.uuid4(),
                status=ClusterStatus.ACTIVE,
                created_by=uuid.uuid4(),
                updated_by=uuid.uuid4(),
                created_at=BASE_TIME,
                updated_at=BASE_TIME,
            )
            session.add(cluster)
            await session.flush()
        for index in range(6):
            resource_id = uuid.uuid5(uuid.NAMESPACE_URL, f"{owner}/{index}")
            created_at = BASE_TIME + timedelta(seconds=index // 2)
            if path == "/v1/work-items":
                ready, not_ready = "pending", "failed"
            elif path == "/v1/clusters":
                ready, not_ready = "active", "error"
            else:
                ready, not_ready = "active", "failed"
            status = ready if index % 2 == 0 else not_ready
            labels = {"env": "prod" if index % 2 == 0 else "dev", "region": "west"}
            if index == 4:
                labels["optional"] = ""
            values = {
                "id": str(resource_id),
                "created_at": created_at,
                "status": status,
                "name": NAMES[index],
                "enabled": index % 2 == 0,
                "endpoint": f"https://target-{index}.test",
                "labels": labels,
            }
            if path == "/v1/work-items":
                row = WorkItem(
                    id=resource_id,
                    client_id=owner,
                    status=WorkItemStatus(status),
                    payload={},
                    created_at=created_at,
                    resource_cleanup_status="pending" if index % 2 == 0 else "done",
                    claimed_at=created_at if index % 2 else None,
                )
                values["resource_cleanup_status"] = row.resource_cleanup_status
            elif path == "/v1/clusters":
                row = Cluster(
                    id=resource_id,
                    source_client_id=owner,
                    source_integration_id=resource_id,
                    name=NAMES[index],
                    endpoint=values["endpoint"],
                    credential_ref=uuid.uuid4(),
                    status=ClusterStatus(status),
                    enabled=values["enabled"],
                    labels=labels,
                    created_by=uuid.uuid4(),
                    updated_by=uuid.uuid4(),
                    created_at=created_at,
                    updated_at=created_at,
                )
                values.update(updated_at=created_at, is_default=False)
            else:
                row = ExecutionTarget(
                    id=resource_id,
                    cluster_id=cluster.id,
                    name=NAMES[index],
                    backend_type=BackendType.VANILLA_K8S,
                    endpoint=values["endpoint"],
                    placement=KubernetesPlacement(namespace="ep"),
                    status=TargetStatus(status),
                    enabled=values["enabled"],
                    is_default=index == 0,
                    credential_ref=uuid.uuid4(),
                    labels=labels,
                    created_by=uuid.uuid4(),
                    updated_by=uuid.uuid4(),
                    created_at=created_at,
                    updated_at=created_at,
                )
                values.update(
                    cluster_id=str(cluster.id), backend_type=row.backend_type.value, is_default=row.is_default
                )
            session.add(row)
            if owner == client_id:
                records.append(values)
            else:
                foreign_ids.append(str(resource_id))
    return SimpleNamespace(path=path, client_id=client_id, records=records, foreign_ids=foreign_ids, db_url=db_url)


@asynccontextmanager
async def list_http_client(db_url: str, client_id: str):
    """Build the same real-route, client-scoped HTTP test harness for every list test."""
    engine = create_async_engine(db_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    app = create_app()
    app.state.ep_engine = engine
    app.state.ep_session_factory = sessions

    async def identity() -> ServiceIdentity:
        return ServiceIdentity(client_id=client_id, scopes=ALL_SCOPES)

    app.dependency_overrides[get_service_identity] = identity
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
            yield client, sessions
    finally:
        await engine.dispose()


def ordered_ids(dataset) -> list[str]:
    """Order expected IDs by the public default creation-time sort and UUID tie-breaker."""
    return [row["id"] for row in sorted(dataset.records, key=lambda row: (row["created_at"], row["id"]))]
