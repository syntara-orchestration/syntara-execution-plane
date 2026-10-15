"""Client-scoped queries of domain resources, without command history."""

import json
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlmodel import col

from execution_plane.api.pagination import decode_cursor
from execution_plane.api.schemas import ClusterRead, ExecutionTargetRead, WorkItemRead
from execution_plane.commands.clusters import ResourceNotFoundError
from execution_plane.models.cluster import Cluster
from execution_plane.models.cluster_binding import ClusterBinding
from execution_plane.models.execution_target import ExecutionTarget
from execution_plane.work_store import WorkStore


def _apply_created_at_cursor(
    statement: Select[Any],
    cursor: dict[str, Any],
    ts_col: Any,  # noqa: ANN401
    id_col: Any,  # noqa: ANN401
) -> Select[Any]:
    """Apply keyset WHERE + ORDER BY for a created_at-sorted cursor."""
    anchor_ts = datetime.fromisoformat(cursor["created_at"])
    anchor_id = UUID(cursor["id"])
    if cursor.get("direction") == "prev":
        return statement.where((ts_col < anchor_ts) | ((ts_col == anchor_ts) & (id_col < anchor_id))).order_by(
            ts_col.desc(), id_col.desc()
        )
    return statement.where((ts_col > anchor_ts) | ((ts_col == anchor_ts) & (id_col > anchor_id))).order_by(
        ts_col.asc(), id_col.asc()
    )


class EPResourceQueries:
    """Read only resources owned by the authenticated service client."""

    def __init__(self, engine: AsyncEngine, client_id: str) -> None:
        """Borrow the API's engine and verified client identity."""
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._work = WorkStore.from_engine(engine)
        self._client_id = client_id

    async def work_items(
        self,
        limit: int,
        *,
        cursor: str | None = None,
        sort_field: str = "created_at",
        sort_direction: str = "desc",
    ) -> list[WorkItemRead]:
        """List work without embedding potentially large results."""
        parsed_cursor = decode_cursor(cursor) if cursor else None
        items = await self._work.list_for_client(
            client_id=self._client_id,
            limit=limit + 1,
            cursor=parsed_cursor,
            sort_field=sort_field,
            sort_direction=sort_direction,
        )
        if parsed_cursor and parsed_cursor.get("direction") == "prev":
            items.reverse()
        return [
            WorkItemRead.model_validate(item).model_copy(
                update={"result": None, "result_omitted": item.result is not None}
            )
            for item in items
        ]

    async def count_work_items(self) -> int:
        """Return total work item count for the authenticated client."""
        return await self._work.count_for_client(client_id=self._client_id)

    async def work_item(self, resource_id: UUID) -> WorkItemRead:
        """Recover the complete stored result without needing command history."""
        item = await self._work.get(resource_id, client_id=self._client_id)
        if item is None:
            raise ResourceNotFoundError
        read = WorkItemRead.model_validate(item)
        if item.result is not None:
            read.result_size_bytes = len(json.dumps(item.result, ensure_ascii=False).encode())
        event = await self._work.get_completion_event(resource_id)
        if event is not None:
            read.completion_event_id, read.state_revision = event.id, event.state_revision
        return read

    async def clusters(self, limit: int, *, cursor: str | None = None) -> list[ClusterRead]:
        """List binding resources, including pending setup and deletion."""
        statement = select(ClusterBinding).where(col(ClusterBinding.client_id) == self._client_id)

        if cursor:
            statement = _apply_created_at_cursor(
                statement,
                decode_cursor(cursor),
                col(ClusterBinding.created_at),
                col(ClusterBinding.source_integration_id),
            )
        else:
            statement = statement.order_by(
                col(ClusterBinding.created_at).asc(),
                col(ClusterBinding.source_integration_id).asc(),
            )

        async with self._sessions() as session:
            rows = list(await session.scalars(statement.limit(limit + 1)))
        if cursor and decode_cursor(cursor).get("direction") == "prev":
            rows.reverse()
        return [ClusterRead.model_validate(row) for row in rows]

    async def count_clusters(self) -> int:
        """Return total cluster count for the authenticated client."""
        async with self._sessions() as session:
            result = await session.execute(
                select(func.count()).select_from(ClusterBinding).where(col(ClusterBinding.client_id) == self._client_id)
            )
            return result.scalar_one()

    async def cluster(self, resource_id: UUID) -> ClusterRead:
        """Read binding state using its stable source integration UUID."""
        async with self._sessions() as session:
            row = await session.get(ClusterBinding, (self._client_id, resource_id))
            if row is None:
                raise ResourceNotFoundError
            return ClusterRead.model_validate(row)

    async def targets(
        self,
        limit: int,
        resource_id: UUID | None = None,
        *,
        cursor: str | None = None,
    ) -> list[ExecutionTargetRead]:
        """Authorize targets through ownership of their physical cluster."""
        statement = select(ExecutionTarget).join(Cluster).where(col(Cluster.source_client_id) == self._client_id)
        if resource_id is not None:
            statement = statement.where(col(ExecutionTarget.id) == resource_id)

        if cursor:
            statement = _apply_created_at_cursor(
                statement,
                decode_cursor(cursor),
                col(ExecutionTarget.created_at),
                col(ExecutionTarget.id),
            )
        else:
            statement = statement.order_by(col(ExecutionTarget.created_at).asc(), col(ExecutionTarget.id).asc())

        async with self._sessions() as session:
            rows = list(await session.scalars(statement.limit(limit + 1)))
        if cursor and decode_cursor(cursor).get("direction") == "prev":
            rows.reverse()
        return [ExecutionTargetRead.model_validate(row) for row in rows]

    async def target(self, resource_id: UUID) -> ExecutionTargetRead:
        """Use the same not-found response for absent and other-client targets."""
        rows = await self.targets(1, resource_id)
        if not rows:
            raise ResourceNotFoundError
        return rows[0]

    async def count_targets(self) -> int:
        """Return total execution target count for the authenticated client."""
        async with self._sessions() as session:
            result = await session.execute(
                select(func.count())
                .select_from(ExecutionTarget)
                .join(Cluster)
                .where(col(Cluster.source_client_id) == self._client_id)
            )
            return result.scalar_one()
