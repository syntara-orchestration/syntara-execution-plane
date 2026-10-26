"""Client-scoped queries of domain resources, without command history.

List orchestration parallels ``syntara.core.services.base.BaseService``:
``_build_list_query`` limits queries to N+1 rows and ``_fetch_and_paginate`` restores the
display order of backward pages. This EP facade delegates SQL filtering and
sorting to domain stores, then constructs credential-free response models;
the route and pagination helper finish the response envelope.
"""

import json
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine

from execution_plane.api.pagination import decode_cursor
from execution_plane.api.schemas import ClusterRead, ExecutionTargetRead, WorkItemRead
from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.commands.clusters import ResourceNotFoundError
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.work_item.work_item_store import WorkStore


class EPResourceQueries:
    """Read only resources owned by the authenticated service client."""

    def __init__(self, engine: AsyncEngine, client_id: str) -> None:
        """Borrow the API's engine and verified client identity."""
        target_queries = ExecutionTargetStore.from_engine(engine)
        self._clusters = ClusterStore.from_engine(engine)
        self._work = WorkStore.from_engine(engine)
        self._targets = target_queries
        self._client_id = client_id

    async def work_items(
        self,
        limit: int,
        *,
        cursor: str | None = None,
        sort_field: str = "created_at",
        sort_direction: str = "desc",
        filters: dict[str, str] | None = None,
    ) -> list[WorkItemRead]:
        """List work without embedding potentially large results."""
        parsed_cursor = (
            decode_cursor(cursor, sort_field=sort_field, sort_direction=sort_direction) if cursor is not None else None
        )
        items = await self._work.list_for_client(
            client_id=self._client_id,
            limit=limit + 1,
            cursor=parsed_cursor,
            sort_field=sort_field,
            sort_direction=sort_direction,
            filters=filters,
        )
        if parsed_cursor and parsed_cursor["direction"] == "prev":
            items.reverse()
        return [
            WorkItemRead.model_validate(item).model_copy(
                update={"result": None, "result_omitted": item.result is not None}
            )
            for item in items
        ]

    async def count_work_items(self, filters: dict[str, str] | None = None) -> int:
        """Return total work item count for the authenticated client."""
        return await self._work.count_for_client(client_id=self._client_id, filters=filters)

    async def work_item(self, resource_id: UUID) -> WorkItemRead:
        """Recover the complete stored result without needing command history."""
        item = await self._work.get(resource_id, client_id=self._client_id)
        if item is None:
            raise ResourceNotFoundError
        read = WorkItemRead.model_validate(item)
        if item.result is not None:
            read.result_size_bytes = len(json.dumps(item.result, ensure_ascii=False).encode())
        return read

    async def clusters(
        self,
        limit: int,
        *,
        cursor: str | None = None,
        sort_direction: str = "asc",
        sort_field: str = "created_at",
        filters: dict[str, str] | None = None,
    ) -> list[ClusterRead]:
        """List binding resources, including pending setup and deletion."""
        parsed_cursor = (
            decode_cursor(cursor, sort_field=sort_field, sort_direction=sort_direction) if cursor is not None else None
        )
        rows = list(
            await self._clusters.list_bindings_for_client(
                self._client_id,
                limit=limit + 1,
                cursor=parsed_cursor,
                sort_direction=sort_direction,
                sort_field=sort_field,
                filters=filters,
            )
        )
        if parsed_cursor and parsed_cursor["direction"] == "prev":
            rows.reverse()
        return [ClusterRead.model_validate(row) for row in rows]

    async def count_clusters(self, filters: dict[str, str] | None = None) -> int:
        """Return total cluster count for the authenticated client."""
        return await self._clusters.count_bindings_for_client(self._client_id, filters=filters)

    async def cluster(self, resource_id: UUID) -> ClusterRead:
        """Read binding state using its stable source integration UUID."""
        row = await self._clusters.get_binding_for_client(self._client_id, resource_id)
        if row is None:
            raise ResourceNotFoundError
        return ClusterRead.model_validate(row)

    async def targets(
        self,
        limit: int,
        resource_id: UUID | None = None,
        *,
        cursor: str | None = None,
        sort_direction: str = "asc",
        sort_field: str = "created_at",
        filters: dict[str, str] | None = None,
    ) -> list[ExecutionTargetRead]:
        """Authorize targets through ownership of their physical cluster."""
        parsed_cursor = (
            decode_cursor(cursor, sort_field=sort_field, sort_direction=sort_direction) if cursor is not None else None
        )
        rows = list(
            await self._targets.list_for_client(
                self._client_id,
                limit=limit + 1,
                target_id=resource_id,
                cursor=parsed_cursor,
                sort_direction=sort_direction,
                sort_field=sort_field,
                filters=filters,
            )
        )
        if parsed_cursor and parsed_cursor["direction"] == "prev":
            rows.reverse()
        return [ExecutionTargetRead.model_validate(row) for row in rows]

    async def target(self, resource_id: UUID) -> ExecutionTargetRead:
        """Use the same not-found response for absent and other-client targets."""
        rows = await self.targets(1, resource_id)
        if not rows:
            raise ResourceNotFoundError
        return rows[0]

    async def count_targets(self, filters: dict[str, str] | None = None) -> int:
        """Return total execution target count for the authenticated client."""
        return await self._targets.count_for_client(self._client_id, filters=filters)
