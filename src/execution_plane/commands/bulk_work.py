"""Commit bulk work-control intent before acknowledging the request."""

from execution_plane.commands.context import EPCommandContext
from execution_plane.work_item.work_item_store import WorkStore
from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.commands.bulk_work import WorkCancelBulk, WorkDeleteBulk
from execution_plane_client.errors import UnsupportedCommand
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import MessageEnvelope


class BulkWorkControlHandler:
    """Apply a validated selector within the authenticated client's ownership."""

    async def handle(
        self, message: MessageEnvelope, payload: CommandPayload, context: EPCommandContext
    ) -> HandlingResult:
        """Return a receipt only after all matching intent updates have committed."""
        del message
        if not isinstance(payload, WorkCancelBulk | WorkDeleteBulk):
            raise UnsupportedCommand
        await WorkStore.from_engine(context.engine).request_bulk_control(
            payload.selector, client_id=context.client_id, delete=isinstance(payload, WorkDeleteBulk)
        )
        return HandlingResult()
