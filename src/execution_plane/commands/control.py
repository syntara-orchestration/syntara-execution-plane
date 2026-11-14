"""Cancel existing work without a command queue or cancellation journal."""

from execution_plane.commands.context import EPCommandContext
from execution_plane.work_item.work_item_store import WorkStore
from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.commands.control import WorkCancel
from execution_plane_client.errors import UnsupportedCommand
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import MessageEnvelope, ResourceReference


class WorkCancelHandler:
    """Apply cancellation to authenticated, existing domain work."""

    async def handle(
        self, message: MessageEnvelope, payload: CommandPayload, context: EPCommandContext
    ) -> HandlingResult:
        """Unknown work is not found; cancellation before arrival is not queued."""
        del message
        if not isinstance(payload, WorkCancel):
            raise UnsupportedCommand
        work_store = WorkStore.from_engine(context.engine)
        item = await work_store.request_cancel(
            payload.work_item_id,
            client_id=context.client_id,
        )
        return HandlingResult(ResourceReference(kind="work_item", resource_id=item.id))
