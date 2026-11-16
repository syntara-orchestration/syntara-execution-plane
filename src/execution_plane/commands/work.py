"""Handle work commands through the existing work lifecycle."""

from execution_plane.commands.context import EPCommandContext
from execution_plane.work_item.work_item_store import WorkStore
from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.commands.work import WorkExecute
from execution_plane_client.errors import UnsupportedCommand
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import MessageEnvelope, ResourceReference


class WorkCommandHandler:
    """Persist work, without retaining the command envelope."""

    async def handle(
        self, message: MessageEnvelope, payload: CommandPayload, context: EPCommandContext
    ) -> HandlingResult:
        """Persist the caller's explicit work-item identity."""
        del message
        if not isinstance(payload, WorkExecute):
            raise UnsupportedCommand
        work_store = WorkStore.from_engine(context.engine)
        item = await work_store.dispatch(
            client_id=context.client_id,
            item_id=payload.work_item_id,
            payload=payload.model_dump(mode="json"),
        )
        return HandlingResult(ResourceReference(kind="work_item", resource_id=item.id))
