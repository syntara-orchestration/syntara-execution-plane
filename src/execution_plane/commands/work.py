"""Handle work commands through the existing work lifecycle."""

from execution_plane.commands.context import EPCommandContext
from execution_plane.work_item.work_item_store import WorkStore
from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.commands.work import WorkDelete, WorkExecute
from execution_plane_client.errors import UnsupportedCommand
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import MessageEnvelope, ResourceReference


class WorkCommandHandler:
    """Persist work, without retaining the command envelope."""

    async def handle(
        self, message: MessageEnvelope, payload: CommandPayload, context: EPCommandContext
    ) -> HandlingResult:
        """Use the caller's command UUID as the work item's stable UUID."""
        if not isinstance(payload, WorkExecute):
            raise UnsupportedCommand
        work_store = WorkStore.from_engine(context.engine)
        item = await work_store.dispatch(
            client_id=context.client_id,
            item_id=message.command_id,
            payload=payload.model_dump(mode="json"),
        )
        return HandlingResult(ResourceReference(kind="work_item", resource_id=item.id))


class WorkDeleteCommandHandler:
    """Persist deletion intent; the worker performs cancellation and cleanup."""

    async def handle(
        self, message: MessageEnvelope, payload: CommandPayload, context: EPCommandContext
    ) -> HandlingResult:
        """Acknowledge repeated deletion of absent work idempotently."""
        del message
        if not isinstance(payload, WorkDelete):
            raise UnsupportedCommand
        await WorkStore.from_engine(context.engine).request_delete(payload.work_item_id, client_id=context.client_id)
        return HandlingResult(ResourceReference(kind="work_item", resource_id=payload.work_item_id))
