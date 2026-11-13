"""Construct transport-neutral messages from typed command payloads."""

from datetime import UTC, datetime
from typing import ClassVar
from uuid import UUID, uuid4

from execution_plane_client.protocol import Command, CommandScope, MessageEnvelope, WireModel


class CommandPayload(WireModel):
    """A typed command knows its wire identity and builds its delivery envelope."""

    command_type: ClassVar[str]
    command_revision: ClassVar[int] = 1

    def _message_payload(self) -> dict[str, object]:
        return self.model_dump(mode="json")

    def to_message(
        self,
        *,
        source_id: str,
        destination_id: str,
        client_id: str,
        occurred_at: datetime | None = None,
        message_id: UUID | None = None,
        sequence: int = 0,
    ) -> MessageEnvelope:
        """Build a delivery message with an independent UUID and UTC timestamp.

        Retain the returned message or supply message_id and occurred_at to
        reconstruct a delivery. Resource identity belongs to the typed payload;
        message_id only correlates the transport receipt, not deduplication.
        """
        payload = self._message_payload()
        type(self).model_validate(payload)
        return MessageEnvelope(
            message_id=uuid4() if message_id is None else message_id,
            source_id=source_id,
            destination_id=destination_id,
            scope=CommandScope(client_id=client_id),
            sequence=sequence,
            command=Command(
                command_type=self.command_type,
                command_revision=self.command_revision,
                payload=payload,
                occurred_at=datetime.now(UTC) if occurred_at is None else occurred_at,
            ),
        )
