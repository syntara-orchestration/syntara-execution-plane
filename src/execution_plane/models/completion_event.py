"""Durable result events delivered from EP to authorized clients."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Column, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel

from execution_plane.models.constants import EP_SCHEMA


class CompletionEvent(SQLModel, table=True):
    """Transactional outbox record for one terminal work-item transition."""

    __tablename__ = "completion_events"
    __table_args__ = (
        UniqueConstraint("work_item_id", name="uq_completion_events_work_item"),
        {"schema": EP_SCHEMA},
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    work_item_id: uuid.UUID = Field(foreign_key=f"{EP_SCHEMA}.work_items.id")
    client_id: str = Field(sa_column=Column(String(128), nullable=False))
    state_revision: int = Field(default=1, sa_column=Column(Integer, nullable=False))
    status: str = Field(sa_column=Column(String(32), nullable=False))
    result: dict[str, Any] = Field(sa_column=Column(JSONB, nullable=False))
    created_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    delivered_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
    attempts: int = Field(default=0, sa_column=Column(Integer, nullable=False, server_default="0"))
    next_attempt_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    last_error: str | None = Field(default=None, sa_column=Column(String(1000), nullable=True))
