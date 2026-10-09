"""Work item persistence model and lifecycle states."""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

import sqlalchemy as sa
from sqlalchemy import Column, String
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import DateTime
from sqlmodel import Field, SQLModel

from execution_plane.models.constants import EP_SCHEMA
from execution_plane.models.encrypted_payload import EncryptedWorkItemPayload


class WorkItemStatus(StrEnum):
    """Lifecycle states of a dispatched work item."""

    PENDING = "pending"
    CLAIMED = "claimed"
    DISPATCHED = "dispatched"
    CANCEL_REQUESTED = "cancel_requested"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class WorkItem(SQLModel, table=True):
    """A unit of work accepted and managed by the Execution Plane service."""

    __tablename__ = "work_items"
    __table_args__ = (
        sa.Index("ix_work_items_status", "status"),
        sa.Index("ix_work_items_pending", "created_at", postgresql_where=sa.text("status = 'pending'")),
        {"schema": EP_SCHEMA},
    )

    id: uuid.UUID = Field(primary_key=True)

    client_id: str = Field(sa_column=Column(String(128), nullable=False))

    status: WorkItemStatus = Field(
        default=WorkItemStatus.PENDING,
        sa_column=Column(
            SAEnum(
                WorkItemStatus,
                native_enum=False,
                values_callable=lambda members: [member.value for member in members],
            ),
            nullable=False,
        ),
    )

    # Set when a worker claims this item.
    execution_target_id: uuid.UUID | None = Field(default=None, foreign_key=f"{EP_SCHEMA}.execution_targets.id")

    # Workload parameters serialized at submission time.
    payload: dict[str, Any] = Field(
        default={},
        sa_column=Column(EncryptedWorkItemPayload(), nullable=False, server_default="{}"),
    )

    # Terminal result is owned and retained by EP independently of AO availability.
    result: dict[str, Any] | None = Field(default=None, sa_column=Column(JSONB, nullable=True))

    created_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    claimed_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
    claim_owner_id: uuid.UUID | None = Field(default=None, nullable=True)
    claim_generation: int = Field(default=0, nullable=False)
    backend_resource_name: str | None = Field(default=None, sa_column=Column(String(253), nullable=True))
    backend_resource_uid: str | None = Field(default=None, sa_column=Column(String(64), nullable=True))
    resource_cleanup_status: str = Field(default="not_required", sa_column=Column(String(32), nullable=False))
    resource_cleanup_error: str | None = Field(default=None, sa_column=Column(String(1000), nullable=True))
    completed_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
