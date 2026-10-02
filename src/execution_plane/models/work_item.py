"""Work item persistence model and lifecycle states."""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

import sqlalchemy as sa
from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import DateTime
from sqlmodel import Field, SQLModel

from execution_plane.models.constants import EP_SCHEMA


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
        sa.Index("ix_work_items_work_correlation_id", "work_correlation_id"),
        sa.Index("ix_work_items_status", "status"),
        sa.Index("ix_work_items_pending", "created_at", postgresql_where=sa.text("status = 'pending'")),
        sa.UniqueConstraint("client_id", "project_id", "request_id", name="uq_work_items_request_scope"),
        {"schema": EP_SCHEMA},
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)

    # Authenticated client and project scope are stored on every record. The API
    # derives client_id from the service token and validates project_id against
    # its signed authorization context.
    client_id: str = Field(sa_column=Column(String(128), nullable=False))
    project_id: uuid.UUID

    # Stable caller key. Transport and activity retries must reuse this value.
    request_id: str = Field(sa_column=Column(String(200), nullable=False))
    request_hash: str = Field(sa_column=Column(String(64), nullable=False))

    # Opaque caller correlation handle, with no Temporal-specific meaning.
    work_correlation_id: uuid.UUID

    status: WorkItemStatus = Field(
        default=WorkItemStatus.PENDING,
        sa_column=Column(sa.String, nullable=False),
    )

    # Set when a worker claims this item.
    execution_target_id: uuid.UUID | None = Field(default=None, foreign_key=f"{EP_SCHEMA}.execution_targets.id")

    # Workload parameters serialized at submission time.
    payload: dict[str, Any] = Field(default={}, sa_column=Column(JSONB, nullable=False, server_default="{}"))

    # Terminal result is owned and retained by EP independently of AO availability.
    result: dict[str, Any] | None = Field(default=None, sa_column=Column(JSONB, nullable=True))

    created_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    claimed_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
    completed_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
