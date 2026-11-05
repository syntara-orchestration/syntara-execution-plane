"""Execution target persistence models and lifecycle states."""

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, ClassVar

from sqlalchemy import Column, Index, UniqueConstraint, text
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import DateTime, Uuid
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from execution_plane.models.cluster import Cluster

from execution_plane.models.constants import EP_SCHEMA
from execution_plane.models.sqlmodel_types import DiscriminatedJSONB
from execution_plane_client.models.execution_target import (
    EXECUTION_TARGET_FILTERABLE_FIELDS,
    EXECUTION_TARGET_SORTABLE_FIELDS,
    BackendType,
    ExecutionTargetPlacement,
    ExecutionTargetPlacementTypes,
    TargetStatus,
)


class ExecutionTarget(SQLModel, table=True):
    """A registered compute environment where worker pods run."""

    __tablename__ = "execution_targets"
    # Query metadata sourced from execution_plane_client - single source of truth
    __sortable_fields__: ClassVar[list[str]] = EXECUTION_TARGET_SORTABLE_FIELDS
    __filterable_fields__: ClassVar[list[str]] = EXECUTION_TARGET_FILTERABLE_FIELDS
    __table_args__ = (
        UniqueConstraint("cluster_id", "name", name="execution_targets_cluster_name_key"),
        Index("ix_execution_targets_name", "name"),
        Index(
            "uq_execution_targets_default_cluster",
            "cluster_id",
            unique=True,
            postgresql_where=text("is_default = true"),
        ),
        {"schema": EP_SCHEMA},
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    cluster_id: uuid.UUID = Field(foreign_key=f"{EP_SCHEMA}.clusters.id", nullable=False)
    name: str
    # The migration stores enum values in VARCHAR columns, not native PG enums.
    backend_type: BackendType = Field(
        sa_column=Column(
            SAEnum(
                BackendType, native_enum=False, values_callable=lambda members: [member.value for member in members]
            ),
            nullable=False,
        ),
    )
    endpoint: str
    placement: ExecutionTargetPlacementTypes = Field(
        sa_type=DiscriminatedJSONB(ExecutionTargetPlacement),  # type: ignore[arg-type, call-overload]
        sa_column_kwargs={"nullable": False},
        discriminator="type",
    )

    status: TargetStatus = Field(
        default=TargetStatus.REGISTERING,
        sa_column=Column(
            SAEnum(
                TargetStatus, native_enum=False, values_callable=lambda members: [member.value for member in members]
            ),
            nullable=False,
        ),
    )
    enabled: bool = True
    is_default: bool = Field(default=False, nullable=False)
    credential_ref: uuid.UUID = Field(sa_column=Column(Uuid(), nullable=False))
    status_message: str | None = Field(default=None, nullable=True)
    labels: dict[str, str] = Field(default_factory=dict, sa_column=Column(JSONB, nullable=False, server_default="{}"))
    created_by: uuid.UUID = Field(nullable=False)
    created_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    updated_by: uuid.UUID = Field(nullable=False)
    updated_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    last_ran_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))

    cluster: "Cluster" = Relationship(back_populates="execution_targets")
