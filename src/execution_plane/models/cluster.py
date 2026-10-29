"""Cluster persistence model and lifecycle states."""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, ClassVar

from sqlalchemy import Column, Index, String, Uuid, text
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import DateTime
from sqlmodel import Field, Relationship, SQLModel

from execution_plane.models.constants import EP_SCHEMA
from execution_plane.models.credential import EncryptedCredential

if TYPE_CHECKING:
    type ExecutionTarget = Any


class ClusterStatus(StrEnum):
    """Lifecycle states of a cluster."""

    REGISTERING = "registering"
    ACTIVE = "active"
    DRAINING = "draining"
    ERROR = "error"


class ClusterType(StrEnum):
    """Host platform the Execution Plane connects to."""

    OPENSHIFT = "openshift"
    RHEL = "rhel"


class Cluster(SQLModel, table=True):
    """A registered control-plane cluster."""

    __tablename__ = "clusters"
    __sortable_fields__: ClassVar[list[str]] = [
        "id",
        "created_at",
        "updated_at",
        "name",
        "endpoint",
        "status",
        "enabled",
        "is_default",
    ]
    __filterable_fields__: ClassVar[list[str]] = [
        "id",
        "created_at",
        "updated_at",
        "name",
        "endpoint",
        "status",
        "status_message",
        "enabled",
        "is_default",
    ]
    __table_args__ = (
        Index("uq_clusters_default", "is_default", unique=True, postgresql_where=text("is_default = true")),
        {"schema": EP_SCHEMA},
    )

    # Caller-supplied physical id, shared with the owning AO Integration (same UUID).
    id: uuid.UUID = Field(primary_key=True)
    name: str = Field(sa_column=Column(String, nullable=False))
    endpoint: str = Field(sa_column=Column(String, nullable=False))
    status: ClusterStatus = Field(
        default=ClusterStatus.REGISTERING,
        sa_column=Column(
            SAEnum(
                ClusterStatus, native_enum=False, values_callable=lambda members: [member.value for member in members]
            ),
            nullable=False,
        ),
    )
    enabled: bool = Field(default=True, nullable=False)
    is_default: bool = Field(default=False, nullable=False)
    status_message: str | None = Field(default=None, nullable=True)
    credential_ref: uuid.UUID = Field(sa_column=Column(Uuid(), nullable=False))
    ca_certificate: str | None = Field(
        default=None,
        sa_column=Column(EncryptedCredential(), nullable=True),
        repr=False,
        exclude=True,
    )
    source_client_id: str | None = Field(default=None, sa_column=Column(String(128), nullable=True))
    cluster_type: ClusterType = Field(
        default=ClusterType.OPENSHIFT,
        sa_column=Column(
            SAEnum(
                ClusterType, native_enum=False, values_callable=lambda members: [member.value for member in members]
            ),
            nullable=False,
        ),
    )
    labels: dict[str, str] = Field(
        default_factory=dict,
        sa_column=Column(JSONB, nullable=False, server_default="{}"),
    )
    created_by: uuid.UUID = Field(nullable=False)
    created_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    updated_by: uuid.UUID = Field(nullable=False)
    updated_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))

    execution_targets: list["ExecutionTarget"] = Relationship(back_populates="cluster")
