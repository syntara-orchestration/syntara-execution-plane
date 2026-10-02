"""Cluster persistence model and lifecycle states."""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from sqlalchemy import Column, Integer, String, UniqueConstraint
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
    __table_args__ = (
        UniqueConstraint("source_client_id", "source_integration_id", name="uq_clusters_source_integration"),
        {"schema": EP_SCHEMA},
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
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
    status_message: str | None = Field(default=None, nullable=True)
    api_key: str = Field(sa_column=Column(EncryptedCredential(), nullable=False), repr=False, exclude=True)
    ca_certificate: str | None = Field(
        default=None,
        sa_column=Column(EncryptedCredential(), nullable=True),
        repr=False,
        exclude=True,
    )
    source_client_id: str | None = Field(default=None, sa_column=Column(String(128), nullable=True))
    source_integration_id: uuid.UUID | None = Field(default=None, nullable=True)
    source_revision: int = Field(default=0, sa_column=Column(Integer, nullable=False, server_default="0"))
    project_ids: list[uuid.UUID] | None = Field(
        default=None,
        sa_column=Column(JSONB, nullable=True),
        description="Null permits all projects; a list limits placement and target visibility.",
    )
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
