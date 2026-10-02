"""Versioned desired-state records received from independent clients."""

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel

from execution_plane.models.constants import EP_SCHEMA
from execution_plane.models.credential import EncryptedCredential


class ClusterBinding(SQLModel, table=True):
    """EP-owned desired state for one client integration and its compute target."""

    __tablename__ = "cluster_bindings"
    __table_args__ = (Index("ix_cluster_bindings_reconcile", "status", "updated_at"), {"schema": EP_SCHEMA})

    client_id: str = Field(sa_column=Column(String(128), primary_key=True))
    source_integration_id: uuid.UUID = Field(primary_key=True)
    revision: int = Field(sa_column=Column(Integer, nullable=False))
    name: str = Field(sa_column=Column(String(255), nullable=False))
    endpoint: str = Field(sa_column=Column(String(2048), nullable=False))
    namespace: str = Field(sa_column=Column(String(63), nullable=False))
    credential: str = Field(sa_column=Column(EncryptedCredential(), nullable=False), repr=False)
    ca_certificate: str | None = Field(
        default=None,
        sa_column=Column(EncryptedCredential(), nullable=True),
        repr=False,
        exclude=True,
    )
    project_ids: list[uuid.UUID] | None = Field(default=None, sa_column=Column(JSONB, nullable=True))
    labels: dict[str, str] = Field(default_factory=dict, sa_column=Column(JSONB, nullable=False))
    enabled: bool = True
    cluster_id: uuid.UUID | None = Field(default=None)
    observed_revision: int = Field(default=0, sa_column=Column(Integer, nullable=False))
    status: str = Field(default="pending", sa_column=Column(String(32), nullable=False))
    status_message: str | None = Field(default=None, sa_column=Column(String(1000), nullable=True))
    created_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    updated_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
