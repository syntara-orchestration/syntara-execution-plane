"""Stable public request and response schemas for the EP API."""

import json
import ssl
import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from execution_plane.models.execution_target import BackendType, TargetStatus
from execution_plane.models.execution_target_placement import ExecutionTargetPlacement
from execution_plane.models.work_item import WorkItemStatus


class WorkItemSubmit(BaseModel):
    """Script work accepted from a trusted client."""

    id: uuid.UUID
    workload_type: Literal["script"] = "script"
    payload: dict[str, Any]

    @field_validator("payload")
    @classmethod
    def limit_payload_size(cls, payload: dict[str, Any]) -> dict[str, Any]:
        """Keep the encoded node request below the HTTP and gRPC framing budgets."""
        if len(json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")) > 512 * 1024:
            msg = "workload payload exceeds the 512 KiB limit"
            raise ValueError(msg)
        return payload

    @field_validator("payload")
    @classmethod
    def validate_node_invocation(cls, payload: dict[str, Any]) -> dict[str, Any]:
        """Accept only the versioned script invocation contract used by node gRPC."""
        invocation = payload.get("invocation")
        image = payload.get("image")
        output_config = payload.get("output_config")
        if not isinstance(invocation, dict) or not isinstance(image, str) or not image:
            msg = "payload must include a versioned invocation and node image"
            raise ValueError(msg)
        if invocation.get("version") != 1 or invocation.get("operation") != "execute":
            msg = "unsupported node invocation version or operation"
            raise ValueError(msg)
        for key in ("inputs", "credentials", "workflow_context", "settings"):
            if not isinstance(invocation.get(key), dict):
                msg = f"invocation.{key} must be an object"
                raise ValueError(msg)  # noqa: TRY004 — Pydantic maps ValueError to HTTP 422
        timeout = invocation.get("timeout_seconds")
        output_limit = invocation.get("max_output_bytes")
        if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout < 1:
            msg = "invocation.timeout_seconds must be a positive integer"
            raise ValueError(msg)
        if isinstance(output_limit, bool) or not isinstance(output_limit, int) or output_limit < 1:
            msg = "invocation.max_output_bytes must be a positive integer"
            raise ValueError(msg)
        if output_config is not None and not isinstance(output_config, dict):
            msg = "payload.output_config must be an object or null"
            raise ValueError(msg)
        return payload


class ClusterBindingUpsert(BaseModel):
    """Versioned OpenShift integration desired state supplied by an authorized client."""

    revision: int = Field(ge=1)
    name: str = Field(min_length=1, max_length=255)
    endpoint: str = Field(min_length=1, max_length=2048)
    namespace: str = Field(min_length=1, max_length=63, pattern=r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
    credential: str = Field(default="", repr=False)
    ca_certificate: str | None = Field(default=None, max_length=65_536, repr=False)
    insecure_skip_tls_verify: bool = False
    project_ids: list[uuid.UUID] | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    enabled: bool = True

    @field_validator("ca_certificate")
    @classmethod
    def validate_ca_certificate(cls, certificate: str | None) -> str | None:
        """Accept only parseable PEM used to verify the configured cluster endpoint."""
        if certificate is None:
            return None
        context = ssl.create_default_context()
        context.load_verify_locations(cadata=certificate)
        return certificate


class ClusterBindingRead(BaseModel):
    """Safe observed state for an AO-owned source integration."""

    client_id: str
    source_integration_id: uuid.UUID
    desired_revision: int
    observed_revision: int
    cluster_id: uuid.UUID | None
    status: str
    status_message: str | None
    enabled: bool
    updated_at: datetime


class WorkItemRead(BaseModel):
    """Safe work-item representation; excludes credentials and persistence internals."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    status: WorkItemStatus
    result: dict[str, Any] | None
    created_at: datetime
    claimed_at: datetime | None
    completed_at: datetime | None
    resource_cleanup_status: str
    resource_cleanup_error: str | None
    completion_event_id: uuid.UUID | None = None
    state_revision: int | None = None


class ExecutionTargetRead(BaseModel):
    """Safe execution-target representation without management credentials."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    cluster_id: uuid.UUID
    name: str
    backend_type: BackendType
    endpoint: str
    placement: ExecutionTargetPlacement
    status: TargetStatus
    enabled: bool
    is_default: bool
    status_message: str | None
    labels: dict[str, str]
    created_at: datetime
    last_ran_at: datetime | None


class CompletionEventRequest(BaseModel):
    """Wire form delivered to the client's configured completion callback."""

    event_id: uuid.UUID
    event_schema_version: Literal[1] = 1
    client_id: str
    work_id: uuid.UUID
    state_revision: int
    status: WorkItemStatus
    result: dict[str, Any]
    completed_at: datetime


class CapabilitiesResponse(BaseModel):
    """Contract and workload features implemented by this service release."""

    api_version: str = "v1"
    workloads: list[str] = Field(default_factory=lambda: ["script"])
    result_events: bool = True
