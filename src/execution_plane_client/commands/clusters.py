"""Cluster desired state, addressed by a caller-supplied physical id.

The cluster's primary key is supplied by the caller as ``resource_id`` and shared
with the owning Automation Orchestrator ``Integration`` (same UUID), mirroring the
WorkItem/ActivityExecution correlation. There is no configuration revision and no
separate correlation key.
"""

import ssl
from typing import Annotated, ClassVar, Self
from uuid import UUID

from pydantic import Field, StrictBool, StrictStr, field_validator, model_validator

from execution_plane_client.commands import ResourceIdentity, validate_endpoint
from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.protocol import WireModel


class ClusterConfigBase(WireModel):
    """Registration configuration fields used by handlers to copy and compare clusters."""

    name: Annotated[StrictStr, Field(min_length=1, max_length=255)]
    endpoint: Annotated[StrictStr, Field(max_length=2048)]
    credential_ref: UUID
    ca_certificate: Annotated[StrictStr, Field(max_length=65536)] | None = Field(default=None, repr=False)
    labels: dict[str, str] = Field(default_factory=dict)
    enabled: StrictBool = True
    is_default: StrictBool = False


class ClusterRegister(ClusterConfigBase, ResourceIdentity, CommandPayload):
    """Create or idempotently replace a cluster's desired configuration by id."""

    command_type: ClassVar[str] = "cluster.register"

    @field_validator("endpoint")
    @classmethod
    def check_endpoint(cls, value: str) -> str:
        """Protect the management credential in transit."""
        return validate_endpoint(value)

    @field_validator("ca_certificate")
    @classmethod
    def check_ca(cls, value: str | None) -> str | None:
        """Reject invalid PEM before accepting desired state."""
        if value is not None:
            try:
                ssl.create_default_context().load_verify_locations(cadata=value)
            except ssl.SSLError as exc:
                message = "CA certificate must contain valid PEM"
                raise ValueError(message) from exc
        return value

    @model_validator(mode="after")
    def check_credential_ref(self) -> Self:
        """Require a resolvable credential reference for enabled clusters."""
        if self.enabled and self.credential_ref == UUID(int=0):
            message = "Enabled clusters require a credential_ref"
            raise ValueError(message)
        return self


class ClusterProvision(ClusterRegister):
    """Provision through the same cluster and default-target reconciler."""

    command_type: ClassVar[str] = "cluster.provision"


class ClusterDelete(ResourceIdentity, CommandPayload):
    """Disable and drain an existing cluster addressed by id."""

    command_type: ClassVar[str] = "cluster.delete"


class ClusterUpdate(ResourceIdentity, CommandPayload):
    """Patch a cluster by id; only CA supports explicit null."""

    command_type: ClassVar[str] = "cluster.update"

    name: StrictStr | None = None
    endpoint: StrictStr | None = None
    credential_ref: UUID | None = None
    ca_certificate: StrictStr | None = Field(default=None, repr=False)
    labels: dict[str, str] | None = None
    enabled: StrictBool | None = None
    is_default: StrictBool | None = None

    def _message_payload(self) -> dict[str, object]:
        return self.model_dump(mode="json", exclude_unset=True)

    @model_validator(mode="after")
    def check_nulls(self) -> Self:
        """Preserve omitted values but reject clearing required fields."""
        if any(getattr(self, key) is None for key in self.model_fields_set - {"ca_certificate"}):
            message = "Required cluster fields cannot be null"
            raise ValueError(message)
        return self
