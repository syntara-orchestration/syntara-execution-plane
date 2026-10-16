"""Cluster-binding desired state, independent of EP persistence models."""

import ssl
from typing import Annotated, ClassVar, Self
from uuid import UUID

from pydantic import Field, StrictBool, StrictStr, field_validator, model_validator

from execution_plane_client.commands import ResourceRevision, validate_endpoint
from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.protocol import WireModel


class ClusterConfigBase(WireModel):
    """Desired configuration fields shared across all cluster-binding commands."""

    name: Annotated[StrictStr, Field(min_length=1, max_length=255)]
    endpoint: Annotated[StrictStr, Field(max_length=2048)]
    namespace: Annotated[StrictStr, Field(min_length=1, max_length=63, pattern=r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")]
    credential: StrictStr = Field(default="", repr=False)
    ca_certificate: Annotated[StrictStr, Field(max_length=65536)] | None = Field(default=None, repr=False)
    project_ids: list[UUID] | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    enabled: StrictBool = True


class ClusterRegister(ClusterConfigBase, ResourceRevision, CommandPayload):
    """Create or replace the complete desired configuration of a binding."""

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
    def check_credential(self) -> Self:
        """Require credentials for enabled bindings."""
        if self.enabled and not self.credential:
            message = "Enabled cluster bindings require a credential"
            raise ValueError(message)
        return self


class ClusterProvision(ClusterRegister):
    """Provision through the same binding and default-target reconciler."""

    command_type: ClassVar[str] = "cluster.provision"


class ClusterDelete(ResourceRevision, CommandPayload):
    """Disable an existing binding at the requested desired-state revision."""

    command_type: ClassVar[str] = "cluster.delete"


class ClusterUpdate(ResourceRevision, CommandPayload):
    """Patch a binding; only CA and project grants support explicit null."""

    command_type: ClassVar[str] = "cluster.update"

    name: StrictStr | None = None
    endpoint: StrictStr | None = None
    namespace: StrictStr | None = None
    credential: StrictStr | None = Field(default=None, repr=False)
    ca_certificate: StrictStr | None = Field(default=None, repr=False)
    project_ids: list[UUID] | None = None
    labels: dict[str, str] | None = None
    enabled: StrictBool | None = None

    def _message_payload(self) -> dict[str, object]:
        return self.model_dump(mode="json", exclude_unset=True)

    @model_validator(mode="after")
    def check_nulls(self) -> Self:
        """Preserve omitted values but reject clearing required fields."""
        if any(getattr(self, key) is None for key in self.model_fields_set - {"ca_certificate", "project_ids"}):
            message = "Required binding fields cannot be null"
            raise ValueError(message)
        return self
