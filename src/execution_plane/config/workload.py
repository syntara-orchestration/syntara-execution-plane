"""Workload lifecycle and network policy settings."""

from __future__ import annotations

import ipaddress

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings


class NodeLifecycleSettings(BaseSettings):
    """Default Kubernetes Job lifecycle settings."""

    node_startup_seconds: int = Field(default=120, validation_alias="EP_NODE_STARTUP_SECONDS", ge=1)
    node_grace_seconds: int = Field(default=30, validation_alias="EP_NODE_GRACE_SECONDS", ge=1)


class WorkloadEgressSettings(BaseSettings):
    """Workload egress allow and deny ranges."""

    workload_allowed_egress_cidrs: list[str] = Field(
        default_factory=list,
        validation_alias="EP_WORKLOAD_ALLOWED_EGRESS_CIDRS",
    )
    workload_forbidden_egress_cidrs: list[str] = Field(
        default_factory=list,
        validation_alias="EP_WORKLOAD_FORBIDDEN_EGRESS_CIDRS",
    )

    @model_validator(mode="after")
    def validate_workload_network_ranges(self) -> WorkloadEgressSettings:
        """Require explicit denies when a broad workload egress range is allowed."""
        allowed_networks = [ipaddress.ip_network(value, strict=False) for value in self.workload_allowed_egress_cidrs]
        forbidden_networks = [
            ipaddress.ip_network(value, strict=False) for value in self.workload_forbidden_egress_cidrs
        ]
        if allowed_networks and not forbidden_networks:
            msg = "EP_WORKLOAD_FORBIDDEN_EGRESS_CIDRS is required when workload egress is allowed"
            raise ValueError(msg)
        for network in allowed_networks:
            for denied in forbidden_networks:
                if isinstance(network, ipaddress.IPv4Network):
                    if not isinstance(denied, ipaddress.IPv4Network):
                        continue
                    partial_overlap = network.overlaps(denied) and not denied.subnet_of(network)
                else:
                    if not isinstance(denied, ipaddress.IPv6Network):
                        continue
                    partial_overlap = network.overlaps(denied) and not denied.subnet_of(network)
                if partial_overlap:
                    msg = f"Forbidden egress range {denied} must be contained by allowed range {network}"
                    raise ValueError(msg)
        return self
