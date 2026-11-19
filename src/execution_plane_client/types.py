"""Type definitions."""

from enum import StrEnum


class BackendType(StrEnum):
    """Supported execution target backends."""

    KUBERNETES = "kubernetes"
    OPENSHELL = "openshell"


class TargetStatus(StrEnum):
    """Lifecycle states of an execution target."""

    REGISTERING = "registering"
    VALIDATING = "validating"
    BOOTSTRAPPING = "bootstrapping"
    ACTIVE = "active"
    DEGRADED = "degraded"
    DRAINING = "draining"
    FAILED = "failed"
