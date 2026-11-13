"""Non-secret caller context shared with the workload."""

from typing import Annotated

from pydantic import BeforeValidator, Field

from execution_plane_client.protocol import validate_json

RuntimeContext = Annotated[
    dict[str, object],
    BeforeValidator(validate_json),
    Field(
        description=(
            "Non-secret caller-owned runtime context, stored once with the work item and passed to the plugin. "
            "Available in reads, completion notifications, and runtime_context[key] filters. "
            "Execution Plane does not interpret these values for identity, authorization, placement, or lifecycle."
        )
    ),
]
