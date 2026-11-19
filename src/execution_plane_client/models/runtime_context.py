"""Non-secret caller context shared with the workload."""

from typing import Annotated

from pydantic import BeforeValidator, Field

from execution_plane_client.protocol import validate_json

RUNTIME_CONTEXT_DESCRIPTION = (
    "Optional caller-defined JSON object, defaulting to {}. Stored once, unencrypted, on the work item and "
    "passed to the plugin as context.runtime_context; also returned in work-item reads and completion notifications. "
    "Do not include secrets. Syntara supplies project_id (Project.id), workflow_id (Workflow.id), and execution_id "
    "(the containing Execution.id); EP does not require these keys. These values do not authorize access, select "
    "placement, identify the work item, or control its lifecycle. List filters runtime_context[key]=value match "
    "exact JSON strings at literal top-level keys; an empty value checks key existence and multiple keys use AND."
)

RuntimeContext = Annotated[
    dict[str, object],
    BeforeValidator(validate_json),
    Field(description=RUNTIME_CONTEXT_DESCRIPTION),
]
