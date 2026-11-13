"""Opaque caller context, independent of placement and workload invocation."""

from typing import Annotated

from pydantic import BeforeValidator, Field

from execution_plane_client.protocol import validate_json

WorkMetadata = Annotated[
    dict[str, object],
    BeforeValidator(validate_json),
    Field(
        description=(
            "Non-secret caller-owned context retained with this work item. Execution Plane "
            "does not interpret these values for identity, authorization, placement, or lifecycle decisions."
        )
    ),
]
