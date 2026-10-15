"""Cursor-based pagination utilities for EP list endpoints."""

from __future__ import annotations

import base64
import json
from datetime import datetime
from typing import Annotated, Any, Generic, TypeVar
from uuid import UUID

from pydantic import BaseModel, Field, StringConstraints

T = TypeVar("T")

VALID_CURSOR_FIELDS = {"id", "created_at", "direction", "sort_field", "sort_direction", "sort_value"}
MAX_CURSOR_BYTES = 1024
_CURSOR_NOT_OBJECT = "cursor must be a JSON object"


class EPListParams(BaseModel):
    """Query parameters accepted by all EP list endpoints.

    Mirrors syntara.core.models.base.query_params.BaseListParams.
    """

    limit: int = Field(default=20, gt=0, le=100)
    cursor: str | None = Field(default=None)
    # sort is accepted for AO compatibility; EP currently only orders by created_at.
    sort: Annotated[str, StringConstraints(pattern=r"^-?[a-z][a-z0-9_]*$")] | None = Field(default=None)
    include_total: bool = Field(default=False)


class ResourcesResponse(BaseModel, Generic[T]):  # noqa: UP046
    """Pagination envelope returned by all EP list endpoints.

    Simplified from syntara.core.models.pagination.ResourcesResponse /
    ResourcesResponseBase (drops ConfigDict extras; keeps the same four fields).
    """

    resources: list[T]
    next: str | None = None
    prev: str | None = None
    total: int | None = None


def serialize_sort_value(value: Any) -> str:  # noqa: ANN401
    """Serialize a sort column value to a string for cursor storage.

    Copied from syntara.core.utils.cursor.serialize_sort_value.
    """
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def encode_cursor(data: dict[str, Any]) -> str:
    """Encode a cursor dict to an opaque base64-JSON token.

    Simplified from syntara.core.utils.cursor.encode_cursor (plain dict instead
    of CursorData TypedDict; otherwise identical).
    """
    return base64.b64encode(json.dumps(data, sort_keys=True).encode()).decode()


def decode_cursor(token: str) -> dict[str, Any]:
    """Decode and whitelist-filter a cursor token; raise ValueError on invalid input.

    Simplified from syntara.core.utils.cursor.decode_cursor (single size check,
    dict-comprehension whitelist instead of typed _filter_to_cursor_data).
    """
    if len(token.encode()) > MAX_CURSOR_BYTES:
        msg = "cursor too large"
        raise ValueError(msg)
    try:
        raw = base64.b64decode(token.encode()).decode()
        data = json.loads(raw)
    except Exception as exc:
        msg = "invalid cursor"
        raise ValueError(msg) from exc
    if not isinstance(data, dict):
        raise TypeError(_CURSOR_NOT_OBJECT)
    return {k: v for k, v in data.items() if k in VALID_CURSOR_FIELDS}


def generate_response(
    items: list[Any],
    limit: int,
    cursor: str | None,
    *,
    sort_field: str = "created_at",
    sort_direction: str = "asc",
) -> tuple[list[Any], str | None, str | None]:
    """Return (trimmed_items, next_cursor, prev_cursor).

    Derived from syntara.core.utils.pagination.generate_response; drops the
    is_first_page flag, sort_value_fn callback, and backward-pagination
    asymmetric trim — EP only supports forward navigation via next cursors.
    """
    has_more = len(items) > limit
    trimmed = items[:limit]

    next_cursor = None
    if has_more and trimmed:
        last = trimmed[-1]
        next_cursor = encode_cursor(
            {
                "id": str(last.id),
                "created_at": last.created_at.isoformat(),
                "direction": "next",
                "sort_field": sort_field,
                "sort_direction": sort_direction,
                "sort_value": serialize_sort_value(getattr(last, sort_field)),
            }
        )

    prev_cursor = None
    if cursor and trimmed:
        first = trimmed[0]
        prev_cursor = encode_cursor(
            {
                "id": str(first.id),
                "created_at": first.created_at.isoformat(),
                "direction": "prev",
                "sort_field": sort_field,
                "sort_direction": sort_direction,
                "sort_value": serialize_sort_value(getattr(first, sort_field)),
            }
        )

    return trimmed, next_cursor, prev_cursor
