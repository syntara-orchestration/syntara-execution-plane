"""EP pagination helpers adapted from Syntara's pagination modules.

Source mapping:
- ``syntara.core.models.base.query_params``: list query controls and page limits.
- ``syntara.core.models.pagination``: the ``ResourcesResponse`` envelope.
- ``syntara.core.utils.cursor``: cursor encoding, decoding, and value serialization.
- ``syntara.core.utils.pagination``: N+1 trimming and next/prev response cursors.

EP combines these pieces here, declares endpoint-specific sortable fields, and
validates cursor anchors and sort compatibility before querying. Creation-time
cursors may omit sort_value as in Syntara; emitted EP cursors retain that field.
"""

from __future__ import annotations

import base64
import json
from datetime import datetime
from typing import Any
from uuid import UUID

VALID_CURSOR_FIELDS = {"id", "created_at", "direction", "sort_field", "sort_direction", "sort_value"}
MAX_CURSOR_BYTES = 1024
_CURSOR_NOT_OBJECT = "cursor must be a JSON object"


def _serialize_sort_value(value: Any) -> str:  # noqa: ANN401
    """Serialize a sort column value to a string for cursor storage.

    Copied from syntara.core.utils.cursor._serialize_sort_value.
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


def _encode_cursor(data: dict[str, Any]) -> str:
    """Encode a cursor dict to an opaque base64-JSON token.

    Simplified from syntara.core.utils.cursor._encode_cursor (plain dict instead
    of CursorData TypedDict; otherwise identical).
    """
    return base64.b64encode(json.dumps(data, sort_keys=True).encode()).decode()


def _validate_timestamp(value: str) -> None:
    """Require an ISO timestamp with a timezone for PostgreSQL keyset comparisons."""
    if datetime.fromisoformat(value).utcoffset() is None:
        msg = "cursor timestamps must include a timezone"
        raise ValueError(msg)


def _decode_cursor(
    token: str,
    *,
    sort_field: str | None = None,
    sort_direction: str | None = None,
) -> dict[str, Any]:
    """Decode and validate a cursor, optionally checking its sort against the query.

    Derived from syntara.core.utils.cursor._decode_cursor. Unknown fields are
    ignored. For created_at sorting, a missing sort_value falls back to
    created_at, matching Syntara's _make_cursor and BaseService cursor handling.
    All other known fields are required and must contain strings. Invalid input
    consistently raises ValueError for an HTTP 422 response.
    """
    if len(token.encode()) > MAX_CURSOR_BYTES:
        msg = "cursor too large"
        raise ValueError(msg)
    try:
        raw = base64.b64decode(token.encode(), validate=True).decode()
        data = json.loads(raw)
    except (ValueError, RecursionError) as exc:
        msg = "invalid cursor"
        raise ValueError(msg) from exc
    if not isinstance(data, dict):
        raise ValueError(_CURSOR_NOT_OBJECT)  # noqa: TRY004 - invalid cursors consistently map to HTTP 422
    if data.get("sort_field") == "created_at" and "sort_value" not in data:
        data["sort_value"] = data.get("created_at")
    if any(not isinstance(data.get(field), str) for field in VALID_CURSOR_FIELDS):
        msg = "cursor requires string id, created_at, direction, sort_field, sort_direction, and sort_value fields"
        raise ValueError(msg)
    if data["direction"] not in {"next", "prev"} or data["sort_direction"] not in {"asc", "desc"}:
        msg = "invalid cursor direction"
        raise ValueError(msg)
    if (sort_field is not None and data["sort_field"] != sort_field) or (
        sort_direction is not None and data["sort_direction"] != sort_direction
    ):
        msg = "cursor sort does not match the requested sort"
        raise ValueError(msg)
    UUID(data["id"])
    _validate_timestamp(data["created_at"])
    if data["sort_field"] == "created_at":
        _validate_timestamp(data["sort_value"])
    elif data["sort_field"] == "id":
        UUID(data["sort_value"])
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

    Derived from syntara.core.utils.pagination.generate_response. Queries
    return up to limit + 1 items in display order. On a backward fetch the
    extra row is at the beginning and indicates an earlier page, while the
    incoming cursor establishes that a later page exists.
    """
    has_more = len(items) > limit
    going_backward = cursor is not None and _decode_cursor(cursor)["direction"] == "prev"
    trimmed = items[-limit:] if going_backward else items[:limit]
    has_next = going_backward or has_more
    has_prev = has_more if going_backward else cursor is not None

    next_cursor = None
    if has_next and trimmed:
        last = trimmed[-1]
        next_cursor = _encode_cursor(
            {
                "id": str(last.id),
                "created_at": last.created_at.isoformat(),
                "direction": "next",
                "sort_field": sort_field,
                "sort_direction": sort_direction,
                "sort_value": _serialize_sort_value(getattr(last, sort_field)),
            }
        )

    prev_cursor = None
    if has_prev and trimmed:
        first = trimmed[0]
        prev_cursor = _encode_cursor(
            {
                "id": str(first.id),
                "created_at": first.created_at.isoformat(),
                "direction": "prev",
                "sort_field": sort_field,
                "sort_direction": sort_direction,
                "sort_value": _serialize_sort_value(getattr(first, sort_field)),
            }
        )

    return trimmed, next_cursor, prev_cursor
