"""Store sorting and keysets adapted from Syntara's sorting, cursor, and service code.

``syntara.core.utils.sorting`` supplies sort parsing and ORDER BY construction.
``syntara.core.utils.cursor`` supplies sort directions and typed cursor value
deserialization. The combined keyset pipeline maps to
``syntara.core.services.base.BaseService._apply_sorting``,
``BaseService._apply_cursor_pagination``, ``BaseService._apply_keyset_filter``,
and ``BaseService._coerce_boolean_keyset``.

EP resolves public field aliases and restricts sorting to non-nullable fields,
rejecting invalid anchors before SQL. Query callers reverse backward fetches
before the pagination helper trims the extra row, following Syntara's pipeline.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from sqlalchemy import Integer, Select, cast
from sqlmodel import SQLModel, asc, desc

from execution_plane.query_filters import column_python_type, query_column


class SortDirection(str, Enum):
    """Adapted from syntara.core.utils.cursor.SortDirection.

    Sort direction enumeration.
    """

    ASC = "asc"
    DESC = "desc"


def _deserialize_bool_sort_value(value: str) -> bool:
    """Adapted from syntara.core.utils.cursor._deserialize_bool_sort_value.

    Parse a cursor boolean ``sort_value``.

    Raises:
        ValueError: If ``value`` is not ``true``/``false`` (case-insensitive).
            Callers wrap this as ``ValueError``; do not raise
            ``ValueError`` here (it subclasses ``ValueError`` and would
            double-wrap).

    """
    normalized = value.lower()
    if normalized not in ("true", "false"):
        msg = f"invalid boolean: {value}"
        raise ValueError(msg)
    return normalized == "true"


def deserialize_sort_value(value: str, python_type: type | None) -> Any:  # noqa: ANN401, PLR0911
    """Adapted from syntara.core.utils.cursor.deserialize_sort_value.

    Deserialize a cursor ``sort_value`` string back to a typed Python value.

    Inverse of :func:`execution_plane.api.pagination._serialize_sort_value`. Callers typically pass
    :func:`column_python_type` (same approach as filter value coercion in
    ``syntara.core.utils.filters``).

    Args:
        value: Serialized sort value from the cursor.
        python_type: Target Python type inferred from the SQLAlchemy column, if known.

    Returns:
        Value suitable for keyset comparison against the sort column.
        Empty strings for datetime/UUID/int targets become ``None``.

    Raises:
        ValueError: If ``value`` cannot be coerced to ``python_type``
            (malformed user-controlled cursor token → API 422).

    """
    if value == "" and python_type in {datetime, UUID, int}:
        return None
    try:
        if python_type is datetime:
            # Match filters._convert_datetime_value ISO/'Z' handling
            iso_value = value.replace("Z", "+00:00") if value.endswith("Z") else value
            return datetime.fromisoformat(iso_value)
        if python_type is UUID:
            return UUID(value)
        if python_type is bool:
            return _deserialize_bool_sort_value(value)
        if python_type is int:
            return int(value)
        if isinstance(python_type, type) and issubclass(python_type, Enum):
            return python_type(value)
    except (ValueError, TypeError) as e:
        msg = f"Invalid cursor sort value: {e}"
        raise ValueError(msg) from e
    return value


def deserialize_column_sort_value(value: str, column: Any) -> Any:  # noqa: ANN401
    """Adapted from syntara.core.utils.cursor.deserialize_column_sort_value.

    Deserialize a cursor sort_value using the column's Python type.
    """
    return deserialize_sort_value(value, column_python_type(column))


def parse_sort(
    sort_param: str | None,
    allowed_fields: list[str],
    default_field: str = "created_at",
    default_direction: SortDirection = SortDirection.DESC,
) -> tuple[str, SortDirection]:
    """Adapted from syntara.core.utils.sorting.parse_sort.

    Parse sort parameter into (field, direction) tuple.

    Args:
        sort_param: Sort parameter from query string (e.g., "name" or "-created_at")
        allowed_fields: List of field names that can be sorted
        default_field: Default field to sort by if sort_param is None/empty
        default_direction: Default direction if sort_param is None/empty

    Returns:
        Tuple of (field_name, sort_direction)

    Raises:
        ValueError: If field name is not in allowed_fields

    Examples:
        >>> parse_sort("name", ["name", "created_at"])
        ("name", SortDirection.ASC)

        >>> parse_sort("-created_at", ["name", "created_at"])
        ("created_at", SortDirection.DESC)

        >>> parse_sort(None, ["created_at"])
        ("created_at", SortDirection.DESC)

    """
    # Use defaults if no sort parameter provided
    if not sort_param:
        if default_field not in allowed_fields:
            msg = f"Default field '{default_field}' not in allowed_fields"
            raise ValueError(msg)
        return default_field, default_direction

    # Check for descending prefix
    if sort_param.startswith("-"):
        field_name = sort_param[1:]  # Remove the minus prefix
        direction = SortDirection.DESC
    else:
        field_name = sort_param
        direction = SortDirection.ASC

    # Validate field name
    if field_name not in allowed_fields:
        msg = f"Invalid field: {field_name}"
        raise ValueError(msg)

    return field_name, direction


def apply_sorting[TP: tuple[Any, ...]](
    query: Select[TP], sort_tuples: list[tuple[str, SortDirection]], model: type[SQLModel]
) -> Select[TP]:
    """Adapted from syntara.core.utils.sorting.apply_sorting.

    Apply sorting to a SQLAlchemy Query using SQLAlchemy object model.

    Args:
        query: SQLAlchemy Query object or SQLModel Select statement to sort
        sort_tuples: List of (field_name, sort_direction) tuples
        model: SQLModel class to get field attributes from

    Returns:
        Sorted query object of the same type as input

    Examples:
        >>> # With SQLModel Select
        >>> stmt = select(Resource)
        >>> sorted_stmt = apply_sorting(stmt, [("name", SortDirection.ASC)], Resource)

        >>> # With multiple sorts
        >>> sorts = [("name", SortDirection.ASC), ("created_at", SortDirection.DESC)]
        >>> sorted_stmt = apply_sorting(stmt, sorts, Resource)

    """
    if not sort_tuples:
        return query

    order_by_clauses = []
    for field_name, direction in sort_tuples:
        # Get the field attribute from the model
        if field_name not in getattr(model, "__sortable_fields__", []):
            msg = f"Invalid sort field: {field_name}"
            raise ValueError(msg)

        field_attr = query_column(model, field_name)

        # Apply sorting direction using SQLAlchemy functions
        if direction == SortDirection.ASC:
            order_by_clauses.append(asc(field_attr))
        else:  # SortDirection.DESC
            order_by_clauses.append(desc(field_attr))

    # Apply all sorting clauses to the query
    return query.order_by(*order_by_clauses)


def apply_cursor_sort[TP: tuple[Any, ...]](
    query: Select[TP],
    model: type[SQLModel],
    cursor: dict[str, Any] | None,
    *,
    sort_field: str = "created_at",
    sort_direction: str = "asc",
) -> Select[TP]:
    """Port BaseService._apply_sorting, _apply_cursor_pagination, and _apply_keyset_filter.

    Origin: syntara.core.services.base.BaseService. Reverse fetch ordering for
    prev cursors; the caller restores display order before N+1 trimming.
    """
    sort_field, direction = parse_sort(
        f"{'-' if sort_direction == 'desc' else ''}{sort_field}",
        getattr(model, "__sortable_fields__", []),
    )
    is_backward = cursor is not None and cursor["direction"] == "prev"
    actual_direction = direction
    if is_backward:
        actual_direction = SortDirection.ASC if direction == SortDirection.DESC else SortDirection.DESC
    query = apply_sorting(query, [(sort_field, actual_direction), ("id", actual_direction)], model)
    if cursor is None:
        return query
    sort_col = query_column(model, sort_field)
    cursor_sv = deserialize_column_sort_value(cursor["sort_value"], sort_col)
    # EP only exposes non-nullable sorts; reject absent/naive anchors before SQL.
    if cursor_sv is None or (isinstance(cursor_sv, datetime) and cursor_sv.utcoffset() is None):
        msg = "Cursor sort value must be non-null and timestamps must include a timezone"
        raise ValueError(msg)
    cursor_id = UUID(cursor["id"])
    # Origin: BaseService._coerce_boolean_keyset; SQLAlchemy rejects bool inequalities.
    if isinstance(cursor_sv, bool):
        sort_col, cursor_sv = cast(sort_col, Integer), int(cursor_sv)
    id_col = query_column(model, "id")
    if actual_direction == SortDirection.ASC:
        return query.filter((sort_col > cursor_sv) | ((sort_col == cursor_sv) & (id_col > cursor_id)))
    return query.filter((sort_col < cursor_sv) | ((sort_col == cursor_sv) & (id_col < cursor_id)))
