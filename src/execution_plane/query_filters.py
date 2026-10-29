"""Store filtering adapted primarily from ``syntara.core.utils.filters``.

That module supplies the filter model, bracket parser, typed value conversion,
and OR/AND grouping. Label parsing and JSONB matching come from
``syntara.core.utils.labels``; column type inspection comes from
``syntara.core.utils.cursor.column_python_type``. The combined filter pipeline
maps to ``syntara.core.services.base.BaseService._apply_standard_filters`` and
``BaseService._apply_label_filters``.

EP adaptations resolve public cluster field aliases, validate UUID values,
interpret timezone-free date filters as UTC, and make empty IN lists match no
rows. List and total queries use the same pipeline inside their client scope.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import UUID

import structlog
from pydantic import BaseModel
from sqlalchemy import Select, false
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import SQLModel, and_, cast, or_

logger = structlog.stdlib.get_logger(__name__)
FilterValue = str | int | float | bool | datetime | Enum | UUID
BRACKET_PATTERN = re.compile(r"^([\w.]+)\[([^]]*)\]$")
LABEL_PARAM_PATTERN = re.compile(r"^labels\[([^]]+)\]$")


def column_python_type(column: Any) -> type | None:  # noqa: ANN401
    """Adapted from syntara.core.utils.cursor.column_python_type.

    Return ``column.type.python_type``, or ``None`` when unavailable.

    Same approach as ``syntara.core.utils.filters._convert_filter_value``:
    some SQLAlchemy types raise ``NotImplementedError`` / lack ``python_type``.
    """
    try:
        return column.type.python_type  # type: ignore[no-any-return]
    except (AttributeError, NotImplementedError):
        return None


def query_column(model: type[SQLModel], field: str) -> Any:  # noqa: ANN401 - SQLAlchemy columns vary by model
    """Resolve public aliases like BaseService._apply_standard_filters special field handlers.

    Origin: syntara.core.services.base.BaseService._apply_standard_filters.
    """
    return getattr(model, getattr(model, "__query_field_aliases__", {}).get(field, field))


class FilterOperator(str, Enum):
    """Adapted from syntara.core.utils.filters.FilterOperator.

    Supported filter operators for query parameters.

    String values match the expected bracket notation syntax:
    - eq: Exact equality match
    - contains: Substring match (case-insensitive)
    - starts_with: Prefix match (case-insensitive)
    - gt: Greater than comparison
    - gte: Greater than or equal comparison
    - lt: Less than comparison
    - lte: Less than or equal comparison
    - isnull: Null check (true = IS NULL, false = IS NOT NULL)
    """

    EQ = "eq"
    IN = "in"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    ISNULL = "isnull"


class Filter(BaseModel):
    """Adapted from syntara.core.utils.filters.Filter.

    Structured filter object parsed from query parameters.

    Attributes:
        field: Name of the field to filter on
        operator: Filter operator to apply
        value: Value to filter by (supports str, int, float, bool, datetime)

    """

    field: str
    operator: FilterOperator
    value: FilterValue


def _split_values(raw: str, field: str, operator: FilterOperator) -> list[Filter]:
    """Adapted from syntara.core.utils.filters._split_values.

    Split a comma-separated parameter value into Filter objects.

    EP keeps an empty IN group as a false predicate. Otherwise, like the origin,
    for the IN operator, blank segments are skipped (e.g. ``status[in]=a,,b``
    produces two filters). For all other operators, empty strings are preserved
    so that ``?field=`` still filters for the empty value.
    """
    if operator == FilterOperator.IN:
        return [Filter(field=field, operator=operator, value=v) for val in raw.split(",") if (v := val.strip())] or [
            Filter(field=field, operator=operator, value="")
        ]
    return [Filter(field=field, operator=operator, value=val.strip()) for val in raw.split(",")]


def parse_filters(params: dict[str, str], allowed_fields: list[str]) -> list[Filter]:
    """Adapted from syntara.core.utils.filters.parse_filters.

    Parse query parameters into structured filter objects.

    Supports both shorthand equality syntax (field=value) and explicit
    bracket notation (field[operator]=value) for advanced filtering.

    Args:
        params: Dictionary of query parameters from request
        allowed_fields: List of field names that can be filtered

    Returns:
        List of Filter objects parsed from parameters

    Raises:
        ValueError: If invalid field name or operator is used

    Examples:
        >>> params = {"name[contains]": "test", "status": "active"}
        >>> parse_filters(params, ["name", "status"])
        [Filter(field="name", operator=CONTAINS, value="test"),
         Filter(field="status", operator=EQ, value="active")]

    """
    filters: list[Filter] = []

    for param_name, param_value in params.items():
        # Skip label parameters - they should be handled by label filtering logic
        if LABEL_PARAM_PATTERN.match(param_name):
            continue

        # Try to match bracket notation first
        bracket_match = BRACKET_PATTERN.match(param_name)

        # Query string params is guaranteed to be dict[str, str] however some unit tests use other types.
        param_str: str = str(param_value)

        if bracket_match:
            # Bracket notation: field[operator]=value
            field_name, operator_str = bracket_match.groups()

            # Validate field name
            if field_name not in allowed_fields:
                msg = f"Invalid field: {field_name}"
                raise ValueError(msg)

            # Validate operator
            if operator_str not in FilterOperator.__members__.values():
                msg = f"Invalid operator: {operator_str}"
                raise ValueError(msg)

            operator = FilterOperator(operator_str)

            filters.extend(_split_values(param_str, field_name, operator))

        else:
            # Shorthand notation: field=value (implies equality)
            field_name = param_name

            # Validate field name
            if field_name not in allowed_fields:
                msg = f"Invalid field: {field_name}"
                raise ValueError(msg)

            filters.extend(_split_values(param_str, field_name, FilterOperator.EQ))

    return filters


def _sanitize_like_value(value: FilterValue) -> str:
    """Adapted from syntara.core.utils.filters._sanitize_like_value.

    Sanitize value for use in LIKE/ILIKE patterns to prevent injection.

    Args:
        value: Value to sanitize for LIKE pattern

    Returns:
        Sanitized string value with special characters escaped

    Security Note:
        Escapes % and _ characters that have special meaning in SQL LIKE patterns
        to prevent injection attacks through pattern manipulation.

    """
    if not isinstance(value, str):
        value = str(value)

    # Escape special LIKE pattern characters
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _convert_datetime_value(value: str, field_attr: Any) -> datetime:  # noqa: ANN401
    """Adapted from syntara.core.utils.filters._convert_datetime_value.

    Convert string value to datetime.

    Args:
        value: String value to convert
        field_attr: SQLAlchemy field attribute for error context

    Returns:
        Parsed datetime object

    Raises:
        ValueError: If datetime conversion fails

    """
    try:
        # Try parsing as ISO 8601 format (with 'Z' suffix or timezone)
        # Replace 'Z' with '+00:00' for fromisoformat compatibility
        iso_value = value.replace("Z", "+00:00") if value.endswith("Z") else value
        parsed = datetime.fromisoformat(iso_value)
        return parsed if parsed.utcoffset() is not None else parsed.replace(tzinfo=UTC)
    except ValueError as e:
        # Log warning and re-raise - malformed timestamps should fail loudly
        logger.warning(
            "Failed to parse datetime value",
            value=value,
            field=getattr(field_attr, "key", "unknown"),
            error=str(e),
        )
        msg = f"Invalid datetime format: {value}. Expected ISO 8601 format (e.g., '2025-01-15T10:30:00Z')"
        raise ValueError(msg) from e


def _convert_boolean_value(value: str) -> bool:
    """Adapted from syntara.core.utils.filters._convert_boolean_value.

    Convert string value to boolean.

    Args:
        value: String value to convert

    Returns:
        Boolean value

    Raises:
        ValueError: If boolean conversion fails

    """
    try:
        # Convert common string representations to boolean
        lower_value = value.lower().strip()
        if lower_value in ("true", "1", "yes", "on"):
            return True
        if lower_value in ("false", "0", "no", "off"):
            return False
        msg = f"Invalid boolean value: {value}. Expected 'true', 'false', '1', '0', 'yes', 'no', 'on', or 'off'"
        raise ValueError(msg)
    except AttributeError as e:
        # If value doesn't have lower() method, it's not a string
        msg = f"Invalid boolean value: {value}. Expected string representation of boolean"
        raise ValueError(msg) from e


def _convert_numeric_value(value: str, python_type: type[int | float], field_attr: Any) -> int | float:  # noqa: ANN401
    """Adapted from syntara.core.utils.filters._convert_numeric_value.

    Convert string value to numeric type.

    Args:
        value: String value to convert
        python_type: Target numeric type (int or float)
        field_attr: SQLAlchemy field attribute for error context

    Returns:
        Converted numeric value

    Raises:
        ValueError: If numeric conversion fails

    """
    try:
        converted_value = python_type(value)
        # Ensure we return the expected type
        if python_type is int:
            return int(converted_value)
        if python_type is float:
            return float(converted_value)
        # This shouldn't happen given the function's usage, but for type safety
        return float(converted_value)
    except ValueError as e:
        # Log warning and re-raise - type mismatches should fail loudly
        logger.warning(
            "Failed to convert value to numeric type",
            value=value,
            python_type=python_type.__name__,
            field=getattr(field_attr, "key", "unknown"),
            error=str(e),
        )
        msg = f"Invalid {python_type.__name__} value: {value}"
        raise ValueError(msg) from e


def _convert_enum_value(value: str, python_type: type[Enum], field_attr: Any) -> Enum:  # noqa: ANN401
    """Adapted from syntara.core.utils.filters._convert_enum_value.

    Convert string value to enum type.

    Args:
        value: String value to convert
        python_type: Target enum type
        field_attr: SQLAlchemy field attribute for error context

    Returns:
        Enum instance

    Raises:
        ValueError: If enum value is invalid

    """
    try:
        # Try to create the enum instance from the string value
        return python_type(value)
    except ValueError as e:
        # Get valid values for error message
        valid_values = [item.value for item in python_type.__members__.values()]
        field_name = getattr(field_attr, "key", "unknown")
        logger.warning(
            "Invalid enum value",
            value=value,
            field=field_name,
            valid_values=valid_values,
        )
        msg = f"Invalid value '{value}' for field '{field_name}'. Valid values are: {', '.join(valid_values)}"
        raise ValueError(msg) from e


def _convert_filter_value(value: FilterValue, field_attr: Any) -> FilterValue:  # noqa: ANN401, PLR0911
    """Adapted from syntara.core.utils.filters._convert_filter_value.

    Convert filter value to the appropriate type based on the field.

    Args:
        value: Raw filter value (usually a string from query params)
        field_attr: SQLAlchemy field attribute to infer type from

    Returns:
        Converted value of the appropriate type

    Raises:
        ValueError: If datetime conversion fails with invalid format or if enum value is invalid

    """
    # If value is already the right type, return as-is
    if not isinstance(value, str):
        return value

    # Try to infer type from the field's Python type
    try:
        # Get the Python type from the SQLAlchemy column
        python_type = field_attr.type.python_type
    except (AttributeError, NotImplementedError):
        # Some SQLAlchemy types don't implement python_type (like UUID, custom types)
        # This is expected, just use string comparison for these fields
        logger.debug(
            "Field does not provide python_type, using string comparison",
            field=getattr(field_attr, "key", "unknown"),
        )
        return value

    if python_type is UUID:
        return UUID(value)

    # Handle enum fields with validation
    if isinstance(python_type, type) and issubclass(python_type, Enum):
        return _convert_enum_value(value, python_type, field_attr)

    # Handle datetime fields
    if python_type == datetime or (hasattr(python_type, "__origin__") and python_type.__origin__ == datetime):
        return _convert_datetime_value(value, field_attr)

    # Handle boolean fields
    if python_type is bool:
        return _convert_boolean_value(value)

    # Handle numeric types
    if python_type in (int, float):
        return _convert_numeric_value(value, python_type, field_attr)

    # For other types (str, custom types), use string comparison
    return value


def _build_condition(field_attr: Any, operator: FilterOperator, value: FilterValue) -> Any:  # noqa: ANN401, PLR0911, C901
    """Adapted from syntara.core.utils.filters._build_condition.

    Build a SQLAlchemy condition for a single filter.

    Args:
        field_attr: SQLAlchemy field attribute from the model
        operator: Filter operator to apply
        value: Value to filter by

    Returns:
        SQLAlchemy condition object

    """
    if operator == FilterOperator.ISNULL:
        return field_attr.is_(None) if _convert_boolean_value(str(value)) else field_attr.is_not(None)
    if operator == FilterOperator.IN and value == "":
        return false()
    python_type = column_python_type(field_attr)
    if operator in {FilterOperator.CONTAINS, FilterOperator.STARTS_WITH} and python_type not in {str, None}:
        msg = "Text operators require a string field"
        raise ValueError(msg)
    if python_type is bool and operator not in {FilterOperator.EQ, FilterOperator.IN}:
        msg = "Boolean fields only support eq, in, and isnull"
        raise ValueError(msg)
    # Convert value to appropriate type based on field
    converted_value = _convert_filter_value(value, field_attr)

    match operator:
        case FilterOperator.EQ | FilterOperator.IN:
            return field_attr == converted_value
        case FilterOperator.CONTAINS:
            sanitized_value = _sanitize_like_value(converted_value)
            return field_attr.ilike(f"%{sanitized_value}%")
        case FilterOperator.STARTS_WITH:
            sanitized_value = _sanitize_like_value(converted_value)
            return field_attr.ilike(f"{sanitized_value}%")
        case FilterOperator.GT:
            return field_attr > converted_value
        case FilterOperator.GTE:
            return field_attr >= converted_value
        case FilterOperator.LT:
            return field_attr < converted_value
        case FilterOperator.LTE:
            return field_attr <= converted_value


def apply_filters[TP: tuple[Any, ...]](query: Select[TP], filters: list[Filter], model: type[SQLModel]) -> Select[TP]:
    """Adapted from syntara.core.utils.filters.apply_filters.

    Apply filters to a SQLAlchemy Query or SQLModel Select using Query API.

    Groups filters by ``(field, operator)`` and applies OR logic within each
    group (so comma-separated values of the same operator are unioned), then
    AND logic between different groups.  This means:

    - Same field, same operator, multiple values → OR (the ``?name=alice,bob``
      case).
    - Same field, *different* operators → AND (the range-filter case:
      ``?created_at[gte]=X&created_at[lte]=Y`` becomes
      ``created_at >= X AND created_at <= Y``).
    - Different fields → AND.

    Args:
        query: SQLAlchemy Query object or SQLModel Select statement to filter
        filters: List of Filter objects to apply
        model: SQLModel class to get field attributes from

    Returns:
        Filtered query object of the same type as input

    Examples:
        >>> # Single field with OR (same operator): ?name=alice,bob
        >>> # Becomes: WHERE name = 'alice' OR name = 'bob'

        >>> # Same field, different operators (range): ?created_at[gte]=X&created_at[lte]=Y
        >>> # Becomes: WHERE created_at >= X AND created_at <= Y

        >>> # Different fields with AND: ?name=alice,bob&status=active
        >>> # Becomes: WHERE (name = 'alice' OR name = 'bob') AND status = 'active'

    """
    if not filters:
        return query

    # Group filters by (field, operator) so multiple values of the same
    # operator (e.g. ?name=alice,bob) are OR'd, while different operators on
    # the same field (e.g. [gte] + [lte]) are AND'd via the outer combine.
    operator_groups: dict[tuple[str, FilterOperator], list[Filter]] = {}

    for filter_obj in filters:
        # Validate field exists on model
        if filter_obj.field not in getattr(model, "__filterable_fields__", []):
            msg = f"Invalid filter field: {filter_obj.field}"
            raise ValueError(msg)

        operator_groups.setdefault((filter_obj.field, filter_obj.operator), []).append(filter_obj)

    # Build conditions: OR within each (field, operator) group, AND between groups.
    group_conditions = []

    for (field_name, operator), group_filters in operator_groups.items():
        field_attr = query_column(model, field_name)

        filter_conditions = [_build_condition(field_attr, operator, f.value) for f in group_filters]

        if len(filter_conditions) == 1:
            group_conditions.append(filter_conditions[0])
        else:
            group_conditions.append(or_(*filter_conditions))

    if group_conditions:
        query = query.filter(and_(*group_conditions))

    return query


def parse_label_filter(params: dict[str, str]) -> dict[str, str]:
    """Adapted from syntara.core.utils.labels.parse_label_filter.

    Parse label filter parameters from query parameters.

    Extracts label filters from parameters with the pattern: labels[key]=value

    Args:
        params: Dictionary of query parameters from request

    Returns:
        Dictionary of label key-value pairs to filter by

    Examples:
        >>> params = {
        ...     "labels[environment]": "production",
        ...     "labels[region]": "us-east-1",
        ...     "other_param": "ignored"
        ... }
        >>> parse_label_filter(params)
        {"environment": "production", "region": "us-east-1"}

    """
    label_filters = {}

    for param_name, param_value in params.items():
        # Try to match label parameter pattern
        match = LABEL_PARAM_PATTERN.match(param_name)
        if match:
            label_key = match.group(1)
            label_filters[label_key] = param_value

    return label_filters


def apply_label_filters[TP: tuple[Any, ...]](
    query: Select[TP], label_filters: dict[str, str], model: type[SQLModel]
) -> Select[TP]:
    """Adapted from syntara.core.utils.labels.apply_label_filters.

    Apply label filters to a SQLAlchemy Query using JSON operations.

    Args:
        query: SQLAlchemy Query object or SQLModel Select statement to filter
        label_filters: Label key-value pairs to match
        model: SQLModel class to get labels field attribute from

    Returns:
        Filtered query object of the same type as input

    Examples:
        >>> # With SQLModel Select
        >>> stmt = select(Resource)
        >>> filtered_stmt = apply_label_filters(stmt, {"env": "prod"}, Resource)

        >>> # With SQLAlchemy Query
        >>> query = session.query(Resource)
        >>> filtered_query = apply_label_filters(query, {"region": "us-east-1"}, Resource)

    """
    if not label_filters:
        return query

    # Get the labels field attribute from the model
    if not hasattr(model, "labels"):
        msg = "Label filtering is not supported for this resource"
        raise ValueError(msg)

    labels_field = model.labels
    conditions = []

    for key, value in label_filters.items():
        # Use PostgreSQL JSONB operator for label matching
        # This creates conditions like: Resource.labels::jsonb @> :param_1
        # The @> operator checks if the left JSONB contains the right JSONB
        # Build JSONB dict that will be bound as a parameter (prevents SQL injection)
        json_dict = {key: value}
        labels_field_jsonb = cast(labels_field, JSONB)
        # SQLAlchemy will automatically bind this dict as a JSONB parameter
        condition = labels_field_jsonb.contains(json_dict)
        conditions.append(condition)

    # Apply all conditions with AND logic
    if conditions:
        query = query.filter(and_(*conditions))

    return query


def apply_list_filters[TP: tuple[Any, ...]](
    query: Select[TP],
    params: dict[str, str] | None,
    model: type[SQLModel],
) -> Select[TP]:
    """Port BaseService._apply_standard_filters and _apply_label_filters to EP stores.

    Origin: syntara.core.services.base.BaseService. Both list and count queries
    share this path, including JSONB label equality and key existence checks.
    """
    params = params or {}
    filters = parse_filters(params, getattr(model, "__filterable_fields__", []))
    query = apply_filters(query, filters, model)
    labels = parse_label_filter(params)
    values = {key: value for key, value in labels.items() if value != ""}
    query = apply_label_filters(query, values, model)
    for key, value in labels.items():
        if value == "":
            if not hasattr(model, "labels"):
                msg = "Label filtering is not supported for this resource"
                raise ValueError(msg)
            query = query.filter(model.labels.has_key(key))
    return query
