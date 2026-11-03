"""HTTP list-query adapters following Syntara's service and schema conventions.

Request filter extraction maps to
``syntara.core.services.base.BaseService.list_resources``. OpenAPI documentation
follows ``syntara.core.models.user_schemas.UserListParams`` and
``backend/src/syntara/schemas/users/openapi.yaml``: document each base field once
and describe its dynamic bracket operators alongside it.

This EP module combines those conventions into adapters for the shared stores;
Syntara distributes them between its service, query models, and API schemas.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from execution_plane.query_filters import FilterOperator, column_python_type, query_column

if TYPE_CHECKING:
    from fastapi import Request
    from sqlmodel import SQLModel


def list_query_filters(request: Request) -> dict[str, str]:
    """Extract filters for syntara.core.services.base.BaseService._apply_standard_filters.

    Pagination/sort controls belong to the query model; all remaining keys must
    pass Syntara's field/operator validation rather than being silently ignored.
    """
    return {
        key: value
        for key, value in request.query_params.items()
        if key not in {"limit", "cursor", "sort", "include_total"}
    }


def list_filter_openapi(model: type[SQLModel]) -> dict[str, Any]:
    """Follow syntara.core.models.user_schemas.UserListParams filter documentation.

    Like Syntara's schemas/users/openapi.yaml, document each base field once,
    describing dynamic bracket operators rather than expanding their product.
    """
    parameters = []
    for field in getattr(model, "__filterable_fields__", []):
        python_type = column_python_type(query_column(model, field))
        operators = list(FilterOperator)
        if python_type not in {str, None}:
            operators = [op for op in operators if op not in {FilterOperator.CONTAINS, FilterOperator.STARTS_WITH}]
        if python_type is bool:
            operators = [FilterOperator.EQ, FilterOperator.IN, FilterOperator.ISNULL]
        parameters.append(
            {
                "name": f"{field}[operator]",
                "in": "query",
                "required": False,
                "schema": {"type": "string"},
                "description": (
                    f"Dynamic filter operator for {field}. "
                    f"Operators: {', '.join(op.value for op in operators)}. "
                    "Comma-separated values use OR; different fields/operators use AND."
                ),
            }
        )
    if hasattr(model, "labels"):
        parameters.append(
            {
                "name": "labels[key]",
                "in": "query",
                "required": False,
                "schema": {"type": "string"},
                "description": "Replace key with a label key. Exact value match; empty value tests key existence.",
            }
        )
    return {"parameters": parameters}
