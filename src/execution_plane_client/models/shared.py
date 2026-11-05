"""Shared list controls and pagination envelopes."""

from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints

_SORT_PATTERN = r"^-?[a-z][a-z0-9_]*$"


class EPListParams(BaseModel):
    """Query parameters accepted by all EP list endpoints."""

    limit: int = Field(default=20, gt=0, le=100)
    cursor: str | None = Field(default=None)
    sort: Annotated[str, StringConstraints(pattern=_SORT_PATTERN)] = Field(
        default="created_at", description="Sort field; prefix with '-' for descending order."
    )
    include_total: bool = Field(default=False)

    @property
    def sort_order(self) -> tuple[str, str]:
        """Return the selected field and SQL ordering direction."""
        return self.sort.removeprefix("-"), "desc" if self.sort.startswith("-") else "asc"


class ResourcesResponse[T](BaseModel):
    """Pagination envelope returned by all EP list endpoints."""

    resources: list[T]
    next: str | None = None
    prev: str | None = None
    total: int | None = None
