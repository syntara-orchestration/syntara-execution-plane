"""Shared list controls and pagination envelopes."""

from typing import Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class EPListParams(BaseModel):
    """Query parameters accepted by all EP list endpoints."""

    limit: int = Field(default=20, gt=0, le=100)
    cursor: str | None = Field(default=None)
    sort: str = Field(default="-created_at", description="Sort field; prefix with '-' for descending order.")
    include_total: bool = Field(default=False)

    @property
    def sort_order(self) -> tuple[str, str]:
        """Return the selected field and SQL ordering direction."""
        return self.sort.removeprefix("-"), "desc" if self.sort.startswith("-") else "asc"


class ResourcesResponse(BaseModel, Generic[T]):  # noqa: UP046
    """Pagination envelope returned by all EP list endpoints."""

    resources: list[T]
    next: str | None = None
    prev: str | None = None
    total: int | None = None
