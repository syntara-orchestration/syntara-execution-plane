.PHONY: install test lint format typecheck migrate image

install:
	uv sync --locked --all-groups

test:
	uv run pytest

lint:
	uv run ruff check src tests

format:
	uv run ruff format --check src tests

typecheck:
	uv run mypy --strict src

migrate:
	uv run alembic -c alembic.ini upgrade head

image:
	podman build -f Containerfile -t localhost/execution-plane:dev .
