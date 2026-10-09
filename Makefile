.PHONY: install test test-integration lint format typecheck openapi migrate image secrets certs setup compose-up compose-down generate-token kind-target submit-work

install:
	uv sync --locked --all-groups

test:
	uv run pytest

test-integration:
	uv run pytest tests/integration -m integration -v

lint:
	uv run ruff check src tests

format:
	uv run ruff format --check src tests

typecheck:
	uv run mypy --strict src

openapi:
	uv run python tools/export_openapi.py

migrate:
	uv run alembic -c alembic.ini upgrade head

image:
	podman build -f Containerfile -t localhost/execution-plane:dev .

secrets:
	./tools/generate_secrets.sh

certs:
	uv run python tools/generate_certs.py

setup: install secrets certs

compose-up: setup
	uvx podman-compose -f compose.yaml up --build

compose-down:
	uvx podman-compose -f compose.yaml down

generate-token:
	uv run python tools/generate_jwt_for_ep.py

kind-target:
	uv run python tools/deploy_kind_execution_target.py

submit-work:
	uv run python tools/submit_work_item.py
