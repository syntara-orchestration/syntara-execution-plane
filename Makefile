.PHONY: install test test-coverage coverage-combine test-integration lint format typecheck openapi migrate image secrets certs setup compose-up compose-down generate-token kind-target start-ep submit-work sync-requirements sync-rpms-lock-yaml

TEST_ARGS ?= --ignore=tests/integration
COVERAGE_FILES ?= coverage-artifacts/*/.coverage
JUNIT_FILES ?= coverage-artifacts/*/pytest-results.xml
KIND_ARGS ?=

install:
	uv sync --locked --all-groups

test:
	uv run pytest

test-coverage:
	uv run pytest $(TEST_ARGS) --cov --cov-report=term-missing --cov-report=xml:coverage.xml --junitxml=pytest-results.xml

coverage-combine:
	uv run coverage combine --keep $(COVERAGE_FILES)
	uv run coverage xml -o coverage.xml
	uv run coverage html -d htmlcov
	uv run coverage report
	uv run python -m junitparser merge -- $(JUNIT_FILES) pytest-results.xml

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

# Fully pinned, hashed lockfiles for Cachi2/Hermeto pip prefetch.
# Konflux does not read uv.lock; keep these in sync after dependency changes.
# requirements-build.txt is the PEP 517 backends needed to build those sdists
# offline (setuptools, hatchling, cython, ...).
sync-requirements:
	uv export --frozen --python=3.12 --no-dev --no-editable --no-emit-package colorama --no-emit-package execution-plane-client --no-emit-workspace --output-file requirements.txt >/dev/null
	uv run tools/ci/filter_requirements_hashes.py
	uvx --from pybuild-deps==0.6.1 --python 3.12 pybuild-deps compile --generate-hashes -o requirements-build.txt requirements.txt

# Resolve rpms.in.yaml against the runtime UBI image into rpms.lock.yaml.
# Hermeto uses the lockfile to prefetch RPMs for isolated microdnf installs.
RPM_LOCKFILE_IMAGE ?= localhost/rpm-lockfile-prototype:latest
RPM_LOCKFILE_VERSION ?= v0.30.1

sync-rpms-lock-yaml:
	@if ! podman image exists "$(RPM_LOCKFILE_IMAGE)"; then \
		echo "Building $(RPM_LOCKFILE_IMAGE) from rpm-lockfile-prototype $(RPM_LOCKFILE_VERSION)"; \
		curl -fsSL "https://raw.githubusercontent.com/konflux-ci/rpm-lockfile-prototype/$(RPM_LOCKFILE_VERSION)/Containerfile" \
			| podman build -t "$(RPM_LOCKFILE_IMAGE)" --build-arg "GIT_REF=tags/$(RPM_LOCKFILE_VERSION)" - ; \
	fi
	podman run --rm -v "$(CURDIR):/work:z" -w /work \
		"$(RPM_LOCKFILE_IMAGE)" --outfile=/work/rpms.lock.yaml rpms.in.yaml

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
	uv run python tools/deploy_kind_execution_target.py $(KIND_ARGS)

EP_LOG_DIR ?= integration-test-logs
EP_API_URL ?= http://127.0.0.1:8001

start-ep:
	mkdir -p $(EP_LOG_DIR)
	uv run execution-plane-api    > $(EP_LOG_DIR)/api.log    2>&1 &
	uv run execution-plane-worker > $(EP_LOG_DIR)/worker.log 2>&1 &
	@echo "[INFO] EP API and worker started; waiting for readiness"
	@for i in $$(seq 1 60); do \
		if curl -sf $(EP_API_URL)/healthz/ready -o /dev/null 2>&1; then \
			echo "[INFO] EP API ready ($${i}s)"; exit 0; \
		fi; \
		if [ "$$i" -eq 60 ]; then \
			echo "ERROR: EP API did not start within 60s"; \
			echo "--- api.log ---"; cat $(EP_LOG_DIR)/api.log; \
			echo "--- worker.log ---"; cat $(EP_LOG_DIR)/worker.log; \
			exit 1; \
		fi; \
		sleep 1; \
	done

submit-work:
	uv run python tools/submit_work_item.py
