.PHONY: install test test-coverage coverage-combine test-integration test-e2e lint format typecheck openapi migrate image secrets certs setup ao-setup compose-up compose-down generate-token kind-target submit-work sync-requirements sync-rpms-lock-yaml

TEST_ARGS ?= --ignore=tests/integration
COVERAGE_FILES ?= coverage-artifacts/*/.coverage
JUNIT_FILES ?= coverage-artifacts/*/pytest-results.xml
COMPOSE ?= uvx podman-compose -f compose.yaml
COMPOSE_E2E ?= uvx podman-compose -f compose.yaml -f compose.ao.yaml
COMPOSE_WAIT_TIMEOUT ?= 300
E2E_ARGS ?=
# Temporary: pin AO to feat/ANSTRAT-1803 until that branch lands on devel.
AO_REF ?= feat/ANSTRAT-1803
AO_REMOTE ?= https://github.com/syntara-orchestration/syntara.git
SYNTARA_GIT ?= $(abspath ../syntara)
SYNTARA_ROOT ?= $(CURDIR)/.worktrees/syntara-anstrat-1803
AO_IMAGE ?= localhost/syntara:anstrat-1803
AO_API_PUBLISH_PORT ?= 8000
EP_API_PUBLISH_PORT ?= 8001
# kind/kubectl are often in aap-dev rather than PATH.
E2E_BIN_PATH := $(HOME)/.local/bin:$(HOME)/aap-dev/bin:$(abspath ../aap-dev/bin)

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

# Checkout AO_REF, generate its secrets/certs, and build AO_IMAGE from that tree.
ao-setup:
	AO_REF="$(AO_REF)" AO_REMOTE="$(AO_REMOTE)" SYNTARA_GIT="$(SYNTARA_GIT)" \
		SYNTARA_ROOT="$(SYNTARA_ROOT)" AO_IMAGE="$(AO_IMAGE)" \
		./tools/ao-setup.sh

compose-up: setup
	$(COMPOSE) up --build

compose-down:
	$(COMPOSE_E2E) down -v

# Bring the local EP+AO compose stack up, run tests/e2e, then tear the stack down.
# Always stops compose (and named volumes), including when pytest fails or the
# stack never becomes ready. Volumes must be recreated so AO seed applies the
# current worktree admin password instead of a leftover hash from a prior run.
test-e2e:
	$(MAKE) ao-setup
	$(MAKE) setup
	$(COMPOSE_E2E) down -v >/dev/null 2>&1 || true
	trap '$(COMPOSE_E2E) down -v' EXIT; \
	SYNTARA_ROOT="$(SYNTARA_ROOT)" AO_IMAGE="$(AO_IMAGE)" AO_API_PUBLISH_PORT="$(AO_API_PUBLISH_PORT)" \
		APP_SCRIPT_NODES_ENABLED=true APP_EP_API_URL=https://execution-plane-api:8443 \
		APP_SERVER_PUBLIC_URL=https://syntara:8000 \
		EP_AO_JWT_ISSUER=https://syntara:8000 \
		EP_API_TLS_CERT_FILE="$(SYNTARA_ROOT)/backend/.secrets/certs/execution-plane.crt" \
		EP_API_TLS_KEY_FILE="$(SYNTARA_ROOT)/backend/.secrets/certs/execution-plane.key" \
		EP_API_TLS_CLIENT_CA_FILE="$(SYNTARA_ROOT)/backend/.secrets/certs/ca.pem" \
		EP_CALLBACK_CA_CERT_FILE="$(SYNTARA_ROOT)/backend/.secrets/certs/ca.pem" \
		EP_CALLBACK_CERT_FILE="$(SYNTARA_ROOT)/backend/.secrets/certs/execution-plane.crt" \
		EP_CALLBACK_KEY_FILE="$(SYNTARA_ROOT)/backend/.secrets/certs/execution-plane.key" \
		EP_COMPLETION_CALLBACK_URL=https://syntara:8000/api/execution_plane/v1/events \
		EP_CALLBACK_CA_CERT_PATH=/run/secrets/ep-callback-ca.pem \
		EP_CALLBACK_CERT_PATH=/run/secrets/ep-callback.crt \
		EP_CALLBACK_KEY_PATH=/run/secrets/ep-callback.key \
		EP_CREDENTIAL_RESOLUTION_CALLBACK_URL=http://credential-resolve:8080/credentials/resolve \
		EP_CREDENTIAL_RESOLUTION_CALLBACK_CA_CERT_PATH=/run/secrets/ep-callback-ca.pem \
		EP_CREDENTIAL_RESOLUTION_CALLBACK_CERT_PATH=/run/secrets/ep-callback.crt \
		EP_CREDENTIAL_RESOLUTION_CALLBACK_KEY_PATH=/run/secrets/ep-callback.key \
		$(COMPOSE_E2E) up --build -d --wait --wait-timeout $(COMPOSE_WAIT_TIMEOUT) || { $(COMPOSE_E2E) logs; exit 1; }; \
	PATH="$(E2E_BIN_PATH):$(PATH)" \
		EP_API_CA_FILE="$(SYNTARA_ROOT)/backend/.secrets/certs/ca.pem" \
		EP_AO_JWT_PRIVATE_KEY_FILE="$(SYNTARA_ROOT)/backend/.secrets/jwt-primary.pem" \
		EP_AO_JWT_ISSUER=https://syntara:8000 \
		EP_API_URL="https://127.0.0.1:$(EP_API_PUBLISH_PORT)" \
		AO_API_BASE="https://127.0.0.1:$(AO_API_PUBLISH_PORT)" \
		uv run python tools/deploy_kind_execution_target.py \
			--api-url "https://127.0.0.1:$(EP_API_PUBLISH_PORT)" && \
	AO_API_BASE="https://127.0.0.1:$(AO_API_PUBLISH_PORT)" \
		APP_ADMIN_PASSWORD_PATH="$(SYNTARA_ROOT)/backend/.secrets/admin-password" \
		APP_SCRIPT_NODES_ENABLED=true \
		E2E_POLL_TIMEOUT=360 \
		uv run pytest tests/e2e -v --tb=short $(E2E_ARGS)

generate-token:
	uv run python tools/generate_jwt_for_ep.py

kind-target:
	PATH="$(E2E_BIN_PATH):$(PATH)" \
		EP_API_CA_FILE="$(SYNTARA_ROOT)/backend/.secrets/certs/ca.pem" \
		EP_AO_JWT_PRIVATE_KEY_FILE="$(SYNTARA_ROOT)/backend/.secrets/jwt-primary.pem" \
		EP_AO_JWT_ISSUER=https://syntara:8000 \
		EP_API_URL="https://127.0.0.1:$(EP_API_PUBLISH_PORT)" \
		AO_API_BASE="https://127.0.0.1:$(AO_API_PUBLISH_PORT)" \
		uv run python tools/deploy_kind_execution_target.py \
			--api-url "https://127.0.0.1:$(EP_API_PUBLISH_PORT)"

submit-work:
	uv run python tools/submit_work_item.py
