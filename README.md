# Syntara Execution Plane

The Execution Plane (EP) accepts and runs work dispatched by Syntara. This repository owns the standalone HTTP API, worker package, cluster and execution-target registries, work-item persistence, migrations for the `execution_plane` PostgreSQL schema, and the service container image.

Syntara owns user authorization, workflow dispatch, integrations, and the user interface. EP owns accepted work, execution state, and completion-event delivery. Temporal task tokens stay in Syntara and are not part of the EP API or schema. The first deployment may use the existing PostgreSQL server, but EP requires a distinct database and runtime role.

## Requirements

- Python 3.12, 3.13, or 3.14
- [`uv`](https://docs.astral.sh/uv/)
- PostgreSQL for the service, plus a Kubernetes/OpenShift target for isolated script workloads

## Development

```bash
uv sync --locked --all-groups
uv run pytest
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy --strict src
```

For a separate local EP database, copy `compose.yaml` settings into a local `.env`
or provide them through the shell. The standalone compose setup creates a local
PostgreSQL server, an EP-owned database, and separate migration/runtime roles. It
requires AO's verification key, issuer, and EP credential encryption key.
Local work execution still requires a reachable Kubernetes/OpenShift target and
an image tag available to that cluster; there is no in-process script fallback.

Run the API directly with the EP database URL and AO's public service-token verification key:

```bash
EP_DATABASE_URL=postgresql+asyncpg://user:password@localhost/execution_plane \
EP_AO_JWT_PUBLIC_KEY_PATH=/run/secrets/ao-jwt-public.pem \
EP_AO_JWT_ISSUER=https://syntara.example.com \
EP_API_TLS_CERT_PATH=/run/secrets/ep-api/tls.crt \
EP_API_TLS_KEY_PATH=/run/secrets/ep-api/tls.key \
EP_API_TLS_CLIENT_CA_PATH=/run/secrets/ep-api/ca.pem \
uv run execution-plane-api
```

The API exposes versioned OpenAPI at `/docs`, authenticated work submission, scoped status reads and cancellation under `/v1`, plus liveness and readiness endpoints. Work moves through `pending`, `claimed`, and `dispatched`; cancellation of dispatched work first returns `cancel_requested`, and only becomes `cancelled` once Kubernetes confirms the Job and its pods have stopped. `reconciliation_required` makes an uncertain external outcome visible while EP retries observation without launching a duplicate. Cancellation by stable request ID creates a tombstone when it races ahead of submission, preventing a late request from starting work. Configure EP-owned HTTPS with `EP_API_PORT`, `EP_API_TLS_CERT_PATH`, and `EP_API_TLS_KEY_PATH`; `EP_API_TLS_CLIENT_CA_PATH` makes the API validate client certificates when provided. Kubernetes manifests expect a TLS certificate in `execution-plane-api-tls` whose SAN matches the service DNS name. The first API release also requires signed AO service tokens with an audience, client ID, project ID, and operation scopes.

Run the worker with the same EP database and a fixed AO completion callback URL:

```bash
EP_DATABASE_URL=postgresql+asyncpg://user:password@localhost/execution_plane \
EP_COMPLETION_CALLBACK_URL=https://syntara.example.com/api/execution_plane/v1/events \
EP_CALLBACK_CA_CERT_PATH=/run/secrets/s2s-ca.pem \
EP_CALLBACK_CERT_PATH=/run/secrets/execution-plane.crt \
EP_CALLBACK_KEY_PATH=/run/secrets/execution-plane.key \
uv run execution-plane-worker
```

Apply the EP migrations independently from Syntara's migration chain:

```bash
DATABASE_URL=postgresql+asyncpg://user:password@localhost/database uv run alembic -c alembic.ini upgrade head
```

Build the worker image from this repository root:

```bash
podman build -f Containerfile -t localhost/execution-plane:dev .
```

The independent local compose stack is started from this repository with
`podman-compose -f compose.yaml up --build`. The worker submits each script as a
short-lived Kubernetes Job in the selected execution target. Configure
`EP_WORKLOAD_RUNNER_IMAGE` to an image available to that target cluster and grant
the stored target credential permission to create/read Secrets, Jobs, Pods, Pod
logs, and NetworkPolicies in the configured namespace. `EP_WORKLOAD_ALLOWED_EGRESS_CIDRS`
is an operator-reviewed JSON list of destinations the workload may reach; without
it, workloads can resolve DNS but have no general egress. If a whole IP family is
allowed, also set `EP_WORKLOAD_FORBIDDEN_EGRESS_CIDRS` to the AO, Temporal,
Execution Plane, and database address ranges that must remain unreachable.
Per-integration Kubernetes API trust roots are stored encrypted in EP.

The Kubernetes base manifests are in `deploy/kubernetes/base`; provide the
database URLs, AES key, AO JWT verification key, callback mTLS material, server
TLS certificate, workload runner image, and AO callback/JWT issuer URLs as
Kubernetes Secrets and a ConfigMap before applying them. Pin both service and
workload images to immutable release tags or digests in the deployment overlay.
The workload NetworkPolicy denies ingress and permits only DNS plus the configured
egress CIDRs; verify that the target cluster's CNI enforces NetworkPolicy before
enabling script workloads.

The management image runs as UID 1001. Its default command starts the controller worker; use `execution-plane-api` for the HTTP service. EP does not connect to AO's database or Temporal. User code runs in separate workload pods with bounded CPU and memory, a read-only root filesystem, no service-account token, no Linux capabilities, job-scoped input, and a per-job NetworkPolicy. Target-cluster CNI enforcement, production RBAC, approved egress ranges, cancellation behavior, and live-cluster recovery still require deployment validation before production isolation can be claimed.

## Ownership

Use this repository for EP worker, registry, migration, and placement changes. Make coordinated API, authorization, integration, and workflow changes in [Syntara](https://github.com/syntara-orchestration/syntara), and link the changes across pull requests.
