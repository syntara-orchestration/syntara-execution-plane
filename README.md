# Syntara Execution Plane

The Execution Plane (EP) accepts and runs work dispatched by Syntara. This repository owns the standalone HTTP API, worker package, cluster and execution-target registries, work-item persistence, migrations for the `execution_plane` PostgreSQL schema, and the service container image.

Syntara owns user authorization, workflow dispatch, integrations, and the user interface. EP owns accepted work, execution state, and completion-event delivery. Temporal task tokens stay in Syntara and are not part of the EP API or schema. The first deployment may use the existing PostgreSQL server, but EP requires a distinct database and runtime role.

EP is one of several Syntara repositories. [Syntara](https://github.com/syntara-orchestration/syntara) is the orchestrator that accepts user intent and dispatches work; it will take EP as a dependency, since it submits that work to the EP API. The [Syntara Plugin SDK](https://github.com/syntara-orchestration/syntara-plugin-sdk) is a firmly planned direct dependency: it provides the package the container entrypoint runs and the protobuf contract EP speaks to each node over gRPC, so it is shared by both the container images and the wire protocol. [Syntara Step Types](https://github.com/syntara-orchestration/syntara-step-types) defines the step and payload types a workload acts on; because EP routes and runs work through the gRPC contract and stays largely agnostic to the data being passed, it is only a potential dependency and may never become a required one.

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

For a separate local EP database, copy `.env.example` to `.env` if you need to
override defaults. Generate local secrets and TLS material first (`make setup`,
or `./tools/generate_secrets.sh` and `uv run python tools/generate_certs.py`).
If a sibling Syntara checkout already has `backend/.secrets/jwt-primary.pub`,
the secrets script copies it so EP can verify tokens issued by local AO.
The standalone compose setup creates a local PostgreSQL server, an EP-owned
database, and separate migration/runtime roles.
Local work execution still requires a reachable Kubernetes/OpenShift target and
an image tag available to that cluster; there is no in-process script fallback.

Run the API directly with the EP database URL and AO's public service-token verification key:

```bash
EP_DATABASE_URL=postgresql+asyncpg://user:password@localhost/execution_plane \
EP_CREDENTIAL_ENCRYPTION_KEY=<base64url-encoded-32-byte-key> \
EP_AO_JWT_PUBLIC_KEY_PATH=/run/secrets/ao-jwt-public.pem \
EP_AO_JWT_ISSUER=https://syntara.example.com \
EP_API_TLS_CERT_PATH=/run/secrets/ep-api/tls.crt \
EP_API_TLS_KEY_PATH=/run/secrets/ep-api/tls.key \
EP_API_TLS_CLIENT_CA_PATH=/run/secrets/ep-api/ca.pem \
uv run execution-plane-api
```

The API exposes versioned OpenAPI at `/docs`, authenticated work submission, scoped status reads and cancellation under `/v1`, plus liveness and readiness endpoints. Work moves through `pending`, `claimed`, and `dispatched`; cancellation of dispatched work first returns `cancel_requested`, and only becomes `cancelled` after the gRPC invocation is confirmed stopped. `reconciliation_required` makes an uncertain external outcome visible instead of launching a duplicate. Cancellation by stable request ID creates a tombstone when it races ahead of submission, preventing a late request from starting work. Configure EP-owned HTTPS with `EP_API_PORT`, `EP_API_TLS_CERT_PATH`, and `EP_API_TLS_KEY_PATH`; `EP_API_TLS_CLIENT_CA_PATH` makes the API validate client certificates when provided. Kubernetes manifests expect a TLS certificate in `execution-plane-api-tls` whose SAN matches the service DNS name. The first API release also requires signed AO service tokens with an audience, client ID, project ID, and operation scopes.

Run the worker with the same EP database and a fixed AO completion callback URL:

```bash
EP_DATABASE_URL=postgresql+asyncpg://user:password@localhost/execution_plane \
EP_CREDENTIAL_ENCRYPTION_KEY=<base64url-encoded-32-byte-key> \
EP_COMPLETION_CALLBACK_URL=https://syntara.example.com/api/execution_plane/v1/events \
EP_CALLBACK_CA_CERT_PATH=/run/secrets/s2s-ca.pem \
EP_CALLBACK_CERT_PATH=/run/secrets/execution-plane.crt \
EP_CALLBACK_KEY_PATH=/run/secrets/execution-plane.key \
uv run execution-plane-worker
```

Apply the EP migrations independently from Syntara's migration chain:

```bash
DATABASE_URL=postgresql+asyncpg://user:password@localhost/execution_plane \
EP_CREDENTIAL_ENCRYPTION_KEY=<base64url-encoded-32-byte-key> \
uv run alembic -c alembic.ini upgrade head
```

Build the worker image from this repository root:

```bash
podman build -f Containerfile -t localhost/execution-plane:dev .
```

The independent local compose stack is started from this repository with
`make setup && uvx podman-compose up --build`. The worker submits each script as a
short-lived Kubernetes Job in the selected execution target. Configure
`EP_WORKLOAD_RUNNER_IMAGE` to an image available to that target cluster and grant
the stored target credential permission to create/read Secrets, Jobs, Pods, Pod
logs, and NetworkPolicies in the configured namespace. `EP_WORKLOAD_ALLOWED_EGRESS_CIDRS`
is an operator-reviewed JSON list of destinations the workload may reach; without
it, workloads can resolve DNS but have no general egress. If a whole IP family is
allowed, also set `EP_WORKLOAD_FORBIDDEN_EGRESS_CIDRS` to the AO, Temporal,
Execution Plane, and database address ranges that must remain unreachable.
Per-integration Kubernetes API trust roots are stored encrypted in EP.

The target-cluster service account needs `create/get/delete` on Jobs, `list` on
Pods, `get` on `pods/portforward`, and `create/get/list/patch/delete` on
NetworkPolicies. It does not need workload Secret, Pod-log, or Pod-exec
permissions. That Role is `deploy/kubernetes/execution-target/rbac.yaml`.
`EP_WORKLOAD_ALLOWED_EGRESS_CIDRS` is an operator-reviewed JSON list of
destinations the workload may reach; without it, workloads can resolve DNS but
have no general egress. If a whole IP family is allowed, also set
`EP_WORKLOAD_FORBIDDEN_EGRESS_CIDRS` to the AO, Temporal, Execution Plane, and
database address ranges that must remain unreachable. Validate enforcement with
the target cluster's CNI before enabling workloads. Per-integration Kubernetes
API trust roots and submitted invocation payloads are encrypted at rest by EP.

Local helper scripts under `tools/` cover the rest of a first bring-up. EP does
not issue tokens; `tools/generate_jwt_for_ep.py` signs an AO service JWT with the
local or sibling Syntara ES256 key. Submit requires a `project_id` claim.
`tools/deploy_kind_execution_target.py` creates a kind cluster, applies
`deploy/kubernetes/execution-target/rbac.yaml`, and registers it through a
cluster binding. `tools/submit_work_item.py` posts a script work item to the
local API.

```bash
make setup
uvx podman-compose up --build -d

# Service token for curl. Submit needs --project-id.
uv run python tools/generate_jwt_for_ep.py
uv run python tools/generate_jwt_for_ep.py --project-id 00000000-0000-0000-0000-000000000001

# Kind cluster + default ExecutionTarget (needs kind and kubectl)
uv run python tools/deploy_kind_execution_target.py

# Submit a script work item; --wait polls until it is terminal
uv run python tools/submit_work_item.py
uv run python tools/submit_work_item.py --language bash --code 'echo hello' --wait
```

Makefile aliases: `make generate-token`, `make kind-target`, and `make submit-work`.

The Kubernetes base manifests are in `deploy/kubernetes/base`; provide the
database URLs, AES key, AO JWT verification key, callback mTLS material, server
TLS certificate, and AO callback/JWT issuer URLs as Kubernetes Secrets and a
ConfigMap before applying them. Target-cluster dispatcher RBAC is
`deploy/kubernetes/execution-target/rbac.yaml`. AO supplies the node image reference in the
request and must pin it by digest. EP applies a NetworkPolicy before creating the
Job; it denies ingress and permits only DNS plus configured egress CIDRs. Verify
that the target cluster's CNI enforces NetworkPolicy before enabling script
workloads.

The management image runs as UID 1001. Its default command starts the controller worker; use `execution-plane-api` for the HTTP service. EP does not connect to AO's database or Temporal. User code runs in a separate workload Pod with bounded CPU and memory, a read-only root filesystem, no service-account token, no Linux capabilities, gRPC-only application communication, and a per-attempt NetworkPolicy. A Kubernetes Job can start a container more than once; this implementation uses one completion, no Job retries, an attempt identity, a lease fence, and `reconciliation_required` after an uncertain Execute boundary. The existing node runtime cannot replay a result after EP crashes between receiving it and committing it. Target-cluster CNI enforcement, production RBAC, approved egress ranges, cancellation behavior, and live-cluster recovery still require deployment validation before production isolation can be claimed.

## Ownership

Use this repository for EP worker, registry, migration, and placement changes. Make coordinated API, authorization, integration, and workflow changes in [Syntara](https://github.com/syntara-orchestration/syntara), and link the changes across pull requests.
