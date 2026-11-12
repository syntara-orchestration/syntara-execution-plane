# Combined AO + EP Kind demo

The test target is one script workflow submitted through AO, accepted by the
separate EP API, run by the EP worker as a cold-start Job, returned over the
SDK gRPC protocol, committed to the EP completion notification state, and delivered to AO
for Temporal completion. AO and EP use distinct databases on the same local
PostgreSQL server.

## Required services and configuration

- AO API, Temporal, AO workers, and PostgreSQL from the Syntara checkout.
- EP API, EP worker, and Alembic migration from the standalone
  `syntara-execution-plane` checkout. Build an EP image from that checkout and
  set `EP_IMAGE` for the AO compose stack.
- A separate `execution_plane` database and EP runtime/migration roles. The AO
  compose bootstrap creates these; apply EP Alembic migrations with the same
  EP payload-encryption key that the API and worker use.
- A local Kind cluster with a CNI that enforces NetworkPolicy. The EP worker
  container must reach the Kubernetes API server and use an AO-managed target
  credential scoped to the workload namespace.
- Valid TLS for the EP API, AO completion callback, AO credential-resolution
  callback, plus the verified Kubernetes API CA for the target. Do not disable
  certificate validation to work around a missing or hostname-mismatched CA.
- A script node image pinned by digest and available to the Kind node.

## Flow to exercise

1. Start PostgreSQL, create the separate EP database/roles, run AO migrations,
   and run EP migrations independently.
2. Start the EP API/worker and AO/Temporal services. Configure AO's EP URL,
   AO JWT issuer/key, EP completion and credential-resolution callback URLs,
   and callback mTLS identity.
3. Create an OpenShift integration through AO's integration API or UI, using a
   ServiceAccount credential scoped to the Kind workload namespace and its
   verified CA. Wait for the integration sync status to report `ready`; do not
   write EP tables directly from AO or the demo harness.
4. Submit a script workflow through AO. Confirm one AO request ID maps to one
   EP WorkItem and one claim-generation Job. Confirm the result fields,
   including stdout/stderr, arrived through gRPC and the AO completion notification completed
   the Temporal activity.
5. Inspect the WorkItem and completion notification state in the EP database, AO's command
   inbox/binding in the AO database, and the worker Job/NetworkPolicy lifecycle.
   Verify both databases remain inaccessible to the other service runtime role.
6. Repeat with completion notification outage, AO/EP restart, cancellation, startup failure,
   and an ambiguous Execute result. An uncertain attempt must remain visible as
   `reconciliation_required` and must not get a second Job automatically.

## Smoke test

Run the happy-path test from the AO `backend` directory with the local AO
and EP smoke stack running:

```bash
APP_BASE_URL=https://localhost:18000 \
APP_ADMIN_PASSWORD_PATH=.secrets/admin-password \
APP_SCRIPT_NODES_ENABLED=true \
uv run --no-sync pytest \
  tests/e2e/workflows/test_script_node_gate.py::TestScriptNodeGateEnabled::test_script_activity_executes_normally \
  -q --tb=short -o log_cli=false
```

Expected evidence is a completed AO execution and script activity, an EP
WorkItem with `completed` status and exit code `0`, the returned `stdout` and
`stderr` in the persisted result, an acknowledged completion notification, and completed
resource cleanup. This test covers the service-to-service happy path, not
restart/retry recovery or network-policy enforcement.

The EP persistence regression tests can be run against an isolated EP database
by setting `EP_TEST_DATABASE_URL` to a migration-capable test role and
`EP_CREDENTIAL_ENCRYPTION_KEY_PATH` to the same key used by the EP API and
worker, then running:

```bash
uv run pytest tests/integration/test_postgres_persistence_types.py -q
```

## NetworkPolicy validation

Kind's default kindnet CNI does not enforce NetworkPolicies. Use an enforcing
CNI, such as Calico, to test allowed and denied destinations. Also verify that
the EP service-account Role can `get` `pods/portforward` but cannot read
`pods/log` or create `pods/exec`.

Record the AO and EP image SHAs, workload image digest, Kubernetes version,
CNI, and scenario results when validating the combined services.
